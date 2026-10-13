import hashlib
import json
import os
import random
import time
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Protocol

from .models import UploadPlanEntry
from .diagnostics import operation_error
from .s3_client import VerificationResult
from .retry_policy import (
    RUN_RETRY_WAIT_BUDGET_SECONDS,
    UPLOAD_MAX_ATTEMPTS,
    UPLOAD_RETRY_BACKOFF_SECONDS,
    upload_error_details,
)
from .runs import read_events

# The SDK remains optional for checks and local generation.
try:
    from botocore.exceptions import BotoCoreError, ClientError
    from boto3.exceptions import S3UploadFailedError
except ImportError:
    STORAGE_ERRORS = (OSError,)
else:
    STORAGE_ERRORS = (OSError, BotoCoreError, ClientError, S3UploadFailedError)


MAX_SINGLE_PUT_BYTES = 5 * 1024 ** 3


class S3UploadClient(Protocol):
    def object_exists(self, bucket, key):
        ...

    def verify_file(self, bucket, key, checksum, size_bytes):
        ...

    def upload_file(self, source_path, bucket, key, checksum):
        ...

    def inspect_object(self, bucket, key):
        ...


@dataclass(frozen=True)
class PushResult:
    role: str
    source_path: str
    bucket: str
    key: str
    status: str
    checksum: Optional[str]
    size_bytes: Optional[int]
    message: str = ""
    required: bool = True
    classification: str = "new"
    previous_checksum: Optional[str] = None
    previous_checksum_type: Optional[str] = None
    observed_size_bytes: Optional[int] = None
    verification_method: Optional[str] = None

    def to_dict(self) -> Dict[str, object]:
        return {
            "role": self.role,
            "source_path": self.source_path,
            "bucket": self.bucket,
            "key": self.key,
            "status": self.status,
            "checksum": self.checksum,
            "size_bytes": self.size_bytes,
            "message": self.message,
            "required": self.required,
            "classification": self.classification,
            "previous_checksum": self.previous_checksum,
            "previous_checksum_type": self.previous_checksum_type,
            "observed_size_bytes": self.observed_size_bytes,
            "verification_method": self.verification_method,
        }


@dataclass(frozen=True)
class PushReport:
    dry_run: bool
    endpoint_url: str
    region: str
    profile: Optional[str]
    planned_count: int
    source_backed_count: int
    generated_count: int
    missing_source_count: int
    missing_generated_count: int
    uploaded_count: int
    skipped_generated_count: int
    role_counts: Dict[str, int]
    results: List[PushResult] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)

    @property
    def failed_count(self) -> int:
        return sum(result.status == "failed" for result in self.results)

    @property
    def held_count(self) -> int:
        return sum(result.classification == "held" for result in self.results)

    @property
    def classification_counts(self) -> Dict[str, int]:
        return dict(sorted(Counter(result.classification for result in self.results).items()))

    @property
    def has_errors(self) -> bool:
        return bool(self.errors or self.failed_count or self.missing_source_count or self.missing_generated_count)

    def to_dict(self) -> Dict[str, object]:
        return {
            "publication": {"state": "held", "reason": "Use inspect-run after successful finalization; this report is not a publication receipt."},
            "errors": self.errors,
            "failed_count": self.failed_count,
            "held_count": self.held_count,
            "classification_counts": self.classification_counts,
            "unattempted_count": sum(r.status == "unattempted" for r in self.results),
            "deferred_count": sum(r.status == "deferred" for r in self.results),
            "dry_run": self.dry_run,
            "endpoint_url": self.endpoint_url,
            "region": self.region,
            "profile": self.profile,
            "planned_count": self.planned_count,
            "source_backed_count": self.source_backed_count,
            "generated_count": self.generated_count,
            "missing_source_count": self.missing_source_count,
            "missing_generated_count": self.missing_generated_count,
            "uploaded_count": self.uploaded_count,
            "skipped_generated_count": self.skipped_generated_count,
            "role_counts": self.role_counts,
            "results": [result.to_dict() for result in self.results],
        }


def push_upload_plan(
    upload_plan: Iterable[UploadPlanEntry],
    input_dir: Path,
    s3_client: Optional[S3UploadClient] = None,
    dry_run: bool = True,
    generated_dir: Optional[Path] = None,
    endpoint_url: Optional[str] = None,
    region: Optional[str] = None,
    profile: Optional[str] = None,
    rollback_path: Optional[Path] = None,
    journal=None,
    preflight_client=None,
) -> PushReport:
    if not dry_run and s3_client is None:
        raise ValueError("Execute requires an upload client.")
    # Keep manifest writes last while preserving plan order within each group.
    entries = sorted(upload_plan, key=lambda entry: entry.role == "manifest")
    resolved_endpoint_url = endpoint_url or os.environ.get("AWS_ENDPOINT_URL", "")
    resolved_region = region or os.environ.get("AWS_REGION", "us-east-1")
    resolved_profile = profile or os.environ.get("AWS_PROFILE", "")
    role_counts = dict(sorted(Counter(entry.role for entry in entries).items()))

    # Establish every local checksum before the first remote write. A failed
    # local check keeps the whole plan from starting.
    readiness_failures = {}
    errors = []
    verified = []
    destinations = {}
    attempt_history, prior_writes = _upload_history(journal)
    retry_wait_seconds = 0.0
    for index, entry in enumerate(entries):
        source_path = _resolve_upload_path(entry, input_dir, generated_dir)
        try:
            status = _entry_status(entry, source_path, dry_run, generated_dir)
            if status in {"missing_source", "missing_generated"}:
                readiness_failures[index] = (status, _status_message(status))
                continue
            if status == "would_generate":
                destinations[index] = {"status": "unchecked", "classification": "held",
                                       "checksum": None, "size_bytes": None}
                continue
            checksum = _checksum_for_entry(source_path)
            size_bytes = source_path.stat().st_size
            if not dry_run and size_bytes > MAX_SINGLE_PUT_BYTES:
                readiness_failures[index] = (
                    "failed", "Artifact exceeds the 5 GiB conditional single-PUT limit; batch held."
                )
                continue
            destinations[index] = {"status": "unchecked", "classification": "new",
                                    "checksum": checksum, "size_bytes": size_bytes}
        except OSError as exc:
            readiness_failures[index] = ("failed", operation_error(exc))

    destination_client = preflight_client or s3_client
    preflight_errors = []
    if not readiness_failures and destination_client is not None:
        for index, entry in enumerate(entries):
            local = destinations[index]
            if local["checksum"] is None or local["size_bytes"] is None:
                continue
            operation = None
            evidence = {**entry.to_dict(), "checksum": local["checksum"], "size_bytes": local["size_bytes"]}
            try:
                if journal:
                    operation = journal.intent("destination-check", evidence)
                exists = destination_client.object_exists(entry.bucket, entry.key)
                if not exists:
                    if (entry.bucket, entry.key) in prior_writes:
                        message = 'Previously written object is absent during resume verification.'
                        local.update(status="conflict", classification="held", message=message)
                        preflight_errors.append(message)
                        result = {**evidence, "status": "conflict", "classification": "held",
                                  "message": message}
                    else:
                        local.update(status="absent", classification="new")
                        result = {**evidence, "status": "absent", "classification": "new"}
                else:
                    remote = destination_client.verify_file(
                        entry.bucket, entry.key, local["checksum"], local["size_bytes"]
                    )
                    if not isinstance(remote, VerificationResult):
                        remote = VerificationResult(False, None, None, "missing-evidence")
                    remote_matches = (
                        remote.matches
                        and remote.checksum == local["checksum"]
                        and remote.size_bytes == local["size_bytes"]
                        and remote.checksum_type == "FULL_OBJECT"
                        and remote.method in {"s3-sha256", "s3-sha256+readback"}
                    )
                    remote_evidence = remote.evidence()
                    is_prior_write = (entry.bucket, entry.key) in prior_writes
                    if is_prior_write and not remote_matches:
                        message = 'Previously written object no longer matches its recorded checksum and size.'
                        local.update(status="conflict", classification="held", message=message)
                        preflight_errors.append(message)
                        result = {**evidence, **remote_evidence, "status": "conflict",
                                  "classification": "held", "message": message}
                    else:
                        local.update(
                            status="unchanged" if remote_matches else ("held" if entry.role == "manifest" else "conflict"),
                            classification="previously_published" if remote_matches else "held",
                            previous_checksum=remote.checksum if remote.checksum_type == "FULL_OBJECT" else None,
                            previous_checksum_type=remote.checksum_type,
                            observed_size_bytes=remote.size_bytes,
                            verification_method=remote.method,
                            message="" if remote_matches else _existing_mismatch_message(
                                entry, local["checksum"],
                                remote.checksum if remote.checksum_type == "FULL_OBJECT" else None,
                            ),
                        )
                        result = {**evidence, **remote_evidence, "status": local["status"],
                                  "classification": local["classification"], "message": local["message"]}
                        if remote_matches:
                            verified.append({**evidence, "verification_method": remote.method})
                        elif entry.role != "manifest":
                            preflight_errors.append(local["message"])
                if journal:
                    journal.result(operation, "destination-check", result)
            except STORAGE_ERRORS as exc:
                message = "Destination readiness failed: " + operation_error(exc)
                local.update(status="failed", classification="held", message=message)
                preflight_errors.append(message)
                if journal:
                    journal.result(operation, "destination-check", {
                        **evidence, "status": "failed", "classification": "held", "message": message
                    })
    elif not dry_run and destination_client is None and not readiness_failures:
        preflight_errors.append("Destination readiness failed: no read-only S3 client is available.")

    errors.extend(preflight_errors)
    results: List[PushResult] = []
    stopped = bool(readiness_failures or preflight_errors)
    for index, entry in enumerate(entries):
        source_path = _resolve_upload_path(entry, input_dir, generated_dir)
        checksum = None
        size_bytes = None
        operation_id = None
        remote_unknown = False
        attempt_events_written = False
        classification = "held" if stopped else "new"
        previous_checksum = None
        previous_checksum_type = None
        observed_size_bytes = None
        verification_method = None
        destination = destinations.get(index, {})
        prior_attempts, prior_operation_id = attempt_history.get((entry.bucket, entry.key), (0, None))
        if readiness_failures:
            status, message = readiness_failures.get(index, (
                "unattempted", "Not attempted because batch upload readiness failed."
            ))
            classification = "held"
        elif destination.get("status") == "unchanged":
            status = "unchanged"
            message = "Previously published object verified by SHA-256; skipped."
            classification = "previously_published"
            checksum = destination.get("checksum")
            size_bytes = destination.get("size_bytes")
            previous_checksum = destination.get("previous_checksum")
            previous_checksum_type = destination.get("previous_checksum_type")
            observed_size_bytes = destination.get("observed_size_bytes")
            verification_method = destination.get("verification_method")
        elif destination.get("status") == "held":
            status = "held"
            message = destination.get("message", "Manifest replacement required; batch publication held.")
            classification = "held"
            checksum = destination.get("checksum")
            size_bytes = destination.get("size_bytes")
            previous_checksum = destination.get("previous_checksum")
            previous_checksum_type = destination.get("previous_checksum_type")
            observed_size_bytes = destination.get("observed_size_bytes")
            verification_method = destination.get("verification_method")
            errors.append(message)
        elif destination.get("status") == "conflict":
            status = "failed"
            message = destination.get("message", "Existing content cannot be overwritten.")
            classification = "held"
            checksum = destination.get("checksum")
            size_bytes = destination.get("size_bytes")
            previous_checksum = destination.get("previous_checksum")
            previous_checksum_type = destination.get("previous_checksum_type")
            observed_size_bytes = destination.get("observed_size_bytes")
        elif destination.get("status") == "failed":
            status = "failed"
            message = destination.get("message", "Destination readiness failed.")
            classification = "held"
        elif stopped:
            status = "unattempted"
            message = "Not attempted because batch upload readiness failed."
            classification = destination.get("classification", "held")
        else:
            try:
                status = _entry_status(entry, source_path, dry_run, generated_dir)
                if status == "would_upload":
                    checksum = destination.get("checksum") or _checksum_for_entry(source_path)
                    size_bytes = destination.get("size_bytes") or source_path.stat().st_size
                    if not dry_run:
                        if size_bytes > MAX_SINGLE_PUT_BYTES:
                            raise OSError("Artifact exceeds conditional single-PUT limit.")
                        (status, message, operation_id, remote_unknown,
                         retry_wait_seconds) = _upload_with_retries(
                            s3_client, source_path, entry, checksum, size_bytes, journal,
                            retry_wait_seconds, prior_attempts, prior_operation_id,
                        )
                        attempt_events_written = journal is not None
                        classification = 'new' if status == 'uploaded' else 'held'
                    else:
                        message = _status_message(status)
                else:
                    message = _status_message(status)
                if dry_run and destination_client is None and destination.get("status") == "unchecked":
                    classification = "held"
                    message = "Remote destinations were not checked; dry-run does not establish prior publication."
            except STORAGE_ERRORS as exc:
                status = "failed"
                message = operation_error(exc)
                classification = "held"
            if not dry_run and status in {"missing_source", "missing_generated", "failed"}:
                stopped = True
            if destination.get("status") == "unchecked":
                classification = destination.get("classification", "held")
        if status == "failed" and message not in errors:
            errors.append(message)
        results.append(PushResult(
            role=entry.role,
            source_path=str(source_path) if entry.generated and generated_dir is not None else entry.source_path,
            bucket=entry.bucket,
            key=entry.key,
            status=status,
            checksum=checksum,
            size_bytes=size_bytes,
            message=message,
            required=True,
            classification=classification,
            previous_checksum=previous_checksum,
            previous_checksum_type=previous_checksum_type,
            observed_size_bytes=observed_size_bytes,
            verification_method=verification_method,
        ))

        if journal and not attempt_events_written:
            journal.result(operation_id, "push", {**entry.to_dict(), **results[-1].to_dict(),
                                                  "remote_outcome": "unknown" if remote_unknown else status})

        if not dry_run and status == 'uploaded':
            identity = {**entry.to_dict(), 'checksum': checksum, 'size_bytes': size_bytes}
            verification = journal.intent('verify-upload', identity) if journal else None
            try:
                matches = s3_client.verify_file(entry.bucket, entry.key, checksum, size_bytes)
                if not isinstance(matches, VerificationResult):
                    matches = VerificationResult(False, None, None, 'missing-evidence')
                verified_upload = (
                    matches.matches
                    and matches.checksum == checksum
                    and matches.size_bytes == size_bytes
                    and matches.checksum_type == 'FULL_OBJECT'
                    and matches.method in {'s3-sha256', 's3-sha256+readback'}
                )
                verification_message = '' if verified_upload else 'Remote bytes do not match the uploaded artifact.'
            except STORAGE_ERRORS as exc:
                matches = False
                verified_upload = False
                verification_message = operation_error(exc)
            if journal:
                observed = matches.evidence() if hasattr(matches, 'evidence') else {}
                journal.result(verification, 'verify-upload', {**identity, **observed,
                                                               'status': 'verified' if verified_upload else 'failed',
                                                               'message': verification_message})
            if verified_upload:
                verified.append(identity)
            else:
                errors.append('Upload verification failed: ' + verification_message)
                stopped = True

    if rollback_path is not None and not dry_run:
        try:
            _write_rollback_file(
                rollback_path,
                [result for result in results if result.status == "uploaded"],
                run_id=journal.run_id if journal else None,
            )
        except OSError as exc:
            errors.append(f"Rollback inventory could not be written: {operation_error(exc)}")

    if journal and not dry_run and not errors and len(verified) == len(entries) and entries:
        journal.append('context', stage='push', verified_batch={
            'run_id': journal.run_id, 'verification_contract': 's3-sha256-v1',
            'verified_count': len(verified), 'manifest_count': sum(e.role == 'manifest' for e in entries),
            'publication_owner': 'workbench-lite',
            'plan_sha256': hashlib.sha256((journal.path / 'upload-plan.json').read_bytes()).hexdigest(),
        })

    return PushReport(
        dry_run=dry_run,
        endpoint_url=resolved_endpoint_url,
        region=resolved_region,
        profile=resolved_profile,
        planned_count=len(entries),
        source_backed_count=sum(1 for entry in entries if entry.source_path and not entry.generated),
        generated_count=sum(1 for entry in entries if entry.generated),
        missing_source_count=sum(1 for result in results if result.status == "missing_source"),
        missing_generated_count=sum(1 for result in results if result.status == "missing_generated"),
        uploaded_count=sum(1 for result in results if result.status == "uploaded"),
        skipped_generated_count=sum(1 for result in results if result.status == "skipped_generated"),
        role_counts=role_counts,
        results=results,
        errors=errors,
    )


def _upload_history(journal):
    if not journal:
        return {}, set()
    events, error = read_events(journal.path)
    if error:
        raise RuntimeError('Run journal cannot be used for upload attempt accounting.')
    current_push = max((index for index, event in enumerate(events)
                        if event['kind'] == 'stage_started' and event['stage'] == 'push'),
                       default=len(events))
    prior_operation_ids = {
        event['operation_id'] for index, event in enumerate(events[:current_push])
        if event['kind'] == 'intent' and event.get('stage') == 'push'
    }
    history = {}
    prior_writes = set()
    for index, event in enumerate(events):
        if event['kind'] == 'intent' and event.get('stage') == 'push':
            identity = (event.get('bucket'), event.get('key'))
            prior_attempts, _ = history.get(identity, (0, None))
            history[identity] = (max(prior_attempts, event.get('attempt', 1)), event['operation_id'])
        elif event['kind'] == 'result' and event.get('stage') == 'push':
            if (event.get('operation_id') in prior_operation_ids
                    and event.get('remote_outcome') == 'written'
                    and event.get('status') in {'uploaded', 'confirmed'}):
                prior_writes.add((event.get('bucket'), event.get('key')))
    return history, prior_writes


def _upload_with_retries(client, source_path, entry, checksum, size_bytes, journal,
                         retry_wait_seconds, prior_attempts=0, prior_operation_id=None):
    """Perform bounded conditional uploads, reconciling every failed attempt."""
    identity = {**entry.to_dict(), 'checksum': checksum, 'size_bytes': size_bytes}
    operation_id = prior_operation_id
    if retry_wait_seconds >= RUN_RETRY_WAIT_BUDGET_SECONDS:
        return ('failed', 'Invocation retry wait budget was already exhausted; no upload was attempted.',
                operation_id, False, retry_wait_seconds)
    for attempt_in_invocation in range(1, UPLOAD_MAX_ATTEMPTS + 1):
        attempt = prior_attempts + attempt_in_invocation
        if journal:
            operation_id = journal.intent(
                'push', identity, operation_id=operation_id, attempt=attempt
            )
        try:
            client.upload_file(source_path, entry.bucket, entry.key, checksum)
        except STORAGE_ERRORS as exc:
            details = upload_error_details(exc)
            inspect = getattr(client, 'inspect_object', None)
            if not callable(inspect):
                return _record_upload_attempt(
                    journal, operation_id, attempt, entry, identity,
                    status='failed', remote_outcome='unknown', disposition='unknown',
                    message='Upload failed and the storage client cannot inspect the destination.',
                    error=details, reconciled=False,
                ) + (retry_wait_seconds,)
            try:
                observed = inspect(entry.bucket, entry.key)
            except STORAGE_ERRORS as head_error:
                return _record_upload_attempt(
                    journal, operation_id, attempt, entry, identity,
                    status='failed', remote_outcome='unknown', disposition='unknown',
                    message=operation_error(exc) + ' Destination inspection failed; outcome remains unknown.',
                    error=details, reconciliation_error_class=type(head_error).__name__,
                    reconciled=False,
                ) + (retry_wait_seconds,)

            if observed is not None:
                observed_checksum = getattr(observed, 'checksum', None)
                observed_size = getattr(observed, 'size_bytes', None)
                observed_type = getattr(observed, 'checksum_type', None)
                if (observed_checksum == checksum and observed_size == size_bytes
                        and observed_type == 'FULL_OBJECT'):
                    return _record_upload_attempt(
                        journal, operation_id, attempt, entry, identity,
                        status='uploaded', remote_outcome='written',
                        disposition='reconciled_written',
                        message='Upload was confirmed by destination checksum and size after an unacknowledged response.',
                        error=details, observed_checksum=observed_checksum,
                        observed_size_bytes=observed_size, checksum_type=observed_type,
                        reconciled=True,
                    ) + (retry_wait_seconds,)
                return _record_upload_attempt(
                    journal, operation_id, attempt, entry, identity,
                    status='failed', remote_outcome='conflict', disposition='conflict',
                    message='Destination contains different or unverifiable content; batch stopped.',
                    error=details, observed_checksum=observed_checksum,
                    observed_size_bytes=observed_size, checksum_type=observed_type,
                    reconciled=False,
                ) + (retry_wait_seconds,)

            if not details['retryable']:
                return _record_upload_attempt(
                    journal, operation_id, attempt, entry, identity,
                    status='failed', remote_outcome='absent', disposition='not_retryable',
                    message=operation_error(exc) + ' Destination is absent; this error is not retryable.',
                    error=details, reconciled=False,
                ) + (retry_wait_seconds,)
            if attempt_in_invocation >= UPLOAD_MAX_ATTEMPTS:
                return _record_upload_attempt(
                    journal, operation_id, attempt, entry, identity,
                    status='failed', remote_outcome='absent', disposition='retries_exhausted',
                    message=f"Transient upload attempts exhausted after {_error_label(details)}; destination is confirmed absent.",
                    error=details, reconciled=False,
                ) + (retry_wait_seconds,)

            backoff = UPLOAD_RETRY_BACKOFF_SECONDS[attempt_in_invocation - 1]
            remaining = RUN_RETRY_WAIT_BUDGET_SECONDS - retry_wait_seconds
            if remaining <= 0:
                return _record_upload_attempt(
                    journal, operation_id, attempt, entry, identity,
                    status='failed', remote_outcome='absent', disposition='retries_exhausted',
                    message=f"Invocation retry wait budget exhausted after {_error_label(details)}; destination is confirmed absent.",
                    error=details, reconciled=False,
                ) + (retry_wait_seconds,)
            delay = min(random.uniform(0, backoff), remaining)
            started_wait = time.monotonic()
            time.sleep(delay)
            elapsed_wait = max(delay, time.monotonic() - started_wait)
            retry_wait_seconds += elapsed_wait
            if retry_wait_seconds >= RUN_RETRY_WAIT_BUDGET_SECONDS:
                return _record_upload_attempt(
                    journal, operation_id, attempt, entry, identity,
                    status='failed', remote_outcome='absent', disposition='retries_exhausted',
                    message=f"Invocation retry wait budget exhausted after {_error_label(details)}; destination is confirmed absent.",
                    error=details, actual_retry_wait_seconds=elapsed_wait,
                    reconciled=False,
                ) + (retry_wait_seconds,)
            _record_upload_attempt(
                journal, operation_id, attempt, entry, identity,
                status='failed', remote_outcome='absent', disposition='retrying',
                message='Transient upload failure; destination is confirmed absent before retry.',
                error=details, delay_before_next_attempt=elapsed_wait,
                actual_retry_wait_seconds=elapsed_wait, reconciled=False,
            )
        else:
            return _record_upload_attempt(
                journal, operation_id, attempt, entry, identity,
                status='uploaded', remote_outcome='written', disposition='written',
                message='Source file uploaded.', reconciled=False,
            ) + (retry_wait_seconds,)

    raise AssertionError('bounded upload loop ended without a disposition')


def _record_upload_attempt(journal, operation_id, attempt, entry, identity, *,
                           status, remote_outcome, disposition, message, error=None,
                           **evidence):
    details = error or {}
    event = {
        **entry.to_dict(), **identity, 'status': status,
        'classification': 'new' if status == 'uploaded' else 'held',
        'remote_outcome': remote_outcome, 'disposition': disposition,
        'message': message, 'error_class': details.get('error_class'),
        'error_code': details.get('error_code'), 'http_status': details.get('http_status'),
        'delay_before_next_attempt': None, **evidence,
    }
    if journal:
        journal.result(operation_id, 'push', event, attempt=attempt)
    return status, message, operation_id, remote_outcome == 'unknown'


def _error_label(details):
    if details.get('error_code'):
        return details['error_code']
    if details.get('http_status'):
        return f"HTTP {details['http_status']}"
    return details.get('error_class') or 'storage error'


def _existing_mismatch_message(entry: UploadPlanEntry, planned_checksum: str, previous_checksum: Optional[str]) -> str:
    previous = previous_checksum or "unavailable"
    if entry.role == "manifest":
        return (f"manifest replacement required for {entry.bucket}/{entry.key}; "
                f"previous SHA-256 {previous}, planned SHA-256 {planned_checksum}.")
    return (f"Changed or unverifiable content at existing key {entry.bucket}/{entry.key}; "
            f"previous SHA-256 {previous}, planned SHA-256 {planned_checksum}. Refusing to overwrite.")


def _entry_status(
    entry: UploadPlanEntry,
    source_path: Path,
    dry_run: bool,
    generated_dir: Optional[Path],
) -> str:
    if entry.generated:
        if generated_dir is None:
            return "would_generate" if dry_run else "missing_generated"
        if not source_path.is_file():
            return "missing_generated"
        return "would_upload"
    if not entry.source_path or not source_path.is_file():
        return "missing_source"
    return "would_upload"


def _checksum_for_entry(path: Path) -> Optional[str]:
    if not path.is_file():
        return None

    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _resolve_source_path(source_path: str, input_dir: Path) -> Path:
    path = Path(source_path)
    if path.is_absolute():
        return path
    return input_dir / path


def _resolve_upload_path(
    entry: UploadPlanEntry,
    input_dir: Path,
    generated_dir: Optional[Path],
) -> Path:
    if entry.generated and generated_dir is not None:
        return generated_dir / entry.key.lstrip("/")
    return _resolve_source_path(entry.source_path, input_dir)


def _status_message(status: str) -> str:
    messages = {
        "would_generate": "Generated artifact is planned but not created by this slice.",
        "skipped_generated": "Generated artifact must be created before upload.",
        "missing_generated": "Generated artifact file is missing.",
        "missing_source": "Source file is missing.",
        "would_upload": "Source file would be uploaded.",
        "uploaded": "Source file uploaded.",
    }
    return messages.get(status, "")


def _write_rollback_file(rollback_path: Path, uploaded: list[PushResult], run_id=None) -> None:
    rollback_payload = {
        "kind": "legacy-upload-list",
        "deletion_authorized": False,
        "run_id": run_id,
        "written": [entry.to_dict() for entry in uploaded],
        "count": len(uploaded),
    }
    rollback_path.write_text(
        json.dumps(rollback_payload, indent=2, sort_keys=True, ensure_ascii=False),
        encoding="utf-8",
    )
