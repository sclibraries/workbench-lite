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
from .id_claims import check_id_claims, verify_claims

# The SDK remains optional for checks and local generation.
try:
    from botocore.exceptions import BotoCoreError, ClientError
    from boto3.exceptions import S3UploadFailedError
except ImportError:
    STORAGE_ERRORS = (OSError,)
else:
    STORAGE_ERRORS = (OSError, BotoCoreError, ClientError, S3UploadFailedError)


MAX_SINGLE_PUT_BYTES = 5 * 1024**3


class S3UploadClient(Protocol):
    def object_exists(self, bucket, key): ...

    def verify_file(self, bucket, key, checksum, size_bytes): ...

    def upload_file(self, source_path, bucket, key, checksum): ...

    def inspect_object(self, bucket, key): ...


@dataclass(frozen=True)
class PushResult:
    role: str
    source_path: str
    bucket: str
    key: str
    status: str
    checksum: Optional[str]
    size_bytes: Optional[int]
    message: str = ''
    required: bool = True
    classification: str = 'new'
    previous_checksum: Optional[str] = None
    previous_checksum_type: Optional[str] = None
    observed_size_bytes: Optional[int] = None
    verification_method: Optional[str] = None

    def to_dict(self) -> Dict[str, object]:
        return {
            'role': self.role,
            'source_path': self.source_path,
            'bucket': self.bucket,
            'key': self.key,
            'status': self.status,
            'checksum': self.checksum,
            'size_bytes': self.size_bytes,
            'message': self.message,
            'required': self.required,
            'classification': self.classification,
            'previous_checksum': self.previous_checksum,
            'previous_checksum_type': self.previous_checksum_type,
            'observed_size_bytes': self.observed_size_bytes,
            'verification_method': self.verification_method,
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
    claim_check: Optional[dict] = None
    refusal_code: int = 0

    @property
    def failed_count(self) -> int:
        return sum(result.status == 'failed' for result in self.results)

    @property
    def held_count(self) -> int:
        return sum(result.classification == 'held' for result in self.results)

    @property
    def classification_counts(self) -> Dict[str, int]:
        return dict(sorted(Counter(result.classification for result in self.results).items()))

    @property
    def has_errors(self) -> bool:
        return bool(
            self.errors
            or self.failed_count
            or self.missing_source_count
            or self.missing_generated_count
        )

    def to_dict(self) -> Dict[str, object]:
        return {
            'publication': {
                'state': 'held',
                'reason': 'Use inspect-run after successful finalization; '
                'this report is not a publication receipt.',
            },
            'errors': self.errors,
            'failed_count': self.failed_count,
            'held_count': self.held_count,
            'classification_counts': self.classification_counts,
            'unattempted_count': sum(r.status == 'unattempted' for r in self.results),
            'deferred_count': sum(r.status == 'deferred' for r in self.results),
            'dry_run': self.dry_run,
            'endpoint_url': self.endpoint_url,
            'region': self.region,
            'profile': self.profile,
            'planned_count': self.planned_count,
            'source_backed_count': self.source_backed_count,
            'generated_count': self.generated_count,
            'missing_source_count': self.missing_source_count,
            'missing_generated_count': self.missing_generated_count,
            'uploaded_count': self.uploaded_count,
            'skipped_generated_count': self.skipped_generated_count,
            'role_counts': self.role_counts,
            'results': [result.to_dict() for result in self.results],
        }


@dataclass
class _PushEntryResult:
    result: PushResult
    operation_id: Optional[str] = None
    remote_unknown: bool = False
    attempt_events_written: bool = False
    retry_wait_seconds: float = 0.0


@dataclass
class _PushEntryOutcome:
    result: PushResult
    errors: List[str] = field(default_factory=list)
    stopped: bool = False
    retry_wait_seconds: float = 0.0
    verified_identity: Optional[dict] = None


@dataclass
class _PushSettings:
    input_dir: Path
    generated_dir: Optional[Path]
    s3_client: Optional[S3UploadClient]
    dry_run: bool
    destination_client: Optional[S3UploadClient]
    rollback_path: Optional[Path]
    journal: object
    endpoint: str
    region: str
    profile: str


@dataclass
class _PushRun:
    settings: _PushSettings
    entries: list
    role_counts: dict
    attempt_history: dict
    prior_writes: set
    readiness_failures: dict
    destinations: dict
    errors: List[str] = field(default_factory=list)
    results: list = field(default_factory=list)
    verified: list = field(default_factory=list)
    stopped: bool = False
    retry_wait_seconds: float = 0.0
    claim_check: Optional[dict] = None
    refusal_code: int = 0


@dataclass
class _PushEntryContext:
    entry: UploadPlanEntry
    index: int
    source_path: Path
    source_label: str
    destination: dict
    prior_attempts: int = 0
    prior_operation_id: Optional[str] = None
    status: Optional[str] = None
    message: str = ''
    checksum: Optional[str] = None
    size_bytes: Optional[int] = None
    operation_id: Optional[str] = None
    remote_unknown: bool = False
    attempt_events_written: bool = False
    classification: str = 'new'
    retry_wait_seconds: float = 0.0


@dataclass
class _PushResultDetails:
    status: str
    message: str
    classification: str
    checksum: Optional[str] = None
    size_bytes: Optional[int] = None
    previous_checksum: Optional[str] = None
    previous_checksum_type: Optional[str] = None
    observed_size_bytes: Optional[int] = None
    verification_method: Optional[str] = None


@dataclass
class _UploadAttempt:
    run: _PushRun
    target: _PushEntryContext
    identity: dict
    retry_wait_seconds: float
    operation_id: Optional[str]
    prior_attempts: int = 0


@dataclass
class _AttemptResult:
    status: str
    remote_outcome: str
    disposition: str
    message: str
    error: Optional[dict] = None
    evidence: dict = field(default_factory=dict)


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
    id_claims=None,
) -> PushReport:
    if not dry_run and s3_client is None:
        raise ValueError('Execute requires an upload client.')
    endpoint, region_name, profile_name = _resolved_target(endpoint_url, region, profile)
    destination_client = preflight_client or s3_client
    settings = _PushSettings(
        input_dir,
        generated_dir,
        s3_client,
        dry_run,
        destination_client,
        rollback_path,
        journal,
        endpoint,
        region_name,
        profile_name,
    )
    run = _prepare_push_run(upload_plan, settings)
    _check_push_claims(run, id_claims)
    _process_push_entries(run)
    _finish_push_run(run, id_claims)
    return _build_push_report(run)


def _check_push_claims(run, claims):
    if claims is None:
        return
    if run.stopped:
        run.claim_check = {'status': 'skipped', 'expected_count': len(claims)}
        return
    result = check_id_claims(
        claims,
        run.settings.destination_client,
        run.settings.s3_client,
        run.settings.journal,
        dry_run=run.settings.dry_run,
    )
    run.claim_check = result.to_dict()
    run.errors.extend(result.errors)
    run.refusal_code = result.refusal_code
    run.stopped = bool(result.refusal_code)


def _prepare_push_run(upload_plan, settings):
    entries = _ordered_entries(upload_plan)
    role_counts = dict(sorted(Counter(entry.role for entry in entries).items()))
    attempt_history, prior_writes = _upload_history(settings.journal)
    readiness_failures, destinations = _local_readiness(
        entries, settings.input_dir, settings.generated_dir, settings.dry_run
    )
    run = _PushRun(
        settings,
        entries,
        role_counts,
        attempt_history,
        prior_writes,
        readiness_failures,
        destinations,
    )
    preflight_errors, verified = _destination_readiness(run)
    run.errors.extend(preflight_errors)
    run.verified.extend(verified)
    run.stopped = bool(readiness_failures or preflight_errors)
    return run


def _process_push_entries(run):
    for index, entry in enumerate(run.entries):
        outcome = _push_entry(run, entry, index)
        run.results.append(outcome.result)
        run.errors.extend(outcome.errors)
        run.stopped = outcome.stopped
        run.retry_wait_seconds = outcome.retry_wait_seconds
        if outcome.verified_identity:
            run.verified.append(outcome.verified_identity)


def _finish_push_run(run, id_claims):
    settings = run.settings
    verified_claims = _verify_final_claims(run, id_claims)
    _write_legacy_rollback(
        settings.rollback_path, settings.dry_run, run.results, settings.journal, run.errors
    )
    _record_verified_batch(
        settings.journal,
        settings.dry_run,
        run.entries,
        run.verified,
        verified_claims,
        run.errors,
    )


def _verify_final_claims(run, claims):
    settings = run.settings
    if settings.dry_run or claims is None or run.stopped or run.errors:
        return []
    verified, errors = verify_claims(claims, settings.s3_client)
    run.errors.extend(errors)
    return verified


def _ordered_entries(upload_plan):
    # Keep manifest writes last while preserving plan order within each group.
    return sorted(upload_plan, key=lambda entry: entry.role == 'manifest')


def _resolved_target(endpoint_url, region, profile):
    return (
        endpoint_url or os.environ.get('AWS_ENDPOINT_URL', ''),
        region or os.environ.get('AWS_REGION', 'us-east-1'),
        profile or os.environ.get('AWS_PROFILE', ''),
    )


def _local_readiness(entries, input_dir, generated_dir, dry_run):
    failures = {}
    destinations = {}
    for index, entry in enumerate(entries):
        source_path = _resolve_upload_path(entry, input_dir, generated_dir)
        try:
            status = _entry_status(entry, source_path, dry_run, generated_dir)
            if status in {'missing_source', 'missing_generated'}:
                failures[index] = (status, _status_message(status))
                continue
            if status == 'would_generate':
                destinations[index] = _unchecked_destination('held')
                continue
            destination = _ready_destination(source_path)
            if not dry_run and destination['size_bytes'] > MAX_SINGLE_PUT_BYTES:
                failures[index] = (
                    'failed',
                    'Artifact exceeds the 5 GiB conditional single-PUT limit; batch held.',
                )
                continue
            destinations[index] = destination
        except OSError as exc:
            failures[index] = ('failed', operation_error(exc))
    return failures, destinations


def _unchecked_destination(classification):
    return {
        'status': 'unchecked',
        'classification': classification,
        'checksum': None,
        'size_bytes': None,
    }


def _ready_destination(source_path):
    checksum = _checksum_for_entry(source_path)
    size_bytes = source_path.stat().st_size
    return {
        'status': 'unchecked',
        'classification': 'new',
        'checksum': checksum,
        'size_bytes': size_bytes,
    }


def _destination_readiness(run):
    errors = []
    verified = []
    settings = run.settings
    if not run.readiness_failures and settings.destination_client is not None:
        for index, entry in enumerate(run.entries):
            local = run.destinations[index]
            if local['checksum'] is None or local['size_bytes'] is None:
                continue
            found_errors, verified_item = _check_destination(
                entry,
                local,
                settings.destination_client,
                run.prior_writes,
                settings.journal,
            )
            errors.extend(found_errors)
            if verified_item:
                verified.append(verified_item)
    elif (
        not settings.dry_run and settings.destination_client is None and not run.readiness_failures
    ):
        errors.append('Destination readiness failed: no read-only S3 client is available.')
    return errors, verified


def _check_destination(entry, local, client, prior_writes, journal):
    evidence = {**entry.to_dict(), 'checksum': local['checksum'], 'size_bytes': local['size_bytes']}
    operation = None
    try:
        if journal:
            operation = journal.intent('destination-check', evidence)
        if client.object_exists(entry.bucket, entry.key):
            result, errors, verified = _existing_destination(
                entry, local, client, evidence, prior_writes
            )
        else:
            result, errors, verified = _missing_destination(entry, evidence, local, prior_writes)
        if journal:
            journal.result(operation, 'destination-check', result)
    except STORAGE_ERRORS as exc:
        result, errors, verified = _failed_destination(evidence, local, exc)
        if journal:
            journal.result(operation, 'destination-check', result)
    return errors, verified


def _missing_destination(entry, evidence, local, prior_writes):
    if (entry.bucket, entry.key) in prior_writes:
        message = 'Previously written object is absent during resume verification.'
        local.update(status='conflict', classification='held', message=message)
        return (
            {**evidence, 'status': 'conflict', 'classification': 'held', 'message': message},
            [message],
            None,
        )
    local.update(status='absent', classification='new')
    return {**evidence, 'status': 'absent', 'classification': 'new'}, [], None


def _existing_destination(entry, local, client, evidence, prior_writes):
    remote = client.verify_file(entry.bucket, entry.key, local['checksum'], local['size_bytes'])
    if not isinstance(remote, VerificationResult):
        remote = VerificationResult(False, None, None, 'missing-evidence')
    remote_matches = _verification_matches(remote, local['checksum'], local['size_bytes'])
    if (entry.bucket, entry.key) in prior_writes and not remote_matches:
        return _changed_prior_destination(evidence, local, remote.evidence())
    return _record_existing_destination(entry, local, evidence, remote, remote_matches)


def _changed_prior_destination(evidence, local, remote_evidence):
    message = 'Previously written object no longer matches its recorded checksum and size.'
    local.update(status='conflict', classification='held', message=message)
    return (
        {
            **evidence,
            **remote_evidence,
            'status': 'conflict',
            'classification': 'held',
            'message': message,
        },
        [message],
        None,
    )


def _verification_matches(remote, checksum, size_bytes):
    return (
        remote.matches
        and remote.checksum == checksum
        and remote.size_bytes == size_bytes
        and remote.checksum_type == 'FULL_OBJECT'
        and remote.method in {'s3-sha256', 's3-sha256+readback'}
    )


def _record_existing_destination(entry, local, evidence, remote, remote_matches):
    remote_evidence = remote.evidence()
    local.update(
        status='unchanged'
        if remote_matches
        else ('held' if entry.role == 'manifest' else 'conflict'),
        classification='previously_published' if remote_matches else 'held',
        previous_checksum=remote.checksum if remote.checksum_type == 'FULL_OBJECT' else None,
        previous_checksum_type=remote.checksum_type,
        observed_size_bytes=remote.size_bytes,
        verification_method=remote.method,
        message=''
        if remote_matches
        else _existing_mismatch_message(
            entry,
            local['checksum'],
            remote.checksum if remote.checksum_type == 'FULL_OBJECT' else None,
        ),
    )
    result = {
        **evidence,
        **remote_evidence,
        'status': local['status'],
        'classification': local['classification'],
        'message': local['message'],
    }
    error = [] if remote_matches or entry.role == 'manifest' else [local['message']]
    verified = {**evidence, 'verification_method': remote.method} if remote_matches else None
    return result, error, verified


def _failed_destination(evidence, local, exc):
    message = 'Destination readiness failed: ' + operation_error(exc)
    local.update(status='failed', classification='held', message=message)
    result = {
        **evidence,
        'status': 'failed',
        'classification': 'held',
        'message': message,
    }
    return result, [message], None


def _push_entry(run, entry, index):
    target = _push_entry_context(run, entry, index)
    result = _entry_result(run, target)
    return _finish_push_entry(run, target, result)


def _push_entry_context(run, entry, index):
    settings = run.settings
    source_path = _resolve_upload_path(entry, settings.input_dir, settings.generated_dir)
    source_label = (
        str(source_path)
        if entry.generated and settings.generated_dir is not None
        else entry.source_path
    )
    prior_attempts, prior_operation_id = run.attempt_history.get(
        (entry.bucket, entry.key), (0, None)
    )
    return _PushEntryContext(
        entry=entry,
        index=index,
        source_path=source_path,
        source_label=source_label,
        destination=run.destinations.get(index, {}),
        prior_attempts=prior_attempts,
        prior_operation_id=prior_operation_id,
        retry_wait_seconds=run.retry_wait_seconds,
    )


def _finish_push_entry(run, target, entry_result):
    if entry_result.retry_wait_seconds == 0:
        entry_result.retry_wait_seconds = run.retry_wait_seconds
    errors = _push_entry_errors(run, target, entry_result.result)
    _record_push_entry_result(run, target, entry_result)
    verified_identity = None
    stopped = run.stopped
    settings = run.settings
    if not settings.dry_run and entry_result.result.status == 'uploaded':
        verified_identity, verification_error = _verify_uploaded_entry(
            target.entry, entry_result.result, settings.s3_client, settings.journal
        )
        if verification_error:
            errors.append('Upload verification failed: ' + verification_error)
            stopped = True
    if not settings.dry_run and entry_result.result.status in {
        'missing_source',
        'missing_generated',
        'failed',
    }:
        stopped = True
    return _PushEntryOutcome(
        result=entry_result.result,
        errors=errors,
        stopped=stopped,
        retry_wait_seconds=entry_result.retry_wait_seconds,
        verified_identity=verified_identity,
    )


def _push_entry_errors(run, target, result):
    errors = []
    if target.destination.get('status') == 'held':
        errors.append(result.message)
    elif result.status == 'failed' and result.message not in run.errors:
        errors.append(result.message)
    return errors


def _record_push_entry_result(run, target, entry_result):
    journal = run.settings.journal
    if journal and not entry_result.attempt_events_written:
        journal.result(
            entry_result.operation_id,
            'push',
            {
                **target.entry.to_dict(),
                **entry_result.result.to_dict(),
                'remote_outcome': (
                    'unknown' if entry_result.remote_unknown else entry_result.result.status
                ),
            },
        )


def _entry_result(run, target):
    blocked = _blocked_entry_result(run, target)
    if blocked is not None:
        return blocked
    return _ready_entry_result(run, target)


def _blocked_entry_result(run, target):
    if run.readiness_failures:
        status, message = run.readiness_failures.get(
            target.index, ('unattempted', 'Not attempted because batch upload readiness failed.')
        )
        return _make_entry_result(target, _PushResultDetails(status, message, 'held'))
    destination_status = target.destination.get('status')
    if destination_status in {'unchanged', 'held', 'conflict', 'failed'}:
        return _known_destination_result(target)
    if run.stopped:
        return _make_entry_result(
            target,
            _PushResultDetails(
                'unattempted',
                'Not attempted because batch upload readiness failed.',
                target.destination.get('classification', 'held'),
            ),
        )
    return None


def _known_destination_result(target):
    destination = target.destination
    status = destination['status']
    message = destination.get('message', '')
    classification = 'held'
    if status == 'unchanged':
        message = 'Previously published object verified by SHA-256; skipped.'
        classification = 'previously_published'
    elif status == 'held':
        message = message or 'Manifest replacement required; batch publication held.'
    elif status == 'conflict':
        status = 'failed'
        message = message or 'Existing content cannot be overwritten.'
    elif status == 'failed':
        message = message or 'Destination readiness failed.'
    return _make_entry_result(
        target,
        _PushResultDetails(
            status=status,
            message=message,
            classification=classification,
            checksum=destination.get('checksum'),
            size_bytes=destination.get('size_bytes'),
            previous_checksum=destination.get('previous_checksum'),
            previous_checksum_type=destination.get('previous_checksum_type'),
            observed_size_bytes=destination.get('observed_size_bytes'),
            verification_method=destination.get('verification_method'),
        ),
    )


def _ready_entry_result(run, target):
    settings = run.settings
    try:
        target.status = _entry_status(
            target.entry, target.source_path, settings.dry_run, settings.generated_dir
        )
        if target.status == 'would_upload':
            target.checksum, target.size_bytes = _destination_identity(
                target.destination, target.source_path
            )
        else:
            target.checksum, target.size_bytes = None, None
        return _upload_entry(run, target)
    except STORAGE_ERRORS as exc:
        return _make_entry_result(
            target,
            _PushResultDetails('failed', operation_error(exc), 'held'),
        )


def _destination_identity(destination, source_path):
    checksum = destination.get('checksum')
    size_bytes = destination.get('size_bytes')
    if checksum is None:
        checksum = _checksum_for_entry(source_path)
    if size_bytes is None:
        size_bytes = source_path.stat().st_size
    return checksum, size_bytes


def _upload_entry(run, target):
    settings = run.settings
    target.classification = 'new'
    if target.status == 'would_upload':
        if not settings.dry_run and target.size_bytes > MAX_SINGLE_PUT_BYTES:
            raise OSError('Artifact exceeds conditional single-PUT limit.')
        if not settings.dry_run:
            _upload_entry_content(run, target)
        else:
            target.message = _status_message(target.status)
    else:
        target.message = _status_message(target.status)
    _classify_push_entry(run, target)
    return _PushEntryResult(
        result=_make_entry_result(
            target,
            _PushResultDetails(
                target.status,
                target.message,
                target.classification,
                checksum=target.checksum,
                size_bytes=target.size_bytes,
            ),
        ).result,
        operation_id=target.operation_id,
        remote_unknown=target.remote_unknown,
        attempt_events_written=target.attempt_events_written,
        retry_wait_seconds=target.retry_wait_seconds,
    )


def _upload_entry_content(run, target):
    settings = run.settings
    attempt = _UploadAttempt(
        run,
        target,
        {
            **target.entry.to_dict(),
            'checksum': target.checksum,
            'size_bytes': target.size_bytes,
        },
        run.retry_wait_seconds,
        target.prior_operation_id,
        target.prior_attempts,
    )
    (
        target.status,
        target.message,
        target.operation_id,
        target.remote_unknown,
        target.retry_wait_seconds,
    ) = _upload_with_retries(attempt)
    target.attempt_events_written = settings.journal is not None
    target.classification = 'new' if target.status == 'uploaded' else 'held'


def _classify_push_entry(run, target):
    settings = run.settings
    if (
        settings.dry_run
        and settings.destination_client is None
        and target.destination.get('status') == 'unchecked'
    ):
        target.classification = 'held'
        target.message = (
            'Remote destinations were not checked; dry-run does not establish prior publication.'
        )
    if target.destination.get('status') == 'unchecked':
        target.classification = target.destination.get('classification', 'held')


def _make_entry_result(target, details):
    return _PushEntryResult(
        result=PushResult(
            role=target.entry.role,
            source_path=target.source_label,
            bucket=target.entry.bucket,
            key=target.entry.key,
            status=details.status,
            checksum=details.checksum,
            size_bytes=details.size_bytes,
            message=details.message,
            required=True,
            classification=details.classification,
            previous_checksum=details.previous_checksum,
            previous_checksum_type=details.previous_checksum_type,
            observed_size_bytes=details.observed_size_bytes,
            verification_method=details.verification_method,
        )
    )


def _verify_uploaded_entry(entry, result, client, journal):
    identity = {**entry.to_dict(), 'checksum': result.checksum, 'size_bytes': result.size_bytes}
    verification = journal.intent('verify-upload', identity) if journal else None
    try:
        matches = client.verify_file(entry.bucket, entry.key, result.checksum, result.size_bytes)
        if not isinstance(matches, VerificationResult):
            matches = VerificationResult(False, None, None, 'missing-evidence')
        verified = _verification_matches(matches, result.checksum, result.size_bytes)
        message = '' if verified else 'Remote bytes do not match the uploaded artifact.'
    except STORAGE_ERRORS as exc:
        matches = False
        verified = False
        message = operation_error(exc)
    if journal:
        observed = matches.evidence() if hasattr(matches, 'evidence') else {}
        journal.result(
            verification,
            'verify-upload',
            {
                **identity,
                **observed,
                'status': 'verified' if verified else 'failed',
                'message': message,
            },
        )
    return (identity, '') if verified else (None, message)


def _write_legacy_rollback(rollback_path, dry_run, results, journal, errors):
    if rollback_path is None or dry_run:
        return
    try:
        uploaded = [result for result in results if result.status == 'uploaded']
        _write_rollback_file(
            rollback_path,
            uploaded,
            run_id=journal.run_id if journal else None,
        )
    except OSError as exc:
        errors.append(f'Rollback inventory could not be written: {operation_error(exc)}')


def _record_verified_batch(journal, dry_run, entries, verified, verified_claims, errors):
    if not journal or dry_run or errors or len(verified) != len(entries) or not entries:
        return
    journal.append(
        'context',
        stage='push',
        verified_batch={
            'run_id': journal.run_id,
            'verification_contract': 's3-sha256-v1',
            'verified_count': len(verified),
            'verified_claims': verified_claims,
            'manifest_count': sum(entry.role == 'manifest' for entry in entries),
            'publication_owner': 'workbench-lite',
            'plan_sha256': hashlib.sha256(
                (journal.path / 'upload-plan.json').read_bytes()
            ).hexdigest(),
        },
    )


def _build_push_report(run):
    settings = run.settings
    results = run.results
    return PushReport(
        dry_run=settings.dry_run,
        endpoint_url=settings.endpoint,
        region=settings.region,
        profile=settings.profile,
        planned_count=len(run.entries),
        source_backed_count=sum(
            1 for entry in run.entries if entry.source_path and not entry.generated
        ),
        generated_count=sum(1 for entry in run.entries if entry.generated),
        missing_source_count=sum(1 for result in results if result.status == 'missing_source'),
        missing_generated_count=sum(
            1 for result in results if result.status == 'missing_generated'
        ),
        uploaded_count=sum(1 for result in results if result.status == 'uploaded'),
        skipped_generated_count=sum(
            1 for result in results if result.status == 'skipped_generated'
        ),
        role_counts=run.role_counts,
        results=results,
        errors=run.errors,
        claim_check=run.claim_check,
        refusal_code=run.refusal_code,
    )


def _upload_history(journal):
    if not journal:
        return {}, set()
    events, error = read_events(journal.path)
    if error:
        raise RuntimeError('Run journal cannot be used for upload attempt accounting.')
    current_push = max(
        (
            index
            for index, event in enumerate(events)
            if event['kind'] == 'stage_started' and event['stage'] == 'push'
        ),
        default=len(events),
    )
    prior_operation_ids = {
        event['operation_id']
        for event in events[:current_push]
        if event['kind'] == 'intent' and event.get('stage') == 'push'
    }
    history = {}
    prior_writes = set()
    for event in events:
        if event['kind'] == 'intent' and event.get('stage') == 'push':
            identity = (event.get('bucket'), event.get('key'))
            prior_attempts, _ = history.get(identity, (0, None))
            history[identity] = (
                max(prior_attempts, event.get('attempt', 1)),
                event['operation_id'],
            )
        elif event['kind'] == 'result' and event.get('stage') == 'push':
            if (
                event.get('operation_id') in prior_operation_ids
                and event.get('remote_outcome') == 'written'
                and event.get('status') in {'uploaded', 'confirmed'}
            ):
                prior_writes.add((event.get('bucket'), event.get('key')))
    return history, prior_writes


def _upload_with_retries(context):
    """Perform bounded conditional uploads, reconciling every failed attempt."""
    run = context.run
    target = context.target
    journal = run.settings.journal
    if context.retry_wait_seconds >= RUN_RETRY_WAIT_BUDGET_SECONDS:
        return _retry_budget_exhausted(context.operation_id, context.retry_wait_seconds)
    for attempt_in_invocation in range(1, UPLOAD_MAX_ATTEMPTS + 1):
        attempt = context.prior_attempts + attempt_in_invocation
        if journal:
            context.operation_id = journal.intent(
                'push', context.identity, operation_id=context.operation_id, attempt=attempt
            )
        try:
            run.settings.s3_client.upload_file(
                target.source_path,
                target.entry.bucket,
                target.entry.key,
                target.checksum,
            )
        except STORAGE_ERRORS as exc:
            terminal = _handle_failed_upload(context, attempt, attempt_in_invocation, exc)
            if terminal is not None:
                return terminal + (context.retry_wait_seconds,)
        else:
            result = _AttemptResult(
                'uploaded',
                'written',
                'written',
                'Source file uploaded.',
                evidence={'reconciled': False},
            )
            return _record_upload_attempt(context, attempt, result) + (context.retry_wait_seconds,)

    raise AssertionError('bounded upload loop ended without a disposition')


def _handle_failed_upload(context, attempt, attempt_in_invocation, exc):
    details = upload_error_details(exc)
    reconciled = _reconcile_failed_upload(context, attempt, exc, details)
    if reconciled is not None:
        return reconciled
    terminal = _absent_upload_failure(context, attempt, attempt_in_invocation, exc, details)
    if terminal is not None:
        return terminal
    context.retry_wait_seconds, terminal = _wait_before_upload_retry(
        context, attempt, details, UPLOAD_RETRY_BACKOFF_SECONDS[attempt_in_invocation - 1]
    )
    return terminal


def _retry_budget_exhausted(operation_id, retry_wait_seconds):
    return (
        'failed',
        'Invocation retry wait budget was already exhausted; no upload was attempted.',
        operation_id,
        False,
        retry_wait_seconds,
    )


def _reconcile_failed_upload(context, attempt, exc, details):
    settings = context.run.settings
    target = context.target
    client = settings.s3_client
    inspect = getattr(client, 'inspect_object', None)
    if not callable(inspect):
        return _record_upload_attempt(
            context,
            attempt,
            _AttemptResult(
                'failed',
                'unknown',
                'unknown',
                'Upload failed and the storage client cannot inspect the destination.',
                error=details,
                evidence={'reconciled': False},
            ),
        )
    try:
        observed = inspect(target.entry.bucket, target.entry.key)
    except STORAGE_ERRORS as head_error:
        return _record_upload_attempt(
            context,
            attempt,
            _AttemptResult(
                'failed',
                'unknown',
                'unknown',
                operation_error(exc) + ' Destination inspection failed; outcome remains unknown.',
                error=details,
                evidence={
                    'reconciliation_error_class': type(head_error).__name__,
                    'reconciled': False,
                },
            ),
        )
    if observed is None:
        return None
    return _record_observed_upload(context, attempt, observed, details)


def _record_observed_upload(context, attempt, observed, details):
    result = _observed_upload_result(context.target, observed, details)
    return _record_upload_attempt(context, attempt, result)


def _observed_upload_result(target, observed, details):
    observed_checksum = getattr(observed, 'checksum', None)
    observed_size = getattr(observed, 'size_bytes', None)
    observed_type = getattr(observed, 'checksum_type', None)
    matches = (
        observed_checksum == target.checksum
        and observed_size == target.size_bytes
        and observed_type == 'FULL_OBJECT'
    )
    if matches:
        return _AttemptResult(
            'uploaded',
            'written',
            'reconciled_written',
            'Upload was confirmed by destination checksum and size '
            'after an unacknowledged response.',
            error=details,
            evidence={
                'observed_checksum': observed_checksum,
                'observed_size_bytes': observed_size,
                'checksum_type': observed_type,
                'reconciled': True,
            },
        )
    return _AttemptResult(
        'failed',
        'conflict',
        'conflict',
        'Destination contains different or unverifiable content; batch stopped.',
        error=details,
        evidence={
            'observed_checksum': observed_checksum,
            'observed_size_bytes': observed_size,
            'checksum_type': observed_type,
            'reconciled': False,
        },
    )


def _absent_upload_failure(context, attempt, attempt_in_invocation, exc, details):
    if not details['retryable']:
        return _record_upload_attempt(
            context,
            attempt,
            _AttemptResult(
                'failed',
                'absent',
                'not_retryable',
                operation_error(exc) + ' Destination is absent; this error is not retryable.',
                error=details,
                evidence={'reconciled': False},
            ),
        )
    if attempt_in_invocation >= UPLOAD_MAX_ATTEMPTS:
        return _record_upload_attempt(
            context,
            attempt,
            _AttemptResult(
                'failed',
                'absent',
                'retries_exhausted',
                f'Transient upload attempts exhausted after '
                f'{_error_label(details)}; destination is confirmed absent.',
                error=details,
                evidence={'reconciled': False},
            ),
        )
    return None


def _wait_before_upload_retry(context, attempt, details, backoff):
    remaining = RUN_RETRY_WAIT_BUDGET_SECONDS - context.retry_wait_seconds
    if remaining <= 0:
        return context.retry_wait_seconds, _retry_wait_exhausted(context, attempt, details)
    delay = min(random.uniform(0, backoff), remaining)
    started_wait = time.monotonic()
    time.sleep(delay)
    elapsed_wait = max(delay, time.monotonic() - started_wait)
    context.retry_wait_seconds += elapsed_wait
    if context.retry_wait_seconds >= RUN_RETRY_WAIT_BUDGET_SECONDS:
        return context.retry_wait_seconds, _retry_wait_exhausted(
            context, attempt, details, actual_retry_wait_seconds=elapsed_wait
        )
    _record_upload_attempt(
        context,
        attempt,
        _AttemptResult(
            'failed',
            'absent',
            'retrying',
            'Transient upload failure; destination is confirmed absent before retry.',
            error=details,
            evidence={
                'delay_before_next_attempt': elapsed_wait,
                'actual_retry_wait_seconds': elapsed_wait,
                'reconciled': False,
            },
        ),
    )
    return context.retry_wait_seconds, None


def _retry_wait_exhausted(context, attempt, details, **evidence):
    return _record_upload_attempt(
        context,
        attempt,
        _AttemptResult(
            'failed',
            'absent',
            'retries_exhausted',
            f'Invocation retry wait budget exhausted after '
            f'{_error_label(details)}; destination is confirmed absent.',
            error=details,
            evidence={'reconciled': False, **evidence},
        ),
    )


def _record_upload_attempt(context, attempt, result):
    details = result.error or {}
    event = {
        **context.target.entry.to_dict(),
        **context.identity,
        'status': result.status,
        'classification': 'new' if result.status == 'uploaded' else 'held',
        'remote_outcome': result.remote_outcome,
        'disposition': result.disposition,
        'message': result.message,
        'error_class': details.get('error_class'),
        'error_code': details.get('error_code'),
        'http_status': details.get('http_status'),
        'delay_before_next_attempt': None,
        **result.evidence,
    }
    journal = context.run.settings.journal
    if journal:
        journal.result(context.operation_id, 'push', event, attempt=attempt)
    return (
        result.status,
        result.message,
        context.operation_id,
        result.remote_outcome == 'unknown',
    )


def _error_label(details):
    if details.get('error_code'):
        return details['error_code']
    if details.get('http_status'):
        return f'HTTP {details["http_status"]}'
    return details.get('error_class') or 'storage error'


def _existing_mismatch_message(
    entry: UploadPlanEntry, planned_checksum: str, previous_checksum: Optional[str]
) -> str:
    previous = previous_checksum or 'unavailable'
    if entry.role == 'manifest':
        return (
            f'manifest replacement required for {entry.bucket}/{entry.key}; '
            f'previous SHA-256 {previous}, planned SHA-256 {planned_checksum}.'
        )
    return (
        f'Changed or unverifiable content at existing key {entry.bucket}/{entry.key}; '
        f'previous SHA-256 {previous}, planned SHA-256 {planned_checksum}. Refusing to overwrite.'
    )


def _entry_status(
    entry: UploadPlanEntry,
    source_path: Path,
    dry_run: bool,
    generated_dir: Optional[Path],
) -> str:
    if entry.generated:
        if generated_dir is None:
            return 'would_generate' if dry_run else 'missing_generated'
        if not source_path.is_file():
            return 'missing_generated'
        return 'would_upload'
    if not entry.source_path or not source_path.is_file():
        return 'missing_source'
    return 'would_upload'


def _checksum_for_entry(path: Path) -> Optional[str]:
    if not path.is_file():
        return None

    digest = hashlib.sha256()
    with path.open('rb') as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b''):
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
        return generated_dir / entry.key.lstrip('/')
    return _resolve_source_path(entry.source_path, input_dir)


def _status_message(status: str) -> str:
    messages = {
        'would_generate': 'Generated artifact is planned but not created by this slice.',
        'skipped_generated': 'Generated artifact must be created before upload.',
        'missing_generated': 'Generated artifact file is missing.',
        'missing_source': 'Source file is missing.',
        'would_upload': 'Source file would be uploaded.',
        'uploaded': 'Source file uploaded.',
    }
    return messages.get(status, '')


def _write_rollback_file(rollback_path: Path, uploaded: list[PushResult], run_id=None) -> None:
    rollback_payload = {
        'kind': 'legacy-upload-list',
        'deletion_authorized': False,
        'run_id': run_id,
        'written': [entry.to_dict() for entry in uploaded],
        'count': len(uploaded),
    }
    rollback_path.write_text(
        json.dumps(rollback_payload, indent=2, sort_keys=True, ensure_ascii=False),
        encoding='utf-8',
    )
