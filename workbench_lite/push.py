import hashlib
import json
import os
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Protocol

from .models import UploadPlanEntry
from .diagnostics import operation_error
from .s3_client import VerificationResult

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
    def has_errors(self) -> bool:
        return bool(self.errors or self.failed_count or self.missing_source_count or self.missing_generated_count)

    def to_dict(self) -> Dict[str, object]:
        return {
            "publication": {"state": "held", "reason": "Use inspect-run after successful finalization; this report is not a publication receipt."},
            "errors": self.errors,
            "failed_count": self.failed_count,
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
    # Stable partition across the batch, not within individual objects.
    entries = sorted(upload_plan, key=lambda entry: entry.role == "manifest")
    resolved_endpoint_url = endpoint_url or os.environ.get("AWS_ENDPOINT_URL", "")
    resolved_region = region or os.environ.get("AWS_REGION", "us-east-1")
    resolved_profile = profile or os.environ.get("AWS_PROFILE", "")
    role_counts = dict(sorted(Counter(entry.role for entry in entries).items()))

    # Catch all knowable missing-file failures before the first remote write.
    # Operation-time checks below still handle files lost after this scan.
    readiness_failures = {}
    errors = []
    verified = []
    if not dry_run:
        for index, entry in enumerate(entries):
            source_path = _resolve_upload_path(entry, input_dir, generated_dir)
            try:
                status = _entry_status(entry, source_path, False, generated_dir)
                if status in {"missing_source", "missing_generated"}:
                    readiness_failures[index] = (status, _status_message(status))
            except OSError as exc:
                readiness_failures[index] = ("failed", operation_error(exc))

    if not dry_run and not readiness_failures:
        for entry in entries:
            source = _resolve_upload_path(entry, input_dir, generated_dir)
            operation = None
            try:
                if source.stat().st_size > MAX_SINGLE_PUT_BYTES:
                    errors.append('Artifact exceeds the 5 GiB conditional single-PUT limit; batch held.')
                    break
                if journal:
                    operation = journal.intent('destination-check', entry.to_dict())
                exists = (preflight_client or s3_client).object_exists(entry.bucket, entry.key)
                if journal:
                    journal.result(operation, 'destination-check', {**entry.to_dict(), 'status': 'conflict' if exists else 'absent'})
                if exists:
                    errors.append(f'Existing destination blocks the entire batch: {entry.bucket}/{entry.key}')
                    break
            except STORAGE_ERRORS as exc:
                errors.append('Destination readiness failed: ' + operation_error(exc))
                if journal:
                    journal.result(operation, 'destination-check', {**entry.to_dict(), 'status': 'failed', 'message': operation_error(exc)})
                break

    results: List[PushResult] = []
    stopped = bool(errors)
    for index, entry in enumerate(entries):
        source_path = _resolve_upload_path(entry, input_dir, generated_dir)
        checksum = None
        size_bytes = None
        operation_id = None
        remote_unknown = False
        if readiness_failures:
            status, message = readiness_failures.get(index, (
                "unattempted", "Not attempted because batch upload readiness failed."
            ))
        elif stopped:
            status = "unattempted"
            message = "Not attempted after a required operation failed."
        else:
            upload_started = False
            try:
                status = _entry_status(entry, source_path, dry_run, generated_dir)
                if status == "would_upload":
                    checksum = _checksum_for_entry(source_path)
                    size_bytes = source_path.stat().st_size
                    if not dry_run:
                        if journal:
                            operation_id = journal.intent("push", {**entry.to_dict(), "checksum": checksum, "size_bytes": size_bytes})
                        if size_bytes > MAX_SINGLE_PUT_BYTES:
                            raise OSError("Artifact exceeds conditional single-PUT limit.")
                        upload_started = True
                        s3_client.upload_file(source_path, entry.bucket, entry.key, checksum)
                        status = "uploaded"
                message = _status_message(status)
            except STORAGE_ERRORS as exc:
                status = "failed"
                message = operation_error(exc)
                if upload_started:
                    remote_unknown = True
                    message += " Remote outcome unknown; inspect storage before retry or rollback."
            if not dry_run and status in {"missing_source", "missing_generated", "failed"}:
                stopped = True
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
        ))

        if journal:
            journal.result(operation_id, "push", {**entry.to_dict(), **results[-1].to_dict(),
                                                  "remote_outcome": "unknown" if remote_unknown else status})

        if not dry_run and status == 'uploaded':
            identity = {**entry.to_dict(), 'checksum': checksum, 'size_bytes': size_bytes}
            verification = journal.intent('verify-upload', identity) if journal else None
            try:
                matches = s3_client.verify_file(entry.bucket, entry.key, checksum, size_bytes)
                if not isinstance(matches, VerificationResult):
                    matches = VerificationResult(False, None, None, 'missing-evidence')
                verification_message = '' if matches else 'Remote bytes do not match the uploaded artifact.'
            except STORAGE_ERRORS as exc:
                matches = False
                verification_message = operation_error(exc)
            if journal:
                observed = matches.evidence() if hasattr(matches, 'evidence') else {}
                journal.result(verification, 'verify-upload', {**identity, **observed, 'status': 'verified' if matches else 'failed', 'message': verification_message})
            if matches:
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
