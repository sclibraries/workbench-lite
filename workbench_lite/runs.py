"""Owner-private run evidence. A journal receipt is not remote reconciliation."""

import csv
import io
import hashlib
import json
import os
from dataclasses import dataclass
from pathlib import Path
import socket
import tempfile
from datetime import datetime, timezone
from uuid import uuid4

from .diagnostics import sanitize
from . import __version__
from .id_claims import claim_identities_from_plan, reduce_claim_evidence, validate_claim_event

SCHEMA_VERSION = 1
EXIT_STATES = {0: 'completed', 4: 'completed-with-errors', 130: 'interrupted', 143: 'interrupted'}
CONFIG_FIELDS = {
    'input_csv',
    'input_dir',
    'batch_id',
    's3_prefix',
    's3_public_prefix',
    's3_private_bucket',
    's3_public_bucket',
    'manifest_base_url',
    'cantaloupe_base_url',
    'cantaloupe_source_prefix',
    'manifest_key_prefix',
    'additional_files',
    'allow_missing_files',
}


@dataclass(frozen=True)
class _ReceiptEvidence:
    plan: list
    expected: set
    destinations: dict
    uploads: dict
    reused: dict
    verified_events: list
    verified_identities: dict
    receipt: dict


class JournalError(RuntimeError):
    """Evidence cannot be safely written or reused; stop the command."""


def now():
    return datetime.now(timezone.utc).isoformat()


def digest(path):
    sha = hashlib.sha256()
    with Path(path).open('rb') as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b''):
            sha.update(chunk)
    return sha.hexdigest()


def atomic_write(path, content):
    """Replace an owner-private file and synchronize its directory entry."""
    path = Path(path)
    temporary = None
    try:
        fd, temporary = tempfile.mkstemp(prefix='.' + path.name, dir=path.parent)
        with os.fdopen(fd, 'wb') as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    except OSError as exc:
        raise JournalError('Cannot persist run evidence; command aborted.') from exc
    finally:
        if temporary and os.path.exists(temporary):
            os.unlink(temporary)


def json_bytes(value):
    return (json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + '\n').encode('utf-8')


def evidence_fields(value):
    """Keep canonical identities or explicitly named redacted displays and hashes.

    Consumers must never treat a display value as an exact storage key.
    """
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, list):
        return [evidence_fields(item) for item in value]
    if not isinstance(value, dict):
        return sanitize(value)
    result = {}
    for key, item in value.items():
        safe = sanitize({key: item})[key]
        if isinstance(item, str) and safe != item and key != 'message':
            result[key + '_display'] = safe
            result[key + '_sha256'] = hashlib.sha256(item.encode('utf-8')).hexdigest()
        else:
            result[key] = evidence_fields(item) if isinstance(item, (dict, list)) else safe
    return result


def csv_snapshot(path):
    with Path(path).open(encoding='utf-8-sig', newline='') as handle:
        rows = list(csv.reader(handle))
    output = io.StringIO(newline='')
    writer = csv.writer(output)
    headers = rows[0] if rows else []
    if rows:
        writer.writerow(headers)
    for row in rows[1:]:
        cells = []
        for index, cell in enumerate(row):
            field = headers[index] if index < len(headers) else 'extra'
            cells.append(sanitize({field: cell})[field])
        writer.writerow(cells)
    return output.getvalue().encode('utf-8')


def read_events(path):
    events = []
    error = None
    try:
        with (Path(path) / 'events.jsonl').open('rb') as handle:
            for line in handle:
                try:
                    event = json.loads(line)
                    if not line.endswith(b'\n') or event['sequence'] != len(events) + 1:
                        raise ValueError('Incomplete or out-of-order event')
                    if event['kind'] not in {
                        'stage_started',
                        'stage_finished',
                        'intent',
                        'result',
                        'context',
                    }:
                        raise ValueError('Unknown event kind')
                    required = {
                        'stage_started': ('stage', 'pid', 'hostname'),
                        'stage_finished': ('stage', 'state', 'exit_code'),
                        'intent': ('stage', 'operation_id'),
                        'result': ('stage', 'operation_id'),
                        'context': ('stage',),
                    }[event['kind']]
                    if any(key not in event for key in required):
                        raise ValueError('Missing event fields')
                    if event['kind'] == 'stage_started' and (
                        not isinstance(event['pid'], int) or event['pid'] <= 0
                    ):
                        raise ValueError('Invalid process identity')
                    validate_claim_event(event, events)
                    events.append(event)
                except (ValueError, KeyError, TypeError, UnicodeDecodeError):
                    error = 'Malformed or truncated journal; valid prefix retained.'
                    break
    except OSError:
        error = 'Journal is unavailable.'
    return events, error


def _held_publication(reason='No durable complete-batch receipt; do not hand off to publication.'):
    return {'state': 'held', 'reason': reason}


def publication_status(path, events, state, pending, error):
    rollback_hold = (
        'Rollback has started; this source run remains held permanently. Republish with a new run.'
    )
    rollback_marker = _marker_state(path, 'rollback-hold.json')
    if rollback_marker is None:
        return _held_publication()
    if rollback_marker:
        return _held_publication(rollback_hold)
    if not _publication_prerequisites(events, state, pending, error):
        return _held_publication()
    pending_marker = _marker_state(path, 'publication-pending')
    if pending_marker is not False:
        return _held_publication()
    receipt = events[-1].get('publication_receipt')
    if not _valid_receipt_event(events[-1], receipt):
        return _held_publication()
    if not _valid_publication_receipt(path, events, receipt):
        return _held_publication()
    return {
        'state': 'ready_for_review',
        'receipt': receipt,
        'reason': 'Verified complete batch; separate authorized ASpace review still required.',
    }


def _marker_state(path, marker):
    try:
        return (Path(path) / marker).exists()
    except OSError:
        return None


def _publication_prerequisites(events, state, pending, error):
    return not error and not pending and state == 'completed' and bool(events)


def _valid_receipt_event(event, receipt):
    return (
        receipt
        and event.get('stage') == 'push'
        and event['kind'] == 'stage_finished'
        and event.get('exit_code') == 0
    )


def _valid_publication_receipt(path, events, receipt):
    if not isinstance(receipt, dict) or receipt.get('verification_contract') != 's3-sha256-v1':
        return False
    try:
        plan_path = Path(path) / 'upload-plan.json'
        if digest(plan_path) != receipt['plan_sha256']:
            return False
        if not _verified_claims_match_plan(path, receipt):
            return False
        plan = json.loads(plan_path.read_text())['entries']
        results = push_stage_results(events)
        expected = {(entry['bucket'], entry['key']) for entry in plan}
        destinations = _destination_statuses(results, expected)
        if destinations is None:
            return False
        uploads = _uploaded_identities(results)
        reused = _verified_reused_identities(results)
        if reused is None:
            return False
        verified = _verified_upload_identities(results)
        if verified is None:
            return False
        verified_events, verified_identities = verified
        return _receipt_matches_plan(
            _ReceiptEvidence(
                plan,
                expected,
                destinations,
                uploads,
                reused,
                verified_events,
                verified_identities,
                receipt,
            )
        )
    except (OSError, ValueError, KeyError, TypeError):
        return False


def _verified_claims_match_plan(path, receipt):
    expected = _expected_claims_from_run(path)
    claims = receipt.get('verified_claims')
    if expected is None or not isinstance(claims, list) or len(claims) != len(expected):
        return False
    observed = set()
    required = {'key', 'checksum', 'size_bytes', 'checksum_type'}
    for claim in claims:
        if not isinstance(claim, dict) or set(claim) != required:
            return False
        key, checksum, size = claim['key'], claim['checksum'], claim['size_bytes']
        if (
            not isinstance(key, str)
            or not isinstance(checksum, str)
            or not isinstance(size, int)
            or isinstance(size, bool)
            or claim['checksum_type'] != 'FULL_OBJECT'
        ):
            return False
        observed.add((key, checksum, size))
    expected_rows = {(key, checksum, size) for _, key, checksum, size in expected}
    return len(observed) == len(claims) and observed == expected_rows


def push_stage_results(events):
    start = max(
        index
        for index, event in enumerate(events)
        if event['kind'] == 'stage_started' and event['stage'] == 'push'
    )
    return [event for event in events[start:] if event['kind'] == 'result']


def _destination_statuses(results, expected):
    checks = [event for event in results if event['stage'] == 'destination-check']
    destinations = {(event['bucket'], event['key']): event.get('status') for event in checks}
    if (
        len(destinations) != len(checks)
        or set(destinations) != expected
        or any(status not in {'absent', 'unchanged'} for status in destinations.values())
    ):
        return None
    return destinations


def _uploaded_identities(results):
    return {
        (event['bucket'], event['key']): (event['checksum'], event['size_bytes'])
        for event in results
        if event['stage'] == 'push' and event.get('status') == 'uploaded'
    }


def _verified_reused_identities(results):
    reused = {}
    for event in results:
        if event['stage'] != 'destination-check' or event.get('status') != 'unchanged':
            continue
        if (
            event.get('verification_method') not in {'s3-sha256', 's3-sha256+readback'}
            or event.get('checksum_type') != 'FULL_OBJECT'
            or event.get('observed_checksum') != event.get('checksum')
            or event.get('observed_size_bytes') != event.get('size_bytes')
        ):
            return None
        reused[(event['bucket'], event['key'])] = (event['checksum'], event['size_bytes'])
    return reused


def _verified_upload_identities(results):
    verified_events = [
        event
        for event in results
        if event['stage'] == 'verify-upload' and event.get('status') == 'verified'
    ]
    for event in verified_events:
        if (
            event.get('verification_method') not in {'s3-sha256', 's3-sha256+readback'}
            # Before checksum_type was journaled, verified uploads
            # already required a FULL_OBJECT S3 checksum.
            or event.get('checksum_type', 'FULL_OBJECT') != 'FULL_OBJECT'
            or event.get('observed_checksum') != event['checksum']
            or event.get('observed_size_bytes') != event['size_bytes']
        ):
            return None
    verified = {
        (event['bucket'], event['key']): (event['checksum'], event['size_bytes'])
        for event in verified_events
    }
    return verified_events, verified


def _receipt_matches_plan(evidence):
    absent = {identity for identity, status in evidence.destinations.items() if status == 'absent'}
    previously_published = {
        identity for identity, status in evidence.destinations.items() if status == 'unchanged'
    }
    manifest_count = sum(entry['role'] == 'manifest' for entry in evidence.plan)
    return not (
        not evidence.expected
        or set(evidence.uploads) != absent
        or set(evidence.reused) != previously_published
        or set(evidence.uploads).intersection(evidence.reused)
        or set(evidence.uploads) | set(evidence.reused) != evidence.expected
        or evidence.verified_identities != evidence.uploads
        or len(evidence.verified_events) != len(evidence.uploads)
        or evidence.receipt['verified_count'] != len(evidence.plan)
        or manifest_count < 1
        or evidence.receipt['manifest_count'] != manifest_count
    )


def _expected_claims_from_run(path):
    try:
        path = Path(path)
        metadata = json.loads((path / 'run.json').read_text(encoding='utf-8'))
        snapshot_hashes = metadata['snapshot_hashes']
        if not isinstance(snapshot_hashes, dict) or not isinstance(metadata['batch_id'], str):
            return None
        plan_path = path / 'upload-plan.json'
        config_path = path / 'config.json'
        if digest(plan_path) != metadata['plan_sha256']:
            return None
        if digest(config_path) != snapshot_hashes['config']:
            return None
        plan = json.loads(plan_path.read_text(encoding='utf-8'))
        entries = plan['entries']
        if not isinstance(entries, list):
            return None
        config = json.loads(config_path.read_text(encoding='utf-8'))
        config['batch_id'] = metadata['batch_id']
        return claim_identities_from_plan(entries, config, metadata['batch_id'])
    except (OSError, ValueError, KeyError, TypeError):
        return None


def inspect_run(path):
    events, error = read_events(path)
    state, code, active, pending, counts = _summarize_events(events)
    retry_count = _retry_count(events)
    push_artifacts = _latest_push_artifacts(events)
    classification_counts = _classification_counts(push_artifacts)
    incomplete = active is not None or not events
    if active and not _active_run_alive(active):
        state = 'interrupted'
    if error:
        state, code = 'failed', 1
    return {
        'schema_version': SCHEMA_VERSION,
        'run_id': Path(path).name,
        'state': state,
        'exit_code': code,
        'incomplete_stage': incomplete,
        'inferred_interruption': incomplete and state == 'interrupted',
        'journal_error': error,
        'event_count': len(events),
        'counts': counts,
        'claim_counts': reduce_claim_evidence(events).counts,
        'retry_count': retry_count,
        'classification_counts': classification_counts,
        'push_artifacts': push_artifacts,
        'publication': publication_status(path, events, state, pending, error),
        'unknown_operations': [{**event, 'outcome': 'unknown'} for event in pending.values()],
    }


def _summarize_events(events):
    pending = {}
    state, code, active = 'interrupted', None, None
    counts = {}
    for event in events:
        kind = event['kind']
        if kind == 'stage_started':
            state, code, active = 'running', None, event
        elif kind == 'stage_finished':
            state, code, active = event['state'], event['exit_code'], None
        elif kind == 'intent':
            pending[event['operation_id']] = event
        elif kind == 'result':
            if event.get('remote_outcome') == 'unknown':
                pending[event['operation_id']] = event
            else:
                pending.pop(event.get('operation_id'), None)
            status = event.get('status', 'unknown')
            counts[status] = counts.get(status, 0) + 1
    return state, code, active, pending, counts


def _retry_count(events):
    return sum(
        1
        for event in events
        if event['kind'] == 'intent'
        and event.get('stage') == 'push'
        and event.get('attempt', 1) > 1
    )


def _latest_push_artifacts(events):
    starts = [
        index
        for index, event in enumerate(events)
        if event['kind'] == 'stage_started' and event['stage'] in {'push', 'push-dry-run'}
    ]
    if not starts:
        return []
    return _push_artifacts_after(events, starts[-1])


def _push_artifacts_after(events, start):
    push_artifacts = []
    for event in events[start:]:
        if (
            event['kind'] != 'result'
            or event.get('stage') != 'push'
            or not event.get('classification')
        ):
            continue
        push_artifacts.append(
            {
                key: event[key]
                for key in (
                    'bucket',
                    'key',
                    'role',
                    'object_id',
                    'page_id',
                    'status',
                    'classification',
                    'checksum',
                    'previous_checksum',
                    'previous_checksum_type',
                    'observed_checksum',
                    'size_bytes',
                    'observed_size_bytes',
                    'verification_method',
                    'message',
                )
                if key in event
            }
        )
    return push_artifacts


def _classification_counts(push_artifacts):
    classification_counts = {}
    for artifact in push_artifacts:
        classification = artifact['classification']
        classification_counts[classification] = classification_counts.get(classification, 0) + 1
    return classification_counts


def _active_run_alive(active):
    if active.get('hostname') != socket.gethostname():
        return False
    try:
        os.kill(active['pid'], 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


class RunJournal:
    def __init__(self, path, sequence=0):
        self.path = Path(path).resolve()
        self.run_id = self.path.name
        self.sequence = sequence
        self.stage = None

    @classmethod
    def create(cls, root):
        try:
            root = Path(root).resolve()
            root.mkdir(mode=0o700, parents=True, exist_ok=True)
            path = root / (datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ-') + uuid4().hex)
            path.mkdir(mode=0o700)
            with os.fdopen(
                os.open(path / 'events.jsonl', os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), 'wb'
            ) as f:
                f.flush()
                os.fsync(f.fileno())
            run = cls(path)
            package = Path(__file__).parent
            code_hash = hashlib.sha256(
                b''.join(p.read_bytes() for p in sorted(package.glob('*.py')))
            ).hexdigest()
            atomic_write(
                path / 'run.json',
                json_bytes(
                    {
                        'schema_version': SCHEMA_VERSION,
                        'run_id': run.run_id,
                        'created_at': now(),
                        'software': {
                            'name': 'workbench-lite',
                            'version': __version__,
                            'source_sha256': code_hash,
                        },
                    }
                ),
            )
            return run
        except OSError as exc:
            raise JournalError('Cannot create private run directory.') from exc

    @classmethod
    def open(cls, path):
        events, error = read_events(path)
        if error:
            raise JournalError(error)
        return cls(path, len(events))

    def append(self, kind, **fields):
        event = {
            'sequence': self.sequence + 1,
            'timestamp': now(),
            'kind': kind,
            'run_id': self.run_id,
            **evidence_fields(fields),
        }
        # Callers supply canonical identities and safe error classifications only.
        # Never apply free-text replacement to keys or descriptive metadata.
        offset = None
        try:
            with (self.path / 'events.jsonl').open('ab') as handle:
                offset = handle.tell()
                handle.write((json.dumps(event, ensure_ascii=False) + '\n').encode('utf-8'))
                handle.flush()
                os.fsync(handle.fileno())
        except OSError as exc:
            # A flushed but unsynced complete line must not certify publication.
            # Remove this uncertain append before any caller can inspect it.
            if offset is not None:
                try:
                    with (self.path / 'events.jsonl').open('r+b') as handle:
                        handle.truncate(offset)
                        handle.flush()
                        os.fsync(handle.fileno())
                except OSError:
                    pass  # Storage failure remains fatal; no more remote writes.
            raise JournalError('Cannot persist journal event; command aborted.') from exc
        self.sequence += 1
        return event

    def start(self, stage, options=None):
        self.stage = stage
        self.append(
            'stage_started',
            stage=stage,
            pid=os.getpid(),
            hostname=socket.gethostname(),
            options=options or {},
        )
        self.summarize()

    def intent(self, stage, identity, *, operation_id=None, attempt=1):
        operation_id = operation_id or uuid4().hex
        self.append('intent', stage=stage, operation_id=operation_id, attempt=attempt, **identity)
        return operation_id

    def result(self, operation_id, stage, result, *, attempt=1):
        self.append('result', stage=stage, operation_id=operation_id, attempt=attempt, **result)

    def finish(self, code, error=None):
        if self.stage == 'push' and code == 0:
            # Keep the execute stage running until summary writes and lock release
            # succeed. Death here is inferred as interruption, never completion.
            self.summarize()
            return
        state = EXIT_STATES.get(code, 'failed')
        self.append('stage_finished', stage=self.stage, exit_code=code, state=state, error=error)
        self.summarize()

    def complete_publication(self):
        """Commit the receipt while the execute writer locks are still held."""
        if self.stage != 'push':
            return
        events, error = read_events(self.path)
        verified = next(
            (e.get('verified_batch') for e in reversed(events) if e.get('verified_batch')), None
        )
        fields = {}
        if (
            not error
            and verified
            and verified['manifest_count'] >= 1
            and not inspect_run(self.path)['unknown_operations']
            and _valid_publication_receipt(self.path, events, verified)
        ):
            fields['publication_receipt'] = verified
        pending = self.path / 'publication-pending'
        atomic_write(pending, json_bytes({'run_id': self.run_id}))
        self.append(
            'stage_finished', stage='push', exit_code=0, state='completed', error=None, **fields
        )
        try:
            pending.unlink()
        except OSError as exc:
            raise JournalError(
                'Publication finalization is uncertain; batch remains held.'
            ) from exc
        # No required work follows unlink. Its directory entry is deliberately
        # not fsynced: a crash may conservatively restore the hold marker, never
        # authorize an unsynced receipt. Do not auto-remove a surviving marker.

    def summarize(self):
        summary = inspect_run(self.path)
        atomic_write(self.path / 'summary.json', json_bytes(summary))
        artifacts = '\n'.join(
            f'  {item["classification"]} ({item["status"]}): {item["bucket"]}/{item["key"]}'
            for item in summary['push_artifacts']
        )
        if not artifacts:
            artifacts = '  none'
        text = (
            f'Run: {self.run_id}\nState: {summary["state"]}\n'
            f'Exit code: {summary["exit_code"]}\nEvents: {summary["event_count"]}\n'
            f'Retries: {summary["retry_count"]}\n'
            f'Unknown operations: {len(summary["unknown_operations"])}\n'
            f'Counts: {json.dumps(summary["counts"], sort_keys=True)}\n'
            f'Classifications: {json.dumps(summary["classification_counts"], sort_keys=True)}\n'
            f'Artifacts:\n{artifacts}\n'
            'Publication: inspect-run is authoritative; '
            'this cached summary cannot authorize handoff.\n'
        )
        atomic_write(self.path / 'summary.txt', text.encode('utf-8'))

    def inputs(self, config_path, config, csv_path):
        fingerprints = {'config_sha256': digest(config_path), 'csv_sha256': digest(csv_path)}
        metadata = json.loads((self.path / 'run.json').read_text())
        if 'inputs' in metadata:
            if metadata['inputs'] != fingerprints:
                raise JournalError('Inputs changed since generation; create a new generation run.')
            return
        # Keep descriptive fields; strip credential columns and URL credentials.
        atomic_write(self.path / 'input.csv', csv_snapshot(csv_path))
        atomic_write(
            self.path / 'config.json',
            json_bytes(
                sanitize(
                    {
                        key: value
                        for key, value in config.items()
                        if key in CONFIG_FIELDS
                        and (
                            key not in {'cantaloupe_source_prefix', 'manifest_key_prefix'} or value
                        )
                    }
                )
            ),
        )
        metadata.update(
            inputs=fingerprints,
            batch_id=str(config.get('batch_id') or Path(config_path).stem),
            snapshots={'csv': 'input.csv', 'config': 'config.json'},
            snapshot_hashes={
                'csv': digest(self.path / 'input.csv'),
                'config': digest(self.path / 'config.json'),
            },
        )
        atomic_write(self.path / 'run.json', json_bytes(metadata))

    def plan(self, entries):
        plan = [
            {k: v for k, v in entry.to_dict().items() if k != 'source_exists'} for entry in entries
        ]
        payload = {
            'schema_version': SCHEMA_VERSION,
            'run_id': self.run_id,
            'kind': 'immutable-pre-upload-plan',
            'entries': evidence_fields(plan),
        }
        path = self.path / 'upload-plan.json'
        data = json_bytes(payload)
        if path.exists():
            if path.read_bytes() != data:
                raise JournalError('Upload plan changed; create a new generation run.')
        else:
            atomic_write(path, data)
            metadata = json.loads((self.path / 'run.json').read_text())
            metadata['destinations'] = evidence_fields(
                [{'bucket': e.bucket, 'key': e.key, 'public': e.public} for e in entries]
            )
            metadata['plan_sha256'] = hashlib.sha256(data).hexdigest()
            atomic_write(self.path / 'run.json', json_bytes(metadata))
        return data

    def write_audit(self, entries, output_dir):
        data = self.plan(entries)
        entry = next(e for e in entries if e.role == 'audit')
        path = Path(output_dir) / entry.key
        op = self.intent('generate', entry.to_dict())
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        if path.exists() and path.read_bytes() != data:
            raise JournalError('Audit snapshot conflicts with immutable run plan.')
        atomic_write(path, data)
        self.result(
            op,
            'generate',
            {
                **entry.to_dict(),
                'status': 'generated',
                'checksum': digest(path),
                'size_bytes': path.stat().st_size,
            },
        )

    def attach(self, output_dir):
        path = Path(output_dir) / '.workbench-run.json'
        atomic_write(path, json_bytes({'run_id': self.run_id, 'run_path': str(self.path)}))


class RecordedResults(list):
    """Persist each local-file outcome as it happens, including early failures."""

    def __init__(self, journal, stage):
        super().__init__()
        self.journal, self.stage = journal, stage
        self.identity, self.operation_id = {}, None

    def begin(self, identity):
        self.identity = identity
        if self.journal:
            self.operation_id = self.journal.intent(self.stage, identity)

    def append(self, result):
        if self.journal:
            payload = result.to_dict()
            if 'ok' in payload:
                payload['status'] = 'verified' if payload['ok'] else 'failed'
            self.journal.result(self.operation_id, self.stage, {**self.identity, **payload})
        super().append(result)
