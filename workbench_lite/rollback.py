"""Guarded rollback from confirmed new-key inventory entries."""

from datetime import datetime
import hashlib
import json
from pathlib import Path
import re
from types import SimpleNamespace

from botocore.exceptions import BotoCoreError, ClientError

from .config import ConfigError, load_config
from .diagnostics import operation_error
from .locks import lock_destination
from .rollback_inventory import build_inventory
from .runs import JournalError, RunJournal, atomic_write, json_bytes, read_events
from .storage_plan import DEFAULT_PRIVATE_BUCKET, DEFAULT_PUBLIC_BUCKET


ROLLBACK_HOLD = 'rollback-hold.json'
ROLLBACK_SOURCE = 'rollback-source.json'
ROLE_ORDER = {
    'manifest': 0,
    'service_jpg': 1,
    'thumbnail': 1,
    'hocr': 1,
    'pdf': 1,
    'master_tiff': 2,
    'audit': 3,
}
SHA256_RE = re.compile(r'^[0-9a-f]{64}$')


def _read_json(path):
    try:
        return json.loads(Path(path).read_text(encoding='utf-8'))
    except (OSError, ValueError) as exc:
        raise JournalError('Cannot read rollback source evidence.') from exc


def _field(identity, name):
    if isinstance(identity, dict):
        return identity.get(name)
    return getattr(identity, name, None)


def _endpoint(client):
    value = getattr(client, 'endpoint_url', None)
    if value:
        return value
    sdk_client = getattr(client, 'client', None)
    return getattr(getattr(sdk_client, 'meta', None), 'endpoint_url', None)


def _region(client):
    value = getattr(client, 'region_name', None)
    if value:
        return value
    sdk_client = getattr(client, 'client', None)
    return getattr(getattr(sdk_client, 'meta', None), 'region_name', None)


def _endpoint_key(value):
    return value.rstrip('/').lower() if isinstance(value, str) else None


def _config_buckets(config):
    return {
        str(config.get('s3_private_bucket') or DEFAULT_PRIVATE_BUCKET),
        str(config.get('s3_public_bucket') or DEFAULT_PUBLIC_BUCKET),
    }


def _source_context(events):
    push_starts = [
        event for event in events if event['kind'] == 'stage_started' and event['stage'] == 'push'
    ]
    if not push_starts:
        raise JournalError('Source run has no execute push stage.')
    latest_start = push_starts[-1]
    start_time = _parse_time(latest_start.get('timestamp'))
    event_index = events.index(latest_start)
    stage_events = events[event_index:]
    storage = next(
        (
            event.get('storage')
            for event in reversed(stage_events)
            if event['kind'] == 'context'
            and event.get('stage') == 'push'
            and isinstance(event.get('storage'), dict)
        ),
        None,
    )
    if not storage:
        raise JournalError('Source run has no recorded S3 endpoint configuration.')
    options = latest_start.get('options', {})
    endpoint = (
        storage.get('effective_endpoint_url')
        or storage.get('configured_endpoint_url')
        or (options.get('endpoint_url') if isinstance(options, dict) else None)
    )
    region = (
        storage.get('effective_region')
        or storage.get('configured_region')
        or (options.get('region') if isinstance(options, dict) else None)
    )
    if not endpoint:
        raise JournalError('Source run has no exact S3 endpoint to compare.')
    return {'endpoint': endpoint, 'region': region, 'timestamp': start_time}


def _parse_time(value):
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError:
        return None


def _validate_source(run_path, client, config_path=None):  # noqa: C901, PLR0915
    source_path = Path(run_path).resolve()
    inventory = build_inventory(source_path)
    if inventory.get('run_id') != source_path.name:
        raise JournalError('Rollback run ID does not match its source directory.')
    if inventory.get('mode') != 'execute' or inventory.get('state') != 'reviewable':
        reason = ', '.join(inventory.get('evidence_errors') or []) or inventory.get('state')
        raise JournalError(
            f'Rollback requires a complete reviewable execute inventory; found {reason}.'
        )
    if inventory.get('unknown_keys') or inventory.get('pending_operations'):
        raise JournalError('Rollback refused: source run has unknown or pending operations.')
    if inventory.get('replacements'):
        raise JournalError('Rollback refused: replacement inventory is not supported.')

    config_snapshot = (
        _read_json(source_path / 'config.json') if (source_path / 'config.json').exists() else {}
    )
    if not isinstance(config_snapshot, dict):
        raise JournalError('Rollback refused: source configuration snapshot is invalid.')
    recorded_buckets = _config_buckets(config_snapshot)
    if config_path is None:
        raise JournalError('Rollback requires --config with the current bucket configuration.')
    try:
        current_config = load_config(config_path)
    except (ConfigError, UnicodeDecodeError) as exc:
        raise JournalError('Rollback refused: current configuration is invalid.') from exc
    if _config_buckets(current_config) != recorded_buckets:
        raise JournalError(
            'Rollback refused: current bucket configuration differs from the source run.'
        )

    planned = inventory.get('planned') or []
    for item in planned:
        if (
            not isinstance(item.get('bucket'), str)
            or not item.get('bucket')
            or not isinstance(item.get('key'), str)
            or not item.get('key')
            or 'bucket_display' in item
            or 'key_display' in item
        ):
            raise JournalError(
                'Rollback refused: upload plan contains a redacted or invalid destination.'
            )
        if item['bucket'] not in recorded_buckets:
            raise JournalError(
                'Rollback refused: upload plan bucket differs from its configuration snapshot.'
            )

    new_keys = inventory.get('new_keys')
    if not isinstance(new_keys, list):
        raise JournalError('Rollback refused: new-key inventory is invalid.')
    for item in new_keys:
        checksum = item.get('checksum')
        size = item.get('size_bytes')
        if (
            not isinstance(checksum, str)
            or not SHA256_RE.fullmatch(checksum)
            or not isinstance(size, int)
            or isinstance(size, bool)
            or size < 0
        ):
            raise JournalError(
                'Rollback refused: a new key lacks exact uploaded SHA-256 and size evidence.'
            )

    events, error = read_events(source_path)
    if error:
        raise JournalError('Rollback refused: source journal is damaged.')
    source_config = _source_context(events)
    current_endpoint = _endpoint(client)
    if not current_endpoint or _endpoint_key(current_endpoint) != _endpoint_key(
        source_config['endpoint']
    ):
        raise JournalError('Rollback refused: current S3 endpoint differs from the source run.')
    current_region = _region(client)
    if source_config['region'] and current_region and source_config['region'] != current_region:
        raise JournalError('Rollback refused: current S3 region differs from the source run.')
    return source_path, inventory, events, source_config


def _later_reuse(source_path, source_events, source_config, candidate_keys):  # noqa: C901
    source_time = source_config['timestamp']
    if source_time is None:
        raise JournalError('Rollback refused: source execute timestamp is invalid.')
    used_by = {}
    for path in sorted(source_path.parent.iterdir()):
        if not path.is_dir() or path.resolve() == source_path or (path / ROLLBACK_SOURCE).exists():
            continue
        if not (path / 'events.jsonl').exists() or not (path / 'run.json').exists():
            continue
        events, error = read_events(path)
        push_starts = [
            event
            for event in events
            if event['kind'] == 'stage_started' and event['stage'] == 'push'
        ]
        if not push_starts:
            if error and (path / 'upload-plan.json').exists():
                raise JournalError(
                    f'Rollback refused: cannot inspect possible later run {path.name}.'
                )
            continue
        if error:
            raise JournalError(f'Rollback refused: later run {path.name} has a damaged journal.')
        later_time = _parse_time(push_starts[-1].get('timestamp'))
        if later_time is None or later_time <= source_time:
            continue
        inventory = build_inventory(path)
        for item in inventory.get('unchanged_keys') or []:
            identity = (item.get('bucket'), item.get('key'))
            if identity in candidate_keys:
                used_by.setdefault(identity, set()).add(path.name)
    return {key: sorted(values) for key, values in used_by.items()}


def _exclusions(inventory):
    new_ids = {(item.get('bucket'), item.get('key')) for item in inventory.get('new_keys', [])}
    categories = [
        ('unchanged_keys', 'reused by the source run; not a new write'),
        ('held_keys', 'held by publication policy; not written by the source run'),
        ('conflicts', 'preexisting or conflicting object; ownership is not confirmed'),
        ('unknown_keys', 'remote outcome is unknown; reconciliation is required'),
    ]
    reasons = {}
    rows = []
    for field, reason in categories:
        for item in inventory.get(field) or []:
            identity = (item.get('bucket'), item.get('key'))
            reasons[identity] = reason
            rows.append({**item, 'status': 'excluded', 'reason': reason})
    for item in inventory.get('planned') or []:
        identity = (item.get('bucket'), item.get('key'))
        if identity in new_ids or identity in reasons:
            continue
        reason = 'not confirmed in new_keys by the rollback inventory'
        rows.append({**item, 'status': 'excluded', 'reason': reason})
    return rows


def _sort_key(item):
    return (ROLE_ORDER.get(item.get('role'), 1), item.get('bucket', ''), item.get('key', ''))


def _report_base(source_path, inventory, mode, rollback_run_id=None):
    new_keys = sorted(inventory.get('new_keys') or [], key=_sort_key)
    return {
        'schema_version': 1,
        'kind': 'rollback-report',
        'mode': mode,
        'state': 'running' if mode == 'execute' else 'preview',
        'source_run_id': inventory['run_id'],
        'source_run_path': str(source_path),
        'rollback_run_id': rollback_run_id,
        'planned_delete_count': len(new_keys),
        'deleted_count': 0,
        'already_deleted_count': 0,
        'conflict_count': 0,
        'failed_count': 0,
        'results': [],
        'exclusions': _exclusions(inventory),
        'notes': [
            'Delete recoverability depends on bucket versioning, which has not been confirmed '
            'for this bucket.',
            'Runs journaled on other machines are invisible to the local dependency scan.',
            'No ArchivesSpace changes were made. Restore any File Version change from the '
            'reviewed update CSV.',
            'If-Match is sent as an extra guard. Confirm the staging bucket rejects a stale '
            'ETag with HTTP 412 before the first real rollback.',
        ],
    }


def _finalize_report(report, exit_code):
    results = report['results']
    report['deleted_count'] = sum(result['status'] == 'deleted' for result in results)
    report['already_deleted_count'] = sum(
        result['status'] == 'already_deleted' for result in results
    )
    report['conflict_count'] = sum(result['status'] == 'conflict' for result in results)
    report['failed_count'] = sum(
        result['status'] in {'failed', 'not_attempted'} for result in results
    )
    report['exit_code'] = exit_code
    report['state'] = 'completed' if exit_code == 0 else 'completed_with_issues'
    return report


def _current_identity(client, bucket, key):
    return client.inspect_object(bucket, key)


def _matches(item, identity):
    return (
        identity is not None
        and _field(identity, 'checksum') == item.get('checksum')
        and _field(identity, 'size_bytes') == item.get('size_bytes')
        and _field(identity, 'checksum_type') == 'FULL_OBJECT'
    )


def _pending_rollback_intents(path):
    events, error = read_events(path)
    if error:
        raise JournalError('Rollback journal is damaged; do not resume it.')
    intents = {}
    completed = set()
    for event in events:
        if event['kind'] == 'intent' and event.get('stage') == 'rollback-delete':
            intents[event['operation_id']] = event
        elif (
            event['kind'] == 'result'
            and event.get('stage') == 'rollback-delete'
            and event.get('remote_outcome') != 'unknown'
        ):
            completed.add(event['operation_id'])
    pending = {}
    for operation_id, event in intents.items():
        if operation_id in completed:
            continue
        pending[(event.get('bucket'), event.get('key'))] = operation_id
    return events, pending


def _find_or_create_journal(source_path, inventory):
    inventory_hash = hashlib.sha256(json_bytes(inventory)).hexdigest()
    found = []
    for path in source_path.parent.iterdir():
        metadata_path = path / ROLLBACK_SOURCE
        if not path.is_dir() or not metadata_path.exists():
            continue
        metadata = _read_json(metadata_path)
        if metadata.get('source_run_id') == inventory['run_id']:
            if metadata.get('source_run_path') != str(source_path):
                raise JournalError('Rollback journal points to a different source path.')
            if metadata.get('inventory_sha256') != inventory_hash:
                raise JournalError(
                    'Source inventory changed after rollback began; reconciliation is required.'
                )
            found.append(path)
    if len(found) > 1:
        raise JournalError(
            'Multiple rollback journals exist for this source run; inspect them manually.'
        )
    if found:
        return RunJournal.open(found[0]), inventory_hash
    journal = RunJournal.create(source_path.parent)
    atomic_write(
        journal.path / ROLLBACK_SOURCE,
        json_bytes(
            {
                'schema_version': 1,
                'source_run_id': inventory['run_id'],
                'source_run_path': str(source_path),
                'source_plan_sha256': inventory.get('plan_sha256'),
                'inventory_sha256': inventory_hash,
            }
        ),
    )
    return journal, inventory_hash


def _write_source_hold(source_path, inventory, rollback_run_id):
    marker = source_path / ROLLBACK_HOLD
    if marker.exists():
        hold = _read_json(marker)
        if hold.get('source_run_id') != inventory['run_id']:
            raise JournalError('Source rollback hold marker does not match this run.')
        if hold.get('rollback_run_id') != rollback_run_id:
            raise JournalError('Source run is held by a different rollback journal.')
        return
    atomic_write(
        marker,
        json_bytes(
            {
                'schema_version': 1,
                'source_run_id': inventory['run_id'],
                'rollback_run_id': rollback_run_id,
                'started_at': datetime.now().astimezone().isoformat(),
                'review_required': True,
            }
        ),
    )


def _record_result(journal, operation_id, item, status, remote_outcome, message=None, **fields):
    payload = {
        'bucket': item.get('bucket'),
        'key': item.get('key'),
        'role': item.get('role'),
        'object_id': item.get('object_id'),
        'page_id': item.get('page_id'),
        'checksum': item.get('checksum'),
        'size_bytes': item.get('size_bytes'),
        'status': status,
        'remote_outcome': remote_outcome,
    }
    if message:
        payload['message'] = message
    payload.update(fields)
    journal.result(operation_id, 'rollback-delete', payload)


def _run_deletes(inventory, client, journal, report, used_by, pending):  # noqa: C901, PLR0915
    stopped = False
    blocked_objects = set()
    for item in sorted(inventory.get('new_keys') or [], key=_sort_key):
        identity_key = (item['bucket'], item['key'])
        result = {
            key: item.get(key)
            for key in ('bucket', 'key', 'role', 'object_id', 'page_id', 'checksum', 'size_bytes')
        }
        if stopped:
            report['results'].append(
                {
                    **result,
                    'status': 'not_attempted',
                    'reason': 'Earlier S3 deletion failed or had an uncertain outcome.',
                }
            )
            continue
        if item.get('object_id') in blocked_objects:
            report['results'].append(
                {
                    **result,
                    'status': 'not_attempted',
                    'reason': 'Manifest conflict for this object prevents deletion.',
                }
            )
            continue
        if identity_key in used_by:
            names = ', '.join(used_by[identity_key])
            message = f'conflict: used by run {names}'
            if item.get('role') == 'manifest':
                message = f'conflict: manifest used by run {names}'
                blocked_objects.add(item.get('object_id'))
            operation_id = journal.intent(
                'rollback-delete',
                {
                    **result,
                    'expected_checksum': item['checksum'],
                    'expected_size_bytes': item['size_bytes'],
                },
            )
            _record_result(journal, operation_id, item, 'conflict', 'not_deleted', message)
            report['results'].append({**result, 'status': 'conflict', 'reason': message})
            continue

        operation_id = pending.pop(identity_key, None)
        if operation_id is None:
            operation_id = journal.intent(
                'rollback-delete',
                {
                    **result,
                    'expected_checksum': item['checksum'],
                    'expected_size_bytes': item['size_bytes'],
                },
            )
        try:
            current = _current_identity(client, item['bucket'], item['key'])
        except (ClientError, BotoCoreError, OSError) as exc:
            message = operation_error(exc)
            _record_result(journal, operation_id, item, 'failed', 'unknown', message)
            report['results'].append({**result, 'status': 'failed', 'reason': message})
            stopped = True
            continue

        if current is None:
            _record_result(journal, operation_id, item, 'already_deleted', 'not_needed')
            report['results'].append({**result, 'status': 'already_deleted'})
            continue
        if not _matches(item, current):
            message = (
                'conflict: modified after upload; current FULL_OBJECT SHA-256 or size differs.'
            )
            _record_result(
                journal,
                operation_id,
                item,
                'conflict',
                'not_deleted',
                message,
                observed_checksum=_field(current, 'checksum'),
                observed_size_bytes=_field(current, 'size_bytes'),
                checksum_type=_field(current, 'checksum_type'),
            )
            report['results'].append(
                {
                    **result,
                    'status': 'conflict',
                    'reason': message,
                    'observed_checksum': _field(current, 'checksum'),
                    'observed_size_bytes': _field(current, 'size_bytes'),
                }
            )
            if item.get('role') == 'manifest':
                blocked_objects.add(item.get('object_id'))
            continue
        etag = _field(current, 'etag')
        if not isinstance(etag, str) or not etag:
            message = 'conflict: current ETag unavailable; conditional delete cannot be sent.'
            _record_result(
                journal,
                operation_id,
                item,
                'conflict',
                'not_deleted',
                message,
                observed_checksum=_field(current, 'checksum'),
                observed_size_bytes=_field(current, 'size_bytes'),
            )
            report['results'].append({**result, 'status': 'conflict', 'reason': message})
            if item.get('role') == 'manifest':
                blocked_objects.add(item.get('object_id'))
            continue

        journal.append(
            'context',
            stage='rollback',
            operation_id=operation_id,
            bucket=item['bucket'],
            key=item['key'],
            observed_checksum=_field(current, 'checksum'),
            observed_size_bytes=_field(current, 'size_bytes'),
            checksum_type=_field(current, 'checksum_type'),
            if_match=etag,
        )
        try:
            client.delete_object(item['bucket'], item['key'], if_match=etag)
        except ClientError as exc:
            status = exc.response.get('ResponseMetadata', {}).get('HTTPStatusCode')
            code = exc.response.get('Error', {}).get('Code')
            if status == 412 or code in {'PreconditionFailed', '412'}:
                message = 'conflict: S3 rejected the conditional delete (HTTP 412); '
                'object was not deleted.'
                _record_result(
                    journal,
                    operation_id,
                    item,
                    'conflict',
                    'not_deleted',
                    message,
                    if_match=etag,
                    error_code=code,
                    http_status=status,
                )
                report['results'].append(
                    {
                        **result,
                        'status': 'conflict',
                        'reason': message,
                        'if_match': etag,
                        'http_status': status,
                    }
                )
                if item.get('role') == 'manifest':
                    blocked_objects.add(item.get('object_id'))
            else:
                message = operation_error(exc)
                valid_status = isinstance(status, int) and not isinstance(status, bool)
                uncertain = not valid_status or status >= 500 or status in {408, 409, 429}
                remote_outcome = 'unknown' if uncertain else 'not_deleted'
                _record_result(
                    journal,
                    operation_id,
                    item,
                    'failed',
                    remote_outcome,
                    message,
                    if_match=etag,
                    error_code=code,
                    http_status=status,
                )
                report['results'].append({**result, 'status': 'failed', 'reason': message})
                stopped = True
            continue
        except (BotoCoreError, OSError) as exc:
            message = operation_error(exc)
            _record_result(journal, operation_id, item, 'failed', 'unknown', message, if_match=etag)
            report['results'].append({**result, 'status': 'failed', 'reason': message})
            stopped = True
            continue

        try:
            after = _current_identity(client, item['bucket'], item['key'])
        except (ClientError, BotoCoreError, OSError) as exc:
            message = operation_error(exc)
            _record_result(journal, operation_id, item, 'failed', 'unknown', message, if_match=etag)
            report['results'].append({**result, 'status': 'failed', 'reason': message})
            stopped = True
            continue
        if after is not None:
            message = 'Delete request returned, but the object remains visible.'
            _record_result(
                journal,
                operation_id,
                item,
                'failed',
                'unknown',
                message,
                if_match=etag,
                observed_checksum=_field(after, 'checksum'),
                observed_size_bytes=_field(after, 'size_bytes'),
            )
            report['results'].append({**result, 'status': 'failed', 'reason': message})
            stopped = True
            continue
        _record_result(journal, operation_id, item, 'deleted', 'deleted', if_match=etag)
        report['results'].append({**result, 'status': 'deleted', 'if_match': etag})


def run_rollback(run_path, client, *, execute=False, confirm=None, config_path=None, locks):  # noqa: C901, PLR0915
    source_path, inventory, source_events, source_config = _validate_source(
        run_path, client, config_path=config_path
    )
    mode = 'execute' if execute else 'preview'
    if confirm is not None and not execute:
        raise JournalError('--confirm is valid only with --execute.')
    if execute and confirm != inventory['run_id']:
        raise JournalError('--execute requires --confirm matching the source run ID.')
    if execute and not inventory.get('new_keys'):
        raise JournalError('Rollback refused: the source run has no confirmed new keys.')

    planned = inventory.get('planned') or []
    lock_entries = [SimpleNamespace(bucket=item['bucket'], key=item['key']) for item in planned]
    locks.acquire('rollback-source:' + str(source_path))
    endpoint = _endpoint(client)
    lock_destination(locks, _endpoint_key(endpoint), lock_entries)
    candidate_keys = {(item['bucket'], item['key']) for item in inventory.get('new_keys') or []}
    used_by = _later_reuse(source_path, source_events, source_config, candidate_keys)
    report = _report_base(source_path, inventory, mode)
    if not execute:
        blocked_objects = set()
        for item in sorted(inventory.get('new_keys') or [], key=_sort_key):
            result = {
                key: item.get(key)
                for key in (
                    'bucket',
                    'key',
                    'role',
                    'object_id',
                    'page_id',
                    'checksum',
                    'size_bytes',
                )
            }
            if item.get('object_id') in blocked_objects:
                report['results'].append(
                    {
                        **result,
                        'status': 'not_attempted',
                        'reason': 'Manifest conflict for this object prevents deletion.',
                    }
                )
                continue
            if (item['bucket'], item['key']) in used_by:
                names = ', '.join(used_by[(item['bucket'], item['key'])])
                report['results'].append(
                    {
                        **result,
                        'status': 'conflict',
                        'reason': f'manifest conflict: used by run {names}'
                        if item.get('role') == 'manifest'
                        else f'used by run {names}',
                    }
                )
                if item.get('role') == 'manifest':
                    blocked_objects.add(item.get('object_id'))
                continue
            try:
                current = _current_identity(client, item['bucket'], item['key'])
            except (ClientError, BotoCoreError, OSError) as exc:
                report['results'].append(
                    {**result, 'status': 'failed', 'reason': operation_error(exc)}
                )
                continue
            if current is None:
                report['results'].append({**result, 'status': 'already_deleted'})
            elif not _matches(item, current) or not _field(current, 'etag'):
                report['results'].append(
                    {
                        **result,
                        'status': 'conflict',
                        'reason': 'current full-object identity or ETag differs/unavailable',
                        'observed_checksum': _field(current, 'checksum'),
                        'observed_size_bytes': _field(current, 'size_bytes'),
                    }
                )
            else:
                report['results'].append(
                    {**result, 'status': 'would_delete', 'if_match': _field(current, 'etag')}
                )
        exit_code = (
            1 if any(item['status'] in {'conflict', 'failed'} for item in report['results']) else 0
        )
        return _finalize_report(report, exit_code)

    journal, inventory_hash = _find_or_create_journal(source_path, inventory)
    _, pending = _pending_rollback_intents(journal.path)
    journal.start(
        'rollback',
        options={
            'source_run_id': inventory['run_id'],
            'source_path': str(source_path),
            'inventory_sha256': inventory_hash,
        },
    )
    journal.append(
        'context',
        stage='rollback',
        source_run_id=inventory['run_id'],
        inventory_sha256=inventory_hash,
    )
    _write_source_hold(source_path, inventory, journal.run_id)
    report['rollback_run_id'] = journal.run_id
    report['rollback_run_path'] = str(journal.path)
    _run_deletes(inventory, client, journal, report, used_by, pending)
    exit_code = (
        1
        if any(
            item['status'] in {'conflict', 'failed', 'not_attempted'} for item in report['results']
        )
        else 0
    )
    report = _finalize_report(report, exit_code)
    journal.finish(exit_code)
    return report
