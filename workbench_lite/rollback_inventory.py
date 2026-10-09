"""Reconstruct rollback evidence from the durable journal, without remote writes."""
import json
from pathlib import Path

from .diagnostics import sanitize
from .runs import JournalError, atomic_write, digest, json_bytes, read_events

INVENTORY_VERSION = 1


def _read_json(path):
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except (OSError, ValueError) as exc:
        raise JournalError('Cannot read rollback inventory source evidence.') from exc


def _identity(event):
    return event.get('bucket'), event.get('key')


def build_inventory(run_path):
    """Report confirmed writes and uncertainty; never grant deletion authority."""
    path = Path(run_path)
    metadata = _read_json(path / 'run.json')
    metadata_error = None
    if not isinstance(metadata, dict) or metadata.get('run_id') != path.name or not isinstance(metadata.get('inputs'), dict):
        metadata, metadata_error = {}, 'Run metadata has an invalid structure.'
    events, journal_error = read_events(path)
    try:
        payload = _read_json(path / 'upload-plan.json')
        plan = payload['entries']
        if (not isinstance(payload, dict) or not isinstance(plan, list)
                or any(not isinstance(entry, dict) or not isinstance(entry.get('bucket'), str)
                       or not isinstance(entry.get('key'), str) or not isinstance(entry.get('role'), str)
                       for entry in plan)):
            raise ValueError('Upload plan entries have an invalid structure')
    except (JournalError, KeyError, TypeError, ValueError):
        plan = []
        plan_error = 'Immutable upload plan unavailable.'
    else:
        plan_error = None
        try:
            if digest(path / 'upload-plan.json') != metadata.get('plan_sha256'):
                plan_error = 'Immutable upload plan checksum does not match run metadata.'
        except OSError:
            plan_error = 'Immutable upload plan is unreadable.'

    stages = [(index, event) for index, event in enumerate(events)
              if event['kind'] == 'stage_started' and event['stage'] in {'push', 'push-dry-run'}]
    if stages:
        start, stage = stages[-1]
        stage_events = events[start:]
        mode = 'preview' if stage['stage'] == 'push-dry-run' else 'execute'
        options = stage.get('options', {})
        if not isinstance(options, dict):
            options = {}
            metadata_error = 'Push stage options have an invalid structure.'
        stage_complete = any(event['kind'] == 'stage_finished' and event['stage'] == stage['stage']
                             for event in stage_events)
    else:
        stage_events, mode, options, stage_complete = [], 'no_push', {}, False

    intents = {event['operation_id']: event for event in stage_events if event['kind'] == 'intent'}
    results = [event for event in stage_events if event['kind'] == 'result']
    checks = {}
    uploads = {}
    verification = {}
    operations = []
    result_ids = {event['operation_id'] for event in results}
    uncertain_ids = {event['operation_id'] for event in results if event.get('remote_outcome') == 'unknown'}
    for event in results:
        op = intents.get(event['operation_id'])
        operations.append({key: event.get(key) for key in
                           ('sequence', 'stage', 'operation_id', 'bucket', 'key', 'role',
                            'status', 'remote_outcome', 'checksum', 'size_bytes',
                            'observed_checksum', 'observed_size_bytes', 'verification_method')
                           if key in event})
        if not op or _identity(op) != _identity(event):
            continue
        target = {'destination-check': checks, 'push': uploads, 'verify-upload': verification}.get(event['stage'])
        if target is not None:
            target[_identity(event)] = event

    new_keys, unknown_keys, conflicts, unchanged_keys, held_keys, replacements = [], [], [], [], [], []
    ownership_uncertain = False
    for entry in plan:
        identity = _identity(entry)
        check, upload, verified = checks.get(identity), uploads.get(identity), verification.get(identity)
        item = {key: entry.get(key) for key in ('bucket', 'key', 'role', 'object_id', 'page_id', 'public', 'generated')}
        item['destination_status'] = check.get('status') if check else 'not_checked'
        item['upload_status'] = upload.get('status') if upload else 'not_attempted'
        item['verification_status'] = verified.get('status') if verified else 'not_verified'
        if check:
            item['classification'] = check.get('classification')
            for source, target in (('checksum', 'checksum'), ('size_bytes', 'size_bytes'),
                                   ('observed_checksum', 'observed_checksum'),
                                   ('observed_size_bytes', 'previous_size_bytes'),
                                   ('verification_method', 'verification_method'),
                                   ('checksum_type', 'checksum_type')):
                if source in check:
                    item[target] = check[source]
            if check.get('checksum_type') == 'FULL_OBJECT' and check.get('observed_checksum'):
                item['previous_checksum'] = check['observed_checksum']
        if upload and upload.get('status') == 'uploaded':
            item.update({key: upload.get(key) for key in ('checksum', 'size_bytes')})
        if mode != 'execute':
            continue
        if check and check.get('status') == 'unchanged':
            unchanged_keys.append(item)
        elif check and check.get('status') == 'held':
            held_keys.append(item)
        elif check and check.get('status') in {'conflict', 'failed'}:
            conflicts.append(item)
            if upload and upload.get('status') == 'uploaded':
                ownership_uncertain = True
        elif (check and check.get('status') == 'absent' and upload
              and upload.get('status') == 'uploaded' and upload.get('remote_outcome') != 'unknown'):
            new_keys.append(item)
        elif any(_identity(op) == identity and op['stage'] == 'push' and
                 (op['operation_id'] not in result_ids or op['operation_id'] in uncertain_ids)
                 for op in intents.values()):
            unknown_keys.append(item)
        elif upload and upload.get('status') == 'uploaded':
            # An acknowledgement without proof that the destination was absent
            # cannot make a key eligible for a future delete.
            conflicts.append(item)
            ownership_uncertain = True

    config_snapshot = _read_json(path / 'config.json') if (path / 'config.json').exists() else {}
    if not isinstance(config_snapshot, dict):
        config_snapshot = {}
        metadata_error = 'Configuration snapshot has an invalid structure.'
    generate_options = next((event.get('options') for event in reversed(events)
                             if event['kind'] == 'stage_started' and event['stage'] == 'generate'), {})
    if not isinstance(generate_options, dict):
        generate_options = {}
    base = config_snapshot.get('manifest_base_url')
    if base is None:
        base = generate_options.get('manifest_base_url')
    manifest_urls = []
    for item in new_keys:
        if item['role'] == 'manifest' and isinstance(base, str) and base.startswith(('http://', 'https://')):
            manifest_urls.append({'bucket': item['bucket'], 'key': item['key'],
                                  'url': base.rstrip('/') + '/' + item['key'].lstrip('/')})
    generated = [event for event in events if event['kind'] == 'result' and event['stage'] == 'generate'
                 and event.get('status') == 'generated']
    derivatives = [{key: event.get(key) for key in ('role', 'output_path', 'checksum', 'size_bytes')}
                   for event in generated if event.get('role') != 'manifest']

    unresolved = [op for op in intents.values() if
                  not any(result['operation_id'] == op['operation_id'] and result.get('remote_outcome') != 'unknown'
                          for result in results)]
    if journal_error or plan_error or metadata_error or not stages:
        state = 'invalid_evidence'
    elif unknown_keys or unresolved or ownership_uncertain:
        state = 'needs_reconciliation'
    elif not stage_complete:
        state = 'incomplete_stage'
    elif mode == 'preview':
        state = 'preview'
    else:
        state = 'reviewable'
    return {
        'schema_version': INVENTORY_VERSION, 'kind': 'rollback-inventory',
        'run_id': metadata.get('run_id'), 'batch_id': metadata.get('batch_id'),
        'mode': mode, 'state': state, 'deletion_authorized': False,
        'source': sanitize({'config_path': options.get('config'),
                            'csv_path': options.get('input_csv') or config_snapshot.get('input_csv'),
                            'config_sha256': metadata.get('inputs', {}).get('config_sha256'),
                            'csv_sha256': metadata.get('inputs', {}).get('csv_sha256')}),
        'plan_sha256': metadata.get('plan_sha256'), 'planned': plan,
        'operations': operations,
        'pending_operations': [{key: op.get(key) for key in ('sequence', 'stage', 'operation_id', 'bucket', 'key', 'role') if key in op}
                               for op in unresolved],
        'new_keys': new_keys, 'unknown_keys': unknown_keys,
        'conflicts': conflicts, 'unchanged_keys': unchanged_keys, 'held_keys': held_keys,
        'replacements': replacements,
        'local_derivatives': derivatives, 'manifest_urls': manifest_urls,
        'aspace': {'changes': [], 'previous_values': [], 'status': 'deferred-to-reviewed-csv'},
        'evidence_errors': [error for error in (journal_error, plan_error, metadata_error) if error],
    }


def write_inventory(run_path, output_path=None):
    inventory = build_inventory(run_path)
    run_path = Path(run_path).resolve()
    canonical = run_path / 'rollback-inventory.json'
    target = Path(output_path).resolve() if output_path is not None else canonical
    if output_path is not None and target.is_relative_to(run_path):
        raise JournalError('Rollback output cannot replace run evidence.')
    atomic_write(target, json_bytes(inventory))
    return inventory
