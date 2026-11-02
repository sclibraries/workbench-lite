"""Reconstruct rollback evidence from the durable journal, without remote writes."""

import json
from dataclasses import dataclass
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


@dataclass
class _PushStage:
    events: list
    mode: str
    options: dict
    complete: bool
    error: str | None = None


@dataclass
class _OperationEvidence:
    intents: dict
    checks: dict
    initial_checks: dict
    uploads: dict
    verification: dict
    operations: list
    latest_results: dict
    ever_written: set
    unresolved_ids: set


@dataclass
class _InventoryInputs:
    metadata: dict
    events: list
    plan: list
    stage: _PushStage
    config_snapshot: dict
    errors: '_InventoryErrors'


@dataclass
class _InventoryErrors:
    journal: str | None
    plan: str | None
    metadata: str | None


@dataclass
class _InventoryStateInput:
    inputs: _InventoryInputs
    key_groups: dict
    unresolved: list
    ownership_uncertain: bool


@dataclass
class _InventoryDocument:
    inputs: _InventoryInputs
    operations: list
    unresolved: list
    key_groups: dict
    manifest_urls: list
    derivatives: list
    state: str


def build_inventory(run_path):
    """Report confirmed writes and uncertainty; never grant deletion authority."""
    path = Path(run_path)
    inputs = _read_inventory_inputs(path)
    operation_evidence = _collect_operation_evidence(inputs.stage.events)
    key_groups, ownership_uncertain = _classify_planned_objects(
        inputs.plan, inputs.stage.mode, operation_evidence
    )
    _load_inventory_config(inputs, path)
    manifest_urls = _manifest_urls(key_groups['new_keys'], inputs.config_snapshot, inputs.events)
    derivatives = _local_derivatives(inputs.events)
    unresolved = _unresolved_operations(operation_evidence)
    state = _inventory_state(
        _InventoryStateInput(inputs, key_groups, unresolved, ownership_uncertain)
    )
    return _inventory_document(
        _InventoryDocument(
            inputs,
            operation_evidence.operations,
            unresolved,
            key_groups,
            manifest_urls,
            derivatives,
            state,
        )
    )


def _read_inventory_inputs(path):
    metadata, metadata_error = _read_metadata(path)
    events, journal_error = read_events(path)
    plan, plan_error = _read_upload_plan(path, metadata)
    stage = _latest_push_stage(events)
    metadata_error = stage.error or metadata_error
    errors = _InventoryErrors(journal_error, plan_error, metadata_error)
    return _InventoryInputs(metadata, events, plan, stage, {}, errors)


def _load_inventory_config(inputs, path):
    inputs.config_snapshot, config_error = _read_config_snapshot(path)
    inputs.errors.metadata = config_error or inputs.errors.metadata


def _read_metadata(path):
    metadata = _read_json(path / 'run.json')
    if (
        not isinstance(metadata, dict)
        or metadata.get('run_id') != path.name
        or not isinstance(metadata.get('inputs'), dict)
    ):
        return {}, 'Run metadata has an invalid structure.'
    return metadata, None


def _read_upload_plan(path, metadata):
    try:
        payload = _read_json(path / 'upload-plan.json')
        plan = payload['entries']
        if (
            not isinstance(payload, dict)
            or not isinstance(plan, list)
            or any(
                not isinstance(entry, dict)
                or not isinstance(entry.get('bucket'), str)
                or not isinstance(entry.get('key'), str)
                or not isinstance(entry.get('role'), str)
                for entry in plan
            )
        ):
            raise ValueError('Upload plan entries have an invalid structure')
    except (JournalError, KeyError, TypeError, ValueError):
        return [], 'Immutable upload plan unavailable.'
    try:
        if digest(path / 'upload-plan.json') != metadata.get('plan_sha256'):
            return plan, 'Immutable upload plan checksum does not match run metadata.'
    except OSError:
        return plan, 'Immutable upload plan is unreadable.'
    return plan, None


def _latest_push_stage(events):
    stages = [
        (index, event)
        for index, event in enumerate(events)
        if event['kind'] == 'stage_started' and event['stage'] in {'push', 'push-dry-run'}
    ]
    if not stages:
        return _PushStage([], 'no_push', {}, False)
    start, stage = stages[-1]
    stage_events = events[stages[0][0] :]
    options = stage.get('options', {})
    error = None
    if not isinstance(options, dict):
        options = {}
        error = 'Push stage options have an invalid structure.'
    complete = any(
        event['kind'] == 'stage_finished' and event['stage'] == stage['stage']
        for event in events[start:]
    )
    mode = 'preview' if stage['stage'] == 'push-dry-run' else 'execute'
    return _PushStage(stage_events, mode, options, complete, error)


def _collect_operation_evidence(stage_events):
    intents = {event['operation_id']: event for event in stage_events if event['kind'] == 'intent'}
    results = [event for event in stage_events if event['kind'] == 'result']
    evidence = _OperationEvidence(intents, {}, {}, {}, {}, [], {}, set(), set())
    for event in results:
        evidence.latest_results[event['operation_id']] = event
        operation = intents.get(event['operation_id'])
        evidence.operations.append(_operation_summary(event))
        if not operation or _identity(operation) != _identity(event):
            continue
        _index_operation_result(evidence, event)
    evidence.unresolved_ids = _unresolved_operation_ids(intents, evidence.latest_results)
    return evidence


def _operation_summary(event):
    fields = (
        'sequence',
        'stage',
        'operation_id',
        'bucket',
        'key',
        'role',
        'status',
        'remote_outcome',
        'checksum',
        'size_bytes',
        'observed_checksum',
        'observed_size_bytes',
        'verification_method',
    )
    return {key: event.get(key) for key in fields if key in event}


def _index_operation_result(evidence, event):
    identity = _identity(event)
    if event['stage'] == 'destination-check':
        evidence.initial_checks.setdefault(identity, event)
        evidence.checks[identity] = event
    elif event['stage'] == 'push':
        evidence.uploads[identity] = event
        if event.get('status') in {'uploaded', 'confirmed'} and event.get('remote_outcome') not in {
            'unknown',
            'absent',
            'conflict',
        }:
            evidence.ever_written.add(identity)
    elif event['stage'] == 'verify-upload':
        evidence.verification[identity] = event


def _unresolved_operation_ids(intents, latest_results):
    return {
        operation_id
        for operation_id, intent in intents.items()
        if (
            operation_id not in latest_results
            or latest_results[operation_id]['sequence'] < intent['sequence']
            or latest_results[operation_id].get('remote_outcome') == 'unknown'
        )
    }


def _classify_planned_objects(plan, mode, evidence):
    groups = {
        'new_keys': [],
        'unknown_keys': [],
        'conflicts': [],
        'unchanged_keys': [],
        'held_keys': [],
        'replacements': [],
    }
    ownership_uncertain = False
    for entry in plan:
        item, context = _planned_item(entry, evidence)
        if mode != 'execute':
            continue
        category, item_uncertain = _classify_planned_item(context, evidence)
        if category:
            groups[category].append(item)
        ownership_uncertain = ownership_uncertain or item_uncertain
    return groups, ownership_uncertain


def _planned_item(entry, evidence):
    identity = _identity(entry)
    check = evidence.checks.get(identity)
    upload = evidence.uploads.get(identity)
    verified = evidence.verification.get(identity)
    origin_check = evidence.initial_checks.get(identity, check)
    item = {
        key: entry.get(key)
        for key in ('bucket', 'key', 'role', 'object_id', 'page_id', 'public', 'generated')
    }
    item['destination_status'] = origin_check.get('status') if origin_check else 'not_checked'
    if check and check is not origin_check:
        item['current_destination_status'] = check.get('status')
    item['upload_status'] = upload.get('status') if upload else 'not_attempted'
    item['verification_status'] = verified.get('status') if verified else 'not_verified'
    if origin_check:
        _add_destination_evidence(item, origin_check)
    if upload and upload.get('status') in {'uploaded', 'confirmed'}:
        item.update({key: upload.get(key) for key in ('checksum', 'size_bytes')})
    return item, (identity, check, upload, origin_check)


def _add_destination_evidence(item, origin_check):
    item['classification'] = origin_check.get('classification')
    fields = (
        ('checksum', 'checksum'),
        ('size_bytes', 'size_bytes'),
        ('observed_checksum', 'observed_checksum'),
        ('observed_size_bytes', 'previous_size_bytes'),
        ('verification_method', 'verification_method'),
        ('checksum_type', 'checksum_type'),
    )
    for source, target in fields:
        if source in origin_check:
            item[target] = origin_check[source]
    if origin_check.get('checksum_type') == 'FULL_OBJECT' and origin_check.get('observed_checksum'):
        item['previous_checksum'] = origin_check['observed_checksum']


def _classify_planned_item(context, evidence):
    identity, check, upload, origin_check = context
    if _upload_conflicts(upload):
        uncertain = identity in evidence.ever_written and upload.get('remote_outcome') != 'written'
        return 'conflicts', uncertain
    if check and check.get('status') in {'conflict', 'failed'}:
        return 'conflicts', identity in evidence.ever_written
    if origin_check and origin_check.get('status') == 'unchanged':
        return 'unchanged_keys', False
    if origin_check and origin_check.get('status') == 'held':
        return 'held_keys', False
    if _is_confirmed_new_key(origin_check, upload):
        return 'new_keys', False
    if _has_unresolved_push(identity, evidence):
        return 'unknown_keys', False
    if upload and upload.get('remote_outcome') == 'written':
        # Without proof of prior absence, an acknowledgment cannot authorize future deletion.
        return 'conflicts', True
    return None, False


def _upload_conflicts(upload):
    return bool(
        upload
        and (upload.get('status') == 'conflict' or upload.get('remote_outcome') == 'conflict')
    )


def _is_confirmed_new_key(origin_check, upload):
    return bool(
        origin_check
        and origin_check.get('status') == 'absent'
        and upload
        and upload.get('status') in {'uploaded', 'confirmed'}
        and upload.get('remote_outcome') not in {'unknown', 'conflict'}
    )


def _has_unresolved_push(identity, evidence):
    return any(
        _identity(operation) == identity
        and operation['stage'] == 'push'
        and operation['operation_id'] in evidence.unresolved_ids
        for operation in evidence.intents.values()
    )


def _read_config_snapshot(path):
    config = _read_json(path / 'config.json') if (path / 'config.json').exists() else {}
    if not isinstance(config, dict):
        return {}, 'Configuration snapshot has an invalid structure.'
    return config, None


def _manifest_urls(new_keys, config_snapshot, events):
    base = config_snapshot.get('manifest_base_url')
    if base is None:
        base = _generate_options(events).get('manifest_base_url')
    if not isinstance(base, str) or not base.startswith(('http://', 'https://')):
        return []
    return [
        {
            'bucket': item['bucket'],
            'key': item['key'],
            'url': base.rstrip('/') + '/' + item['key'].lstrip('/'),
        }
        for item in new_keys
        if item['role'] == 'manifest'
    ]


def _generate_options(events):
    options = next(
        (
            event.get('options')
            for event in reversed(events)
            if event['kind'] == 'stage_started' and event['stage'] == 'generate'
        ),
        {},
    )
    return options if isinstance(options, dict) else {}


def _local_derivatives(events):
    generated = [
        event
        for event in events
        if event['kind'] == 'result'
        and event['stage'] == 'generate'
        and event.get('status') == 'generated'
        and event.get('role') != 'manifest'
    ]
    return [
        {key: event.get(key) for key in ('role', 'output_path', 'checksum', 'size_bytes')}
        for event in generated
    ]


def _unresolved_operations(evidence):
    return [
        operation
        for operation in evidence.intents.values()
        if operation['operation_id'] in evidence.unresolved_ids
    ]


def _inventory_state(state):
    if (
        state.inputs.errors.journal
        or state.inputs.errors.plan
        or state.inputs.errors.metadata
        or state.inputs.stage.mode == 'no_push'
    ):
        return 'invalid_evidence'
    if state.key_groups['unknown_keys'] or state.unresolved or state.ownership_uncertain:
        return 'needs_reconciliation'
    if not state.inputs.stage.complete:
        return 'incomplete_stage'
    if state.inputs.stage.mode == 'preview':
        return 'preview'
    return 'reviewable'


def _inventory_document(document):
    inputs = document.inputs
    return {
        'schema_version': INVENTORY_VERSION,
        'kind': 'rollback-inventory',
        'run_id': inputs.metadata.get('run_id'),
        'batch_id': inputs.metadata.get('batch_id'),
        'mode': inputs.stage.mode,
        'state': document.state,
        'deletion_authorized': False,
        'source': _inventory_source(inputs),
        'plan_sha256': inputs.metadata.get('plan_sha256'),
        'planned': inputs.plan,
        'operations': document.operations,
        'pending_operations': _pending_operation_summaries(document.unresolved),
        **document.key_groups,
        'local_derivatives': document.derivatives,
        'manifest_urls': document.manifest_urls,
        'aspace': {'changes': [], 'previous_values': [], 'status': 'deferred-to-reviewed-csv'},
        'evidence_errors': _inventory_errors(inputs.errors),
    }


def _inventory_source(inputs):
    return sanitize(
        {
            'config_path': inputs.stage.options.get('config'),
            'csv_path': inputs.stage.options.get('input_csv')
            or inputs.config_snapshot.get('input_csv'),
            'config_sha256': inputs.metadata.get('inputs', {}).get('config_sha256'),
            'csv_sha256': inputs.metadata.get('inputs', {}).get('csv_sha256'),
        }
    )


def _pending_operation_summaries(unresolved):
    return [
        {
            key: operation.get(key)
            for key in ('sequence', 'stage', 'operation_id', 'bucket', 'key', 'role')
            if key in operation
        }
        for operation in unresolved
    ]


def _inventory_errors(errors):
    return [error for error in (errors.journal, errors.plan, errors.metadata) if error]


def write_inventory(run_path, output_path=None):
    inventory = build_inventory(run_path)
    run_path = Path(run_path).resolve()
    canonical = run_path / 'rollback-inventory.json'
    target = Path(output_path).resolve() if output_path is not None else canonical
    if output_path is not None and target.is_relative_to(run_path):
        raise JournalError('Rollback output cannot replace run evidence.')
    atomic_write(target, json_bytes(inventory))
    return inventory
