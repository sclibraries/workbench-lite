"""Scenario implementations keyed by the public rehearsal scenario number."""

import contextlib
import csv
import errno
import json
import os
from pathlib import Path
import shutil
import socket
import time
from unittest.mock import patch

import yaml
from PIL import Image

from rehearsal_harness import clear_dead_test_locks, start_worker
from rehearsal_storage import (
    ENDPOINT,
    full_identity,
    make_config,
    put_bytes,
    sample_inputs,
    storage_listing,
)
from workbench_lite.runs import RunJournal, inspect_run, read_events


class ScenarioMismatch(RuntimeError):
    """Raised when observed CLI behavior differs from the written contract."""


def case_01(ctx, alias, directory, source, config, prefix):
    output = directory / 'generated'
    run_path = ctx.generate(directory, config, output)
    result = ctx.push(directory, config, output)
    ctx.require_exit(result, 0, 'push_execute')
    inventory = ctx.command(
        directory,
        'rollback-inventory <run>',
        ['rollback-inventory', run_path, '--format', 'json'],
    )
    ctx.require_exit(inventory, 0, 'rollback_inventory')
    status = inspect_run(run_path)
    if status['publication']['state'] != 'ready_for_review':
        raise ScenarioMismatch(
            'publication expected ready_for_review, observed '
            f'{status["publication"]["state"]}'
        )
    return {
        'steps': {
            'generate': 0,
            'push_execute': result['exit'],
            'rollback_inventory': inventory['exit'],
        },
        'run_state': status['state'],
        'publication': status['publication']['state'],
        'recovery': 'rollback inventory created without executing deletion',
    }


def case_02(ctx, alias, directory, source, config, prefix):
    variants = {
        label: _run_source_fault(ctx, directory, source, prefix, label)
        for label in ('missing', 'corrupt')
    }
    return {
        'steps': {
            'check_missing_source': variants['missing'],
            'generate_corrupt_tiff': variants['corrupt'],
        },
        'run_state': 'failed',
        'publication': 'held',
        'recovery': 'restore source and validate a fresh run',
    }


def _run_source_fault(ctx, directory, source, prefix, label):
    target = directory / label
    shutil.copytree(source, target, copy_function=os.link)
    sample_inputs(target)
    config = make_config(
        target,
        directory / f'{label}-config',
        ctx.args.private_bucket,
        ctx.args.public_bucket,
        f'{prefix}/{label}',
    )
    tiff = _first_tiff(target)
    tiff.unlink()
    command = 'check' if label == 'missing' else 'generate'
    if label == 'corrupt':
        tiff.write_bytes(b'not image data')
    result = ctx.command(
        directory,
        f'{command} --config <{label}-config>',
        [
            command,
            '--config',
            config,
            '--format',
            'json',
            '--run-dir',
            directory / f'{label}-runs',
        ],
    )
    step = 'check_missing_source' if label == 'missing' else 'generate_corrupt_tiff'
    ctx.require_exit(result, 3, step)
    return result['exit']


def _first_tiff(directory):
    files = sorted(
        path
        for path in directory.rglob('*')
        if path.is_file() and path.suffix.lower() in {'.tif', '.tiff'}
    )
    if not files:
        raise ValueError('Source sample has no TIFF for the fault case.')
    return files[0]


def case_03(ctx, alias, directory, source, config, prefix):
    output = directory / 'generated'
    run_path = ctx.generate(directory, config, output)
    result = ctx.push(directory, config, output, mode='denied')
    ctx.require_exit(result, 1, 'push_execute')
    return {
        'steps': {'push_execute': result['exit']},
        'run_state': inspect_run(run_path)['state'],
        'publication': inspect_run(run_path)['publication']['state'],
        'recovery': 'access denied before writes',
    }


def case_04(ctx, alias, directory, source, config, prefix):
    output = directory / 'generated'
    run_path = ctx.generate(directory, config, output)
    sock = socket.socket()
    sock.bind(('127.0.0.1', 0))
    port = sock.getsockname()[1]
    sock.close()
    closed = f'http://127.0.0.1:{port}'
    result = ctx.push(directory, config, output, endpoint=closed)
    ctx.require_exit(result, 1, 'push_execute')
    state = inspect_run(run_path)
    return {
        'steps': {'push_execute': result['exit']},
        'run_state': state['state'],
        'publication': state['publication']['state'],
        'closed_endpoint': closed,
        'recovery': 'endpoint unavailable before writes',
    }


def case_05(ctx, alias, directory, source, config, prefix):
    output = directory / 'generated'
    run_path = ctx.generate(directory, config, output)
    original = RunJournal.intent

    def fail_push_intent(journal, stage, identity, *, operation_id=None, attempt=1):
        if stage == 'push':
            with contextlib.ExitStack() as stack:
                stack.enter_context(
                    patch(
                        'workbench_lite.runs.os.fsync',
                        side_effect=OSError(errno.ENOSPC, 'synthetic disk full'),
                    )
                )
                return original(
                    journal, stage, identity, operation_id=operation_id, attempt=attempt
                )
        return original(
            journal, stage, identity, operation_id=operation_id, attempt=attempt
        )

    result = ctx.push(
        directory,
        config,
        output,
        extra_patches=(patch.object(RunJournal, 'intent', new=fail_push_intent),),
    )
    ctx.require_exit(result, 1, 'push_execute')
    state = inspect_run(run_path)
    return {
        'steps': {'push_execute': result['exit']},
        'run_state': state['state'],
        'publication': state['publication']['state'],
        'recovery': 'journal failure before PUT',
    }


def case_06(ctx, alias, directory, source, config, prefix):
    signals = (('SIGINT', 2, 130), ('SIGTERM', 15, 143))
    outcomes = {
        label: _run_signal_fault(
            ctx, directory, source, prefix, label, signal, expected
        )
        for label, signal, expected in signals
    }
    return {
        'steps': outcomes,
        'run_state': 'interrupted',
        'publication': 'held',
        'recovery': 'unknown upload outcomes retained for reconciliation',
    }


def _run_signal_fault(ctx, directory, source, prefix, label, stop_signal, expected):
    output = directory / label.lower() / 'generated'
    output.parent.mkdir(parents=True)
    config = make_config(
        source,
        output.parent,
        ctx.args.private_bucket,
        ctx.args.public_bucket,
        f'{prefix}/{label.lower()}',
    )
    run_path = ctx.generate(directory, config, output, f'{label.lower()}-runs')
    argv = _push_argv(config, output, directory / f'{label.lower()}-push-runs')
    child = ctx.worker(
        argv,
        fault='upload_before_2',
        ready=directory / f'{label.lower()}-ready',
        stop_signal=stop_signal,
    )
    _verify_interrupted_signal(ctx, label, run_path, child, expected)
    return child.returncode


def _verify_interrupted_signal(ctx, label, run_path, child, expected):
    if child.returncode != expected:
        raise ScenarioMismatch(
            f'{label}: expected {expected}, observed {child.returncode}; '
            f'{child.stderr[-1000:]}'
        )
    status = inspect_run(run_path)
    interrupted = status['state'] == 'interrupted' and status['unknown_operations']
    if not interrupted:
        raise ScenarioMismatch(
            f'{label}: expected interrupted state and unknown intent'
        )


def _push_argv(config, output, run_directory):
    return [
        'push',
        '--config',
        config,
        '--execute',
        '--generated-dir',
        output,
        '--endpoint-url',
        ENDPOINT,
        '--region',
        'us-east-1',
        '--format',
        'json',
        '--run-dir',
        run_directory,
    ]


def case_07(ctx, alias, directory, source, config, prefix):
    output = directory / 'generated'
    run_path = ctx.generate(directory, config, output)
    child = ctx.worker(
        _push_argv(config, output, directory / 'push-runs'),
        fault='upload_after_1',
        ready=directory / 'kill-ready',
        stop_signal=9,
    )
    removed_locks = _verify_killed_push(ctx, child)
    reconcile = ctx.command(
        directory, 'reconcile <run>', ['reconcile', run_path, '--format', 'json']
    )
    ctx.require_exit(reconcile, 0, 'reconcile')
    resume = ctx.push(directory, config, output, resume=True)
    ctx.require_exit(resume, 0, 'push_resume')
    state = inspect_run(run_path)
    if state['publication']['state'] != 'ready_for_review':
        raise ScenarioMismatch('resume did not produce a ready-for-review receipt')
    return {
        'steps': {
            'push_sigkill': 'signal',
            'reconcile': reconcile['exit'],
            'push_resume': resume['exit'],
        },
        'run_state': state['state'],
        'publication': state['publication']['state'],
        'test_lock_cleanup': removed_locks,
        'recovery': 'verified dead-process test locks, reconcile, then verified resume',
    }


def _verify_killed_push(ctx, child):
    if child.returncode != -9:
        raise ScenarioMismatch(f'SIGKILL expected -9, observed {child.returncode}')
    return clear_dead_test_locks(ctx.lock_root, child.pid)


def case_08(ctx, alias, directory, source, config, prefix):
    output1 = directory / 'version-1-generated'
    run1 = ctx.generate(directory, config, output1, 'version-1-runs')
    first = ctx.push(directory, config, output1, run_name='version-1-push-runs')
    ctx.require_exit(first, 0, 'initial_execute')
    manifest_key = _manifest_key(run1)
    old_manifest_bytes = _read_object(ctx, ctx.args.public_bucket, manifest_key)
    output2 = directory / 'unchanged-generated'
    ctx.generate(directory, config, output2, 'unchanged-runs')
    unchanged = ctx.push(directory, config, output2, run_name='unchanged-push-runs')
    ctx.require_exit(unchanged, 0, 'unchanged_execute')
    _append_synthetic_page(source, config)
    additive, additive_status = _run_additive_version(
        ctx, directory, config, manifest_key, old_manifest_bytes
    )
    unchanged_run = Path(
        json.loads((output2 / '.workbench-run.json').read_text())['run_path']
    )
    unchanged_state = inspect_run(unchanged_run)['publication']['state']
    return {
        'steps': {
            'initial_execute': first['exit'],
            'unchanged_execute': unchanged['exit'],
            'additive_execute': additive['exit'],
        },
        'run_state': 'completed unchanged; failed additive',
        'publication': f'{unchanged_state} unchanged; {additive_status} additive',
        'recovery': 'unchanged content is eligible; additive manifest remains held',
    }


def _manifest_key(run_path):
    entries = json.loads((run_path / 'upload-plan.json').read_text())['entries']
    return next(item['key'] for item in entries if item['role'] == 'manifest')


def _read_object(ctx, bucket, key):
    response = ctx.storage.client.get_object(Bucket=bucket, Key=key)
    body = response['Body']
    try:
        return body.read()
    finally:
        body.close()


def _append_synthetic_page(source, config):
    _, csv_path = sample_inputs(source)
    with csv_path.open(encoding='utf-8-sig', newline='') as handle:
        rows = list(csv.DictReader(handle))
    page_count = sum(row.get('field_model') == 'Page' for row in rows)
    new_number = page_count + 1
    input_root = Path(yaml.safe_load(config.read_text(encoding='utf-8'))['input_dir'])
    new_file = input_root / f'synthetic-added-page-{new_number}.tif'
    Image.new('RGB', (32, 24), (37, 91, 143)).save(new_file)
    fieldnames = list(rows[0])
    parent_id = next(
        row['id'] for row in rows if row.get('field_model') == 'Paged Content'
    )
    row = {field: '' for field in fieldnames}
    row.update(
        {
            'title': f'Synthetic added page {new_number}',
            'id': f'synthetic-page-{new_number}',
            'field_model': 'Page',
            'parent_id': parent_id,
            'field_weight': str(new_number),
            'file': new_file.name,
        }
    )
    with csv_path.open('w', encoding='utf-8', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows([*rows, row])


def _run_additive_version(ctx, directory, config, manifest_key, original_manifest):
    output = directory / 'additive-generated'
    ctx.generate(directory, config, output, 'additive-runs')
    result = ctx.push(directory, config, output, run_name='additive-push-runs')
    ctx.require_exit(result, 1, 'additive_execute')
    current_manifest = _read_object(ctx, ctx.args.public_bucket, manifest_key)
    if current_manifest != original_manifest:
        raise ScenarioMismatch(
            'additive rerun changed the previously published manifest'
        )
    run_path = Path(
        json.loads((output / '.workbench-run.json').read_text())['run_path']
    )
    publication = inspect_run(run_path)['publication']['state']
    if publication != 'held':
        raise ScenarioMismatch('additive rerun unexpectedly has a publication receipt')
    return result, publication


def case_09(ctx, alias, directory, source, config, prefix):
    output = directory / 'generated'
    run_path = ctx.generate(directory, config, output)
    entries = json.loads((run_path / 'upload-plan.json').read_text())['entries']
    target = next(entry for entry in entries if entry['role'] == 'master_tiff')
    put_bytes(
        ctx.storage.client,
        target['bucket'],
        target['key'],
        b'conflicting rehearsal bytes',
    )
    result = ctx.push(directory, config, output)
    ctx.require_exit(result, 1, 'push_execute')
    return {
        'steps': {'push_execute': result['exit']},
        'run_state': inspect_run(run_path)['state'],
        'publication': inspect_run(run_path)['publication']['state'],
        'conflict_key': target['key'],
        'recovery': 'resolve collision outside this run',
    }


def case_10(ctx, alias, directory, source, config, prefix):
    configs, outputs = _prepare_contenders(ctx, directory, source, prefix)
    winner_code, contender = _contend_for_destination(ctx, directory, configs, outputs)
    _verify_contender_runs(outputs)
    return {
        'steps': {'lock_winner': winner_code, 'lock_contender': contender['exit']},
        'run_state': 'winner completed; contender failed',
        'publication': 'winner ready_for_review; contender held',
        'recovery': 'contender refused while winner held destination lock',
    }


def _prepare_contenders(ctx, directory, source, prefix):
    outputs = [directory / 'winner-generated', directory / 'contender-generated']
    configs = [
        make_config(
            source,
            directory / f'package-{index}',
            ctx.args.private_bucket,
            ctx.args.public_bucket,
            prefix,
        )
        for index in range(2)
    ]
    for index, output in enumerate(outputs):
        ctx.generate(directory, configs[index], output, f'generate-{index}')
    return configs, outputs


def _verify_contender_runs(outputs):
    winner_run, contender_run = _run_paths(outputs)
    winner_state, contender_state = inspect_run(winner_run), inspect_run(contender_run)
    if (
        winner_state['publication']['state'] != 'ready_for_review'
        or contender_state['publication']['state'] != 'held'
    ):
        raise ScenarioMismatch(
            'winner/contender publication states did not match lock outcome'
        )


def _contend_for_destination(ctx, directory, configs, outputs):
    ready, release = directory / 'winner-ready', directory / 'winner-release'
    child = _start_lock_winner(ctx, directory, configs[0], outputs[0], ready, release)
    contender = _run_lock_contender(
        ctx, directory, configs[1], outputs[1], child, ready, release
    )
    stdout, stderr = child.communicate(timeout=60)
    _verify_lock_result(child.returncode, contender, ready, stdout, stderr)
    return child.returncode, contender


def _start_lock_winner(ctx, directory, config, output, ready, release):
    argv = _push_argv(config, output, directory / 'winner-push-runs')
    child, _ = start_worker(
        argv,
        storage_root=directory / 'unused-storage',
        lock_root=ctx.lock_root,
        fault='upload_before_1',
        ready_file=ready,
        release_file=release,
        backend='localstack',
    )
    return child


def _run_lock_contender(ctx, directory, config, output, child, ready, release):
    try:
        if not _wait_for_winner(child, ready):
            child.kill()
            return None
        return ctx.push(directory, config, output, run_name='contender-push-runs')
    finally:
        release.write_text('continue', encoding='ascii')


def _wait_for_winner(child, ready):
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline and child.poll() is None and not ready.exists():
        time.sleep(0.02)
    return ready.exists()


def _verify_lock_result(winner_code, contender, ready, stdout, stderr):
    if not ready.exists():
        raise ScenarioMismatch(
            f'winning process missed the first-upload barrier: {stdout}\n{stderr}'
        )
    if winner_code != 0 or contender is None or contender['exit'] != 1:
        observed = None if contender is None else contender['exit']
        raise ScenarioMismatch(
            f'lock results expected winner 0/contender 1; '
            f'observed {winner_code}/{observed}; {stderr}'
        )


def _run_paths(outputs):
    paths = []
    for output in outputs:
        reference = json.loads((output / '.workbench-run.json').read_text())['run_path']
        paths.append(Path(reference))
    return paths


def case_11(ctx, alias, directory, source, config, prefix):
    output = directory / 'generated'
    run_path = ctx.generate(directory, config, output)
    result = ctx.push(directory, config, output, mode='lost_ack')
    ctx.require_exit(result, 0, 'push_execute')
    events, error = read_events(run_path)
    if error or not any(
        event.get('disposition') == 'reconciled_written' for event in events
    ):
        raise ScenarioMismatch(
            'lost acknowledgment did not produce reconciled_written journal evidence'
        )
    return {
        'steps': {'push_execute': result['exit']},
        'run_state': inspect_run(run_path)['state'],
        'publication': inspect_run(run_path)['publication']['state'],
        'recovery': 'HEAD confirmed the write after lost acknowledgment',
    }


def case_12(ctx, alias, directory, source, config, prefix):
    output = directory / 'generated'
    run_path = ctx.generate(directory, config, output)
    with (
        patch('workbench_lite.push.time.sleep', return_value=None),
        patch('workbench_lite.push.random.uniform', return_value=0),
    ):
        first = ctx.push(directory, config, output, mode='retry_three')
    ctx.require_exit(first, 1, 'initial_execute')
    resumed = ctx.push(directory, config, output, resume=True)
    ctx.require_exit(resumed, 0, 'push_resume')
    events, _ = read_events(run_path)
    attempts = [
        event.get('attempt')
        for event in events
        if event['kind'] == 'intent' and event.get('stage') == 'push'
    ]
    if not {1, 2, 3, 4}.issubset(set(attempts)):
        raise ScenarioMismatch(
            f'attempt sequence did not continue across invocation: {attempts}'
        )
    return {
        'steps': {'initial_execute': first['exit'], 'push_resume': resumed['exit']},
        'run_state': inspect_run(run_path)['state'],
        'publication': inspect_run(run_path)['publication']['state'],
        'attempts': attempts,
        'recovery': 'same run resumed with new invocation retry limits',
    }


def case_13(ctx, alias, directory, source, config, prefix):
    output = directory / 'generated'
    source_run = ctx.generate(directory, config, output)
    pushed = ctx.push(directory, config, output)
    ctx.require_exit(pushed, 0, 'initial_execute')
    preview, execute, repeat = _run_rollback_cycle(
        ctx, directory, config, source_run, prefix
    )
    resume, removed_locks = _resume_interrupted_rollback(ctx, directory, source, prefix)
    return {
        'steps': {
            'preview': preview['exit'],
            'execute': execute['exit'],
            'repeat': repeat['exit'],
            'interrupted_execute': 'signal',
            'resume': resume['exit'],
        },
        'run_state': 'rollback completed after resume',
        'publication': 'source run held after rollback starts',
        'test_lock_cleanup': removed_locks,
        'recovery': 'repeated rollback resolved the interrupted deletion',
    }


def _run_rollback_cycle(ctx, directory, config, source_run, prefix):
    preview = _rollback_command(ctx, directory, config, source_run, execute=False)
    ctx.require_exit(preview, 0, 'preview')
    remaining = storage_listing(ctx.storage, ctx.args.private_bucket, prefix)
    remaining.update(storage_listing(ctx.storage, ctx.args.public_bucket, prefix))
    if not remaining:
        raise ScenarioMismatch('rollback preview unexpectedly removed objects')
    execute = _rollback_command(ctx, directory, config, source_run, execute=True)
    ctx.require_exit(execute, 0, 'execute')
    repeat = _rollback_command(
        ctx, directory, config, source_run, execute=True, repeat=True
    )
    ctx.require_exit(repeat, 0, 'repeat')
    return preview, execute, repeat


def _rollback_command(
    ctx, directory, config, source_run, *, execute, repeat=False, label_suffix=''
):
    label = 'rollback --execute <run>' if execute else 'rollback <run>'
    if repeat:
        label += ' (repeat)'
    label += label_suffix
    argv = [
        'rollback',
        source_run,
        '--config',
        config,
        '--endpoint-url',
        ENDPOINT,
        '--region',
        'us-east-1',
    ]
    if execute:
        argv.extend(['--execute', '--confirm', source_run.name])
    argv.extend(['--format', 'json'])
    return ctx.command(directory, label, argv)


def _resume_interrupted_rollback(ctx, directory, source, prefix):
    config = make_config(
        source,
        directory / 'interrupted-config',
        ctx.args.private_bucket,
        ctx.args.public_bucket,
        f'{prefix}/interrupted',
    )
    output = directory / 'interrupted-generated'
    run_path = ctx.generate(directory, config, output, 'interrupted-generate-runs')
    pushed = ctx.push(directory, config, output, run_name='interrupted-push-runs')
    ctx.require_exit(pushed, 0, 'interrupted_source_push')
    child = _interrupt_rollback(ctx, directory, config, run_path)
    if child.returncode != -9:
        raise ScenarioMismatch(
            f'rollback SIGKILL expected -9; observed {child.returncode}'
        )
    removed_locks = clear_dead_test_locks(ctx.lock_root, child.pid)
    resume = _rollback_command(
        ctx, directory, config, run_path, execute=True, label_suffix=' (resume)'
    )
    ctx.require_exit(resume, 0, 'resume')
    if not (run_path / 'rollback-hold.json').exists():
        raise ScenarioMismatch('rollback source hold was not retained')
    remaining = storage_listing(
        ctx.storage, ctx.args.private_bucket, f'{prefix}/interrupted'
    )
    remaining.update(
        storage_listing(ctx.storage, ctx.args.public_bucket, f'{prefix}/interrupted')
    )
    if remaining:
        raise ScenarioMismatch(
            f'resumed rollback left rehearsal objects behind: {sorted(remaining)[:3]}'
        )
    return resume, removed_locks


def _interrupt_rollback(ctx, directory, config, run_path):
    ready = directory / 'delete-ready'
    argv = [
        'rollback',
        run_path,
        '--config',
        config,
        '--endpoint-url',
        ENDPOINT,
        '--region',
        'us-east-1',
        '--execute',
        '--confirm',
        run_path.name,
        '--format',
        'json',
    ]
    return ctx.worker(argv, fault='delete_after_1', ready=ready, stop_signal=9)


def case_14(ctx, alias, directory, source, config, prefix):
    output = directory / 'generated'
    run_path = ctx.generate(directory, config, output)
    pushed = ctx.push(directory, config, output)
    ctx.require_exit(pushed, 0, 'initial_execute')
    inventory = json.loads((run_path / 'rollback-inventory.json').read_text())
    target = inventory['new_keys'][0]
    put_bytes(
        ctx.storage.client,
        target['bucket'],
        target['key'],
        b'modified after initial upload',
    )
    result = _run_modified_rollback(ctx, directory, config, run_path)
    _verify_modified_rollback(ctx, run_path, target)
    return {
        'steps': {'rollback_execute': result['exit']},
        'run_state': 'rollback failed; source held',
        'publication': 'held permanently after rollback started',
        'modified_key': target['key'],
        'recovery': 'republish only in a new run',
    }


def _run_modified_rollback(ctx, directory, config, run_path):
    result = ctx.command(
        directory,
        'rollback --execute <run> (modified object)',
        [
            'rollback',
            run_path,
            '--config',
            config,
            '--endpoint-url',
            ENDPOINT,
            '--region',
            'us-east-1',
            '--execute',
            '--confirm',
            run_path.name,
            '--format',
            'json',
        ],
    )
    ctx.require_exit(result, 1, 'rollback_execute')
    return result


def _verify_modified_rollback(ctx, run_path, target):
    if full_identity(ctx.storage, target['bucket'], target['key']) is None:
        raise ScenarioMismatch('rollback deleted an object whose checksum had changed')
    if not (run_path / 'rollback-hold.json').exists():
        raise ScenarioMismatch('failed rollback did not retain a source hold')


SCENARIO_RUNNERS = {
    1: case_01,
    2: case_02,
    3: case_03,
    4: case_04,
    5: case_05,
    6: case_06,
    7: case_07,
    8: case_08,
    9: case_09,
    10: case_10,
    11: case_11,
    12: case_12,
    13: case_13,
    14: case_14,
}
