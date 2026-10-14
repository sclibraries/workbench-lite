"""Synthetic end-to-end coverage for the documented rehearsal scenarios."""

import errno
import json
from pathlib import Path
import signal
import tempfile
import time
import unittest
from unittest.mock import patch

import csv
from PIL import Image

from rehearsal_harness import (
    SCENARIOS_BY_NUMBER,
    TEST_ENDPOINT,
    TEST_PRIVATE_BUCKET,
    TEST_PUBLIC_BUCKET,
    FileS3Client,
    cli_client_factory,
    clear_dead_test_locks,
    invoke_cli,
    make_synthetic_package,
    spawn_worker,
    start_worker,
)
from workbench_lite.runs import inspect_run, read_events


class RehearsalScenarioTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.storage_root = self.root / 'storage'
        self.store = FileS3Client(self.storage_root)
        self.lock_root = self.root / 'locks'
        self.lock_root.mkdir(mode=0o700)
        self.package_root = self.root / 'package'
        self.config = make_synthetic_package(self.package_root)
        self.store.seed(
            TEST_PRIVATE_BUCKET,
            'sentinels/preexisting.txt',
            b'fixed preexisting sentinel',
        )
        self.store.seed(TEST_PUBLIC_BUCKET, 'sentinels/unrelated.txt', b'fixed unrelated sentinel')
        self.sentinels = {
            (TEST_PRIVATE_BUCKET, 'sentinels/preexisting.txt'): self.store.listing(
                TEST_PRIVATE_BUCKET, 'sentinels/'
            )['sentinels/preexisting.txt']['sha256'],
            (TEST_PUBLIC_BUCKET, 'sentinels/unrelated.txt'): self.store.listing(
                TEST_PUBLIC_BUCKET, 'sentinels/'
            )['sentinels/unrelated.txt']['sha256'],
        }

    def assert_contract_case(self, number):
        self.assertEqual(SCENARIOS_BY_NUMBER[number]['number'], number)

    def assert_sentinels(self):
        for (bucket, key), checksum in self.sentinels.items():
            found = self.store.listing(bucket, key)
            self.assertEqual(found[key]['sha256'], checksum, f'sentinel changed: {bucket}/{key}')

    def invoke(self, *arguments, mode=None, extra_patches=()):
        return invoke_cli(
            arguments,
            cli_client_factory(self.store, mode),
            self.lock_root,
            extra_patches=extra_patches,
        )

    def generate(self, *, config=None, output_name='generated', run_name='runs'):
        config = config or self.config
        output = self.root / output_name
        result = self.invoke(
            'generate',
            '--config',
            config,
            '--output-dir',
            output,
            '--manifests',
            '--thumbnails',
            '--format',
            'json',
            '--run-dir',
            self.root / run_name,
        )
        self.assertEqual(result['exit'], 0, result['stdout'] + result['stderr'])
        reference = json.loads((output / '.workbench-run.json').read_text())
        return output, Path(reference['run_path'])

    def push(self, output, *, resume=False, mode=None, extra_patches=(), run_dir_name='runs'):
        arguments = [
            'push',
            '--config',
            self.config,
            '--execute',
            '--generated-dir',
            output,
            '--endpoint-url',
            TEST_ENDPOINT,
            '--region',
            'us-east-1',
            '--format',
            'json',
            '--run-dir',
            self.root / run_dir_name,
        ]
        if resume:
            arguments.append('--resume')
        return self.invoke(*arguments, mode=mode, extra_patches=extra_patches)

    def test_scenario_01_clean_run_and_inventory(self):
        self.assert_contract_case(1)
        output, run_path = self.generate()
        pushed = self.push(output)
        self.assertEqual(pushed['exit'], 0, pushed['stdout'] + pushed['stderr'])
        self.assertEqual(inspect_run(run_path)['publication']['state'], 'ready_for_review')
        inventory = self.invoke('rollback-inventory', run_path, '--format', 'json')
        self.assertEqual(inventory['exit'], 0, inventory['stdout'] + inventory['stderr'])
        self.assertFalse(inventory['payload']['deletion_authorized'])
        self.assertTrue(self.store.listing(TEST_PRIVATE_BUCKET, 'run/'))
        self.assertTrue(self.store.listing(TEST_PUBLIC_BUCKET, 'run/'))
        self.assert_sentinels()

    def test_scenario_02_missing_source_and_corrupt_image(self):
        self.assert_contract_case(2)
        page = self.package_root / 'page-1.tif'
        page.unlink()
        missing = self.invoke(
            'check',
            '--config',
            self.config,
            '--format',
            'json',
            '--run-dir',
            self.root / 'missing-runs',
        )
        self.assertEqual(missing['exit'], 3)
        page.write_bytes(b'not a TIFF')
        corrupt = self.invoke(
            'generate',
            '--config',
            self.config,
            '--format',
            'json',
            '--run-dir',
            self.root / 'corrupt-runs',
        )
        self.assertEqual(corrupt['exit'], 3)
        self.assertEqual(
            set(self.store.listing(TEST_PRIVATE_BUCKET)), {'sentinels/preexisting.txt'}
        )
        self.assertEqual(set(self.store.listing(TEST_PUBLIC_BUCKET)), {'sentinels/unrelated.txt'})
        self.assert_sentinels()

    def test_scenario_03_denied_storage(self):
        self.assert_contract_case(3)
        output, run_path = self.generate()
        result = self.push(output, mode='denied')
        self.assertEqual(result['exit'], 1)
        self.assertEqual(inspect_run(run_path)['publication']['state'], 'held')
        self.assertEqual(len(self.store.put_calls), 0)
        self.assert_sentinels()

    def test_scenario_04_unavailable_endpoint(self):
        self.assert_contract_case(4)
        output, run_path = self.generate()
        result = self.push(output, mode='unavailable')
        self.assertEqual(result['exit'], 1)
        self.assertEqual(inspect_run(run_path)['publication']['state'], 'held')
        self.assertEqual(len(self.store.put_calls), 0)
        self.assert_sentinels()

    def test_scenario_05_journal_fsync_enospc(self):
        self.assert_contract_case(5)
        output, run_path = self.generate()
        from workbench_lite.runs import RunJournal

        original = RunJournal.intent

        def fail_push_intent(journal, stage, identity, *, operation_id=None, attempt=1):
            if stage == 'push':
                with patch(
                    'workbench_lite.runs.os.fsync',
                    side_effect=OSError(errno.ENOSPC, 'synthetic full disk'),
                ):
                    return original(
                        journal,
                        stage,
                        identity,
                        operation_id=operation_id,
                        attempt=attempt,
                    )
            return original(journal, stage, identity, operation_id=operation_id, attempt=attempt)

        replacement = patch.object(RunJournal, 'intent', new=fail_push_intent)
        result = self.push(output, extra_patches=(replacement,))
        self.assertEqual(result['exit'], 1)
        summary = inspect_run(run_path)
        self.assertEqual(summary['state'], 'failed')
        self.assertEqual(len(self.store.put_calls), 0)
        self.assert_sentinels()

    def test_scenario_06_sigint_and_sigterm_mid_batch(self):
        self.assert_contract_case(6)
        signal_cases = (
            ('interrupt', signal.SIGINT, 130),
            ('terminate', signal.SIGTERM, 143),
        )
        for name, stop_signal, expected in signal_cases:
            with self.subTest(name=name):
                self._run_signal_interruption(name, stop_signal, expected)
        self.assert_sentinels()

    def _run_signal_interruption(self, name, stop_signal, expected):
        config = make_synthetic_package(
            self.root / f'{name}-package', pages=2, prefix=f'{name}-prefix'
        )
        output = self.root / f'{name}-generated'
        generated = self.invoke(
            'generate',
            '--config',
            config,
            '--output-dir',
            output,
            '--manifests',
            '--thumbnails',
            '--format',
            'json',
            '--run-dir',
            self.root / f'{name}-generation-runs',
        )
        self.assertEqual(generated['exit'], 0)
        child = spawn_worker(
            self._push_arguments(config, output, f'{name}-runs'),
            storage_root=self.storage_root,
            lock_root=self.lock_root,
            fault='upload_before_2',
            ready_file=self.root / f'{name}-ready',
            kill_signal=stop_signal,
        )
        self.assertEqual(child.returncode, expected, child.stderr)
        self._assert_interrupted_upload(output)

    def _assert_interrupted_upload(self, output):
        run_path = Path(json.loads((output / '.workbench-run.json').read_text())['run_path'])
        summary = inspect_run(run_path)
        self.assertEqual(summary['state'], 'interrupted')
        self.assertEqual(summary['counts'].get('uploaded'), 1)
        self.assertTrue(summary['unknown_operations'])
        self.assertEqual(summary['publication']['state'], 'held')

    def _push_arguments(self, config, output, run_name):
        return [
            'push',
            '--config',
            config,
            '--execute',
            '--generated-dir',
            output,
            '--endpoint-url',
            TEST_ENDPOINT,
            '--region',
            'us-east-1',
            '--format',
            'json',
            '--run-dir',
            self.root / run_name,
        ]

    def test_scenario_07_sigkill_reconcile_and_resume(self):
        self.assert_contract_case(7)
        output, run_path = self.generate()
        child = spawn_worker(
            self._push_arguments(self.config, output, 'push-runs'),
            storage_root=self.storage_root,
            lock_root=self.lock_root,
            fault='upload_after_1',
            ready_file=self.root / 'kill-ready',
            kill_signal=signal.SIGKILL,
        )
        self.assertEqual(child.returncode, -signal.SIGKILL)
        removed = clear_dead_test_locks(self.lock_root, child.pid)
        self._assert_dead_locks_removed(removed, 'push')
        self.assertEqual(inspect_run(run_path)['state'], 'interrupted')
        self._reconcile_and_resume_push(output, run_path)
        self.assert_sentinels()

    def _assert_dead_locks_removed(self, removed, operation):
        self.assertTrue(
            removed,
            f'lock root contents after {operation} SIGKILL: {list(self.lock_root.iterdir())}',
        )

    def _reconcile_and_resume_push(self, output, run_path):
        reconciled = self.invoke('reconcile', run_path, '--format', 'json')
        self.assertEqual(reconciled['exit'], 0, reconciled['stdout'] + reconciled['stderr'])
        resumed = self.push(output, resume=True)
        self.assertEqual(resumed['exit'], 0, resumed['stdout'] + resumed['stderr'])
        self.assertEqual(inspect_run(run_path)['publication']['state'], 'ready_for_review')
        self._assert_single_put_intent(run_path)

    def _assert_single_put_intent(self, run_path):
        events, error = read_events(run_path)
        self.assertIsNone(error)
        first_push = next(
            event for event in events if event['kind'] == 'intent' and event.get('stage') == 'push'
        )
        self.assertEqual(
            sum(
                event['kind'] == 'intent'
                and event.get('stage') == 'push'
                and event.get('bucket') == first_push['bucket']
                and event.get('key') == first_push['key']
                for event in events
            ),
            1,
        )

    def test_scenario_08_unchanged_and_additive_versions(self):
        self.assert_contract_case(8)
        config = make_synthetic_package(self.package_root, pages=1)
        manifest_path, original_manifest = self._publish_unchanged_versions(config)
        keys_before_additive = self._stored_keys()
        self._append_synthetic_page()
        self._assert_additive_version_held(
            config, manifest_path, original_manifest, keys_before_additive
        )
        self.assert_sentinels()

    def _publish_unchanged_versions(self, config):
        first_output, first_run = self.generate(
            config=config, output_name='first-generated', run_name='first-runs'
        )
        first = self.push(first_output)
        self.assertEqual(first['exit'], 0, first['stdout'] + first['stderr'])
        entries = json.loads((first_run / 'upload-plan.json').read_text())['entries']
        manifest_key = next(entry['key'] for entry in entries if entry['role'] == 'manifest')
        manifest_path = self.store._path(TEST_PUBLIC_BUCKET, manifest_key)
        original_manifest = manifest_path.read_bytes()

        unchanged_output = self.root / 'unchanged-generated'
        generated = self.invoke(
            'generate',
            '--config',
            config,
            '--output-dir',
            unchanged_output,
            '--manifests',
            '--thumbnails',
            '--format',
            'json',
            '--run-dir',
            self.root / 'unchanged-runs',
        )
        self.assertEqual(generated['exit'], 0)
        unchanged = self.push(unchanged_output, run_dir_name='unused-runs')
        self.assertEqual(unchanged['exit'], 0, unchanged['stdout'] + unchanged['stderr'])
        return manifest_path, original_manifest

    def _stored_keys(self):
        return set(self.store.listing(TEST_PRIVATE_BUCKET)) | set(
            self.store.listing(TEST_PUBLIC_BUCKET)
        )

    def _append_synthetic_page(self):
        image_path = self.package_root / 'page-2.tif'
        Image.new('RGB', (12, 10), (90, 20, 100)).save(image_path)
        with (self.package_root / 'input.csv').open('a', encoding='utf-8', newline='') as handle:
            csv.writer(handle).writerow(
                ['Synthetic page 2', 'page-2', 'Page', 'package', '2', image_path.name]
            )

    def _assert_additive_version_held(self, config, manifest_path, original_manifest, keys_before):
        output = self.root / 'additive-generated'
        generated = self.invoke(
            'generate',
            '--config',
            config,
            '--output-dir',
            output,
            '--manifests',
            '--thumbnails',
            '--format',
            'json',
            '--run-dir',
            self.root / 'additive-runs',
        )
        self.assertEqual(generated['exit'], 0)
        additive = self.push(output, run_dir_name='unused-additive-runs')
        self.assertEqual(additive['exit'], 1)
        self.assertEqual(manifest_path.read_bytes(), original_manifest)
        run_path = Path(json.loads((output / '.workbench-run.json').read_text())['run_path'])
        self.assertEqual(inspect_run(run_path)['publication']['state'], 'held')
        self.assertTrue(self._stored_keys() - keys_before)

    def test_scenario_09_conflicting_existing_content(self):
        self.assert_contract_case(9)
        output, run_path = self.generate()
        plan = json.loads((run_path / 'upload-plan.json').read_text())['entries']
        target = next(item for item in plan if item['role'] == 'master_tiff')
        self.store.seed(target['bucket'], target['key'], b'conflicting synthetic bytes')
        result = self.push(output)
        self.assertEqual(result['exit'], 1)
        self.assertEqual(len(self.store.put_calls), 0)
        self.assertEqual(inspect_run(run_path)['publication']['state'], 'held')
        self.assert_sentinels()

    def test_scenario_10_destination_lock_refuses_second_process(self):
        self.assert_contract_case(10)
        outputs = self._generate_lock_outputs()
        self._assert_lock_contention(outputs)
        self.assert_sentinels()

    def _generate_lock_outputs(self):
        outputs = []
        for name in ('first', 'second'):
            output, _ = self.generate(
                output_name=f'{name}-generated', run_name=f'{name}-generation-runs'
            )
            outputs.append(output)
        return outputs

    def _assert_lock_contention(self, outputs):
        ready = self.root / 'winner-ready'
        release = self.root / 'winner-release'
        child = self._start_lock_winner(outputs[0], ready, release)
        try:
            self._invoke_refused_contender(outputs[1], child, ready)
        finally:
            release.write_text('continue', encoding='ascii')
        stdout, stderr = child.communicate(timeout=20)
        self.assertEqual(child.returncode, 0, stdout + stderr)

    def _start_lock_winner(self, output, ready, release):
        child, _ = start_worker(
            self._push_arguments(self.config, output, 'first-push-runs'),
            storage_root=self.storage_root,
            lock_root=self.lock_root,
            fault='upload_before_1',
            ready_file=ready,
            release_file=release,
        )
        return child

    def _invoke_refused_contender(self, output, child, ready):
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline and child.poll() is None and not ready.exists():
            time.sleep(0.02)
        self.assertTrue(ready.exists(), 'winner did not reach first upload')
        result = self.invoke(*self._push_arguments(self.config, output, 'second-push-runs'))
        self.assertEqual(result['exit'], 1)
        return result

    def test_scenario_11_lost_acknowledgment_is_reconciled(self):
        self.assert_contract_case(11)
        output, run_path = self.generate()
        result = self.push(output, mode='lost_ack')
        self.assertEqual(result['exit'], 0, result['stdout'] + result['stderr'])
        events, error = read_events(run_path)
        self.assertIsNone(error)
        reconciled = [event for event in events if event.get('disposition') == 'reconciled_written']
        self.assertTrue(reconciled)
        first_identity = (reconciled[0]['bucket'], reconciled[0]['key'])
        self.assertEqual(self.store.put_calls.count(first_identity), 1)
        self.assertEqual(inspect_run(run_path)['publication']['state'], 'ready_for_review')
        self.assert_sentinels()

    def test_scenario_12_retry_exhaustion_then_resume(self):
        self.assert_contract_case(12)
        output, run_path = self.generate()
        with (
            patch('workbench_lite.push.time.sleep', return_value=None),
            patch('workbench_lite.push.random.uniform', return_value=0),
        ):
            initial = self.push(output, mode='retry_three')
        self.assertEqual(initial['exit'], 1)
        self.assertEqual(inspect_run(run_path)['publication']['state'], 'held')
        resumed = self.push(output, resume=True)
        self.assertEqual(resumed['exit'], 0, resumed['stdout'] + resumed['stderr'])
        events, error = read_events(run_path)
        self.assertIsNone(error)
        attempts = [
            event['attempt']
            for event in events
            if event['kind'] == 'intent' and event.get('stage') == 'push'
        ]
        self.assertIn(1, attempts)
        self.assertIn(2, attempts)
        self.assertIn(3, attempts)
        self.assertIn(4, attempts)
        self.assertEqual(inspect_run(run_path)['publication']['state'], 'ready_for_review')
        self.assert_sentinels()

    def test_scenario_13_rollback_preview_execute_repeat_and_resume(self):
        self.assert_contract_case(13)
        output, source_run = self.generate()
        successful = self.push(output)
        self.assertEqual(successful['exit'], 0)
        self._assert_rollback_preview_execute_repeat(source_run)
        self._assert_interrupted_rollback_resumes()
        self.assert_sentinels()

    def _assert_rollback_preview_execute_repeat(self, source_run):
        preview = self.invoke(*self._rollback_arguments(source_run))
        self.assertEqual(preview['exit'], 0)
        self.assertEqual(preview['payload']['deleted_count'], 0)
        execute = self._execute_rollback(source_run)
        self.assertEqual(execute['exit'], 0, execute['stdout'] + execute['stderr'])
        repeated = self._execute_rollback(source_run)
        self.assertEqual(repeated['exit'], 0)
        self.assertGreater(repeated['payload']['already_deleted_count'], 0)
        self.assertTrue((source_run / 'rollback-hold.json').exists())

    def _rollback_arguments(self, run_path, *, execute=False):
        arguments = [
            'rollback',
            run_path,
            '--config',
            self.config,
            '--endpoint-url',
            TEST_ENDPOINT,
            '--region',
            'us-east-1',
        ]
        if execute:
            arguments.extend(['--execute', '--confirm', run_path.name])
        arguments.extend(['--format', 'json'])
        return arguments

    def _execute_rollback(self, run_path):
        return self.invoke(*self._rollback_arguments(run_path, execute=True))

    def _assert_interrupted_rollback_resumes(self):
        output, run_path = self.generate(
            output_name='interrupt-generated', run_name='interrupt-generation-runs'
        )
        pushed = self.push(output, run_dir_name='interrupt-push-runs')
        self.assertEqual(pushed['exit'], 0)
        child = self._kill_partial_rollback(run_path)
        self.assertEqual(child.returncode, -signal.SIGKILL)
        removed = clear_dead_test_locks(self.lock_root, child.pid)
        self._assert_dead_locks_removed(removed, 'rollback')
        resumed = self._execute_rollback(run_path)
        self.assertEqual(resumed['exit'], 0, resumed['stdout'] + resumed['stderr'])
        self.assertTrue((run_path / 'rollback-hold.json').exists())

    def _kill_partial_rollback(self, run_path):
        return spawn_worker(
            self._rollback_arguments(run_path, execute=True),
            storage_root=self.storage_root,
            lock_root=self.lock_root,
            fault='delete_after_1',
            ready_file=self.root / 'rollback-ready',
            kill_signal=signal.SIGKILL,
        )

    def test_scenario_14_modified_object_stays_and_source_is_held(self):
        self.assert_contract_case(14)
        output, source_run = self.generate()
        successful = self.push(output)
        self.assertEqual(successful['exit'], 0)
        inventory = json.loads((source_run / 'rollback-inventory.json').read_text())
        candidate = inventory['new_keys'][0]
        self.store.seed(candidate['bucket'], candidate['key'], b'modified after upload')
        result = self.invoke(
            'rollback',
            source_run,
            '--config',
            self.config,
            '--endpoint-url',
            TEST_ENDPOINT,
            '--region',
            'us-east-1',
            '--execute',
            '--confirm',
            source_run.name,
            '--format',
            'json',
        )
        self.assertEqual(result['exit'], 1)
        self.assertTrue(self.store.object_exists(candidate['bucket'], candidate['key']))
        self.assertTrue((source_run / 'rollback-hold.json').exists())
        self.assertEqual(inspect_run(source_run)['publication']['state'], 'held')
        self.assert_sentinels()


if __name__ == '__main__':
    unittest.main()
