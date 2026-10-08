import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from workbench_lite.runs import RunJournal, JournalError, inspect_run
from workbench_lite.push import PushResult
from workbench_lite.diagnostics import sanitize


class RunTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)

    def test_canonical_identity_and_field_aware_diagnostics(self):
        name = 'Secret: A history of archives.tif'
        result = PushResult('pdf', name, 'bucket', name, 'uploaded', None, 1)
        self.assertEqual(result.to_dict()['key'], name)
        self.assertEqual(sanitize({'title': name, 'password': 'do-not-save'}),
                         {'title': name, 'password': '[redacted]'})

    def test_intent_without_ack_is_unknown_and_inspection_does_not_write(self):
        run = RunJournal.create(self.root)
        run.start('push')
        op = run.intent('push', {'key': 'Secret: A history', 'bucket': 'test'})
        before = (run.path / 'events.jsonl').read_bytes()
        report = inspect_run(run.path)
        self.assertEqual(report['unknown_operations'][0]['operation_id'], op)
        self.assertEqual(before, (run.path / 'events.jsonl').read_bytes())
        run.result(op, 'push', {'status': 'uploaded', 'key': 'Secret: A history'})
        run.finish(0)
        self.assertEqual(inspect_run(run.path)['state'], 'running')
        run.complete_publication()
        self.assertEqual(inspect_run(run.path)['state'], 'completed')
        self.assertEqual(inspect_run(run.path)['publication']['state'], 'held')
        self.assertEqual(inspect_run(run.path)['unknown_operations'], [])

    def test_truncated_tail_preserves_valid_records(self):
        run = RunJournal.create(self.root)
        run.start('push')
        run.intent('push', {'key': 'one'})
        with (run.path / 'events.jsonl').open('ab') as f:
            f.write(b'{"sequence":')
        report = inspect_run(run.path)
        self.assertTrue(report['journal_error'])
        self.assertEqual(len(report['unknown_operations']), 1)
        with self.assertRaises(JournalError):
            RunJournal.open(run.path)

    def test_fsync_failure_is_fatal(self):
        run = RunJournal.create(self.root)
        with patch('workbench_lite.runs.os.fsync', side_effect=OSError('disk full')):
            with self.assertRaises(JournalError):
                run.start('generate')

    def test_owner_only_files_unique_runs_and_exit_mapping(self):
        for code, state in [(0, 'completed'), (1, 'failed'), (2, 'failed'),
                            (3, 'failed'), (4, 'completed-with-errors'),
                            (130, 'interrupted'), (143, 'interrupted')]:
            run = RunJournal.create(self.root)
            run.start('check')
            run.finish(code)
            self.assertEqual(inspect_run(run.path)['state'], state)
            for path in [run.path, *run.path.iterdir()]:
                self.assertEqual(path.stat().st_mode & 0o077, 0)
        self.assertEqual(len(list(self.root.iterdir())), 7)

    def test_generate_then_push_reuses_identity_and_uploads_required_audit(self):
        import contextlib
        import io
        from test_outcomes import package, Storage
        from cli_test_runner import main
        config = package(self.root)
        csv_path = self.root / 'input.csv'
        output = self.root / 'generated'
        common = ['--config', str(config), '--input-csv', str(csv_path), '--run-dir', str(self.root / 'runs')]
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(main(['generate', *common, '--output-dir', str(output), '--manifests', '--thumbnails']), 0)
        reference = json.loads((output / '.workbench-run.json').read_text())
        run_path = Path(reference['run_path'])
        audit = next(output.rglob('upload-plan.json'))
        self.assertIn(reference['run_id'], str(audit))
        client = Storage()
        rollback_copy = self.root / 'rollback-copy.json'
        with patch('workbench_lite.cli.create_s3_client', return_value=client), contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(main(['push', *common, '--execute', '--generated-dir', str(output),
                                   '--rollback-file', str(rollback_copy)]), 0)
            self.assertEqual(main(['push', *common, '--execute', '--generated-dir', str(output)]), 1)
        events = [json.loads(line) for line in (run_path / 'events.jsonl').read_text().splitlines()]
        uploaded = [e for e in events if e.get('status') == 'uploaded']
        self.assertEqual(uploaded[-1]['role'], 'manifest')
        self.assertTrue(any(e['role'] == 'audit' for e in uploaded))
        self.assertEqual(len(list((self.root / 'runs').iterdir())), 1)
        inventory = json.loads((run_path / 'rollback-inventory.json').read_text())
        self.assertEqual(inventory['run_id'], reference['run_id'])
        self.assertEqual(len(inventory['new_keys']), len(client.written))
        self.assertFalse(inventory['deletion_authorized'])
        self.assertEqual(json.loads(rollback_copy.read_text()), inventory)

    def generate(self):
        import contextlib
        import io
        from test_outcomes import package
        from cli_test_runner import main
        config = package(self.root)
        output = self.root / 'generated'
        args = ['--config', str(config), '--run-dir', str(self.root / 'runs')]
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(main(['generate', *args, '--output-dir', str(output), '--manifests', '--thumbnails']), 0)
        reference = json.loads((output / '.workbench-run.json').read_text())
        return args, output, Path(reference['run_path'])

    def test_dry_run_automatically_writes_preview_inventory(self):
        import contextlib
        import io
        from cli_test_runner import main
        args, output, run_path = self.generate()
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(main(['push', *args, '--dry-run', '--offline', '--generated-dir', str(output)]), 0)
        inventory = json.loads((run_path / 'rollback-inventory.json').read_text())
        self.assertEqual(inventory['mode'], 'preview')
        self.assertEqual(inventory['new_keys'], [])
        self.assertFalse(inventory['deletion_authorized'])

    def test_failed_post_receipt_inventory_refresh_is_rebuildable(self):
        import contextlib
        import io
        from cli_test_runner import main
        from test_outcomes import Storage
        from workbench_lite.rollback_inventory import build_inventory, write_inventory
        args, output, run_path = self.generate()
        calls = 0

        def fail_refresh(*arguments):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise JournalError('simulated final refresh failure')
            return write_inventory(*arguments)

        with patch('workbench_lite.cli.create_s3_client', return_value=Storage()), \
             patch('workbench_lite.cli.write_inventory', side_effect=fail_refresh), \
             contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(main(['push', *args, '--execute', '--generated-dir', str(output)]), 0)
        self.assertEqual(inspect_run(run_path)['publication']['state'], 'ready_for_review')
        self.assertEqual(json.loads((run_path / 'rollback-inventory.json').read_text())['state'], 'incomplete_stage')
        self.assertEqual(build_inventory(run_path)['state'], 'reviewable')

    def test_missing_audit_blocks_all_uploads(self):
        import contextlib
        import io
        from test_outcomes import Storage
        from cli_test_runner import main
        args, output, run_path = self.generate()
        next(output.rglob('upload-plan.json')).unlink()
        client = Storage()
        with patch('workbench_lite.cli.create_s3_client', return_value=client), contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(main(['push', *args, '--execute', '--generated-dir', str(output)]), 1)
        self.assertEqual(client.calls, [])
        self.assertEqual(inspect_run(run_path)['state'], 'failed')

    def test_changed_input_cannot_reuse_plan(self):
        import contextlib
        import io
        from cli_test_runner import main
        args, output, run_path = self.generate()
        before = (run_path / 'upload-plan.json').read_bytes()
        with (self.root / 'input.csv').open('a') as f:
            f.write('Additional,new,Page,parent,2,page.tif\n')
        with patch('workbench_lite.cli.create_s3_client') as client, contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(main(['push', *args, '--execute', '--generated-dir', str(output)]), 1)
        client.assert_not_called()
        self.assertEqual(before, (run_path / 'upload-plan.json').read_bytes())

    def test_new_generation_retains_old_audit_and_uses_new_id(self):
        import contextlib
        import io
        from cli_test_runner import main
        args, output, first = self.generate()
        audit = next(output.rglob('upload-plan.json'))
        before = audit.read_bytes()
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(main(['generate', *args, '--output-dir', str(output), '--manifests', '--thumbnails']), 0)
        second = json.loads((output / '.workbench-run.json').read_text())
        self.assertNotEqual(first.name, second['run_id'])
        self.assertEqual(audit.read_bytes(), before)
        self.assertEqual(len(list(output.rglob('upload-plan.json'))), 2)

    def test_snapshots_and_journal_omit_credentials_preserve_descriptions(self):
        from test_outcomes import package
        from workbench_lite.config import load_config
        config = package(self.root)
        with config.open('a') as f:
            f.write('password: SECRET_CONFIG\nmanifest_base_url: https://user:SECRET_URL@host/path?token=SECRET_QUERY\n')
        (self.root / 'input.csv').write_text('title,password,file\nSecret: A history,SECRET_CSV,https://host/a?token=SECRET_QUERY\n')
        run = RunJournal.create(self.root / 'runs')
        run.inputs(config, load_config(config), self.root / 'input.csv')
        run.start('generate')
        run.intent('generate', {'source_path': 'https://user:SECRET_URL@host/a?token=SECRET_QUERY',
                                'key': 'Secret: A history'})
        evidence = ''.join(p.read_text() for p in run.path.iterdir())
        for secret in ['SECRET_CONFIG', 'SECRET_CSV', 'SECRET_URL', 'SECRET_QUERY']:
            self.assertNotIn(secret, evidence)
        self.assertIn('Secret: A history', evidence)

    def test_abrupt_process_death_retains_unacknowledged_upload(self):
        import subprocess
        script = """from pathlib import Path
import os, signal, sys
from workbench_lite.runs import RunJournal
run = RunJournal.create(Path(sys.argv[1]))
run.start('push')
run.intent('push', {'key': 'first'})
run.result(None, 'push', {'key': 'earlier', 'status': 'uploaded'})
os.kill(os.getpid(), signal.SIGKILL)
"""
        env = {**os.environ, 'PYTHONPATH': str(Path(__file__).resolve().parents[1]) + os.pathsep + str(Path(__file__).resolve().parent)}
        child = subprocess.run([sys.executable, '-c', script, str(self.root)], env=env)
        self.assertNotEqual(child.returncode, 0)
        path = next(self.root.iterdir())
        summary = inspect_run(path)
        self.assertEqual(summary['state'], 'interrupted')
        self.assertTrue(summary['inferred_interruption'])
        self.assertEqual(summary['counts']['uploaded'], 1)
        self.assertEqual(len(summary['unknown_operations']), 1)
        self.assertNotIn('stage_finished', (path / 'events.jsonl').read_text())

    def test_signals_during_real_push_boundary_keep_receipt_and_unknown_intent(self):
        import contextlib
        import io
        import subprocess
        from cli_test_runner import main
        args, output, _ = self.generate()
        script = """import os, signal, sys
import workbench_lite.cli as cli
from cli_test_runner import main as test_main
cli.main = test_main
from test_outcomes import Storage
class Client(Storage):
    def head_bucket(self, **kwargs): return {}
    def list_objects_v2(self, **kwargs): return {"KeyCount": 0}
    def upload_file(self, *args):
        if len(self.calls) == 1:
            os.kill(os.getpid(), getattr(signal, os.environ['FAULT']))
        super().upload_file(*args)
cli.create_s3_client = lambda *args, **kwargs: Client()
sys.exit(cli.entrypoint())
"""
        for fault, code in [('SIGINT', 130), ('SIGTERM', 143), ('SIGKILL', -9)]:
            with self.subTest(fault=fault):
                with contextlib.redirect_stdout(io.StringIO()):
                    self.assertEqual(main(['generate', *args, '--output-dir', str(output), '--manifests', '--thumbnails']), 0)
                run_path = Path(json.loads((output / '.workbench-run.json').read_text())['run_path'])
                env = {**os.environ, 'PYTHONPATH': str(Path(__file__).resolve().parents[1]) + os.pathsep + str(Path(__file__).resolve().parent), 'FAULT': fault}
                child = subprocess.run([sys.executable, '-c', script, 'push', *args, '--execute', '--generated-dir', str(output)],
                                       env=env, capture_output=True, text=True, timeout=10)
                self.assertEqual(child.returncode, code, child.stderr)
                summary = inspect_run(run_path)
                self.assertEqual(summary['state'], 'interrupted')
                self.assertEqual(summary['counts']['uploaded'], 1)
                self.assertEqual(len(summary['unknown_operations']), 1)
                self.assertEqual(summary['inferred_interruption'], fault == 'SIGKILL')

    def test_journal_failure_before_request_and_after_ack_never_claims_success(self):
        import contextlib
        import io
        from test_outcomes import Storage
        from cli_test_runner import main
        from workbench_lite.runs import RunJournal
        args, output, _ = self.generate()
        original = RunJournal.append
        for failing_kind, expected_uploads in [('intent', 0), ('result', 1)]:
            with self.subTest(failing_kind=failing_kind):
                with contextlib.redirect_stdout(io.StringIO()):
                    main(['generate', *args, '--output-dir', str(output), '--manifests', '--thumbnails'])
                run_path = Path(json.loads((output / '.workbench-run.json').read_text())['run_path'])
                def fail(run, kind, **fields):
                    if kind == failing_kind and fields.get('stage') == 'push':
                        raise JournalError('disk full')
                    return original(run, kind, **fields)
                client = Storage()
                with patch.object(RunJournal, 'append', fail), patch('workbench_lite.cli.create_s3_client', return_value=client), contextlib.redirect_stdout(io.StringIO()):
                    self.assertEqual(main(['push', *args, '--execute', '--generated-dir', str(output)]), 1)
                self.assertEqual(len(client.calls), expected_uploads)
                self.assertEqual(inspect_run(run_path)['state'], 'failed')
                self.assertEqual(len(inspect_run(run_path)['unknown_operations']), expected_uploads)

    def test_atomic_summary_replace_failure_preserves_previous_readable_summary(self):
        run = RunJournal.create(self.root)
        run.start('generate')
        before = (run.path / 'summary.json').read_bytes()
        with patch('workbench_lite.runs.os.replace', side_effect=PermissionError('denied')):
            with self.assertRaises(JournalError):
                run.finish(1)
        self.assertEqual((run.path / 'summary.json').read_bytes(), before)
        self.assertEqual(inspect_run(run.path)['state'], 'failed')

    def test_unwritable_audit_aborts_generation(self):
        import contextlib
        import io
        from test_outcomes import package
        from cli_test_runner import main
        from workbench_lite.runs import atomic_write
        config = package(self.root)
        def write(path, content):
            if 'audit' in Path(path).parts:
                raise JournalError('Audit cannot be written')
            return atomic_write(path, content)
        with patch('workbench_lite.runs.atomic_write', side_effect=write), contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(main(['generate', '--config', str(config), '--manifests', '--run-dir', str(self.root/'runs')]), 1)
        run_path = next((self.root/'runs').iterdir())
        self.assertEqual(inspect_run(run_path)['state'], 'failed')

    def test_missing_csv_remains_validation_exit_three(self):
        import contextlib
        import io
        from test_outcomes import package
        from cli_test_runner import main
        config = package(self.root)
        (self.root / 'input.csv').unlink()
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            code = main(['check', '--config', str(config), '--run-dir', str(self.root/'runs'), '--format', 'json'])
        self.assertEqual(code, 3)
        self.assertIn('validation_errors', json.loads(output.getvalue()))

    def test_effective_destination_is_recorded_without_credentials(self):
        import contextlib
        import io
        from types import SimpleNamespace
        from test_outcomes import Storage
        from cli_test_runner import main
        args, output, run_path = self.generate()
        storage = Storage()
        storage.client = SimpleNamespace(meta=SimpleNamespace(endpoint_url='https://host/path?token=SECRET', region_name='local-test'))
        with patch('workbench_lite.cli.create_s3_client', return_value=storage), contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(main(['push', *args, '--execute', '--generated-dir', str(output)]), 0)
        data = (run_path / 'events.jsonl').read_text()
        self.assertIn('local-test', data)
        self.assertNotIn('SECRET', data)

    def test_empty_run_is_incomplete_not_successful(self):
        import contextlib
        import io
        from cli_test_runner import main
        run = RunJournal.create(self.root)
        summary = inspect_run(run.path)
        self.assertTrue(summary['incomplete_stage'])
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(main(['inspect-run', str(run.path)]), 1)

    def test_invalid_execute_flags_precede_existing_reference_validation(self):
        import contextlib
        import io
        from cli_test_runner import main
        args, output, _ = self.generate()
        (self.root / 'input.csv').unlink()
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as caught:
            main(['push', *args, '--execute', '--allow-dummy-files', '--generated-dir', str(output)])
        self.assertEqual(caught.exception.code, 2)

    def test_failed_dry_run_does_not_invalidate_successful_generation(self):
        import contextlib
        import io
        from test_outcomes import Storage
        from cli_test_runner import main
        args, output, run_path = self.generate()
        thumbnail = next(output.rglob('thumbnail.jpg'))
        original = thumbnail.read_bytes()
        thumbnail.unlink()
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(main(['push', *args, '--dry-run', '--offline', '--generated-dir', str(output)]), 1)
        self.assertEqual(inspect_run(run_path)['state'], 'failed')
        thumbnail.write_bytes(original)
        client = Storage()
        with patch('workbench_lite.cli.create_s3_client', return_value=client), contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(main(['push', *args, '--execute', '--generated-dir', str(output)]), 0)
            self.assertEqual(main(['push', *args, '--execute', '--generated-dir', str(output)]), 1)
        self.assertEqual(len(client.calls), 6)

    def test_interrupted_dry_run_does_not_invalidate_successful_generation(self):
        import contextlib
        import io
        from test_outcomes import Storage
        from cli_test_runner import main
        args, output, _ = self.generate()
        for interruption in [KeyboardInterrupt(), SystemExit(143)]:
            with self.subTest(interruption=type(interruption).__name__):
                with contextlib.redirect_stdout(io.StringIO()):
                    self.assertEqual(main(['generate', *args, '--output-dir', str(output), '--manifests', '--thumbnails']), 0)
                with patch('workbench_lite.cli.push_upload_plan', side_effect=interruption), contextlib.redirect_stdout(io.StringIO()):
                    with self.assertRaises(type(interruption)):
                        main(['push', *args, '--dry-run', '--offline', '--generated-dir', str(output)])
                client = Storage()
                with patch('workbench_lite.cli.create_s3_client', return_value=client), contextlib.redirect_stdout(io.StringIO()):
                    self.assertEqual(main(['push', *args, '--execute', '--generated-dir', str(output)]), 0)
                self.assertEqual(len(client.calls), 6)

    def test_newer_incomplete_generation_cannot_reuse_earlier_success(self):
        import contextlib
        import io
        from cli_test_runner import main
        args, output, run_path = self.generate()
        run = RunJournal.open(run_path)
        run.start('generate')
        # Even a later completed read-only stage cannot certify generation.
        run.start('push-dry-run')
        run.finish(0)
        with patch('workbench_lite.cli.create_s3_client', side_effect=AssertionError('Client must not be created')) as client, contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(main(['push', *args, '--execute', '--generated-dir', str(output)]), 1)
        client.assert_not_called()


if __name__ == '__main__':
    unittest.main()
