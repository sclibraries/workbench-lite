"""Publication gates protect existing keys and require complete durable evidence."""
import contextlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from cli_test_runner import main
from test_outcomes import Storage, package
from workbench_lite.check import run_check
from workbench_lite.push import push_upload_plan
from workbench_lite.runs import inspect_run, read_events, RunJournal, JournalError


class PublicationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.config = package(self.root)
        self.output = self.root/'generated'
        self.args = ['--config', str(self.config), '--run-dir', str(self.root/'runs')]
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(main(['generate', *self.args, '--output-dir', str(self.output), '--manifests', '--thumbnails']), 0)
        self.run = Path(json.loads((self.output/'.workbench-run.json').read_text())['run_path'])
        self.plan = run_check(self.config, run_id=self.run.name).upload_plan

    def execute(self, store):
        with patch('workbench_lite.cli.create_s3_client', return_value=store), contextlib.redirect_stdout(io.StringIO()):
            return main(['push', *self.args, '--execute', '--generated-dir', str(self.output)])

    def test_existing_serving_key_blocks_entire_batch_without_writes(self):
        store = Storage()
        key = next(e.key for e in self.plan if e.role == 'service_jpg')
        store.written[key] = b'existing live image'
        self.assertEqual(self.execute(store), 1)
        self.assertEqual(store.calls, [])
        self.assertEqual(store.written[key], b'existing live image')

    def test_remote_mismatch_stops_before_manifest_and_holds_publication(self):
        store = Storage()
        store.verify_file = lambda *args: False
        self.assertEqual(self.execute(store), 1)
        self.assertFalse(any(e.key in store.calls for e in self.plan if e.role == 'manifest'))
        self.assertEqual(inspect_run(self.run)['publication']['state'], 'held')

    def test_success_has_inspectable_complete_batch_receipt(self):
        store = Storage()
        self.assertEqual(self.execute(store), 0)
        publication = inspect_run(self.run).get('publication', {})
        self.assertEqual(publication.get('state'), 'ready_for_review')
        self.assertEqual(publication['receipt']['verified_count'], len(self.plan))
        self.assertEqual(publication['receipt']['publication_owner'], 'workbench-lite')

    def multi_package(self):
        csv_path = self.root/'input.csv'
        with csv_path.open('a') as handle:
            for number in range(2, 5):
                handle.write(f'Parent {number},parent{number},Paged Content,,,parent.pdf\n')
                handle.write(f'Page {number},page{number},Page,parent{number},1,page.tif\n')
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(main(['generate', *self.args, '--output-dir', str(self.output), '--manifests', '--thumbnails']), 0)
        self.run = Path(json.loads((self.output/'.workbench-run.json').read_text())['run_path'])
        self.plan = run_check(self.config, run_id=self.run.name).upload_plan

    def test_multi_manifest_batch_gets_receipt_only_when_every_object_succeeds(self):
        self.multi_package()
        store = Storage()
        self.assertEqual(self.execute(store), 0)
        manifests = [e.key for e in self.plan if e.role == 'manifest']
        self.assertEqual(store.calls[-4:], manifests)
        publication = inspect_run(self.run)['publication']
        self.assertEqual(publication['state'], 'ready_for_review')
        self.assertEqual(publication['receipt']['manifest_count'], 4)

    def test_partial_manifest_phase_holds_all_objects_and_stops_later_manifests(self):
        self.multi_package()
        manifests = [e.key for e in self.plan if e.role == 'manifest']
        store = Storage(manifests[2])
        self.assertEqual(self.execute(store), 1)
        self.assertTrue(all(key in store.written for key in manifests[:2]))
        self.assertNotIn(manifests[3], store.calls)
        summary = inspect_run(self.run)
        self.assertEqual(summary['publication']['state'], 'held')
        self.assertTrue(summary['unknown_operations'])

    def test_final_receipt_failure_never_authorizes_handoff(self):
        original = RunJournal.append
        def fail(run, kind, **fields):
            if fields.get('publication_receipt'):
                raise JournalError('injected receipt failure')
            return original(run, kind, **fields)
        with patch.object(RunJournal, 'append', fail):
            with self.assertRaises(SystemExit) as error:
                self.execute(Storage())
        self.assertEqual(error.exception.code, 1)
        self.assertEqual(inspect_run(self.run)['publication']['state'], 'held')

    def test_manifest_lost_ack_is_held_and_uncertain(self):
        store = Storage(next(e.key for e in self.plan if e.role == 'manifest'))
        self.assertEqual(self.execute(store), 1)
        summary = inspect_run(self.run)
        self.assertEqual(summary.get('publication', {}).get('state'), 'held')
        self.assertTrue(summary['unknown_operations'])

    def test_summary_failure_after_uploads_holds_even_with_success_terminal_event(self):
        original = RunJournal.summarize
        def fail(run):
            if run.stage == 'push' and any(e.get('verified_batch') for e in read_events(run.path)[0]):
                raise JournalError('injected summary failure')
            return original(run)
        with patch.object(RunJournal, 'summarize', fail):
            with self.assertRaises(SystemExit):
                self.execute(Storage())
        self.assertEqual(inspect_run(self.run)['publication']['state'], 'held')

    def test_verification_receipt_failure_stops_writes_and_keeps_upload_ack(self):
        original = RunJournal.append
        def fail(run, kind, **fields):
            if kind == 'result' and fields.get('stage') == 'verify-upload':
                raise JournalError('injected verification receipt failure')
            return original(run, kind, **fields)
        store = Storage()
        with patch.object(RunJournal, 'append', fail):
            self.assertEqual(self.execute(store), 1)
        self.assertEqual(len(store.calls), 1)
        summary = inspect_run(self.run)
        self.assertEqual(summary['counts']['uploaded'], 1)
        self.assertEqual(summary['publication']['state'], 'held')
        self.assertEqual(len(summary['unknown_operations']), 1)

    def test_manifest_verification_failure_holds_already_addressable_manifest(self):
        store = Storage()
        original = store.verify_file
        manifest = next(e.key for e in self.plan if e.role == 'manifest')
        store.verify_file = lambda bucket, key, checksum, size: False if key == manifest else original(bucket, key, checksum, size)
        self.assertEqual(self.execute(store), 1)
        self.assertIn(manifest, store.written)
        self.assertEqual(inspect_run(self.run)['publication']['state'], 'held')

    def test_no_receipt_for_dry_run_or_tampered_plan(self):
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(main(['push', *self.args, '--dry-run', '--offline', '--generated-dir', str(self.output)]), 0)
        self.assertEqual(inspect_run(self.run)['publication']['state'], 'held')
        self.assertEqual(self.execute(Storage()), 0)
        (self.run/'upload-plan.json').write_text('{}')
        self.assertEqual(inspect_run(self.run)['publication']['state'], 'held')

    def test_oversized_source_is_rejected_before_any_write(self):
        store = Storage()
        with patch('workbench_lite.push.MAX_SINGLE_PUT_BYTES', 1):
            self.assertEqual(self.execute(store), 1)
        self.assertEqual(store.calls, [])

    def test_lock_release_failure_cannot_authorize_handoff(self):
        from workbench_lite.locks import WriterLocks, LockError
        original = WriterLocks.release
        def fail(locks):
            original(locks)
            raise LockError('injected release failure')
        with patch.object(WriterLocks, 'release', fail):
            with self.assertRaises(SystemExit):
                self.execute(Storage())
        self.assertEqual(inspect_run(self.run)['publication']['state'], 'held')

    def test_completion_fsync_failure_cannot_authorize_handoff(self):
        original = RunJournal.append
        def fail(run, kind, **fields):
            if fields.get('publication_receipt'):
                with patch('workbench_lite.runs.os.fsync', side_effect=OSError('injected sync failure')):
                    return original(run, kind, **fields)
            return original(run, kind, **fields)
        with patch.object(RunJournal, 'append', fail):
            with self.assertRaises(SystemExit):
                self.execute(Storage())
        self.assertEqual(inspect_run(self.run)['publication']['state'], 'held')

    def test_uncertain_commit_cannot_authorize_even_if_truncation_fails(self):
        original = RunJournal.append
        real_open = Path.open
        def no_truncate(path, mode='r', *args, **kwargs):
            if path.name == 'events.jsonl' and mode == 'r+b':
                raise OSError('cannot truncate')
            return real_open(path, mode, *args, **kwargs)
        def fail(run, kind, **fields):
            if fields.get('publication_receipt'):
                with patch('workbench_lite.runs.os.fsync', side_effect=OSError('sync failed')), patch.object(Path, 'open', no_truncate):
                    return original(run, kind, **fields)
            return original(run, kind, **fields)
        with patch.object(RunJournal, 'append', fail):
            with self.assertRaises(SystemExit):
                self.execute(Storage())
        self.assertTrue((self.run/'publication-pending').exists())
        self.assertEqual(inspect_run(self.run)['publication']['state'], 'held')

    def test_destination_read_failure_holds_before_uploads(self):
        store = Storage()
        store.object_exists = lambda *args: (_ for _ in ()).throw(OSError('denied'))
        self.assertEqual(self.execute(store), 1)
        self.assertEqual(store.calls, [])
        self.assertEqual(inspect_run(self.run)['publication']['state'], 'held')

    def test_remote_read_failure_stops_after_acknowledged_upload(self):
        store = Storage()
        store.verify_file = lambda *args: (_ for _ in ()).throw(OSError('connection reset'))
        self.assertEqual(self.execute(store), 1)
        self.assertEqual(len(store.calls), 1)
        self.assertEqual(inspect_run(self.run)['publication']['state'], 'held')

    def test_hold_marker_removal_failure_cannot_authorize_handoff(self):
        original = Path.unlink
        def fail(path, *args, **kwargs):
            if path.name == 'publication-pending':
                raise OSError('cannot clear hold')
            return original(path, *args, **kwargs)
        with patch.object(Path, 'unlink', fail):
            with self.assertRaises(SystemExit):
                self.execute(Storage())
        self.assertTrue((self.run/'publication-pending').exists())
        self.assertEqual(inspect_run(self.run)['publication']['state'], 'held')

    def test_restored_hold_marker_overrides_a_complete_receipt(self):
        self.assertEqual(self.execute(Storage()), 0)
        (self.run/'publication-pending').write_text('{}')
        self.assertEqual(inspect_run(self.run)['publication']['state'], 'held')

    def test_released_lock_gap_rejects_new_execute_and_dry_run_before_journal_write(self):
        import os
        import subprocess
        import sys
        import time
        shared = self.root/'shared-locks'
        shared.mkdir(mode=0o770)
        ready, release = self.root/'ready', self.root/'release'
        package_root = Path(__file__).resolve().parents[1]
        env = dict(os.environ, PYTHONPATH=str(package_root)+os.pathsep+str(package_root/'tests'),
                   WBL_TEST_LOCK_DIR=str(shared), READY=str(ready), RELEASE=str(release))
        script = """import os, time
from pathlib import Path
from cli_test_runner import cli, main
from workbench_lite.runs import RunJournal
from test_outcomes import Storage
original = RunJournal.complete_publication
def paused(run):
    if run.stage == 'push':
        assert not list(Path(os.environ['WBL_TEST_LOCK_DIR']).iterdir())
        Path(os.environ['READY']).touch()
        deadline = time.monotonic() + 15
        while not Path(os.environ['RELEASE']).exists():
            if time.monotonic() > deadline: raise RuntimeError('test timeout')
            time.sleep(.02)
    return original(run)
RunJournal.complete_publication = paused
cli.create_s3_client = lambda *args, **kwargs: Storage()
cli.main = main
raise SystemExit(cli.entrypoint())
"""
        common = ['push', *self.args, '--generated-dir', str(self.output)]
        child = subprocess.Popen([sys.executable, '-c', script, *common, '--execute'], env=env,
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            deadline = time.monotonic() + 10
            while not ready.exists() and child.poll() is None and time.monotonic() < deadline:
                time.sleep(.02)
            self.assertTrue(ready.exists())
            before = (self.run/'events.jsonl').read_bytes()
            other_cwd = self.root/'other-cwd'
            other_cwd.mkdir()
            for mode in [['--execute'], ['--dry-run', '--offline']]:
                contender = subprocess.run([sys.executable, '-m', 'cli_test_runner', *common, *mode],
                                           env=env, cwd=other_cwd, capture_output=True, text=True, timeout=10)
                self.assertEqual(contender.returncode, 1, contender.stderr)
                self.assertIn('already attempted execute', contender.stdout + contender.stderr)
                self.assertEqual((self.run/'events.jsonl').read_bytes(), before)
            release.touch()
            stdout, stderr = child.communicate(timeout=10)
            self.assertEqual(child.returncode, 0, stderr)
        finally:
            if child.poll() is None:
                child.kill()
                child.communicate()
        self.assertEqual(inspect_run(self.run)['publication']['state'], 'ready_for_review')

    def test_inspection_rejects_inconsistent_observed_checksum_evidence(self):
        self.assertEqual(self.execute(Storage()), 0)
        events, error = read_events(self.run)
        event = next(e for e in events if e['kind'] == 'result' and e['stage'] == 'verify-upload')
        event.update(verification_method='s3-sha256', observed_checksum='0'*64, observed_size_bytes=event['size_bytes'])
        (self.run/'events.jsonl').write_text(''.join(json.dumps(e)+'\n' for e in events))
        self.assertEqual(inspect_run(self.run)['publication']['state'], 'held')

    def test_inspection_rejects_missing_verification_evidence(self):
        self.assertEqual(self.execute(Storage()), 0)
        original, error = read_events(self.run)
        for field in ['verification_method', 'observed_checksum', 'observed_size_bytes']:
            with self.subTest(field=field):
                events = json.loads(json.dumps(original))
                event = next(e for e in events if e['kind'] == 'result' and e['stage'] == 'verify-upload')
                event.pop(field, None)
                (self.run/'events.jsonl').write_text(''.join(json.dumps(e)+'\n' for e in events))
                self.assertEqual(inspect_run(self.run)['publication']['state'], 'held')

    def test_bare_success_boolean_cannot_replace_checksum_evidence(self):
        store = Storage()
        store.verify_file = lambda *args: True
        self.assertEqual(self.execute(store), 1)
        self.assertEqual(inspect_run(self.run)['publication']['state'], 'held')

    def test_legacy_receipt_without_verification_contract_is_held(self):
        self.assertEqual(self.execute(Storage()), 0)
        events, error = read_events(self.run)
        events[-1]['publication_receipt'].pop('verification_contract')
        (self.run/'events.jsonl').write_text(''.join(json.dumps(e)+'\n' for e in events))
        self.assertEqual(inspect_run(self.run)['publication']['state'], 'held')
