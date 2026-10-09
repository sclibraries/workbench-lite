"""Publication gates protect existing keys and require complete durable evidence."""
import contextlib
import hashlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from PIL import Image

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

    def execute_json(self, store):
        output = io.StringIO()
        with patch('workbench_lite.cli.create_s3_client', return_value=store), contextlib.redirect_stdout(output):
            code = main(['push', *self.args, '--execute', '--generated-dir', str(self.output), '--format', 'json'])
        return code, json.loads(output.getvalue())

    def regenerate(self):
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(main(['generate', *self.args, '--output-dir', str(self.output), '--manifests', '--thumbnails']), 0)
        self.run = Path(json.loads((self.output/'.workbench-run.json').read_text())['run_path'])
        self.plan = run_check(self.config, run_id=self.run.name).upload_plan

    def test_existing_serving_key_blocks_entire_batch_without_writes(self):
        store = Storage()
        key = next(e.key for e in self.plan if e.role == 'service_jpg')
        store.written[key] = b'existing live image'
        self.assertEqual(self.execute(store), 1)
        self.assertEqual(store.calls, [])
        self.assertEqual(store.written[key], b'existing live image')

    def test_unchanged_package_reuses_verified_objects_and_uploads_only_new_audit(self):
        store = Storage()
        self.assertEqual(self.execute(store), 0)
        prior_calls = list(store.calls)
        prior_objects = dict(store.written)

        self.regenerate()
        code, report = self.execute_json(store)

        self.assertEqual(code, 0)
        audit = next(entry for entry in self.plan if entry.role == 'audit')
        self.assertEqual(store.calls[len(prior_calls):], [audit.key])
        self.assertTrue(all(store.written[key] == value for key, value in prior_objects.items()))
        classifications = {row['key']: row['classification'] for row in report['results']}
        self.assertEqual(classifications[audit.key], 'new')
        self.assertTrue(all(value == 'previously_published' for key, value in classifications.items() if key != audit.key))
        self.assertEqual(report['classification_counts']['previously_published'], len(self.plan) - 1)
        summary = inspect_run(self.run)
        self.assertEqual(summary['publication']['state'], 'ready_for_review')
        self.assertTrue(all(item['classification'] == 'previously_published'
                            for item in summary['push_artifacts'] if item['status'] == 'unchanged'))
        self.assertIn('previously_published (unchanged)', (self.run/'summary.txt').read_text())

    def test_inspection_rejects_mismatched_checksum_for_reused_key(self):
        store = Storage()
        self.assertEqual(self.execute(store), 0)
        self.regenerate()
        self.assertEqual(self.execute(store), 0)

        events, error = read_events(self.run)
        self.assertIsNone(error)
        reused = next(event for event in events if event['kind'] == 'result'
                      and event['stage'] == 'destination-check' and event.get('status') == 'unchanged')
        reused['observed_checksum'] = '0' * 64
        (self.run/'events.jsonl').write_text(''.join(json.dumps(event)+'\n' for event in events))

        summary = inspect_run(self.run)
        self.assertEqual(summary['publication']['state'], 'held')

    def test_previous_receipt_without_checksum_type_remains_inspectable(self):
        self.assertEqual(self.execute(Storage()), 0)
        events, error = read_events(self.run)
        self.assertIsNone(error)
        for event in events:
            if event.get('stage') == 'verify-upload':
                event.pop('checksum_type', None)
        (self.run/'events.jsonl').write_text(''.join(json.dumps(event)+'\n' for event in events))

        self.assertEqual(inspect_run(self.run)['publication']['state'], 'ready_for_review')

    def test_appended_page_writes_only_new_keys_and_holds_changed_parent_manifest(self):
        store = Storage()
        self.assertEqual(self.execute(store), 0)
        prior_calls = list(store.calls)
        old_manifest = next(entry.key for entry in self.plan if entry.role == 'manifest')
        old_manifest_bytes = store.written[old_manifest]

        Image.new('RGB', (9, 7), (20, 40, 60)).save(self.root/'page2.tif')
        with (self.root/'input.csv').open('a', encoding='utf-8') as handle:
            handle.write('Later page,page2,Page,parent,2,page2.tif\n')
        self.regenerate()
        code, report = self.execute_json(store)

        self.assertEqual(code, 1)
        new_entries = {entry.key for entry in self.plan if entry.page_id == 'page2'}
        audit_key = next(entry.key for entry in self.plan if entry.role == 'audit')
        self.assertEqual(set(store.calls[len(prior_calls):]), new_entries | {audit_key})
        self.assertEqual(store.written[old_manifest], old_manifest_bytes)
        manifest = next(row for row in report['results'] if row['key'] == old_manifest)
        self.assertEqual(manifest['status'], 'held')
        self.assertEqual(manifest['classification'], 'held')
        self.assertEqual(manifest['previous_checksum'], hashlib.sha256(old_manifest_bytes).hexdigest())
        self.assertEqual(manifest['checksum'], hashlib.sha256((self.output/old_manifest).read_bytes()).hexdigest())
        self.assertIn('manifest replacement required', manifest['message'])
        summary = inspect_run(self.run)
        self.assertEqual(summary['publication']['state'], 'held')
        held_summary = next(item for item in summary['push_artifacts'] if item['key'] == old_manifest)
        self.assertEqual(held_summary['classification'], 'held')
        self.assertEqual(held_summary['previous_checksum'], manifest['previous_checksum'])
        self.assertEqual(held_summary['checksum'], manifest['checksum'])
        inventory = json.loads((self.run/'rollback-inventory.json').read_text())
        self.assertEqual({entry['key'] for entry in inventory['new_keys']}, new_entries | {audit_key})
        self.assertIn(old_manifest, {entry['key'] for entry in inventory['held_keys']})

    def test_appended_page_leaves_other_parent_manifests_untouched(self):
        self.multi_package()
        store = Storage()
        self.assertEqual(self.execute(store), 0)
        prior_calls = list(store.calls)
        manifest_keys = {entry.object_id: entry.key for entry in self.plan if entry.role == 'manifest'}
        prior_manifests = {key: store.written[key] for key in manifest_keys.values()}

        Image.new('RGB', (11, 6), (35, 55, 75)).save(self.root/'page2b.tif')
        with (self.root/'input.csv').open('a', encoding='utf-8') as handle:
            handle.write('Inserted page,page2b,Page,parent2,2,page2b.tif\n')
        self.regenerate()
        code, report = self.execute_json(store)

        self.assertEqual(code, 1)
        changed_manifest = manifest_keys['parent2']
        unchanged_manifests = set(manifest_keys.values()) - {changed_manifest}
        new_entries = {entry.key for entry in self.plan if entry.page_id == 'page2b'}
        audit_key = next(entry.key for entry in self.plan if entry.role == 'audit')
        self.assertEqual(set(store.calls[len(prior_calls):]), new_entries | {audit_key})
        self.assertTrue(all(store.written[key] == value for key, value in prior_manifests.items()))
        self.assertFalse(unchanged_manifests.intersection(store.calls[len(prior_calls):]))
        self.assertEqual(next(row for row in report['results'] if row['key'] == changed_manifest)['status'], 'held')

    def test_new_parent_manifest_uses_new_key_and_can_complete_batch(self):
        store = Storage()
        self.assertEqual(self.execute(store), 0)
        prior_calls = list(store.calls)

        (self.root/'parent2.pdf').write_bytes(b'%PDF-1.4\n%%EOF\n')
        Image.new('RGB', (7, 8), (70, 40, 20)).save(self.root/'page2.tif')
        with (self.root/'input.csv').open('a', encoding='utf-8') as handle:
            handle.write('Another parent,parent2,Paged Content,,,parent2.pdf\n')
            handle.write('Another page,page2,Page,parent2,1,page2.tif\n')
        self.regenerate()
        code, report = self.execute_json(store)

        self.assertEqual(code, 0)
        new_entries = {entry.key for entry in self.plan if entry.object_id == 'parent2'}
        audit_key = next(entry.key for entry in self.plan if entry.role == 'audit')
        self.assertEqual(set(store.calls[len(prior_calls):]), new_entries | {audit_key})
        new_manifest = next(entry.key for entry in self.plan if entry.role == 'manifest' and entry.object_id == 'parent2')
        manifest_result = next(row for row in report['results'] if row['key'] == new_manifest)
        self.assertEqual(manifest_result['classification'], 'new')
        self.assertEqual(manifest_result['status'], 'uploaded')
        self.assertEqual(inspect_run(self.run)['publication']['state'], 'ready_for_review')

    def test_changed_existing_content_fails_before_any_write(self):
        store = Storage()
        self.assertEqual(self.execute(store), 0)
        prior_calls = list(store.calls)
        before = dict(store.written)

        Image.new('RGB', (10, 10), (90, 10, 30)).save(self.root/'page.tif')
        self.regenerate()
        code, report = self.execute_json(store)

        self.assertEqual(code, 1)
        self.assertEqual(store.calls, prior_calls)
        self.assertEqual(store.written, before)
        changed = next(entry for entry in self.plan if entry.role == 'master_tiff')
        row = next(result for result in report['results'] if result['key'] == changed.key)
        self.assertEqual(row['classification'], 'held')
        self.assertIn(changed.key, ' '.join(report['errors']))

    def test_composite_manifest_checksum_is_not_reported_as_prior_sha256(self):
        from workbench_lite.s3_client import VerificationResult

        entry = next(entry for entry in self.plan if entry.role == 'manifest')
        store = Storage()
        prior = b'prior manifest bytes'
        store.written[entry.key] = prior
        store.verify_file = lambda *args: VerificationResult(
            False, hashlib.sha256(prior).hexdigest(), len(prior), 's3-sha256', 'COMPOSITE'
        )

        report = push_upload_plan([entry], self.root, store, False, self.output)

        self.assertEqual(report.results[0].status, 'held')
        self.assertIsNone(report.results[0].previous_checksum)
        self.assertEqual(report.results[0].previous_checksum_type, 'COMPOSITE')
        self.assertIn('previous SHA-256 unavailable', report.results[0].message)
        self.assertEqual(store.calls, [])

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
