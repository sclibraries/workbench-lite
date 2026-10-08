"""Rollback evidence must never promote a plan into a confirmed write."""
import json
import contextlib
import io
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from workbench_lite.models import UploadPlanEntry
from workbench_lite.runs import RunJournal
from workbench_lite.rollback_inventory import build_inventory
from cli_test_runner import main


class RollbackInventoryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.config = self.root / 'package.yml'
        self.csv = self.root / 'package.csv'
        self.config.write_text('batch_id: batch-one\n')
        self.csv.write_text('title\nexample\n')
        self.run = RunJournal.create(self.root / 'runs')
        self.run.inputs(self.config, {'batch_id': 'batch-one', 'input_csv': str(self.csv)}, self.csv)
        self.entries = [UploadPlanEntry('master_tiff', 'source.tif', 'private', 'batch/new.tif', False, 'obj', 'page', False, True, None),
                        UploadPlanEntry('manifest', 'obj.json', 'public', 'batch/obj.json', True, 'obj', '', True, True, None)]
        self.run.plan(self.entries)

    def record(self, stage, entry, status, *, outcome=None):
        identity = entry.to_dict()
        op = self.run.intent(stage, identity)
        result = {**identity, 'status': status}
        if outcome:
            result['remote_outcome'] = outcome
        self.run.result(op, stage, result)

    def test_dry_run_is_preview_only(self):
        self.run.start('push-dry-run')
        for entry in self.entries:
            self.record('push', entry, 'would_upload')
        self.run.finish(0)
        inventory = build_inventory(self.run.path)
        self.assertEqual(inventory['mode'], 'preview')
        self.assertEqual(inventory['batch_id'], 'batch-one')
        self.assertEqual(inventory['new_keys'], [])
        self.assertFalse(inventory['deletion_authorized'])
        self.assertEqual(len(inventory['planned']), 2)

    def test_confirmed_new_key_survives_later_failure(self):
        self.run.start('push')
        for entry in self.entries:
            self.record('destination-check', entry, 'absent')
        self.record('push', self.entries[0], 'uploaded')
        self.record('push', self.entries[1], 'failed', outcome='unknown')
        inventory = build_inventory(self.run.path)
        self.assertEqual([item['key'] for item in inventory['new_keys']], ['batch/new.tif'])
        self.assertEqual([item['key'] for item in inventory['unknown_keys']], ['batch/obj.json'])
        self.assertEqual(inventory['state'], 'needs_reconciliation')
        self.assertFalse(inventory['deletion_authorized'])

    def test_preexisting_destination_never_becomes_new(self):
        self.run.start('push')
        self.record('destination-check', self.entries[0], 'conflict')
        self.record('push', self.entries[0], 'uploaded')
        inventory = build_inventory(self.run.path)
        self.assertEqual(inventory['new_keys'], [])
        self.assertEqual([item['key'] for item in inventory['conflicts']], ['batch/new.tif'])
        self.assertEqual(inventory['unchanged_keys'], [])

    def test_interrupted_before_first_upload_is_not_reviewable(self):
        self.run.start('push')
        self.assertEqual(build_inventory(self.run.path)['state'], 'incomplete_stage')
        op = self.run.intent('destination-check', self.entries[0].to_dict())
        inventory = build_inventory(self.run.path)
        self.assertEqual(inventory['state'], 'needs_reconciliation')
        self.assertEqual(inventory['pending_operations'][0]['operation_id'], op)

    def test_output_must_not_overwrite_run_evidence(self):
        from workbench_lite.rollback_inventory import write_inventory
        from workbench_lite.runs import JournalError
        self.run.start('push-dry-run')
        self.run.finish(0)
        before = (self.run.path / 'events.jsonl').read_bytes()
        with self.assertRaises(JournalError):
            write_inventory(self.run.path, self.run.path / 'events.jsonl')
        self.assertEqual(before, (self.run.path / 'events.jsonl').read_bytes())

    def test_truncated_journal_retains_prefix_without_certifying_it(self):
        self.run.start('push')
        self.record('destination-check', self.entries[0], 'absent')
        self.record('push', self.entries[0], 'uploaded')
        with (self.run.path / 'events.jsonl').open('ab') as handle:
            handle.write(b'{"sequence":')
        inventory = build_inventory(self.run.path)
        self.assertEqual([item['key'] for item in inventory['new_keys']], ['batch/new.tif'])
        self.assertEqual(inventory['state'], 'invalid_evidence')
        self.assertFalse(inventory['deletion_authorized'])

    def test_changed_plan_is_invalid_even_with_acknowledged_upload(self):
        self.run.start('push')
        self.record('destination-check', self.entries[0], 'absent')
        self.record('push', self.entries[0], 'uploaded')
        plan = self.run.path / 'upload-plan.json'
        payload = json.loads(plan.read_text())
        payload['entries'][0]['key'] = 'batch/other.tif'
        plan.write_text(json.dumps(payload))
        inventory = build_inventory(self.run.path)
        self.assertEqual(inventory['state'], 'invalid_evidence')
        self.assertFalse(inventory['deletion_authorized'])

    def test_parseable_but_wrong_evidence_shapes_report_invalid(self):
        self.run.start('push')
        self.run.finish(1)
        run_file = self.run.path / 'run.json'
        run_file.write_text('[]')
        inventory = build_inventory(self.run.path)
        self.assertEqual(inventory['state'], 'invalid_evidence')
        self.assertTrue(inventory['evidence_errors'])
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(main(['rollback-inventory', str(self.run.path)]), 1)
        run_file.write_text(json.dumps({'run_id': self.run.run_id}))
        plan_file = self.run.path / 'upload-plan.json'
        plan_file.write_text(json.dumps({'entries': [42]}))
        inventory = build_inventory(self.run.path)
        self.assertEqual(inventory['state'], 'invalid_evidence')
        self.assertTrue(inventory['evidence_errors'])
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(main(['rollback-inventory', str(self.run.path)]), 1)

    def test_reconstruction_command_copies_evidence_without_remote_calls(self):
        self.run.start('push')
        self.record('destination-check', self.entries[0], 'absent')
        self.record('push', self.entries[0], 'uploaded')
        self.run.finish(1)
        output = self.root / 'copy.json'
        with patch('workbench_lite.cli.create_s3_client', side_effect=AssertionError('remote call')):
            self.assertEqual(main(['rollback-inventory', str(self.run.path), '--output', str(output)]), 0)
        self.assertEqual(json.loads(output.read_text())['new_keys'][0]['key'], 'batch/new.tif')
        self.assertEqual(output.stat().st_mode & 0o077, 0)

    def test_manifest_url_uses_generate_flag_when_yaml_has_no_base(self):
        from test_outcomes import Storage, package
        config = package(self.root)
        output = self.root / 'generated'
        base_url = 'https://digital.example.edu/manifests'
        common = ['--config', str(config), '--run-dir', str(self.root / 'runs')]
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(main(['generate', *common, '--output-dir', str(output),
                                   '--manifests', '--thumbnails', '--manifest-base-url', base_url]), 0)
        reference = json.loads((output / '.workbench-run.json').read_text())
        run_path = Path(reference['run_path'])
        with patch('workbench_lite.cli.create_s3_client', return_value=Storage()), \
             contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(main(['push', *common, '--execute', '--generated-dir', str(output)]), 0)
        inventory = build_inventory(run_path)
        self.assertEqual(len(inventory['manifest_urls']), 1)
        manifest = output / inventory['manifest_urls'][0]['key']
        self.assertEqual(inventory['manifest_urls'][0]['url'], json.loads(manifest.read_text())['@id'])


if __name__ == '__main__':
    unittest.main()
