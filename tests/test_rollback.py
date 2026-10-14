"""Rollback command previews and deletes only journaled writes."""

import contextlib
import hashlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

import botocore.session
from botocore.exceptions import ClientError

from workbench_lite.cli import main
from workbench_lite.locks import WriterLocks
from workbench_lite.models import UploadPlanEntry
from workbench_lite.runs import RunJournal
from workbench_lite.s3_client import Boto3S3Client


ENDPOINT = 'http://localstack:4566'
BUCKET = 'wbl-local-test-sources'
KEY = 'unique-prefix/package/originals/page.tif'
BODY = b'synthetic master bytes'
CHECKSUM = hashlib.sha256(BODY).hexdigest()
UNCHANGED_KEY = 'unique-prefix/package/originals/reused.tif'
UNCHANGED_BODY = b'already published bytes'
UNCHANGED_CHECKSUM = hashlib.sha256(UNCHANGED_BODY).hexdigest()
HELD_KEY = 'unique-prefix/package/manifests/package.json'
CONFLICT_KEY = 'unique-prefix/package/originals/conflict.tif'
UNRELATED_KEY = 'unique-prefix/unrelated/sentinel.txt'


class MemoryS3:
    endpoint_url = ENDPOINT
    region_name = 'us-east-1'

    def __init__(self):
        self.objects = {
            (BUCKET, KEY): {
                'checksum': CHECKSUM,
                'size_bytes': len(BODY),
                'checksum_type': 'FULL_OBJECT',
                'etag': '"probe-etag"',
            },
            (BUCKET, UNCHANGED_KEY): {
                'checksum': UNCHANGED_CHECKSUM,
                'size_bytes': len(UNCHANGED_BODY),
                'checksum_type': 'FULL_OBJECT',
                'etag': '"reused-etag"',
            },
            (BUCKET, HELD_KEY): {
                'checksum': hashlib.sha256(b'held manifest').hexdigest(),
                'size_bytes': 13,
                'checksum_type': 'FULL_OBJECT',
                'etag': '"held-etag"',
            },
            (BUCKET, CONFLICT_KEY): {
                'checksum': hashlib.sha256(b'conflict').hexdigest(),
                'size_bytes': 8,
                'checksum_type': 'FULL_OBJECT',
                'etag': '"conflict-etag"',
            },
            (BUCKET, UNRELATED_KEY): {
                'checksum': hashlib.sha256(b'sentinel').hexdigest(),
                'size_bytes': 8,
                'checksum_type': 'FULL_OBJECT',
                'etag': '"unrelated-etag"',
            },
        }
        self.inspections = []
        self.deletes = []
        self.delete_error = None
        self.lose_delete_ack = False
        self.inspect_error_on_call = None
        self.inspect_error = None

    def inspect_object(self, bucket, key):
        self.inspections.append((bucket, key))
        if self.inspect_error_on_call == len(self.inspections):
            raise self.inspect_error
        return self.objects.get((bucket, key))

    def delete_object(self, bucket, key, *, if_match):
        self.deletes.append((bucket, key, if_match))
        if self.delete_error:
            raise self.delete_error
        del self.objects[(bucket, key)]
        if self.lose_delete_ack:
            raise OSError('connection closed after delete')


class RollbackCommandTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.runs = self.root / 'runs'
        self.run = RunJournal.create(self.runs)
        config = self.root / 'package.yml'
        config.write_text(
            's3_private_bucket: wbl-local-test-sources\n'
            's3_public_bucket: wbl-local-test-derivatives\n'
            'batch_id: package\ns3_prefix: unique-prefix\n'
        )
        self.config = config
        csv_path = self.root / 'package.csv'
        csv_path.write_text('title,id\nSynthetic,package\n')
        self.run.inputs(
            config,
            {
                's3_private_bucket': 'wbl-local-test-sources',
                's3_public_bucket': 'wbl-local-test-derivatives',
                'batch_id': 'package',
                's3_prefix': 'unique-prefix',
            },
            csv_path,
        )
        entry = UploadPlanEntry(
            role='master_tiff',
            source_path='page.tif',
            bucket=BUCKET,
            key=KEY,
            public=False,
            object_id='package',
            page_id='page',
            generated=False,
            source_exists=True,
            checksum=None,
        )
        unchanged = UploadPlanEntry(
            role='master_tiff',
            source_path='reused.tif',
            bucket=BUCKET,
            key=UNCHANGED_KEY,
            public=False,
            object_id='package',
            page_id='reused',
            generated=False,
            source_exists=True,
            checksum=None,
        )
        held = UploadPlanEntry(
            role='manifest',
            source_path='',
            bucket=BUCKET,
            key=HELD_KEY,
            public=True,
            object_id='package',
            page_id='',
            generated=True,
            source_exists=False,
            checksum=None,
        )
        conflict = UploadPlanEntry(
            role='master_tiff',
            source_path='conflict.tif',
            bucket=BUCKET,
            key=CONFLICT_KEY,
            public=False,
            object_id='package',
            page_id='conflict',
            generated=False,
            source_exists=True,
            checksum=None,
        )
        self.run.plan([entry, unchanged, held, conflict])
        self.run.start('push', options={'endpoint_url': ENDPOINT, 'region': 'us-east-1'})
        self.run.append(
            'context',
            stage='push',
            storage={
                'effective_endpoint_url': ENDPOINT,
                'effective_region': 'us-east-1',
            },
        )
        identity = {**entry.to_dict(), 'checksum': CHECKSUM, 'size_bytes': len(BODY)}
        operation = self.run.intent('destination-check', identity)
        self.run.result(operation, 'destination-check', {**identity, 'status': 'absent'})
        operation = self.run.intent('push', identity)
        self.run.result(operation, 'push', {**identity, 'status': 'uploaded'})
        operation = self.run.intent('verify-upload', identity)
        self.run.result(
            operation,
            'verify-upload',
            {
                **identity,
                'status': 'verified',
                'observed_checksum': CHECKSUM,
                'observed_size_bytes': len(BODY),
                'checksum_type': 'FULL_OBJECT',
                'verification_method': 's3-sha256',
            },
        )
        unchanged_identity = {
            **unchanged.to_dict(),
            'checksum': UNCHANGED_CHECKSUM,
            'size_bytes': len(UNCHANGED_BODY),
            'observed_checksum': UNCHANGED_CHECKSUM,
            'observed_size_bytes': len(UNCHANGED_BODY),
            'checksum_type': 'FULL_OBJECT',
            'verification_method': 's3-sha256',
        }
        operation = self.run.intent('destination-check', unchanged_identity)
        self.run.result(
            operation,
            'destination-check',
            {
                **unchanged_identity,
                'status': 'unchanged',
            },
        )
        for held_entry, status in ((held, 'held'), (conflict, 'conflict')):
            held_identity = {**held_entry.to_dict(), 'checksum': CHECKSUM, 'size_bytes': len(BODY)}
            operation = self.run.intent('destination-check', held_identity)
            self.run.result(
                operation,
                'destination-check',
                {
                    **held_identity,
                    'status': status,
                },
            )
        self.run.finish(1)

    def run_cli(self, client, *options, source=None, config=None):
        output = io.StringIO()
        error = io.StringIO()
        locks = self.root / 'locks'
        locks.mkdir(mode=0o700, exist_ok=True)
        command = [
            'rollback',
            str(source or self.run.path),
            '--config',
            str(config or self.config),
            '--endpoint-url',
            ENDPOINT,
            '--region',
            'us-east-1',
            '--format',
            'json',
            *options,
        ]
        with (
            patch('workbench_lite.cli.create_s3_client', return_value=client),
            contextlib.redirect_stdout(output),
            contextlib.redirect_stderr(error),
        ):
            code = main(command, lock_factory=lambda: WriterLocks(locks))
        try:
            payload = json.loads(output.getvalue())
        except ValueError:
            payload = None
        return code, payload, output.getvalue(), error.getvalue()

    def make_preview_run(self):
        run = RunJournal.create(self.runs)
        config = self.root / 'preview.yml'
        config.write_text(
            's3_private_bucket: wbl-local-test-sources\n'
            's3_public_bucket: wbl-local-test-derivatives\n'
            'batch_id: package\ns3_prefix: unique-prefix\n'
        )
        csv_path = self.root / 'preview.csv'
        csv_path.write_text('title,id\nSynthetic,package\n')
        run.inputs(
            config,
            {
                's3_private_bucket': 'wbl-local-test-sources',
                's3_public_bucket': 'wbl-local-test-derivatives',
                'batch_id': 'package',
                's3_prefix': 'unique-prefix',
            },
            csv_path,
        )
        entry = UploadPlanEntry(
            role='master_tiff',
            source_path='page.tif',
            bucket=BUCKET,
            key=KEY,
            public=False,
            object_id='package',
            page_id='page',
            generated=False,
            source_exists=True,
            checksum=None,
        )
        run.plan([entry])
        run.start('push-dry-run')
        operation = run.intent(
            'destination-check',
            {
                **entry.to_dict(),
                'checksum': CHECKSUM,
                'size_bytes': len(BODY),
            },
        )
        run.result(
            operation,
            'destination-check',
            {
                **entry.to_dict(),
                'checksum': CHECKSUM,
                'size_bytes': len(BODY),
                'status': 'would_upload',
            },
        )
        run.finish(0)
        return run

    def make_later_reuse_run(self, role='master_tiff', key=KEY):
        later = RunJournal.create(self.runs)
        config = self.root / 'later-package.yml'
        config.write_text(
            's3_private_bucket: wbl-local-test-sources\n'
            's3_public_bucket: wbl-local-test-derivatives\n'
            'batch_id: package\ns3_prefix: unique-prefix\n'
        )
        csv_path = self.root / 'later-package.csv'
        csv_path.write_text('title,id\nSynthetic,package\n')
        later.inputs(
            config,
            {
                's3_private_bucket': 'wbl-local-test-sources',
                's3_public_bucket': 'wbl-local-test-derivatives',
                'batch_id': 'package',
                's3_prefix': 'unique-prefix',
            },
            csv_path,
        )
        entry = UploadPlanEntry(
            role=role,
            source_path='manifest.json' if role == 'manifest' else 'page.tif',
            bucket=BUCKET,
            key=key,
            public=role == 'manifest',
            object_id='package',
            page_id='' if role == 'manifest' else 'page',
            generated=role == 'manifest',
            source_exists=True,
            checksum=None,
        )
        later.plan([entry])
        later.start('push', options={'endpoint_url': ENDPOINT, 'region': 'us-east-1'})
        later.append(
            'context',
            stage='push',
            storage={
                'effective_endpoint_url': ENDPOINT,
                'effective_region': 'us-east-1',
            },
        )
        identity = {
            **entry.to_dict(),
            'checksum': CHECKSUM,
            'size_bytes': len(BODY),
            'observed_checksum': CHECKSUM,
            'observed_size_bytes': len(BODY),
            'checksum_type': 'FULL_OBJECT',
            'verification_method': 's3-sha256',
        }
        operation = later.intent('destination-check', identity)
        later.result(operation, 'destination-check', {**identity, 'status': 'unchanged'})
        later.finish(1)
        return later

    def make_order_run(self):
        run = RunJournal.create(self.runs)
        csv_path = self.root / 'order.csv'
        csv_path.write_text('title,id\nSynthetic,package\n')
        run.inputs(
            self.config,
            {
                's3_private_bucket': 'wbl-local-test-sources',
                's3_public_bucket': 'wbl-local-test-derivatives',
                'batch_id': 'package',
                's3_prefix': 'unique-prefix',
            },
            csv_path,
        )
        entries = [
            UploadPlanEntry(
                'audit',
                '',
                BUCKET,
                'unique-prefix/package/audit/run.json',
                False,
                '',
                '',
                True,
                True,
                None,
            ),
            UploadPlanEntry(
                'master_tiff',
                'page.tif',
                BUCKET,
                'unique-prefix/package/originals/page.tif',
                False,
                'package',
                'page',
                False,
                True,
                None,
            ),
            UploadPlanEntry(
                'service_jpg',
                'service.jpg',
                BUCKET,
                'unique-prefix/package/derivatives/page.jpg',
                True,
                'package',
                'page',
                True,
                True,
                None,
            ),
            UploadPlanEntry(
                'manifest',
                'manifest.json',
                BUCKET,
                'unique-prefix/package/manifests/package.json',
                True,
                'package',
                '',
                True,
                True,
                None,
            ),
        ]
        run.plan(entries)
        run.start('push', options={'endpoint_url': ENDPOINT, 'region': 'us-east-1'})
        run.append(
            'context',
            stage='push',
            storage={
                'effective_endpoint_url': ENDPOINT,
                'effective_region': 'us-east-1',
            },
        )
        payloads = {}
        for index, entry in enumerate(entries):
            body = f'synthetic {entry.role}'.encode()
            checksum = hashlib.sha256(body).hexdigest()
            payload = {**entry.to_dict(), 'checksum': checksum, 'size_bytes': len(body)}
            payloads[(entry.bucket, entry.key)] = {
                'checksum': checksum,
                'size_bytes': len(body),
                'checksum_type': 'FULL_OBJECT',
                'etag': f'"etag-{index}"',
            }
            operation = run.intent('destination-check', payload)
            run.result(operation, 'destination-check', {**payload, 'status': 'absent'})
            operation = run.intent('push', payload)
            run.result(operation, 'push', {**payload, 'status': 'uploaded'})
            operation = run.intent('verify-upload', payload)
            run.result(
                operation,
                'verify-upload',
                {
                    **payload,
                    'status': 'verified',
                    'observed_checksum': checksum,
                    'observed_size_bytes': len(body),
                    'checksum_type': 'FULL_OBJECT',
                    'verification_method': 's3-sha256',
                },
            )
        run.finish(1)
        return run, payloads

    def test_s3_adapter_sends_if_match_and_preserves_412(self):
        raw = Mock()
        raw.meta = SimpleNamespace(
            service_model=botocore.session.get_session().get_service_model('s3')
        )
        raw.delete_object.side_effect = ClientError(
            {'Error': {'Code': 'PreconditionFailed'}, 'ResponseMetadata': {'HTTPStatusCode': 412}},
            'DeleteObject',
        )
        adapter = Boto3S3Client(raw)
        delete = getattr(adapter, 'delete_object', None)
        self.assertTrue(callable(delete), 'S3 adapter must expose conditional delete')
        with self.assertRaises(ClientError) as raised:
            delete(BUCKET, KEY, if_match='"recorded-etag"')

        self.assertEqual(raised.exception.response['ResponseMetadata']['HTTPStatusCode'], 412)
        raw.delete_object.assert_called_once_with(Bucket=BUCKET, Key=KEY, IfMatch='"recorded-etag"')

    def test_s3_adapter_refuses_sdk_without_conditional_delete_support(self):
        raw = Mock()
        service_model = Mock()
        service_model.operation_model.return_value = SimpleNamespace(
            input_shape=SimpleNamespace(members={})
        )
        raw.meta = SimpleNamespace(service_model=service_model)

        with self.assertRaisesRegex(OSError, 'lacks conditional DeleteObject'):
            Boto3S3Client(raw).delete_object(BUCKET, KEY, if_match='"etag"')

        raw.delete_object.assert_not_called()

    def test_current_configuration_is_required(self):
        command = [
            'rollback',
            str(self.run.path),
            '--endpoint-url',
            ENDPOINT,
            '--region',
            'us-east-1',
            '--format',
            'json',
        ]
        with (
            patch('workbench_lite.cli.create_s3_client') as create_client,
            contextlib.redirect_stdout(io.StringIO()),
            contextlib.redirect_stderr(io.StringIO()),
            self.assertRaises(SystemExit) as raised,
        ):
            main(command, lock_factory=lambda: WriterLocks(self.root / 'locks'))

        self.assertEqual(raised.exception.code, 2)
        create_client.assert_not_called()

    def test_deletion_order_is_manifest_derivatives_masters_then_audit(self):
        run, objects = self.make_order_run()
        client = MemoryS3()
        client.objects = objects

        code, report, _, _ = self.run_cli(
            client, '--execute', '--confirm', run.run_id, source=run.path
        )

        self.assertEqual(code, 0)
        self.assertEqual(
            [item['role'] for item in report['results']],
            ['manifest', 'service_jpg', 'master_tiff', 'audit'],
        )
        expected_keys = [
            'unique-prefix/package/manifests/package.json',
            'unique-prefix/package/derivatives/page.jpg',
            'unique-prefix/package/originals/page.tif',
            'unique-prefix/package/audit/run.json',
        ]
        self.assertEqual([key for _, key, _ in client.deletes], expected_keys)

    def test_default_command_previews_without_deleting(self):
        client = MemoryS3()
        code, report, _, _ = self.run_cli(client)

        self.assertEqual(code, 0)
        self.assertEqual(report['mode'], 'preview')
        self.assertEqual(report['results'][0]['status'], 'would_delete')
        self.assertEqual(report['results'][0]['checksum'], CHECKSUM)
        excluded_keys = {item['key'] for item in report['exclusions']}
        self.assertTrue({UNCHANGED_KEY, HELD_KEY, CONFLICT_KEY}.issubset(excluded_keys))
        self.assertEqual(client.deletes, [])
        self.assertIn((BUCKET, KEY), client.inspections)

    def test_execute_deletes_matching_new_key_and_permanently_holds_source(self):
        client = MemoryS3()
        before = (self.run.path / 'events.jsonl').read_bytes()

        code, report, _, _ = self.run_cli(client, '--execute', '--confirm', self.run.run_id)

        self.assertEqual(code, 0)
        self.assertEqual(report['results'][0]['status'], 'deleted')
        self.assertEqual(client.deletes, [(BUCKET, KEY, '"probe-etag"')])
        self.assertNotIn((BUCKET, KEY), client.objects)
        self.assertIn((BUCKET, UNCHANGED_KEY), client.objects)
        self.assertIn((BUCKET, HELD_KEY), client.objects)
        self.assertIn((BUCKET, CONFLICT_KEY), client.objects)
        self.assertIn((BUCKET, UNRELATED_KEY), client.objects)
        self.assertTrue((self.run.path / 'rollback-hold.json').exists())
        self.assertEqual((self.run.path / 'events.jsonl').read_bytes(), before)
        self.assertIn('rollback', inspect_publication_reason(self.run.path).lower())

    def test_execute_requires_matching_confirmation_before_storage_reads(self):
        client = MemoryS3()

        code, _, output, _ = self.run_cli(client, '--execute')

        self.assertEqual(code, 1)
        self.assertIn('--confirm', output)
        self.assertEqual(client.inspections, [])
        self.assertEqual(client.deletes, [])
        self.assertFalse((self.run.path / 'rollback-hold.json').exists())

    def test_execute_with_wrong_confirmation_refuses_before_storage_reads(self):
        client = MemoryS3()

        code, _, output, _ = self.run_cli(client, '--execute', '--confirm', 'different-run-id')

        self.assertEqual(code, 1)
        self.assertIn('matching the source run ID', output)
        self.assertEqual(client.inspections, [])
        self.assertEqual(client.deletes, [])
        self.assertFalse((self.run.path / 'rollback-hold.json').exists())

    def test_unknown_source_operations_are_refused(self):
        source = RunJournal.open(self.run.path)
        source.intent(
            'push',
            {
                'bucket': BUCKET,
                'key': 'unique-prefix/package/unresolved.bin',
                'role': 'master_tiff',
                'checksum': CHECKSUM,
                'size_bytes': len(BODY),
            },
        )
        client = MemoryS3()

        code, _, output, _ = self.run_cli(client, source=self.run.path)

        self.assertEqual(code, 1)
        self.assertIn('needs_reconciliation', output)
        self.assertEqual(client.inspections, [])
        self.assertEqual(client.deletes, [])

    def test_incomplete_source_stage_is_refused(self):
        events = self.run.path / 'events.jsonl'
        lines = events.read_text().splitlines()
        self.assertEqual(json.loads(lines[-1])['kind'], 'stage_finished')
        events.write_text('\n'.join(lines[:-1]) + '\n')
        client = MemoryS3()

        code, _, output, _ = self.run_cli(client, source=self.run.path)

        self.assertEqual(code, 1)
        self.assertIn('incomplete_stage', output)
        self.assertEqual(client.inspections, [])
        self.assertEqual(client.deletes, [])

    def test_damaged_source_plan_is_refused(self):
        with (self.run.path / 'upload-plan.json').open('ab') as handle:
            handle.write(b' ')
        client = MemoryS3()

        code, _, output, _ = self.run_cli(client, source=self.run.path)

        self.assertEqual(code, 1)
        self.assertIn('Immutable upload plan checksum does not match', output)
        self.assertEqual(client.inspections, [])
        self.assertEqual(client.deletes, [])

    def test_preview_inventory_is_refused_without_storage_reads(self):
        preview = self.make_preview_run()
        client = MemoryS3()

        code, _, output, _ = self.run_cli(client, source=preview.path)

        self.assertEqual(code, 1)
        self.assertIn('reviewable execute', output)
        self.assertEqual(client.inspections, [])
        self.assertEqual(client.deletes, [])

    def test_conditional_412_is_recorded_as_conflict_without_deleting(self):
        client = MemoryS3()
        client.delete_error = ClientError(
            {'Error': {'Code': 'PreconditionFailed'}, 'ResponseMetadata': {'HTTPStatusCode': 412}},
            'DeleteObject',
        )

        code, report, _, _ = self.run_cli(client, '--execute', '--confirm', self.run.run_id)

        self.assertEqual(code, 1)
        self.assertEqual(report['results'][0]['status'], 'conflict')
        self.assertIn('HTTP 412', report['results'][0]['reason'])
        self.assertIn((BUCKET, KEY), client.objects)
        self.assertEqual(client.deletes, [(BUCKET, KEY, '"probe-etag"')])
        journal_path = next(
            path for path in self.runs.iterdir() if (path / 'rollback-source.json').exists()
        )
        results = [
            json.loads(line) for line in (journal_path / 'events.jsonl').read_text().splitlines()
        ]
        conflict = next(
            event
            for event in results
            if event.get('kind') == 'result' and event.get('stage') == 'rollback-delete'
        )
        self.assertEqual(conflict['remote_outcome'], 'not_deleted')
        self.assertEqual(conflict['status'], 'conflict')

    def test_delete_503_is_journaled_as_unknown_and_stops(self):
        client = MemoryS3()
        client.delete_error = ClientError(
            {'Error': {'Code': 'ServiceUnavailable'}, 'ResponseMetadata': {'HTTPStatusCode': 503}},
            'DeleteObject',
        )

        code, report, _, _ = self.run_cli(client, '--execute', '--confirm', self.run.run_id)

        self.assertEqual(code, 1)
        self.assertEqual(report['results'][0]['status'], 'failed')
        self.assertIn((BUCKET, KEY), client.objects)
        journal_path = next(
            path for path in self.runs.iterdir() if (path / 'rollback-source.json').exists()
        )
        events = [
            json.loads(line) for line in (journal_path / 'events.jsonl').read_text().splitlines()
        ]
        event = next(
            item
            for item in events
            if item.get('kind') == 'result' and item.get('stage') == 'rollback-delete'
        )
        self.assertEqual(event['remote_outcome'], 'unknown')

    def test_post_delete_head_failure_is_journaled_as_unknown(self):
        client = MemoryS3()
        client.inspect_error_on_call = 2
        client.inspect_error = OSError('head connection closed')

        code, report, _, _ = self.run_cli(client, '--execute', '--confirm', self.run.run_id)

        self.assertEqual(code, 1)
        self.assertEqual(report['results'][0]['status'], 'failed')
        self.assertNotIn((BUCKET, KEY), client.objects)
        journal_path = next(
            path for path in self.runs.iterdir() if (path / 'rollback-source.json').exists()
        )
        events = [
            json.loads(line) for line in (journal_path / 'events.jsonl').read_text().splitlines()
        ]
        event = next(
            item
            for item in events
            if item.get('kind') == 'result' and item.get('stage') == 'rollback-delete'
        )
        self.assertEqual(event['remote_outcome'], 'unknown')

    def test_changed_full_object_checksum_conflicts_without_delete(self):
        client = MemoryS3()
        client.objects[(BUCKET, KEY)]['checksum'] = '0' * 64

        code, report, _, _ = self.run_cli(client, '--execute', '--confirm', self.run.run_id)

        self.assertEqual(code, 1)
        self.assertEqual(report['results'][0]['status'], 'conflict')
        self.assertIn('modified after upload', report['results'][0]['reason'])
        self.assertEqual(client.deletes, [])
        self.assertIn((BUCKET, KEY), client.objects)

    def test_modified_object_survives_a_lost_delete_ack_and_resume(self):
        client = MemoryS3()
        client.lose_delete_ack = True

        first_code, first_report, _, _ = self.run_cli(
            client, '--execute', '--confirm', self.run.run_id
        )
        self.assertEqual(first_code, 1)
        self.assertEqual(first_report['results'][0]['status'], 'failed')
        self.assertNotIn((BUCKET, KEY), client.objects)
        client.lose_delete_ack = False

        second_code, second_report, _, _ = self.run_cli(
            client, '--execute', '--confirm', self.run.run_id
        )

        self.assertEqual(second_code, 0)
        self.assertEqual(second_report['results'][0]['status'], 'already_deleted')
        rollback_dirs = [
            path for path in self.runs.iterdir() if (path / 'rollback-source.json').exists()
        ]
        self.assertEqual(len(rollback_dirs), 1)

    def test_repeating_a_completed_rollback_reports_already_deleted(self):
        client = MemoryS3()
        options = ('--execute', '--confirm', self.run.run_id)

        first_code, first_report, _, _ = self.run_cli(client, *options)
        second_code, second_report, _, _ = self.run_cli(client, *options)

        self.assertEqual(first_code, 0)
        self.assertEqual(second_code, 0)
        self.assertEqual(first_report['results'][0]['status'], 'deleted')
        self.assertEqual(second_report['results'][0]['status'], 'already_deleted')
        self.assertEqual(len(client.deletes), 1)

    def test_key_reused_by_a_later_run_is_a_conflict_and_survives(self):
        later = self.make_later_reuse_run()
        client = MemoryS3()

        code, report, _, _ = self.run_cli(client, '--execute', '--confirm', self.run.run_id)

        self.assertEqual(code, 1)
        self.assertEqual(report['results'][0]['status'], 'conflict')
        self.assertIn(f'used by run {later.run_id}', report['results'][0]['reason'])
        self.assertEqual(client.deletes, [])
        self.assertIn((BUCKET, KEY), client.objects)
        rollback_path = next(
            path for path in self.runs.iterdir() if (path / 'rollback-source.json').exists()
        )
        results = [
            json.loads(line) for line in (rollback_path / 'events.jsonl').read_text().splitlines()
        ]
        conflict = next(
            event
            for event in results
            if event.get('kind') == 'result' and event.get('stage') == 'rollback-delete'
        )
        self.assertEqual(conflict['status'], 'conflict')
        self.assertEqual(conflict['remote_outcome'], 'not_deleted')
        self.assertIn(f'used by run {later.run_id}', conflict['message'])

    def test_later_run_reusing_only_manifest_blocks_object_deletion(self):
        source, objects = self.make_order_run()
        manifest_key = 'unique-prefix/package/manifests/package.json'
        derivative_key = 'unique-prefix/package/derivatives/page.jpg'
        master_key = 'unique-prefix/package/originals/page.tif'
        later = self.make_later_reuse_run(role='manifest', key=manifest_key)
        client = MemoryS3()
        client.objects = objects

        preview_code, preview, _, _ = self.run_cli(client, source=source.path)
        preview_results = {item['key']: item for item in preview['results']}

        self.assertEqual(preview_code, 1)
        self.assertEqual(preview_results[manifest_key]['status'], 'conflict')
        self.assertIn(later.run_id, preview_results[manifest_key]['reason'])
        for key in (derivative_key, master_key):
            self.assertEqual(preview_results[key]['status'], 'not_attempted')
            self.assertIn('manifest conflict', preview_results[key]['reason'].lower())
        self.assertEqual(client.deletes, [])

        code, report, _, _ = self.run_cli(
            client, '--execute', '--confirm', source.run_id, source=source.path
        )
        results = {item['key']: item for item in report['results']}

        self.assertEqual(code, 1)
        self.assertEqual(results[manifest_key]['status'], 'conflict')
        self.assertEqual(results[derivative_key]['status'], 'not_attempted')
        self.assertEqual(results[master_key]['status'], 'not_attempted')
        for key in (manifest_key, derivative_key, master_key):
            self.assertIn((BUCKET, key), client.objects)
            self.assertNotIn(
                (BUCKET, key), [(bucket, deleted_key) for bucket, deleted_key, _ in client.deletes]
            )

    def test_current_bucket_config_must_match_the_source_snapshot(self):
        client = MemoryS3()
        current = self.root / 'current.yml'
        current.write_text(
            's3_private_bucket: another-test-bucket\ns3_public_bucket: wbl-local-test-derivatives\n'
        )

        code, _, output, _ = self.run_cli(client, config=current)

        self.assertEqual(code, 1)
        self.assertIn('bucket configuration differs', output)
        self.assertEqual(client.inspections, [])
        self.assertEqual(client.deletes, [])


def inspect_publication_reason(run_path):
    from workbench_lite.runs import inspect_run

    return inspect_run(run_path)['publication']['reason']


if __name__ == '__main__':
    unittest.main()
