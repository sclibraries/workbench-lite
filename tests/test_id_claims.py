"""Synthetic command coverage for package ID claim records."""

import base64
import csv
import hashlib
import json
from pathlib import Path
import signal
import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock, patch

from botocore.exceptions import ClientError
from PIL import Image

from rehearsal_harness import (
    TEST_ENDPOINT,
    TEST_PRIVATE_BUCKET,
    TEST_PUBLIC_BUCKET,
    FaultClient,
    FileS3Client,
    claim_root,
    clear_dead_test_locks,
    cli_client_factory,
    invoke_cli,
    start_worker,
)
from workbench_lite import cli
from workbench_lite.locks import WriterLocks
from workbench_lite.runs import _valid_publication_receipt, inspect_run, read_events
from workbench_lite.s3_client import ObjectIdentity


def claim_body(raw_id, canonical_id, kind, batch_segment, parent_id=None):
    value = {
        'schema_version': 1,
        'id': raw_id,
        'canonical_id': canonical_id,
        'kind': kind,
        'batch_id': batch_segment,
    }
    if kind == 'page':
        value['parent_id'] = parent_id
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode(
        'utf-8'
    )


READ_CONFLICT_CASES = (
    (
        'raw-id',
        ('parent',),
        (),
        'parent',
        claim_body('PARENT', 'parent', 'object', 'package'),
        ('package', 'object'),
        'PARENT',
    ),
    (
        'kind',
        ('parent',),
        (),
        'parent',
        claim_body('parent', 'parent', 'page', 'package', 'elsewhere'),
        ('package', 'page'),
        'parent',
    ),
    (
        'parent',
        ('parent',),
        (('page-1', 'parent'),),
        'page-1',
        claim_body('page-1', 'page-1', 'page', 'package', 'another-parent'),
        ('package', 'page'),
        'page-1',
    ),
    ('malformed', ('parent',), (), 'parent', b'{', 'owner unavailable', 'parent'),
)


class ClaimOperationFileS3Client(FileS3Client):
    def __init__(self, root, prefix):
        super().__init__(root)
        self.claim_root = claim_root(prefix)
        self.claim_operations = []

    def head_object(self, **kwargs):
        key = kwargs['Key']
        if key.startswith(self.claim_root):
            self.claim_operations.append(('head', key, kwargs.get('ChecksumMode')))
        return super().head_object(**kwargs)

    def put_object(self, **kwargs):
        key = kwargs['Key']
        if key.startswith(self.claim_root):
            self.claim_operations.append(('put', key, None))
        return super().put_object(**kwargs)


class ClaimVerificationMismatchClient(FileS3Client):
    def __init__(self, root, prefix, field):
        super().__init__(root)
        self.claim_root = claim_root(prefix)
        self.field = field
        self.acknowledged_claims = set()

    def put_object(self, **kwargs):
        result = super().put_object(**kwargs)
        key = kwargs['Key']
        if key.startswith(self.claim_root):
            self.acknowledged_claims.add(key)
        return result

    def head_object(self, **kwargs):
        result = super().head_object(**kwargs)
        if kwargs['Key'] in self.acknowledged_claims:
            if self.field == 'ChecksumSHA256':
                result[self.field] = base64.b64encode(
                    hashlib.sha256(b'different synthetic claim').digest()
                ).decode('ascii')
            elif self.field == 'ContentLength':
                result[self.field] += 1
            else:
                result[self.field] = 'COMPOSITE'
        return result


class SecondClaimConflictClient(FileS3Client):
    def __init__(self, root, prefix='claims'):
        super().__init__(root)
        self.conflict_key = f'{claim_root(prefix)}beta.json'
        self.injected = False

    def put_object(self, **kwargs):
        if kwargs['Key'] == self.conflict_key and not self.injected:
            self.injected = True
            self.seed(kwargs['Bucket'], kwargs['Key'], b'concurrent claim bytes')
        return super().put_object(**kwargs)


class InterleavedFileS3Client(FileS3Client):
    """Make two readers observe absence before either conditional claim PUT."""

    def __init__(self, root, prefix='claims'):
        super().__init__(root)
        self.claim_root = claim_root(prefix)
        self._claim_read_lock = threading.Lock()
        self._claim_read_counts = {}
        self._claim_barriers = {}
        self.claim_heads_by_thread = {}

    def head_object(self, **kwargs):
        key = kwargs['Key']
        if not key.startswith(self.claim_root):
            return super().head_object(**kwargs)
        thread_id = threading.get_ident()
        self.claim_heads_by_thread.setdefault(thread_id, []).append(key)
        with self._claim_read_lock:
            count = self._claim_read_counts.get(key, 0)
            self._claim_read_counts[key] = count + 1
            if count >= 2:
                return super().head_object(**kwargs)
            barrier = self._claim_barriers.setdefault(key, threading.Barrier(2))
            try:
                response = super().head_object(**kwargs)
                error = None
            except ClientError as exc:
                response, error = None, exc
        barrier.wait(timeout=10)
        if error:
            raise error
        return response


class ClaimReadDenied(FaultClient):
    def __init__(self, client, prefix):
        super().__init__(client, claim_prefix=prefix)
        self.claim_root = claim_root(prefix)

    def _denied(self):
        raise ClientError(
            {
                'Error': {'Code': 'AccessDenied'},
                'ResponseMetadata': {'HTTPStatusCode': 403},
            },
            'HeadObject',
        )

    def head_object(self, **kwargs):
        if kwargs['Key'].startswith(self.claim_root):
            self._denied()
        return super().head_object(**kwargs)

    def inspect_object(self, bucket, key):
        if key.startswith(self.claim_root):
            self._denied()
        return self._wrapped.inspect_object(bucket, key)


class ClaimWriteDenied(FaultClient):
    def __init__(self, client, prefix):
        super().__init__(client, claim_prefix=prefix)
        self.claim_root = claim_root(prefix)

    def put_object(self, **kwargs):
        if kwargs['Key'].startswith(self.claim_root):
            raise ClientError(
                {
                    'Error': {'Code': 'AccessDenied'},
                    'ResponseMetadata': {'HTTPStatusCode': 403},
                },
                'PutObject',
            )
        return super().put_object(**kwargs)


class PreflightDenied(FaultClient):
    def head_bucket(self, **kwargs):
        raise ClientError(
            {
                'Error': {'Code': 'AccessDenied'},
                'ResponseMetadata': {'HTTPStatusCode': 403},
            },
            'HeadBucket',
        )


class AlteredClaimHead(FaultClient):
    def __init__(self, client, field, value, prefix):
        super().__init__(client, claim_prefix=prefix)
        self.claim_root = claim_root(prefix)
        self.field = field
        self.value = value

    def head_object(self, **kwargs):
        response = super().head_object(**kwargs)
        if kwargs['Key'].startswith(self.claim_root):
            response[self.field] = self.value
        return response

    def inspect_object(self, bucket, key):
        if not key.startswith(self.claim_root):
            return self._wrapped.inspect_object(bucket, key)
        response = self.head_object(Bucket=bucket, Key=key, ChecksumMode='ENABLED')
        import base64

        checksum = base64.b64decode(response['ChecksumSHA256']).hex()
        return ObjectIdentity(
            checksum,
            response['ContentLength'],
            response['ChecksumType'],
            response['ETag'],
        )


class InterleavedIdClaimTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.storage_root = self.root / 'storage'
        self.store = FileS3Client(self.storage_root)
        self.lock_root = self.root / 'locks'
        self.lock_root.mkdir(mode=0o700)
        self.package_prefixes = set()
        self.package_prefix_by_config = {}

    def package(
        self,
        name,
        *,
        batch_id='package',
        parent_ids=('parent',),
        pages=(),
        prefix='claims',
        private_bucket=TEST_PRIVATE_BUCKET,
        public_bucket=TEST_PUBLIC_BUCKET,
    ):
        root = self.root / name
        root.mkdir(parents=True)
        csv_path = root / 'input.csv'
        with csv_path.open('w', encoding='utf-8', newline='') as handle:
            writer = csv.writer(handle)
            writer.writerow(['title', 'id', 'field_model', 'parent_id', 'field_weight', 'file'])
            for index, identifier in enumerate(parent_ids, 1):
                filename = f'parent-{index}.pdf'
                (root / filename).write_bytes(b'%PDF-1.4\nsynthetic\n%%EOF\n')
                writer.writerow([f'Parent {index}', identifier, 'Paged Content', '', '', filename])
            for index, (page_id, parent_id) in enumerate(pages, 1):
                filename = f'page-{index}.tif'
                Image.new('RGB', (12, 10), (index * 20, 30, 80)).save(root / filename)
                writer.writerow([f'Page {index}', page_id, 'Page', parent_id, index, filename])
        config = root / 'package.yml'
        prefix_setting = '' if prefix is None else f's3_prefix: {prefix}\n'
        config.write_text(
            f'input_dir: .\ninput_csv: input.csv\nbatch_id: {batch_id}\n'
            f'{prefix_setting}s3_private_bucket: {private_bucket}\n'
            f's3_public_bucket: {public_bucket}\n',
            encoding='utf-8',
        )
        effective_prefix = (prefix or 'workbench-lite').strip('/')
        self.package_prefixes.add(effective_prefix)
        self.package_prefix_by_config[config] = effective_prefix
        return config

    def generate(self, config, name):
        output = self.root / name
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
            self.root / f'{name}-generation-runs',
        )
        self.assertEqual(result['exit'], 0, result['stdout'] + result['stderr'])
        return output

    def push(
        self,
        config,
        output=None,
        *,
        name='push-runs',
        resume=False,
        dry_run=False,
        offline=False,
        factory=None,
        extra_patches=(),
    ):
        arguments = ['push', '--config', config, '--dry-run' if dry_run else '--execute']
        arguments.extend(['--offline'] if offline else [])
        arguments.extend(['--generated-dir', output] if output is not None else [])
        arguments.extend(['--resume'] if resume else [])
        arguments.extend(
            ['--endpoint-url', TEST_ENDPOINT, '--region', 'us-east-1', '--format', 'json']
        )
        arguments.extend(['--run-dir', self.root / name])
        return self.invoke(
            *arguments,
            factory=factory
            or cli_client_factory(self.store, claim_prefix=self.package_prefix_by_config[config]),
            extra_patches=extra_patches,
        )

    def invoke(self, *arguments, factory=None, extra_patches=()):
        return invoke_cli(
            arguments,
            factory or cli_client_factory(self.store),
            self.lock_root,
            extra_patches=extra_patches,
        )

    @staticmethod
    def run_path(output):
        return Path(json.loads((output / '.workbench-run.json').read_text())['run_path'])

    @staticmethod
    def claim_key(identifier, prefix='claims'):
        return f'{claim_root(prefix)}{identifier}.json'

    def seed_claim(self, identifier, body, *, bucket=TEST_PRIVATE_BUCKET, prefix='claims'):
        return self.store.seed(bucket, self.claim_key(identifier, prefix), body)

    def claim_keys(self, bucket=TEST_PRIVATE_BUCKET, prefix='claims'):
        return set(self.store.listing(bucket, claim_root(prefix)))

    def content_puts(self, prefix=None):
        prefixes = {prefix.strip('/')} if prefix is not None else self.package_prefixes
        roots = tuple(claim_root(value) for value in prefixes)
        return [(bucket, key) for bucket, key in self.store.put_calls if not key.startswith(roots)]

    def test_read_pass_refuses_other_normalized_batch_before_any_write(self):
        config = self.package(
            'read-other-batch', batch_id='Current Batch', parent_ids=('alpha', 'beta')
        )
        output = self.generate(config, 'read-other-batch-generated')
        self.seed_claim('beta', claim_body('beta', 'beta', 'object', 'Older_Batch'))

        result = self.push(config, output)

        self.assertEqual(result['exit'], 3, result['stdout'] + result['stderr'])
        self.assertIn('Older_Batch', result['stdout'] + result['stderr'])
        self.assertIn('object', result['stdout'] + result['stderr'])
        self.assertIn('beta', result['stdout'] + result['stderr'])
        self.assertEqual(self.store.put_calls, [])

    def test_read_pass_refuses_raw_id_kind_parent_and_malformed_claims(self):
        for case in READ_CONFLICT_CASES:
            with self.subTest(conflict=case[0]):
                self._assert_read_pass_conflict(case)

    def _assert_read_pass_conflict(self, case):
        label, parents, pages, identifier, body, owner, conflicting_raw_id = case
        prefix = f'read-{label}'
        config = self.package(f'read-{label}', parent_ids=parents, pages=pages, prefix=prefix)
        output = self.generate(config, f'read-{label}-generated')
        self.seed_claim(identifier, body, prefix=prefix)
        result = self.push(config, output, name=f'read-{label}-push-runs')
        details = owner if isinstance(owner, tuple) else (owner,)
        self.assertEqual(result['exit'], 3, result['stdout'] + result['stderr'])
        self.assertIn(identifier, result['stdout'] + result['stderr'])
        self.assertIn(conflicting_raw_id, result['stdout'] + result['stderr'])
        for detail in details:
            self.assertIn(detail, result['stdout'] + result['stderr'])
        self.assertEqual(self.store.put_calls, [])
        self.store.put_calls.clear()

    def test_parent_and_page_ids_share_claim_namespace_across_batches(self):
        first = self.package('namespace-object', batch_id='first', parent_ids=('shared',))
        first_output = self.generate(first, 'namespace-object-generated')
        self.assertEqual(self.push(first, first_output, name='namespace-object-runs')['exit'], 0)
        second = self.package(
            'namespace-page',
            batch_id='second',
            parent_ids=('container',),
            pages=(('shared', 'container'),),
        )
        second_output = self.generate(second, 'namespace-page-generated')
        self.store.put_calls.clear()

        result = self.push(second, second_output, name='namespace-page-runs')

        self.assertEqual(result['exit'], 3, result['stdout'] + result['stderr'])
        self.assertIn('shared', result['stdout'] + result['stderr'])
        self.assertEqual(self.store.put_calls, [])

    def test_create_pass_412_keeps_earlier_claims_without_content_writes(self):
        class SecondClaimConflict(FileS3Client):
            def __init__(self, root):
                super().__init__(root)
                self.injected = False

            def put_object(self, **kwargs):
                if kwargs['Key'] == 'claims/_ids/beta.json' and not self.injected:
                    self.injected = True
                    self.seed(kwargs['Bucket'], kwargs['Key'], b'concurrent claim bytes')
                return super().put_object(**kwargs)

        self.store = SecondClaimConflict(self.storage_root)
        config = self.package('create-412', parent_ids=('alpha', 'beta'))
        output = self.generate(config, 'create-412-generated')

        result = self.push(config, output)

        self.assertEqual(result['exit'], 3, result['stdout'] + result['stderr'])
        self.assertEqual(self.content_puts(), [])
        self.assertIn('claims/_ids/alpha.json', self.claim_keys())
        self.assertEqual(
            self.store._path(TEST_PRIVATE_BUCKET, 'claims/_ids/beta.json').read_bytes(),
            b'concurrent claim bytes',
        )

    def test_opposite_row_order_interleaves_to_one_writer_with_real_locks(self):
        self.store = InterleavedFileS3Client(self.storage_root)
        first = self.package('interleave-first', batch_id='first', parent_ids=('alpha', 'beta'))
        second = self.package('interleave-second', batch_id='second', parent_ids=('beta', 'alpha'))
        first_output = self.generate(first, 'interleave-first-generated')
        second_output = self.generate(second, 'interleave-second-generated')
        codes = self._run_interleaved_packages(first, second, first_output, second_output)

        self.assertEqual(sorted(codes), [0, 3])
        self.assertEqual(
            [keys[:2] for keys in self.store.claim_heads_by_thread.values()],
            [['claims/_ids/alpha.json', 'claims/_ids/beta.json']] * 2,
        )

    def _run_interleaved_packages(self, first, second, first_output, second_output):
        lock_roots = [self.root / 'host-one-locks', self.root / 'host-two-locks']
        for lock_root in lock_roots:
            lock_root.mkdir(mode=0o700)
        with (
            patch('workbench_lite.cli.create_s3_client', return_value=self.store),
            patch('sys.stdout'),
            patch('sys.stderr'),
            ThreadPoolExecutor(max_workers=2) as executor,
        ):
            futures = [
                executor.submit(
                    self._execute_with_lock_root,
                    first,
                    first_output,
                    'host-one-runs',
                    lock_roots[0],
                ),
                executor.submit(
                    self._execute_with_lock_root,
                    second,
                    second_output,
                    'host-two-runs',
                    lock_roots[1],
                ),
            ]
            return [future.result(timeout=20) for future in futures]

    def _execute_with_lock_root(self, config, output, run_name, lock_root):
        arguments = [
            'push',
            '--config',
            str(config),
            '--execute',
            '--generated-dir',
            str(output),
            '--endpoint-url',
            TEST_ENDPOINT,
            '--region',
            'us-east-1',
            '--format',
            'json',
            '--run-dir',
            str(self.root / run_name),
        ]
        return cli.main(arguments, lock_factory=lambda: WriterLocks(lock_root))

    def test_same_batch_unchanged_rerun_reuses_deterministic_claims(self):
        config = self.package('same-batch', pages=(('page-1', 'parent'),))
        first_output = self.generate(config, 'same-batch-first-generated')
        first = self.push(config, first_output, name='same-batch-first-runs')
        self.assertEqual(first['exit'], 0, first['stdout'] + first['stderr'])
        before = {
            key: self.store._path(TEST_PRIVATE_BUCKET, key).read_bytes()
            for key in self.claim_keys()
        }
        self.assertEqual(set(before), {'claims/_ids/parent.json', 'claims/_ids/page-1.json'})

        second_output = self.generate(config, 'same-batch-second-generated')
        second = self.push(config, second_output, name='same-batch-second-runs')

        self.assertEqual(second['exit'], 0, second['stdout'] + second['stderr'])
        self.assertEqual(self.claim_keys(), set(before))
        self.assertEqual(
            {key: self.store._path(TEST_PRIVATE_BUCKET, key).read_bytes() for key in before},
            before,
        )
        self.assertTrue(
            inspect_run(self.run_path(second_output))['publication']['state'] == 'ready_for_review'
        )

    def test_same_batch_new_parent_gets_a_claim_and_succeeds(self):
        config = self.package('add-parent', parent_ids=('parent-a',))
        first_output = self.generate(config, 'add-parent-first-generated')
        self.assertEqual(self.push(config, first_output, name='add-parent-first-runs')['exit'], 0)
        with (self.root / 'add-parent' / 'input.csv').open(
            'a', encoding='utf-8', newline=''
        ) as handle:
            csv.writer(handle).writerow(
                ['Parent B', 'parent-b', 'Paged Content', '', '', 'parent-2.pdf']
            )
        (self.root / 'add-parent' / 'parent-2.pdf').write_bytes(b'%PDF-1.4\nsecond\n%%EOF\n')
        second_output = self.generate(config, 'add-parent-second-generated')

        result = self.push(config, second_output, name='add-parent-second-runs')

        self.assertEqual(result['exit'], 0, result['stdout'] + result['stderr'])
        self.assertIn('claims/_ids/parent-b.json', self.claim_keys())

    def test_additive_page_claim_passes_before_existing_manifest_hold(self):
        root = self.root / 'add-page'
        config = self.package('add-page', pages=(('page-1', 'parent'),))
        first_output = self.generate(config, 'add-page-first-generated')
        self.assertEqual(self.push(config, first_output, name='add-page-first-runs')['exit'], 0)
        Image.new('RGB', (12, 10), (80, 40, 20)).save(root / 'page-2.tif')
        with (root / 'input.csv').open('a', encoding='utf-8', newline='') as handle:
            csv.writer(handle).writerow(['Page 2', 'page-2', 'Page', 'parent', 2, 'page-2.tif'])
        second_output = self.generate(config, 'add-page-second-generated')

        result = self.push(config, second_output, name='add-page-second-runs')

        self.assertEqual(result['exit'], 1, result['stdout'] + result['stderr'])
        self.assertEqual(inspect_run(self.run_path(second_output))['publication']['state'], 'held')
        self.assertIn('claims/_ids/page-2.json', self.claim_keys())

    def test_prefix_with_ids_segment_uploads_and_rolls_back_only_exact_claim_root(self):
        prefix = 'archive/_ids'
        config = self.package(
            'prefix-with-ids-segment',
            prefix=prefix,
            pages=(('page-1', 'parent'),),
        )
        output = self.generate(config, 'prefix-with-ids-segment-generated')
        run_path = self.run_path(output)
        content_keys, expected_claims = self._assert_prefix_uploads(
            config, output, run_path, prefix
        )
        self._assert_prefix_rollback(config, run_path, prefix, content_keys, expected_claims)

    def _assert_prefix_uploads(self, config, output, run_path, prefix):
        plan = json.loads((run_path / 'upload-plan.json').read_text())['entries']
        content_keys = {(entry['bucket'], entry['key']) for entry in plan}
        expected_claims = {
            f'{claim_root(prefix)}parent.json',
            f'{claim_root(prefix)}page-1.json',
        }

        pushed = self.push(config, output, name='prefix-with-ids-segment-push-runs')

        self.assertEqual(pushed['exit'], 0, pushed['stdout'] + pushed['stderr'])
        self.assertTrue(content_keys)
        self.assertTrue(
            any(
                key.startswith(f'{prefix}/') and not key.startswith(claim_root(prefix))
                for _, key in content_keys
            )
        )
        for bucket, key in content_keys:
            self.assertTrue(self.store._path(bucket, key).is_file(), key)
        self.assertEqual(set(self.content_puts(prefix)), content_keys)
        self.assertEqual(self.claim_keys(prefix=prefix), expected_claims)
        return content_keys, expected_claims

    def _assert_prefix_rollback(self, config, run_path, prefix, content_keys, expected_claims):
        rollback_args = [
            'rollback',
            run_path,
            '--config',
            config,
            '--endpoint-url',
            TEST_ENDPOINT,
            '--region',
            'us-east-1',
            '--format',
            'json',
        ]
        preview = self.invoke(*rollback_args)
        self.assertEqual(preview['exit'], 0, preview['stdout'] + preview['stderr'])
        self.assertEqual(preview['payload']['planned_delete_count'], len(content_keys))
        self.assertEqual(preview['payload']['deleted_count'], 0)
        self.assertEqual(
            {(item['bucket'], item['key']) for item in preview['payload']['results']},
            content_keys,
        )
        self.assertTrue(
            all(item['status'] == 'would_delete' for item in preview['payload']['results'])
        )
        removed = self.invoke(
            *rollback_args[:-2],
            '--execute',
            '--confirm',
            run_path.name,
            *rollback_args[-2:],
        )
        self.assertEqual(removed['exit'], 0, removed['stdout'] + removed['stderr'])
        self.assertEqual(
            set(self.store.listing(TEST_PRIVATE_BUCKET, f'{prefix}/')),
            expected_claims,
        )
        self.assertEqual(self.store.listing(TEST_PUBLIC_BUCKET, f'{prefix}/'), {})

    def test_claim_prefix_defaults_and_trims_boundary_slashes(self):
        omitted = self.package('prefix-omitted', prefix=None)
        omitted_output = self.generate(omitted, 'prefix-omitted-generated')
        omitted_result = self.push(omitted, omitted_output, name='prefix-omitted-runs')
        self.assertEqual(
            omitted_result['exit'], 0, omitted_result['stdout'] + omitted_result['stderr']
        )
        self.assertEqual(
            self.claim_keys(prefix='workbench-lite'),
            {'workbench-lite/_ids/parent.json'},
        )

        slash_prefix = self.package('prefix-slashes', prefix='/trimmed/')
        slash_output = self.generate(slash_prefix, 'prefix-slashes-generated')
        slash_result = self.push(slash_prefix, slash_output, name='prefix-slashes-runs')
        self.assertEqual(slash_result['exit'], 0, slash_result['stdout'] + slash_result['stderr'])
        self.assertEqual(self.claim_keys(prefix='trimmed'), {'trimmed/_ids/parent.json'})

        plain_prefix = self.package('prefix-plain', prefix='trimmed')
        plain_output = self.generate(plain_prefix, 'prefix-plain-generated')
        plain_result = self.push(plain_prefix, plain_output, name='prefix-plain-runs')
        self.assertEqual(plain_result['exit'], 0, plain_result['stdout'] + plain_result['stderr'])
        self.assertEqual(self.claim_keys(prefix='trimmed'), {'trimmed/_ids/parent.json'})

    def test_existing_blockers_still_precede_all_claim_writes(self):
        self._assert_invalid_package_precedes_claim_writes()
        self.assertEqual(self.store.put_calls, [])

        config = self.package('blockers', pages=(('page-1', 'parent'),))
        output = self.generate(config, 'blockers-generated')
        self._assert_missing_artifact_precedes_claim_writes(config, output)
        output = self.generate(config, 'blockers-size-generated')
        self._assert_oversized_plan_precedes_claim_writes(config, output)
        output = self.generate(config, 'blockers-preflight-generated')
        self._assert_preflight_precedes_claim_writes(config, output)
        output = self.generate(config, 'blockers-destination-generated')
        self._assert_destination_conflict_precedes_claim_writes(config, output)

    def _assert_invalid_package_precedes_claim_writes(self):
        invalid = self.package('blockers-invalid', parent_ids=('duplicate', 'duplicate'))
        no_client = Mock(side_effect=AssertionError('validation failure created an S3 client'))
        invalid_result = self.push(
            invalid,
            name='invalid-package-runs',
            factory=no_client,
        )
        self.assertEqual(invalid_result['exit'], 3)
        no_client.assert_not_called()
        self.assertEqual(self.store.put_calls, [])

    def _assert_missing_artifact_precedes_claim_writes(self, config, output):
        run_path = self.run_path(output)
        plan = json.loads((run_path / 'upload-plan.json').read_text())['entries']
        manifest = next(entry for entry in plan if entry['role'] == 'manifest')
        (output / manifest['key']).unlink()

        missing = self.push(config, output, name='missing-artifact-runs')

        self.assertEqual(missing['exit'], 1)
        self.assertEqual(self.store.put_calls, [])
        self.assertEqual(self.claim_keys(), set())

    def _assert_oversized_plan_precedes_claim_writes(self, config, output):
        with patch('workbench_lite.push.MAX_SINGLE_PUT_BYTES', 1):
            oversized = self.push(config, output, name='oversized-runs')
        self.assertEqual(oversized['exit'], 1)
        self.assertEqual(self.store.put_calls, [])
        self.assertEqual(self.claim_keys(), set())

    def _assert_preflight_precedes_claim_writes(self, config, output):
        def denied_factory(*args, **kwargs):
            return PreflightDenied(self.store)

        preflight = self.push(
            config,
            output,
            name='preflight-denied-runs',
            factory=denied_factory,
        )
        self.assertEqual(preflight['exit'], 1)
        self.assertEqual(self.store.put_calls, [])
        self.assertEqual(self.claim_keys(), set())

    def _assert_destination_conflict_precedes_claim_writes(self, config, output):
        run_path = self.run_path(output)
        plan = json.loads((run_path / 'upload-plan.json').read_text())['entries']
        master = next(entry for entry in plan if entry['role'] == 'master_tiff')
        self.store.seed(master['bucket'], master['key'], b'different existing content')
        conflict = self.push(config, output, name='destination-conflict-runs')
        self.assertEqual(conflict['exit'], 1)
        self.assertEqual(self.claim_keys(), set())
        self.assertEqual(self.content_puts(), [])

    def test_claim_bytes_match_object_and_page_fixtures_and_use_full_object_head(self):
        config = self.package('byte-fixture', pages=(('page-1', 'parent'),))
        output = self.generate(config, 'byte-fixture-generated')

        result = self.push(config, output)

        self.assertEqual(result['exit'], 0, result['stdout'] + result['stderr'])
        fixtures = {
            'claims/_ids/parent.json': (
                b'{"batch_id":"package","canonical_id":"parent","id":"parent",'
                b'"kind":"object","schema_version":1}'
            ),
            'claims/_ids/page-1.json': (
                b'{"batch_id":"package","canonical_id":"page-1","id":"page-1",'
                b'"kind":"page","parent_id":"parent","schema_version":1}'
            ),
        }
        for key, expected in fixtures.items():
            path = self.store._path(TEST_PRIVATE_BUCKET, key)
            self.assertTrue(path.is_file(), f'missing retained claim: {key}')
            self.assertEqual(path.read_bytes(), expected)
        claim_heads = [
            call for call in self.store.head_calls if call['Key'].startswith(claim_root('claims'))
        ]
        self.assertTrue(claim_heads)
        self.assertTrue(all(call.get('ChecksumMode') == 'ENABLED' for call in claim_heads))

    def test_each_created_claim_is_followed_by_a_checksum_enabled_verification_head(self):
        prefix = 'claim-verify-order'
        self.store = ClaimOperationFileS3Client(self.storage_root, prefix)
        config = self.package('claim-verify-order', prefix=prefix, pages=(('page-1', 'parent'),))
        output = self.generate(config, 'claim-verify-order-generated')

        result = self.push(config, output)

        self.assertEqual(result['exit'], 0, result['stdout'] + result['stderr'])
        puts = [
            (index, key)
            for index, (operation, key, _) in enumerate(self.store.claim_operations)
            if operation == 'put'
        ]
        self.assertEqual(
            {key for _, key in puts},
            {f'{claim_root(prefix)}parent.json', f'{claim_root(prefix)}page-1.json'},
        )
        for put_index, key in puts:
            verification = next(
                (
                    checksum_mode
                    for operation, head_key, checksum_mode in self.store.claim_operations[
                        put_index + 1 :
                    ]
                    if operation == 'head' and head_key == key
                ),
                None,
            )
            self.assertEqual(verification, 'ENABLED', f'missing verification HEAD for {key}')

    def test_created_claim_verification_mismatch_holds_without_content_writes(self):
        cases = (
            'ChecksumSHA256',
            'ContentLength',
            'ChecksumType',
        )
        for field in cases:
            with self.subTest(field=field):
                prefix = f'claim-verify-mismatch-{field.lower()}'
                self.store = ClaimVerificationMismatchClient(self.storage_root, prefix, field)
                config = self.package(f'claim-verify-mismatch-{field.lower()}', prefix=prefix)
                output = self.generate(config, f'claim-verify-mismatch-{field.lower()}-generated')

                result = self.push(
                    config,
                    output,
                    name=f'claim-verify-mismatch-{field.lower()}-runs',
                )

                self.assertEqual(result['exit'], 1, result['stdout'] + result['stderr'])
                self.assertEqual(inspect_run(self.run_path(output))['publication']['state'], 'held')
                expected_key = f'{claim_root(prefix)}parent.json'
                self.assertIn(expected_key, self.store.acknowledged_claims)
                self.assertEqual(self.content_puts(prefix), [])

    def test_claim_head_rejects_non_full_checksum_and_wrong_size(self):
        for field in ('ChecksumType', 'ContentLength'):
            with self.subTest(field=field):
                prefix = f'claim-head-{field.lower()}'
                config = self.package(f'claim-head-{field}', prefix=prefix)
                output = self.generate(config, f'claim-head-{field}-generated')
                body = claim_body('parent', 'parent', 'object', 'package')
                value = 'COMPOSITE' if field == 'ChecksumType' else len(body) + 1
                self.seed_claim('parent', body, prefix=prefix)

                def factory(*args, field=field, value=value, prefix=prefix, **kwargs):
                    return AlteredClaimHead(self.store, field, value, prefix)

                result = self.push(
                    config,
                    output,
                    name=f'claim-head-{field}-runs',
                    factory=factory,
                )

                self.assertEqual(result['exit'], 3, result['stdout'] + result['stderr'])
                self.assertEqual(self.store.put_calls, [])

    def test_interrupted_claim_put_is_inspected_reconciled_head_only_and_resumed(self):
        config = self.package('claim-interruption')
        output = self.generate(config, 'claim-interruption-generated')
        run_path = self.run_path(output)
        self._interrupt_claim_write(config, output)
        self._assert_interrupted_claim_is_unknown(run_path, config)
        self._assert_reconcile_is_head_only(run_path)
        resumed = self.push(config, output, name='claim-interruption-runs', resume=True)
        self.assertEqual(resumed['exit'], 0, resumed['stdout'] + resumed['stderr'])
        self.assertEqual(inspect_run(run_path)['publication']['state'], 'ready_for_review')

    def _interrupt_claim_write(self, config, output):
        ready = self.root / 'claim-put-ready'
        arguments = self._claim_push_arguments(config, output, 'claim-interruption-runs')
        process, _ = start_worker(
            arguments,
            storage_root=self.storage_root,
            lock_root=self.lock_root,
            fault='claim_after_1',
            ready_file=ready,
            hold_barrier=True,
            claim_prefix='claims',
        )
        self._kill_at_barrier(process, ready, 'claim PUT')

    def _claim_push_arguments(self, config, output, name):
        return [
            'push',
            '--config',
            str(config),
            '--execute',
            '--generated-dir',
            str(output),
            '--endpoint-url',
            TEST_ENDPOINT,
            '--region',
            'us-east-1',
            '--format',
            'json',
            '--run-dir',
            str(self.root / name),
        ]

    def _kill_at_barrier(self, process, ready, operation):
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline and process.poll() is None and not ready.exists():
            time.sleep(0.02)
        if not ready.exists():
            stdout, stderr = process.communicate(timeout=15)
            self.fail(f'push never reached the {operation} barrier: {stdout}\n{stderr}')
        process.send_signal(signal.SIGKILL)
        process.communicate(timeout=15)
        clear_dead_test_locks(self.lock_root, process.pid)

    def _assert_interrupted_claim_is_unknown(self, run_path, config):
        inspected = inspect_run(run_path)
        self.assertTrue(
            any(item.get('stage') == 'claim' for item in inspected['unknown_operations'])
        )
        rollback = self.invoke(
            'rollback',
            run_path,
            '--config',
            config,
            '--endpoint-url',
            TEST_ENDPOINT,
            '--region',
            'us-east-1',
            '--execute',
            '--confirm',
            run_path.name,
            '--format',
            'json',
        )
        self.assertEqual(rollback['exit'], 1)

    def _assert_reconcile_is_head_only(self, run_path):
        before_gets = len(self.store.get_calls)
        reconciled = self.invoke('reconcile', run_path, '--format', 'json')
        self.assertEqual(reconciled['exit'], 0, reconciled['stdout'] + reconciled['stderr'])
        events, error = read_events(run_path)
        self.assertIsNone(error)
        results = [
            event for event in events if event['kind'] == 'result' and event.get('stage') == 'claim'
        ]
        self.assertTrue(any(event.get('status') == 'matched' for event in results))
        self.assertFalse(any(event.get('status') == 'created' for event in results))
        self.assertEqual(len(self.store.get_calls), before_gets)

    def test_claim_conflict_on_resume_exits_three(self):
        config = self.package('claim-resume-conflict')
        output = self.generate(config, 'claim-resume-conflict-generated')
        ready = self.root / 'claim-before-ready'
        arguments = self._claim_push_arguments(config, output, 'claim-resume-conflict-runs')
        process, _ = start_worker(
            arguments,
            storage_root=self.storage_root,
            lock_root=self.lock_root,
            fault='claim_before_1',
            ready_file=ready,
            hold_barrier=True,
            claim_prefix='claims',
        )
        self._kill_at_barrier(process, ready, 'claim PUT')
        self.seed_claim('parent', claim_body('other-parent', 'parent', 'object', 'package'))

        resumed = self.push(config, output, name='claim-resume-conflict-runs', resume=True)

        self.assertEqual(resumed['exit'], 3, resumed['stdout'] + resumed['stderr'])
        self.assertEqual(self.content_puts(), [])

    def test_receipt_requires_claim_evidence_to_be_present_settled_and_unaltered(self):
        cases = ('remove', 'conflict', 'unknown', 'altered', 'empty')
        for label in cases:
            with self.subTest(evidence=label):
                config = self.package(f'receipt-{label}', prefix=f'receipt-{label}')
                output = self.generate(config, f'receipt-{label}-generated')
                pushed = self.push(config, output, name=f'receipt-{label}-runs')
                self.assertEqual(pushed['exit'], 0, pushed['stdout'] + pushed['stderr'])
                run_path = self.run_path(output)
                self._mutate_claim_journal(run_path, label)

                self.assertEqual(inspect_run(run_path)['publication']['state'], 'held')

    def test_receipt_derives_empty_and_reduced_expected_sets_from_the_plan(self):
        for case in ('empty', 'reduced', 'altered_expected'):
            with self.subTest(expected_claims=case):
                config = self.package(
                    f'receipt-plan-{case}',
                    parent_ids=('alpha', 'beta'),
                    prefix=f'receipt-plan-{case}',
                )
                output = self.generate(config, f'receipt-plan-{case}-generated')
                pushed = self.push(config, output, name=f'receipt-plan-{case}-runs')
                self.assertEqual(pushed['exit'], 0, pushed['stdout'] + pushed['stderr'])
                run_path = self.run_path(output)
                self._mutate_claim_journal(run_path, case)
                self.assertEqual(inspect_run(run_path)['publication']['state'], 'held')

    def test_claim_evidence_reducer_agrees_across_inspection_receipt_and_inventory(self):
        config = self.package('claim-reducer')
        output = self.generate(config, 'claim-reducer-generated')
        self.assertEqual(self.push(config, output)['exit'], 0)
        run_path = self.run_path(output)
        self._append_pending_claim_intent(run_path)

        inspected = inspect_run(run_path)
        inventory = self.invoke('rollback-inventory', run_path, '--format', 'json')
        events, error = read_events(run_path)
        receipt = self._publication_receipt(events)

        self.assertIsNone(error)
        self.assertEqual(inspected['publication']['state'], 'held')
        self.assertEqual(inspected['claim_counts'], inventory['payload']['claim_counts'])
        self.assertEqual(inspected['claim_counts']['unsettled'], 1)
        self.assertFalse(_valid_publication_receipt(run_path, events, receipt))

    def test_invalid_expected_claims_make_inventory_error_and_refuse_rollback(self):
        for case in ('empty', 'altered_expected'):
            with self.subTest(expected_claims=case):
                prefix = f'invalid-claim-rollback-{case}'
                config, run_path = self._published_claim_run(prefix)
                self._mutate_claim_journal(run_path, case)
                content_before = self.store.listing(TEST_PUBLIC_BUCKET, f'{prefix}/')

                inventory = self.invoke('rollback-inventory', run_path, '--format', 'json')
                self._assert_invalid_claim_inventory(inventory)
                rollback = self.invoke(*self._rollback_execute_arguments(config, run_path))

                self.assertEqual(rollback['exit'], 1, rollback['stdout'] + rollback['stderr'])
                self.assertEqual(self.store.delete_calls, [])
                self.assertEqual(
                    self.store.listing(TEST_PUBLIC_BUCKET, f'{prefix}/'), content_before
                )

    def test_valid_settled_claim_conflict_remains_rollback_eligible(self):
        prefix = 'settled-claim-conflict'
        config, run_path = self._published_claim_run(prefix)
        self._mutate_claim_journal(run_path, 'conflict')

        inventory = self.invoke('rollback-inventory', run_path, '--format', 'json')
        self.assertEqual(inventory['exit'], 0, inventory['stdout'] + inventory['stderr'])
        self.assertEqual(inventory['payload']['state'], 'reviewable')
        self.assertEqual(inventory['payload']['claim_counts']['conflict'], 1)
        self.assertEqual(inventory['payload']['claim_counts']['unsettled'], 0)

        rollback = self.invoke(*self._rollback_execute_arguments(config, run_path))

        self.assertEqual(rollback['exit'], 0, rollback['stdout'] + rollback['stderr'])
        self.assertTrue(self.store.delete_calls)
        self.assertTrue(self.claim_keys(prefix=prefix))

    def _published_claim_run(self, prefix):
        config = self.package(prefix, parent_ids=('alpha', 'beta'), prefix=prefix)
        output = self.generate(config, f'{prefix}-generated')
        pushed = self.push(config, output, name=f'{prefix}-runs')
        self.assertEqual(pushed['exit'], 0, pushed['stdout'] + pushed['stderr'])
        return config, self.run_path(output)

    @staticmethod
    def _rollback_execute_arguments(config, run_path):
        return (
            'rollback',
            run_path,
            '--config',
            config,
            '--endpoint-url',
            TEST_ENDPOINT,
            '--region',
            'us-east-1',
            '--execute',
            '--confirm',
            run_path.name,
            '--format',
            'json',
        )

    def _assert_invalid_claim_inventory(self, inventory):
        self.assertEqual(inventory['exit'], 1, inventory['stdout'] + inventory['stderr'])
        self.assertEqual(inventory['payload']['state'], 'invalid_evidence')
        self.assertTrue(
            any('claim' in error.lower() for error in inventory['payload']['evidence_errors'])
        )

    def _mutate_claim_journal(self, run_path, case):
        events, error = read_events(run_path)
        self.assertIsNone(error)
        context = next(
            event
            for event in events
            if event['kind'] == 'context' and event.get('stage') == 'claim'
        )
        expected = context['expected_claims']
        if case in {'empty', 'reduced', 'altered_expected'}:
            if case == 'empty':
                context['expected_claims'] = []
            elif case == 'reduced':
                context['expected_claims'] = expected[:-1]
            else:
                expected[-1]['checksum'] = '0' * 64
            self._write_events(run_path, events)
            return
        operation = self._created_claim_operation(events, expected[-1])
        if case == 'remove':
            events = [event for event in events if event.get('operation_id') != operation]
        else:
            result = next(
                event
                for event in events
                if event.get('operation_id') == operation and event['kind'] == 'result'
            )
            self._alter_claim_result(result, case)
        self._write_events(run_path, events)

    @staticmethod
    def _created_claim_operation(events, identity):
        def matches(event):
            return all(event.get(key) == value for key, value in identity.items())

        result = next(
            event
            for event in reversed(events)
            if event['kind'] == 'result'
            and event.get('stage') == 'claim'
            and event.get('status') == 'created'
            and matches(event)
        )
        return result['operation_id']

    @staticmethod
    def _alter_claim_result(result, case):
        if case == 'altered':
            result['observed_checksum'] = '0' * 64
        else:
            result['status'] = case
            result['remote_outcome'] = case

    @staticmethod
    def _write_events(run_path, events):
        for sequence, event in enumerate(events, 1):
            event['sequence'] = sequence
        (Path(run_path) / 'events.jsonl').write_text(
            ''.join(json.dumps(event, sort_keys=True) + '\n' for event in events),
            encoding='utf-8',
        )

    def _append_pending_claim_intent(self, run_path):
        events, error = read_events(run_path)
        self.assertIsNone(error)
        identity = next(
            event['expected_claims'][0]
            for event in events
            if event['kind'] == 'context' and event.get('stage') == 'claim'
        )
        events.append(
            {
                'sequence': len(events) + 1,
                'kind': 'intent',
                'stage': 'claim',
                'operation_id': 'synthetic-pending-claim',
                'attempt': 1,
                **identity,
            }
        )
        self._write_events(run_path, events)

    @staticmethod
    def _publication_receipt(events):
        return next(
            event['publication_receipt']
            for event in reversed(events)
            if event.get('publication_receipt')
        )

    def test_claim_only_run_rolls_back_without_deleting_claims_and_later_batch_is_refused(self):
        first = self.package('claim-only', parent_ids=('alpha', 'beta'))
        first_output = self.generate(first, 'claim-only-generated')
        self._refuse_second_claim(first, first_output, 'claim-only-runs')
        run_path = self.run_path(first_output)
        self._assert_claim_only_rollback(first, run_path)
        self._assert_later_claim_batch_refused()

    def _refuse_second_claim(self, config, output, run_name):
        self.store = SecondClaimConflictClient(self.storage_root)
        refused = self.push(config, output, name=run_name)
        self.assertEqual(refused['exit'], 3, refused['stdout'] + refused['stderr'])

    def _assert_claim_only_rollback(self, config, run_path):
        inventory = self.invoke('rollback-inventory', run_path, '--format', 'json')
        self.assertEqual(inventory['exit'], 0, inventory['stdout'] + inventory['stderr'])
        self.assertEqual(inventory['payload']['new_keys'], [])
        preview = self.invoke(
            'rollback',
            run_path,
            '--config',
            config,
            '--endpoint-url',
            TEST_ENDPOINT,
            '--region',
            'us-east-1',
            '--format',
            'json',
        )
        self.assertEqual(preview['exit'], 0, preview['stdout'] + preview['stderr'])
        self.assertEqual(preview['payload']['deleted_count'], 0)
        removed = self.invoke(
            'rollback',
            run_path,
            '--config',
            config,
            '--endpoint-url',
            TEST_ENDPOINT,
            '--region',
            'us-east-1',
            '--execute',
            '--confirm',
            run_path.name,
            '--format',
            'json',
        )
        self.assertEqual(removed['exit'], 0, removed['stdout'] + removed['stderr'])
        self.assertIn('claims/_ids/alpha.json', self.claim_keys())
        self.assertEqual(self.content_puts(), [])

    def _assert_later_claim_batch_refused(self):
        second = self.package('claim-only-later', batch_id='later', parent_ids=('alpha',))
        output = self.generate(second, 'claim-only-later-generated')
        self.store.put_calls.clear()
        later = self.push(second, output, name='claim-only-later-runs')
        self.assertEqual(later['exit'], 3, later['stdout'] + later['stderr'])
        self.assertEqual(self.store.put_calls, [])

    def test_created_claim_still_allows_rollback_after_refused_resume(self):
        config = self.package('claim-resume-rollback', parent_ids=('alpha', 'beta'))
        output = self.generate(config, 'claim-resume-rollback-generated')
        self._refuse_second_claim(config, output, 'claim-resume-rollback-runs')
        resumed = self.push(config, output, name='claim-resume-rollback-runs', resume=True)
        self.assertEqual(resumed['exit'], 3, resumed['stdout'] + resumed['stderr'])
        run_path = self.run_path(output)
        inventory = self.invoke('rollback-inventory', run_path, '--format', 'json')
        statuses = {item['key']: item['status'] for item in inventory['payload']['retained_claims']}
        self.assertEqual(inventory['exit'], 0, inventory['stdout'] + inventory['stderr'])
        self.assertEqual(statuses['claims/_ids/alpha.json'], 'matched')
        self.assertEqual(inventory['payload']['claim_counts']['conflict'], 1)
        self._assert_claim_only_rollback(config, run_path)

    def test_online_dry_run_checks_claims_with_and_without_generated_files(self):
        for generated in (False, True):
            with self.subTest(generated_dir=generated):
                config = self.package(f'online-dry-{generated}', batch_id='new-batch')
                output = (
                    self.generate(config, f'online-dry-{generated}-generated')
                    if generated
                    else None
                )
                self.seed_claim('parent', claim_body('parent', 'parent', 'object', 'old-batch'))

                result = self.push(
                    config,
                    output,
                    name=f'online-dry-{generated}-runs',
                    dry_run=True,
                )

                self.assertEqual(result['exit'], 3, result['stdout'] + result['stderr'])
                self.assertEqual(self.store.put_calls, [])
                self.store._path(TEST_PRIVATE_BUCKET, self.claim_key('parent')).unlink()

    def test_offline_dry_run_reports_claim_check_not_run_without_a_client(self):
        config = self.package('offline-dry-run')
        no_client = Mock(side_effect=AssertionError('offline dry run created a client'))

        result = self.push(config, dry_run=True, offline=True, factory=no_client)

        self.assertEqual(result['exit'], 0, result['stdout'] + result['stderr'])
        payload = result['payload']
        self.assertIn('claim_check', payload['preflight'])
        self.assertEqual(payload['preflight']['claim_check']['status'], 'not_run')
        no_client.assert_not_called()

    def test_claim_read_and_write_failures_exit_one_and_hold_the_batch(self):
        for label, wrapper in (
            ('read', ClaimReadDenied),
            ('write', ClaimWriteDenied),
        ):
            with self.subTest(operation=label):
                prefix = f'claim-storage-{label}'
                config = self.package(f'claim-storage-{label}', prefix=prefix)
                output = self.generate(config, f'claim-storage-{label}-generated')

                def factory(*args, wrapper=wrapper, prefix=prefix, **kwargs):
                    return wrapper(self.store, prefix)

                result = self.push(
                    config,
                    output,
                    name=f'claim-storage-{label}-runs',
                    factory=factory,
                )
                self.assertEqual(result['exit'], 1, result['stdout'] + result['stderr'])
                self.assertEqual(inspect_run(self.run_path(output))['publication']['state'], 'held')
                self.assertEqual(self.content_puts(), [])
                self.assertEqual(self.store.put_calls, [])
                self.store.put_calls.clear()

    def test_claims_use_private_bucket_for_separate_and_shared_bucket_layouts(self):
        for shared in (False, True):
            with self.subTest(shared_buckets=shared):
                public = TEST_PRIVATE_BUCKET if shared else TEST_PUBLIC_BUCKET
                config = self.package(
                    f'bucket-layout-{shared}',
                    prefix=f'bucket-layout-{shared}',
                    private_bucket=TEST_PRIVATE_BUCKET,
                    public_bucket=public,
                )
                output = self.generate(config, f'bucket-layout-{shared}-generated')

                result = self.push(config, output, name=f'bucket-layout-{shared}-runs')

                self.assertEqual(result['exit'], 0, result['stdout'] + result['stderr'])
                key = self.claim_key('parent', prefix=f'bucket-layout-{shared}')
                self.assertTrue(self.store._path(TEST_PRIVATE_BUCKET, key).is_file())
                if not shared:
                    self.assertFalse(self.store._path(TEST_PUBLIC_BUCKET, key).exists())
                claim_puts = [
                    bucket for bucket, object_key in self.store.put_calls if object_key == key
                ]
                self.assertEqual(claim_puts, [TEST_PRIVATE_BUCKET])
                self.store.put_calls.clear()
