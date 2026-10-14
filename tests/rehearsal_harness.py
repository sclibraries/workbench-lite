"""Scenario contract and test-only helpers for the command-line rehearsal."""

import contextlib
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
from types import SimpleNamespace
from unittest.mock import patch

from PIL import Image

from workbench_lite import cli
from workbench_lite.locks import WriterLocks
from workbench_lite.s3_client import ObjectIdentity, VerificationResult

# Expectations are transcribed from the public command, publication-hold and
# rollback-inventory contracts before any rehearsal execution.
SCENARIO_EXPECTATIONS = [
    {
        'number': 1,
        'name': 'clean generate, execute, and inventory',
        'contracts': (
            'docs/cli-outcomes.md#exit-statuses',
            'docs/publication-holds.md#publication-boundary',
            'docs/rollback-inventory.md',
        ),
        'expected': {
            'steps': {'generate': 0, 'push_execute': 0, 'rollback_inventory': 0},
            'run_state': 'completed',
            'publication': 'ready_for_review',
            'journal': (
                'Every planned key has a destination check, durable upload intent '
                'and result, and matching verification; the final push event '
                'contains a complete-batch receipt.'
            ),
            'storage': (
                'All planned objects exist with their planned full-object SHA-256; '
                'sentinels are unchanged.'
            ),
            'recovery': (
                'Not required; the rollback inventory is reviewable and '
                'deletion_authorized is false.'
            ),
        },
    },
    {
        'number': 2,
        'name': 'missing source and corrupt TIFF',
        'contracts': (
            'docs/cli-outcomes.md#exit-statuses',
            'docs/publication-holds.md#stage-and-error-matrix',
        ),
        'expected': {
            'steps': {'check_missing_source': 3, 'generate_corrupt_tiff': 3},
            'run_state': 'failed',
            'publication': 'held',
            'journal': (
                'Validation records the missing or unreadable source; there are no '
                'push intents or storage writes.'
            ),
            'storage': 'Unchanged from before the scenario; sentinels are unchanged.',
            'recovery': 'Restore a valid source file and start a new validated run.',
        },
    },
    {
        'number': 3,
        'name': 'denied storage access',
        'contracts': (
            'docs/cli-outcomes.md#exit-statuses',
            'docs/publication-holds.md#stage-and-error-matrix',
        ),
        'expected': {
            'steps': {'push_execute': 1},
            'run_state': 'failed',
            'publication': 'held',
            'journal': ('The denied remote access check is recorded; no push intent is recorded.'),
            'storage': 'No planned object is written; sentinels are unchanged.',
            'recovery': 'Resolve access and use a new execute run.',
        },
    },
    {
        'number': 4,
        'name': 'unavailable endpoint',
        'contracts': (
            'docs/cli-outcomes.md#exit-statuses',
            'docs/publication-holds.md#stage-and-error-matrix',
        ),
        'expected': {
            'steps': {'push_execute': 1},
            'run_state': 'failed',
            'publication': 'held',
            'journal': (
                'The failed destination readiness check is recorded; no push intent is recorded.'
            ),
            'storage': ('No planned object is written; LocalStack sentinels are unchanged.'),
            'recovery': 'Restore the endpoint and use a new execute run.',
        },
    },
    {
        'number': 5,
        'name': 'journal fsync reports no space',
        'contracts': (
            'docs/cli-outcomes.md#evidence-files',
            'docs/publication-holds.md#stage-and-error-matrix',
        ),
        'expected': {
            'steps': {'push_execute': 1},
            'run_state': 'failed',
            'publication': 'held',
            'journal': (
                'The ENOSPC persistence failure stops the command before the '
                'affected remote write; incomplete evidence cannot certify '
                'publication.'
            ),
            'storage': ('No object for the failed write is created; sentinels are unchanged.'),
            'recovery': (
                'Retain the run evidence, inspect its valid journal prefix, and do '
                'not treat it as publishable.'
            ),
        },
    },
    {
        'number': 6,
        'name': 'graceful SIGINT and SIGTERM mid-batch',
        'contracts': (
            'docs/cli-outcomes.md#exit-statuses',
            'docs/publication-holds.md#stage-and-error-matrix',
        ),
        'expected': {
            'steps': {'SIGINT': 130, 'SIGTERM': 143},
            'run_state': 'interrupted',
            'publication': 'held',
            'journal': (
                'The active operation is retained as unknown if it lacks a result; '
                'prior confirmed writes remain recorded, and no publication '
                'receipt is written.'
            ),
            'storage': (
                'Prior confirmed writes may remain; no preexisting or unrelated '
                'object changes; sentinels are unchanged.'
            ),
            'recovery': ('Reconcile unknown outcomes before any resume or rollback decision.'),
        },
    },
    {
        'number': 7,
        'name': 'abrupt termination, reconcile, and resume',
        'contracts': (
            'docs/cli-outcomes.md#exit-statuses',
            'docs/cli-outcomes.md#run-locations-and-workflow',
            'docs/rollback-inventory.md',
        ),
        'expected': {
            'steps': {'push_sigkill': 'signal', 'reconcile': 0, 'push_resume': 0},
            'run_state': 'completed after resume',
            'publication': 'ready_for_review after resume',
            'journal': (
                'SIGKILL leaves an incomplete push and unknown intent; '
                'reconciliation records confirmed, absent, or conflict from HEAD; '
                'resume records verified prior writes and a final receipt.'
            ),
            'storage': (
                'A write made before the lost acknowledgment is not duplicated; '
                'sentinels are unchanged.'
            ),
            'recovery': (
                'Reconcile first, then resume the same run only when reconciliation is clear.'
            ),
        },
    },
    {
        'number': 8,
        'name': 'unchanged rerun and additive package version',
        'contracts': (
            'docs/publication-holds.md#publication-boundary',
            'docs/rollback-inventory.md',
        ),
        'expected': {
            'steps': {
                'initial_execute': 0,
                'unchanged_execute': 0,
                'additive_execute': 1,
            },
            'run_state': 'completed for unchanged rerun; failed for additive rerun',
            'publication': (
                'ready_for_review for unchanged rerun; held for additive rerun '
                'with a changed existing manifest'
            ),
            'journal': (
                'Unchanged content is verified and skipped; only the new run audit '
                'is uploaded. The additive version records the changed manifest as '
                'held and new keys with normal upload verification.'
            ),
            'storage': (
                'Existing keys remain byte-identical; only the additive version '
                'new keys and its audit are created; sentinels are unchanged.'
            ),
            'recovery': (
                'The unchanged run is eligible for review; the additive run '
                'remains held for manifest review and is not a publication receipt.'
            ),
        },
    },
    {
        'number': 9,
        'name': 'conflicting existing content',
        'contracts': (
            'docs/publication-holds.md#publication-boundary',
            'docs/publication-holds.md#stage-and-error-matrix',
        ),
        'expected': {
            'steps': {'push_execute': 1},
            'run_state': 'failed',
            'publication': 'held',
            'journal': (
                'The planned content key is a destination conflict; no push intent '
                'follows the readiness failure.'
            ),
            'storage': (
                'The conflicting bytes remain; no other planned key is written; '
                'sentinels are unchanged.'
            ),
            'recovery': ('Resolve the collision outside this run and start a new execute run.'),
        },
    },
    {
        'number': 10,
        'name': 'two processes contend for one destination',
        'contracts': (
            'docs/publication-holds.md#publication-boundary',
            'docs/preflight-and-locks.md',
        ),
        'expected': {
            'steps': {'lock_winner': 0, 'lock_contender': 1},
            'run_state': 'winner completed; contender failed',
            'publication': 'winner ready_for_review; contender held',
            'journal': (
                'The winning run records normal writes and receipt; the refused '
                'run records its lock refusal and no push intent.'
            ),
            'storage': ('Exactly one run creates destination objects; sentinels are unchanged.'),
            'recovery': (
                'No recovery is needed for the winner; retry the refused run only '
                'after the destination lock is released and if its run state '
                'permits.'
            ),
        },
    },
    {
        'number': 11,
        'name': 'lost upload acknowledgment',
        'contracts': (
            'docs/publication-holds.md#publication-boundary',
            'docs/cli-outcomes.md#upload-and-generation-outcomes',
        ),
        'expected': {
            'steps': {'push_execute': 0},
            'run_state': 'completed',
            'publication': 'ready_for_review',
            'journal': (
                'The failed acknowledgment is followed by HEAD-confirmed checksum '
                'and size evidence with reconciled_written; the key has one PUT '
                'attempt.'
            ),
            'storage': 'The write is present exactly once; sentinels are unchanged.',
            'recovery': (
                'Automatic per-attempt reconciliation confirms the write; no '
                'separate reconcile command is required.'
            ),
        },
    },
    {
        'number': 12,
        'name': 'retry exhaustion and verified resume',
        'contracts': (
            'docs/cli-outcomes.md#upload-and-generation-outcomes',
            'docs/publication-holds.md#publication-boundary',
        ),
        'expected': {
            'steps': {'initial_execute': 1, 'push_resume': 0},
            'run_state': 'failed initially; completed after resume',
            'publication': 'held initially; ready_for_review after resume',
            'journal': (
                'Three transient attempts end retries_exhausted with confirmed '
                'absence; resumed attempts continue journal numbering and finish '
                'with verification and receipt.'
            ),
            'storage': (
                'The failed first invocation leaves the key absent; resume creates '
                'it once; sentinels are unchanged.'
            ),
            'recovery': (
                'Resume the same run; the invocation receives fresh bounded retry limits.'
            ),
        },
    },
    {
        'number': 13,
        'name': 'rollback preview, execution, repeat, interruption, and resume',
        'contracts': (
            'docs/rollback-inventory.md',
            'docs/publication-holds.md#after-a-failed-execute',
        ),
        'expected': {
            'steps': {
                'preview': 0,
                'execute': 0,
                'repeat': 0,
                'interrupted_execute': 'signal',
                'resume': 0,
            },
            'run_state': 'rollback operation completes after resume',
            'publication': 'source run remains held after rollback starts',
            'journal': (
                'Preview records would_delete without deletes; execute records '
                'deleted; repeat records already_deleted; interruption retains '
                'pending delete evidence; resume resolves it and completes.'
            ),
            'storage': (
                'Only matching confirmed new keys are removed; all keys are absent '
                'after resumed rollback; sentinels are unchanged.'
            ),
            'recovery': (
                'Rerun rollback against the same source run; its rollback hold remains permanent.'
            ),
        },
    },
    {
        'number': 14,
        'name': 'failed rollback holds publication',
        'contracts': (
            'docs/rollback-inventory.md',
            'docs/publication-holds.md#after-a-failed-execute',
        ),
        'expected': {
            'steps': {'rollback_execute': 1},
            'run_state': 'rollback failed',
            'publication': 'held permanently after rollback starts',
            'journal': (
                'The modified object is recorded as a checksum conflict; rollback '
                'does not delete it.'
            ),
            'storage': (
                'The modified object and unrelated sentinels remain; no mismatched '
                'object is deleted.'
            ),
            'recovery': 'The source run remains held; republishing requires a new run.',
        },
    },
]


SCENARIOS_BY_NUMBER = {scenario['number']: scenario for scenario in SCENARIO_EXPECTATIONS}

SCENARIO_INJECTIONS = {
    1: 'none; execute against the unique rehearsal prefix',
    2: ('unlink one copied source image; unlink and replace one copied TIFF with invalid bytes'),
    3: 'S3-shaped wrapper returns AccessDenied with HTTP 403 at HeadBucket',
    4: 'client targets a closed loopback TCP port',
    5: 'RunJournal fsync raises OSError(ENOSPC) on the first push intent',
    6: ('subprocess pauses before upload two; parent sends SIGINT and SIGTERM in separate runs'),
    7: ('subprocess writes object one then pauses before acknowledgment; parent sends SIGKILL'),
    8: 'execute initial version, unchanged version, then append one synthetic page',
    9: 'pre-place different bytes at a planned master key',
    10: ('winner pauses after destination lock acquisition; contender starts before release'),
    11: 'wrapper writes object then raises a connection error before acknowledgment',
    12: ('wrapper returns HTTP 503 for three upload attempts; resume uses a fresh client'),
    13: (
        'preview, execute, repeat; second rollback deletes one key then '
        'pauses before acknowledgment'
    ),
    14: 'replace a confirmed object with different bytes before rollback execute',
}

for _scenario in SCENARIO_EXPECTATIONS:
    _scenario['injection'] = SCENARIO_INJECTIONS[_scenario['number']]


TEST_ENDPOINT = 'http://synthetic.invalid'
TEST_PRIVATE_BUCKET = 'wbl-local-test-sources'
TEST_PUBLIC_BUCKET = 'wbl-local-test-derivatives'


def stream_identity(stream, chunk_size=1024 * 1024):
    """Return a readable stream's SHA-256 digest and byte count."""
    digest = hashlib.sha256()
    size = 0
    for chunk in iter(lambda: stream.read(chunk_size), b''):
        digest.update(chunk)
        size += len(chunk)
    return digest.hexdigest(), size


class FileS3Client:
    """Small checksum-aware S3 test double shared by signal subprocesses."""

    def __init__(self, root, endpoint=TEST_ENDPOINT):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.endpoint_url = endpoint
        self.region_name = 'us-east-1'
        self.meta = SimpleNamespace(endpoint_url=endpoint, region_name=self.region_name)
        self.client = SimpleNamespace(meta=self.meta)
        self.put_calls = []
        self.delete_calls = []

    def _path(self, bucket, key):
        if not bucket or not key or key.startswith('/') or '\\' in key:
            raise ValueError('Unsafe synthetic object identity.')
        parts = Path(key).parts
        if any(part in {'', '.', '..'} for part in parts):
            raise ValueError('Unsafe synthetic object identity.')
        return self.root / bucket / Path(*parts)

    def head_bucket(self, **kwargs):
        self.root.joinpath(kwargs['Bucket']).mkdir(parents=True, exist_ok=True)
        return {}

    def list_objects_v2(self, **kwargs):
        bucket_root = self.root / kwargs['Bucket']
        prefix = kwargs.get('Prefix', '')
        count = (
            sum(
                1
                for path in bucket_root.rglob('*')
                if path.is_file() and path.relative_to(bucket_root).as_posix().startswith(prefix)
            )
            if bucket_root.exists()
            else 0
        )
        return {'KeyCount': min(count, kwargs.get('MaxKeys', count))}

    def object_exists(self, bucket, key):
        return self._path(bucket, key).is_file()

    def inspect_object(self, bucket, key):
        path = self._path(bucket, key)
        if not path.is_file():
            return None
        checksum, size = _file_identity(path)
        return ObjectIdentity(checksum, size, 'FULL_OBJECT', f'"{checksum[:32]}"')

    def verify_file(self, bucket, key, checksum, size_bytes):
        identity = self.inspect_object(bucket, key)
        if identity is None:
            return VerificationResult(False, None, None, 'missing')
        return VerificationResult(
            identity.checksum == checksum and identity.size_bytes == size_bytes,
            identity.checksum,
            identity.size_bytes,
            's3-sha256',
        )

    def upload_file(self, source_path, bucket, key, checksum):
        destination = self._path(bucket, key)
        destination.parent.mkdir(parents=True, exist_ok=True)
        self.put_calls.append((bucket, key))
        try:
            with (
                Path(source_path).open('rb') as source,
                destination.open('xb') as target,
            ):
                shutil.copyfileobj(source, target, 1024 * 1024)
        except FileExistsError as exc:
            from botocore.exceptions import ClientError

            raise ClientError(
                {
                    'Error': {'Code': 'PreconditionFailed'},
                    'ResponseMetadata': {'HTTPStatusCode': 412},
                },
                'PutObject',
            ) from exc
        observed, _ = _file_identity(destination)
        if observed != checksum:
            destination.unlink(missing_ok=True)
            raise OSError('Synthetic upload checksum mismatch.')

    def delete_object(self, bucket, key, *, if_match):
        path = self._path(bucket, key)
        identity = self.inspect_object(bucket, key)
        self.delete_calls.append((bucket, key, if_match))
        if identity is None:
            return {}
        if identity.etag != if_match:
            from botocore.exceptions import ClientError

            raise ClientError(
                {
                    'Error': {'Code': 'PreconditionFailed'},
                    'ResponseMetadata': {'HTTPStatusCode': 412},
                },
                'DeleteObject',
            )
        path.unlink()
        return {}

    def seed(self, bucket, key, body):
        path = self._path(bucket, key)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)
        return _file_identity(path)[0]

    def listing(self, bucket, prefix=''):
        bucket_root = self.root / bucket
        result = {}
        if not bucket_root.exists():
            return result
        for path in sorted(item for item in bucket_root.rglob('*') if item.is_file()):
            key = path.relative_to(bucket_root).as_posix()
            if key.startswith(prefix):
                checksum, size = _file_identity(path)
                result[key] = {'sha256': checksum, 'size_bytes': size}
        return result


class FaultClient:
    """Inject one documented storage failure around an S3-shaped client."""

    def __init__(self, client, mode=None):
        self._wrapped = client
        self.client = getattr(client, 'client', client)
        self.mode = mode
        wrapped_meta = getattr(self.client, 'meta', None)
        self.endpoint_url = getattr(client, 'endpoint_url', None) or getattr(
            wrapped_meta, 'endpoint_url', TEST_ENDPOINT
        )
        self.region_name = getattr(client, 'region_name', None) or getattr(
            wrapped_meta, 'region_name', 'us-east-1'
        )
        self.meta = getattr(
            self.client,
            'meta',
            getattr(
                client,
                'meta',
                SimpleNamespace(endpoint_url=self.endpoint_url, region_name=self.region_name),
            ),
        )
        self.put_calls = getattr(client, 'put_calls', [])
        self.delete_calls = getattr(client, 'delete_calls', [])
        self.failures_remaining = 3 if mode == 'retry_three' else 0

    def __getattr__(self, name):
        return getattr(self._wrapped, name)

    def head_bucket(self, **kwargs):
        if self.mode == 'denied':
            from botocore.exceptions import ClientError

            raise ClientError(
                {
                    'Error': {'Code': 'AccessDenied'},
                    'ResponseMetadata': {'HTTPStatusCode': 403},
                },
                'HeadBucket',
            )
        if self.mode == 'unavailable':
            from botocore.exceptions import EndpointConnectionError

            raise EndpointConnectionError(endpoint_url='http://127.0.0.1:1')
        return self._wrapped.head_bucket(**kwargs)

    def upload_file(self, source_path, bucket, key, checksum):
        if self.mode == 'lost_ack':
            self._wrapped.upload_file(source_path, bucket, key, checksum)
            from botocore.exceptions import EndpointConnectionError

            raise EndpointConnectionError(endpoint_url='http://synthetic.invalid')
        if self.mode == 'retry_three' and self.failures_remaining:
            self.failures_remaining -= 1
            from botocore.exceptions import ClientError

            raise ClientError(
                {
                    'Error': {'Code': 'ServiceUnavailable'},
                    'ResponseMetadata': {'HTTPStatusCode': 503},
                },
                'PutObject',
            )
        return self._wrapped.upload_file(source_path, bucket, key, checksum)


def _file_identity(path):
    with Path(path).open('rb') as handle:
        return stream_identity(handle)


def make_synthetic_package(root, *, pages=2, batch_id='package', prefix='run'):
    """Create a package only from generated, synthetic content."""
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    lines = ['title,id,field_model,parent_id,field_weight,file']
    lines.append(f'Synthetic package,{batch_id},Paged Content,,,parent.pdf')
    for index in range(1, pages + 1):
        lines.append(
            f'Synthetic page {index},page-{index},Page,{batch_id},{index},page-{index}.tif'
        )
    (root / 'input.csv').write_text('\n'.join(lines) + '\n', encoding='utf-8')
    (root / 'parent.pdf').write_bytes(b'%PDF-1.4\nsynthetic\n%%EOF\n')
    for index in range(1, pages + 1):
        Image.new('RGB', (12, 10), (index * 30, 20, 100)).save(root / f'page-{index}.tif')
    config = root / 'package.yml'
    config.write_text(
        f'input_dir: {root}\ninput_csv: input.csv\n'
        f'batch_id: {batch_id}\ns3_prefix: {prefix}\n'
        f's3_private_bucket: {TEST_PRIVATE_BUCKET}\n'
        f's3_public_bucket: {TEST_PUBLIC_BUCKET}\n',
        encoding='utf-8',
    )
    return config


def invoke_cli(argv, client_factory, lock_root, *, extra_patches=()):
    """Invoke the production CLI in-process with an injected storage adapter."""
    stdout, stderr = io.StringIO(), io.StringIO()
    patches = [patch('workbench_lite.cli.create_s3_client', side_effect=client_factory)]
    patches.extend(extra_patches)
    with contextlib.ExitStack() as stack:
        for item in patches:
            stack.enter_context(item)
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            code = cli.main(
                [str(value) for value in argv],
                lock_factory=lambda: WriterLocks(lock_root),
            )
    try:
        payload = json.loads(stdout.getvalue())
    except (ValueError, TypeError):
        payload = None
    return {
        'exit': code,
        'payload': payload,
        'stdout': stdout.getvalue(),
        'stderr': stderr.getvalue(),
    }


def cli_client_factory(store, mode=None):
    return lambda *args, **kwargs: FaultClient(store, mode)


def spawn_worker(
    arguments,
    *,
    storage_root,
    lock_root,
    fault=None,
    ready_file=None,
    release_file=None,
    backend='file',
    timeout=20,
    kill_signal=None,
):
    """Run a CLI command in a child with the file-backed synthetic adapter."""
    process, _ = start_worker(
        arguments,
        storage_root=storage_root,
        lock_root=lock_root,
        fault=fault,
        ready_file=ready_file,
        release_file=release_file,
        backend=backend,
        hold_barrier=kill_signal is not None,
    )
    return _finish_worker(process, ready_file, timeout, kill_signal)


def _finish_worker(process, ready_file, timeout, kill_signal):
    if ready_file is not None:
        deadline = time.monotonic() + timeout
        while (
            time.monotonic() < deadline and process.poll() is None and not Path(ready_file).exists()
        ):
            time.sleep(0.02)
        if not Path(ready_file).exists():
            process.kill()
            stdout, stderr = process.communicate(timeout=timeout)
            raise RuntimeError(f'Worker did not reach its barrier: {stdout}\n{stderr}')
        if kill_signal is not None:
            process.send_signal(kill_signal)
    stdout, stderr = process.communicate(timeout=timeout)
    return SimpleNamespace(
        returncode=process.returncode, stdout=stdout, stderr=stderr, pid=process.pid
    )


def start_worker(
    arguments,
    *,
    storage_root,
    lock_root,
    fault=None,
    ready_file=None,
    release_file=None,
    backend='file',
    hold_barrier=False,
):
    """Start a test worker, leaving barriers under the caller's control."""
    root = Path(__file__).resolve().parents[1]
    tests = Path(__file__).resolve().parent
    env = _worker_environment(root, tests, storage_root, lock_root, fault, backend, hold_barrier)
    if ready_file is not None:
        env['REHEARSAL_READY_FILE'] = str(ready_file)
    if release_file is not None:
        env['REHEARSAL_RELEASE_FILE'] = str(release_file)
    process = _spawn_worker_process(arguments, root, tests, env)
    return process, env


def _worker_environment(root, tests, storage_root, lock_root, fault, backend, hold_barrier):
    env = {key: value for key, value in os.environ.items() if not key.startswith('AWS_')}
    env.update(
        {
            'PYTHONPATH': os.pathsep.join((str(root), str(tests))),
            'AWS_EC2_METADATA_DISABLED': 'true',
            'AWS_ACCESS_KEY_ID': 'test',
            'AWS_SECRET_ACCESS_KEY': 'test',
            'AWS_DEFAULT_REGION': 'us-east-1',
            'REHEARSAL_STORAGE_ROOT': str(storage_root),
            'REHEARSAL_LOCK_ROOT': str(lock_root),
            'REHEARSAL_FAULT': fault or '',
            'REHEARSAL_BACKEND': backend,
            'REHEARSAL_HOLD_BARRIER': 'true' if hold_barrier else 'false',
        }
    )
    return env


def _spawn_worker_process(arguments, root, tests, env):
    return subprocess.Popen(
        [sys.executable, str(tests / 'rehearsal_worker.py'), *map(str, arguments)],
        cwd=root,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def clear_dead_test_locks(lock_root, process_id):
    """Remove only test locks whose owner is the child just confirmed dead."""
    root = Path(lock_root)
    removed = []
    for lock in root.iterdir() if root.exists() else ():
        owner_path = lock / 'owner.json'
        try:
            owner = json.loads(owner_path.read_text(encoding='utf-8'))
        except (OSError, ValueError, TypeError):
            continue
        if owner.get('pid') != process_id:
            continue
        try:
            os.kill(process_id, 0)
        except ProcessLookupError:
            owner_path.unlink(missing_ok=True)
            try:
                lock.rmdir()
            except OSError:
                continue
            removed.append(lock.name)
        except PermissionError:
            continue
    return removed
