"""Orchestrate scenario execution and the LocalStack rehearsal lifecycle."""

import json
import hashlib
import os
from pathlib import Path
import platform
import re
import shutil
import sys
import time
from types import SimpleNamespace
from uuid import uuid4

from rehearsal_evidence import EvidenceWriter
from rehearsal_harness import (
    SCENARIOS_BY_NUMBER,
    FaultClient,
    invoke_cli,
    start_worker,
)
from rehearsal_scenarios import SCENARIO_RUNNERS, ScenarioMismatch
from rehearsal_storage import (
    ENDPOINT,
    SENTINEL_BODIES,
    checksum_sentinels,
    cleanup_prefix,
    copy_sample,
    ensure_bucket,
    hardlink_tree,
    make_config,
    put_bytes,
    storage_listing,
)
from workbench_lite.s3_client import create_s3_client


def cli_factory(mode=None):
    def factory(endpoint_url=None, region=None, profile=None, **kwargs):
        if profile:
            raise RuntimeError('Rehearsal refuses named AWS profiles.')
        closed_loopback = bool(
            endpoint_url and re.fullmatch(r'http://127\.0\.0\.1:\d+', endpoint_url)
        )
        if endpoint_url != ENDPOINT and not closed_loopback:
            raise RuntimeError(f'Rehearsal refused unexpected endpoint: {endpoint_url!r}')
        adapter = create_s3_client(endpoint_url, region or 'us-east-1', None, **kwargs)
        return FaultClient(adapter, mode)

    return factory


class ScenarioRunner:
    def __init__(
        self,
        args,
        artifact_root,
        samples,
        original_samples,
        storage,
        base_prefix,
        sentinels,
        evidence,
    ):
        self.args = args
        self.artifact_root = artifact_root
        self.samples = samples
        self.original_samples = original_samples
        self.storage = storage
        self.base_prefix = base_prefix
        self.sentinels = sentinels
        self.evidence = evidence
        self.lock_root = artifact_root / 'locks'
        self.lock_root.mkdir(mode=0o700, exist_ok=True)
        self.stop = False
        self.timing = None

    @property
    def results(self):
        return self.evidence.results

    @property
    def findings(self):
        return self.evidence.findings

    def scenario_prefix(self, alias, number):
        return f'{self.base_prefix}/{alias}/case-{number:02d}'

    def command(self, directory, display, argv, *, mode=None, extra_patches=()):
        result = invoke_cli(argv, cli_factory(mode), self.lock_root, extra_patches=extra_patches)
        self.evidence.record_command(directory, display, argv, result)
        return result

    def require_exit(self, result, expected, step):
        if result['exit'] == expected:
            return
        raise ScenarioMismatch(
            f'{step}: expected exit {expected}, observed {result["exit"]}; '
            f'stdout={result["stdout"][-1200:]!r}; '
            f'stderr={result["stderr"][-1200:]!r}'
        )

    def generate(self, directory, config, output, run_name='runs'):
        argv = [
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
            directory / run_name,
        ]
        result = self.command(
            directory,
            'generate --config <scenario-config> --manifests --thumbnails',
            argv,
        )
        self.require_exit(result, 0, 'generate')
        run_path = json.loads((output / '.workbench-run.json').read_text())['run_path']
        return Path(run_path)

    def push(
        self,
        directory,
        config,
        output,
        *,
        resume=False,
        mode=None,
        run_name='push-runs',
        endpoint=ENDPOINT,
        extra_patches=(),
    ):
        argv = [
            'push',
            '--config',
            config,
            '--execute',
            '--generated-dir',
            output,
            '--endpoint-url',
            endpoint,
            '--region',
            'us-east-1',
            '--format',
            'json',
            '--run-dir',
            directory / run_name,
        ]
        if resume:
            argv.append('--resume')
        label = 'push --execute' + (' --resume' if resume else '')
        return self.command(directory, label, argv, mode=mode, extra_patches=extra_patches)

    def run_all(self):
        for alias in ('single', 'triple'):
            for number in range(1, 15):
                self.run_case(alias, number)
                if self.stop:
                    return
        for number in (1, 8):
            self.run_case('large', number)
            if self.stop:
                return

    def run_case(self, alias, number):
        directory, prefix, config = self._prepare_case(alias, number)
        before = self._storage_snapshot(prefix)
        started = time.monotonic()
        observed, error = self._execute_case(alias, number, directory, config, prefix)
        record = self._case_record(
            alias,
            number,
            directory,
            prefix,
            before,
            observed,
            error,
            time.monotonic() - started,
        )
        self.evidence.record_scenario(directory, record)

    def _prepare_case(self, alias, number):
        directory = self.artifact_root / 'scenarios' / alias / f'case-{number:02d}'
        directory.mkdir(parents=True, exist_ok=False)
        prefix = self.scenario_prefix(alias, number)
        config = make_config(
            self.samples[alias],
            directory,
            self.args.private_bucket,
            self.args.public_bucket,
            prefix,
        )
        return directory, prefix, config

    def _storage_snapshot(self, prefix):
        return {
            bucket: storage_listing(self.storage, bucket, prefix)
            for bucket in (self.args.private_bucket, self.args.public_bucket)
        }

    def _execute_case(self, alias, number, directory, config, prefix):
        try:
            observed = SCENARIO_RUNNERS[number](
                self, alias, directory, self.samples[alias], config, prefix
            )
            return observed, None
        except Exception as exc:
            error = f'{type(exc).__name__}: {exc}'
            self.evidence.add_finding(
                {
                    'sample_alias': alias,
                    'scenario': number,
                    'expected': self._expected(number),
                    'reproduction': {
                        'command_sequence': [
                            item['command'] for item in self.evidence.commands[-8:]
                        ],
                        'observed_error': error,
                    },
                }
            )
            return {'steps': {}, 'recovery': 'scenario did not complete'}, error

    def _case_record(self, alias, number, directory, prefix, before, observed, error, elapsed):
        after = self._storage_snapshot(prefix)
        sentinel_ok, sentinel_state = checksum_sentinels(self.storage, self.sentinels)
        if not sentinel_ok:
            self.evidence.add_finding(
                {
                    'sample_alias': alias,
                    'scenario': number,
                    'expected': 'all sentinel checksums unchanged',
                    'observed': sentinel_state,
                    'reproduction': {
                        'commands': [item['command'] for item in self.evidence.commands[-8:]],
                    },
                }
            )
            self.stop = True
        return {
            'order': len(self.results) + 1,
            'seed': self.base_prefix.removeprefix('rehearsal-'),
            'sample_alias': alias,
            'scenario': number,
            'scenario_name': self._scenario(number)['name'],
            'injection': self._scenario(number)['injection'],
            'expected': self._expected(number),
            'observed': observed,
            'storage_before': before,
            'storage_after': after,
            'sentinels_unchanged': sentinel_ok,
            'sentinels': sentinel_state,
            'journal_runs': self._event_records(directory),
            'elapsed_seconds': round(elapsed, 3),
            'finding': error,
        }

    @staticmethod
    def _scenario(number):
        return SCENARIOS_BY_NUMBER[number]

    def _expected(self, number):
        return self._scenario(number)['expected']

    @staticmethod
    def _event_records(directory):
        from rehearsal_evidence import event_records

        return event_records(directory)

    def worker(self, argv, *, fault, ready, stop_signal):
        process, _ = start_worker(
            argv,
            storage_root=self.artifact_root / 'worker-storage',
            lock_root=self.lock_root,
            backend='localstack',
            fault=fault,
            ready_file=ready,
            hold_barrier=True,
        )
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline and process.poll() is None and not ready.exists():
            time.sleep(0.02)
        if not ready.exists():
            process.kill()
            stdout, stderr = process.communicate(timeout=30)
            raise ScenarioMismatch(f'fault worker missed barrier: {stdout}\n{stderr}')
        process.send_signal(stop_signal)
        stdout, stderr = process.communicate(timeout=60)
        return SimpleNamespace(
            returncode=process.returncode, stdout=stdout, stderr=stderr, pid=process.pid
        )

    def run_timing(self, directory=None):
        directory = directory or self.artifact_root / 'timing'
        directory.mkdir(exist_ok=True)
        return _measure_large_sample(self, directory)


def _measure_large_sample(runner, directory):
    source = copy_sample(runner.original_samples['large'], directory / 'sample-copy')
    config = make_config(
        source,
        directory,
        runner.args.private_bucket,
        runner.args.public_bucket,
        f'{runner.base_prefix}/timing',
    )
    check_seconds = _time_check(runner, directory, config)
    dry_run_seconds = _time_offline_dry_run(runner, directory, config)
    return {
        'check_seconds': round(check_seconds, 3),
        'dry_run_seconds': round(dry_run_seconds, 3),
        'baseline_check_seconds': runner.args.baseline_check_seconds,
        'baseline_dry_run_seconds': runner.args.baseline_dry_run_seconds,
        'machine': platform.platform(),
        'python': sys.version.split()[0],
    }


def _time_check(runner, directory, config):
    started = time.perf_counter()
    result = runner.command(
        directory,
        'check --format json (large sample)',
        [
            'check',
            '--config',
            config,
            '--format',
            'json',
            '--run-dir',
            directory / 'check-runs',
        ],
    )
    elapsed = time.perf_counter() - started
    runner.require_exit(result, 0, 'timing_check')
    return elapsed


def _time_offline_dry_run(runner, directory, config):
    generated = directory / 'timing-generated'
    runner.generate(directory, config, generated, 'timing-generation-runs')
    dry_copy = directory / 'generated-copy'
    hardlink_tree(generated, dry_copy)
    (dry_copy / '.workbench-run.json').unlink(missing_ok=True)
    started = time.perf_counter()
    result = runner.command(
        directory,
        'push --dry-run --offline (large sample)',
        [
            'push',
            '--config',
            config,
            '--dry-run',
            '--offline',
            '--generated-dir',
            dry_copy,
            '--format',
            'json',
            '--run-dir',
            directory / 'dry-runs',
        ],
    )
    elapsed = time.perf_counter() - started
    runner.require_exit(result, 0, 'timing_push_dry_run')
    return elapsed


class RehearsalController:
    def __init__(self, args):
        self.args = args
        self.artifact_root = args.artifact_root.expanduser().resolve()
        repository_root = Path(__file__).resolve().parents[1]
        self.evidence = EvidenceWriter(repository_root)
        self.prefix = f'rehearsal-{uuid4().hex}'
        self.storage = None
        self.runner = None
        self.samples = dict(args.sample)
        self.original_samples = dict(self.samples)
        self.copied = {}
        self.cleanup = {'complete': False, 'before': {}, 'after': {}}
        self.error = None
        self.started = time.perf_counter()
        self.free_bytes_before = 0

    def run(self):
        self.artifact_root.mkdir(parents=True, exist_ok=True)
        self._prepare_existing_root()
        self.free_bytes_before = shutil.disk_usage(self.artifact_root).free
        disabled_aws_environment()
        try:
            self._prepare_run()
            self._run_scenarios()
        except Exception as exc:
            self._record_error(exc)
        finally:
            self._finalize()
        self._print_result()
        return int(bool(self.error or self.evidence.findings or not self.cleanup.get('complete')))

    def _prepare_existing_root(self):
        entries = list(self.artifact_root.iterdir())
        history = [
            item for item in entries if item.is_dir() and item.name.startswith('startup-check-')
        ]
        active = [item for item in entries if item not in history]
        if not active:
            return
        allowed = {item.name for item in active} == {
            'summary.md',
            'cleanup.json',
            'locks',
        }
        locks = self.artifact_root / 'locks'
        empty_locks = not locks.exists() or not any(locks.iterdir())
        if not allowed or not empty_locks:
            raise ValueError('artifact root already contains rehearsal data')
        archive = self.artifact_root / f'startup-check-{len(history) + 1}'
        archive.mkdir()
        for item in active:
            shutil.move(str(item), archive / item.name)

    def _prepare_run(self):
        self.storage = create_s3_client(self.args.endpoint, 'us-east-1', None)
        raw = self.storage.client
        self._ensure_buckets(raw)
        sentinels = self._create_sentinels(raw)
        self._copy_samples()
        self.runner = ScenarioRunner(
            self.args,
            self.artifact_root,
            self.samples,
            self.original_samples,
            self.storage,
            self.prefix,
            sentinels,
            self.evidence,
        )

    def _ensure_buckets(self, client):
        for bucket in (self.args.private_bucket, self.args.public_bucket):
            ensure_bucket(client, bucket)

    def _create_sentinels(self, client):
        sentinels = []
        pairs = (
            (self.args.private_bucket, 'preexisting.txt'),
            (self.args.public_bucket, 'unrelated.txt'),
        )
        for bucket, filename in pairs:
            body = SENTINEL_BODIES[filename]
            key = f'{self.prefix}/sentinels/{filename}'
            put_bytes(client, bucket, key, body)
            checksum = hashlib.sha256(body).hexdigest()
            sentinels.append((bucket, key, checksum))
        return sentinels

    def _copy_samples(self):
        for alias, original in self.original_samples.items():
            destination = self.artifact_root / 'sample-copies' / alias
            self.samples[alias] = copy_sample(original, destination)
            self.copied[alias] = sum(
                path.stat().st_size for path in destination.rglob('*') if path.is_file()
            )

    def _run_scenarios(self):
        self.runner.run_all()
        if not self.runner.stop:
            self.runner.timing = self.runner.run_timing()

    def _record_error(self, exc):
        self.error = f'{type(exc).__name__}: {exc}'
        self.evidence.add_finding(
            {
                'sample_alias': 'rehearsal',
                'scenario': None,
                'reproduction': {'observed_error': self.error},
            }
        )

    def _finalize(self):
        self._cleanup_storage()
        cleanup_path = self.artifact_root / 'cleanup.json'
        cleanup_path.write_text(json.dumps(self.cleanup, indent=2), encoding='utf-8')
        resources = self._resource_report()
        self.evidence.write_summary(
            self.artifact_root / 'summary.md',
            self.args,
            self.cleanup,
            self.prefix,
            self.copied,
            self.error,
            resources,
        )

    def _cleanup_storage(self):
        if self.storage is None:
            return
        try:
            self.cleanup = cleanup_prefix(
                self.storage,
                (self.args.private_bucket, self.args.public_bucket),
                self.prefix,
            )
        except Exception as exc:
            self.cleanup = {
                'complete': False,
                'cleanup_error': f'{type(exc).__name__}: {exc}',
            }

    def _resource_report(self):
        files = (item for item in self.artifact_root.rglob('*') if item.is_file())
        return {
            'elapsed_seconds': time.perf_counter() - self.started,
            'copied_bytes': sum(self.copied.values()),
            'artifact_bytes': sum(item.stat().st_size for item in files),
            'free_bytes_before': self.free_bytes_before,
            'free_bytes_after': shutil.disk_usage(self.artifact_root).free,
            'timing': self.runner.timing if self.runner else None,
        }

    def _print_result(self):
        results = self.runner.results if self.runner else []
        print(f'Artifacts: {self.artifact_root}')
        print(f'Scenarios recorded: {len(results)}; findings: {len(self.evidence.findings)}')
        print(f'Cleanup complete: {self.cleanup.get("complete")}')
        print(f'Summary: {self.artifact_root / "summary.md"}')


def disabled_aws_environment():
    for name in list(os.environ):
        if name.startswith('AWS_'):
            os.environ.pop(name, None)
    os.environ.update(
        {
            'AWS_ACCESS_KEY_ID': 'test',
            'AWS_SECRET_ACCESS_KEY': 'test',
            'AWS_DEFAULT_REGION': 'us-east-1',
            'AWS_EC2_METADATA_DISABLED': 'true',
            'AWS_SHARED_CREDENTIALS_FILE': '/dev/null',
            'AWS_CONFIG_FILE': '/dev/null',
        }
    )
