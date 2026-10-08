import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from workbench_lite.locks import WriterLocks, LockError


class LockTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.root.chmod(0o770)

    def test_competitor_rejected_and_owner_releases(self):
        with WriterLocks(self.root) as first:
            first.acquire('output:/same/path')
            with WriterLocks(self.root) as second:
                with self.assertRaises(LockError):
                    second.acquire('output:/same/path')
        with WriterLocks(self.root) as third:
            third.acquire('output:/same/path')
        self.assertEqual(list(self.root.iterdir()), [])

    def test_missing_world_writable_or_symlink_root_never_falls_back(self):
        for root in [self.root/'missing', self.root/'alias']:
            if root.name == 'alias':
                root.symlink_to(self.root, target_is_directory=True)
            with self.assertRaises(LockError):
                with WriterLocks(root) as locks:
                    locks.acquire('one')
        self.root.chmod(0o777)
        with self.assertRaises(LockError):
            with WriterLocks(self.root) as locks:
                locks.acquire('one')

    def test_stale_owner_is_not_stolen(self):
        locks = WriterLocks(self.root)
        locks.acquire('one')
        path = next(self.root.iterdir())
        owner = json.loads((path/'owner.json').read_text())
        owner['pid'] = 999999999
        (path/'owner.json').write_text(json.dumps(owner))
        with WriterLocks(self.root) as competing:
            with self.assertRaisesRegex(LockError, 'recovery'):
                competing.acquire('one')
        self.assertTrue(path.exists())

    def test_partial_acquisition_failure_releases_owned_locks_only(self):
        with WriterLocks(self.root) as first:
            first.acquire('taken')
            with self.assertRaises(LockError):
                with WriterLocks(self.root) as second:
                    second.acquire('free')
                    second.acquire('taken')
            self.assertEqual(len(list(self.root.iterdir())), 1)

    def test_cli_shared_output_races_are_excluded_before_journal_open(self):
        import contextlib
        import io
        import os
        import subprocess
        import time
        import shutil
        from cli_test_runner import main
        from test_outcomes import package
        from workbench_lite.runs import read_events
        config = package(self.root)
        output = self.root/'generated'
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(main(['generate', '--config', str(config), '--output-dir', str(output), '--manifests', '--thumbnails', '--run-dir', str(self.root/'runs')]), 0)
        reference = json.loads((output/'.workbench-run.json').read_text())
        script = """import os, time
from pathlib import Path
from cli_test_runner import cli, main
from test_outcomes import Storage
original = cli._open_run
def held(args):
    if os.environ['HOLD'] == 'yes':
        Path(os.environ['READY']).touch()
        deadline = time.monotonic() + 10
        while not Path(os.environ['RELEASE']).exists():
            if time.monotonic() > deadline: raise RuntimeError('test timeout')
            time.sleep(.02)
    return original(args)
cli._open_run = held
cli.create_s3_client = lambda *args, **kwargs: Storage()
cli.main = main
raise SystemExit(cli.entrypoint())
"""
        for second_mode, alias in [('--execute', False), ('--dry-run', False), ('--execute', True)]:
            with self.subTest(mode=second_mode, alias=alias):
                with contextlib.redirect_stdout(io.StringIO()):
                    self.assertEqual(main(['generate', '--config', str(config), '--output-dir', str(output), '--manifests', '--thumbnails', '--run-dir', str(self.root/'runs')]), 0)
                reference = json.loads((output/'.workbench-run.json').read_text())
                shared = self.root/'locks'
                shared.mkdir(exist_ok=True)
                shared.chmod(0o770)
                ready, release = self.root/'ready', self.root/'release'
                ready.unlink(missing_ok=True)
                release.unlink(missing_ok=True)
                cwd_a, cwd_b = self.root/'cwd-a', self.root/'cwd-b'
                cwd_a.mkdir(exist_ok=True)
                cwd_b.mkdir(exist_ok=True)
                other = output
                if alias:
                    other = self.root/'other-generated'
                    other.mkdir(exist_ok=True)
                    shutil.copyfile(output/'.workbench-run.json', other/'.workbench-run.json')
                env = {**os.environ, 'PYTHONPATH': str(Path(__file__).resolve().parents[1]) + os.pathsep + str(Path(__file__).parent),
                       'WBL_TEST_LOCK_DIR': str(shared), 'READY': str(ready), 'RELEASE': str(release), 'HOLD': 'yes'}
                base = [sys.executable, '-c', script, 'push', '--config', str(config)]
                first = subprocess.Popen([*base, '--execute', '--generated-dir', str(output), '--run-dir', 'logs-a'], cwd=cwd_a, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                try:
                    deadline = time.monotonic() + 10
                    while not ready.exists() and time.monotonic() < deadline and first.poll() is None:
                        time.sleep(.02)
                    self.assertTrue(ready.exists())
                    second = subprocess.run([*base, second_mode, '--generated-dir', str(other), '--run-dir', 'logs-b', '--endpoint-url', 'http://different-destination:4566'],
                                            cwd=cwd_b, env={**env, 'HOLD': 'no'}, capture_output=True, text=True, timeout=10)
                    self.assertEqual(second.returncode, 1, second.stdout + second.stderr)
                    self.assertIn('Writer lock is held', second.stdout)
                finally:
                    release.touch()
                    stdout, stderr = first.communicate(timeout=10)
                self.assertEqual(first.returncode, 0, stdout + stderr)
                events, error = read_events(reference['run_path'])
                self.assertIsNone(error)
                self.assertEqual(sum(e['kind'] == 'stage_started' and e['stage'] == 'push' for e in events), 1)
                self.assertEqual(list(shared.iterdir()), [])

    def test_production_entrypoint_ignores_test_lock_environment(self):
        import os
        import subprocess
        from test_outcomes import package
        config = package(self.root)
        script = """from pathlib import Path
from workbench_lite import cli
from workbench_lite.locks import WriterLocks, LockError
def root_check(self):
    assert self.root == Path('/var/lib/workbench-lite/locks')
    raise LockError('fixed root verified')
WriterLocks._validate_root = root_check
raise SystemExit(cli.entrypoint())
"""
        env = {**os.environ, 'PYTHONPATH': str(Path(__file__).resolve().parents[1]), 'WBL_TEST_LOCK_DIR': str(self.root/'redirect')}
        result = subprocess.run([sys.executable, '-c', script, 'generate', '--config', str(config)], env=env, capture_output=True, text=True)
        self.assertEqual(result.returncode, 1)
        self.assertIn('fixed root verified', result.stdout)
        self.assertFalse((self.root/'redirect').exists())

    def test_destination_exclusion_covers_prefix_overlap_and_endpoint_aliases(self):
        from dataclasses import replace
        from workbench_lite.locks import lock_destination
        from workbench_lite.check import run_check
        from test_outcomes import package
        plan = run_check(package(self.root)).upload_plan
        overlapping = [replace(entry, key='workbench-lite/' + entry.key) for entry in plan]
        with WriterLocks(self.root) as first:
            lock_destination(first, 'http://localhost:4566', plan)
            with WriterLocks(self.root) as second:
                with self.assertRaises(LockError):
                    lock_destination(second, 'http://127.0.0.1:4566', overlapping)
