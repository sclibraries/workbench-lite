"""Local manifest publication tests."""

import contextlib
import hashlib
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from cli_test_runner import main
from test_outcomes import Storage, package
from workbench_lite.cli import main as cli_main
from workbench_lite.locks import WriterLocks
from workbench_lite.publish import (
    PublishError,
    _copy_each,
    _manifest_source,
    _plan_destinations,
    _preflight,
)
from workbench_lite.runs import RunJournal, inspect_run


class PublishTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self._create_completed_run()

    def _create_completed_run(self):
        self.config = package(self.root)
        self.output = self.root / 'generated'
        self.publish_dir = self.root / 'public'
        self.publish_dir.mkdir()
        self._configure_run()
        self.storage = Storage()
        self.run = self._generate_and_push()
        self.assertEqual(inspect_run(self.run)['publication']['state'], 'ready_for_review')

    def _configure_run(self):
        with (self.root / 'input.csv').open('a', encoding='utf-8') as handle:
            handle.write('Parent Two,parent-two,Paged Content,,,parent-two.pdf\n')
        (self.root / 'parent-two.pdf').write_bytes(b'%PDF-1.4\n%%EOF\n')
        with self.config.open('a', encoding='utf-8') as handle:
            handle.write('manifest_key_prefix: workbench-lite/sample\n')
            handle.write(f'manifest_publish_dir: {self.publish_dir}\n')

    def _generate_and_push(self, output=None, run_root=None):
        output = output or self.output
        run_root = run_root or self.root / 'runs'
        common = ['--config', str(self.config), '--run-dir', str(run_root)]
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(
                main(
                    [
                        'generate',
                        *common,
                        '--output-dir',
                        str(output),
                        '--manifests',
                        '--thumbnails',
                    ]
                ),
                0,
            )
        run = Path(json.loads((output / '.workbench-run.json').read_text())['run_path'])
        with patch('workbench_lite.cli.create_s3_client', return_value=self.storage):
            with (
                contextlib.redirect_stdout(io.StringIO()),
                contextlib.redirect_stderr(io.StringIO()),
            ):
                self.assertEqual(
                    main(
                        [
                            'push',
                            *common,
                            '--execute',
                            '--generated-dir',
                            str(output),
                        ]
                    ),
                    0,
                )
        return run

    def publish(self, *options):
        return main(
            [
                'publish-manifests',
                str(self.run),
                '--config',
                str(self.config),
                '--generated-dir',
                str(self.output),
                *options,
            ]
        )

    def test_preview_then_execute_copies_receipted_manifests_and_preserves_push_journal(self):
        journal = (self.run / 'events.jsonl').read_bytes()
        log = self.run / 'publication.jsonl'
        self.assertFalse(log.exists())

        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(self.publish(), 0)
        self.assertFalse(log.exists())
        self.assertEqual(list(self.publish_dir.rglob('*.json')), [])

        other_cwd = self.root / 'other-working-directory'
        other_cwd.mkdir()
        old_umask = os.umask(0o077)
        try:
            with contextlib.chdir(other_cwd), contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(self.publish('--execute', '--confirm', self.run.name), 0)
        finally:
            os.umask(old_umask)
        manifest = self._assert_created_publication(journal, log)
        self._assert_unchanged_rerun(manifest, journal, log)

    def _assert_created_publication(self, journal, log):
        manifest = self.publish_dir / 'manifests' / 'parent.json'
        payload = json.loads(manifest.read_text(encoding='utf-8'))
        self.assertEqual(payload['@id'], 'https://manifests.example.org/manifests/parent.json')
        self.assertTrue((self.publish_dir / 'manifests' / 'parent-two.json').is_file())
        source = self.output / 'workbench-lite' / 'sample' / 'manifests' / 'parent.json'
        self.assertEqual(manifest.read_bytes(), source.read_bytes())
        self.assertEqual(manifest.stat().st_mode & 0o777, 0o644)
        self.assertEqual(manifest.parent.stat().st_mode & 0o777, 0o755)
        self.assertEqual((self.run / 'events.jsonl').read_bytes(), journal)
        events = [json.loads(line) for line in log.read_text(encoding='utf-8').splitlines()]
        self.assertEqual([event['kind'] for event in events], ['intent', 'result'] * 2)
        self.assertEqual({event['root'] for event in events}, {str(self.publish_dir.resolve())})
        results = [event for event in events if event['kind'] == 'result']
        self.assertEqual([event['status'] for event in results], ['created', 'created'])
        self.assertEqual(results[0]['path'], 'manifests/parent.json')
        return manifest

    def _assert_unchanged_rerun(self, manifest, journal, log):
        manifest.chmod(0o600)
        manifest.parent.chmod(0o700)
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(self.publish('--execute', '--confirm', self.run.name), 0)
        self.assertEqual(manifest.stat().st_mode & 0o777, 0o600)
        self.assertEqual(manifest.parent.stat().st_mode & 0o777, 0o700)
        self.assertEqual((self.run / 'events.jsonl').read_bytes(), journal)
        events = [json.loads(line) for line in log.read_text(encoding='utf-8').splitlines()]
        self.assertEqual(
            [event['status'] for event in events if event['kind'] == 'result'],
            ['created', 'created', 'unchanged', 'unchanged'],
        )

    def test_non_publish_commands_ignore_manifest_publish_dir(self):
        config = self.config.read_text(encoding='utf-8').replace(
            f'manifest_publish_dir: {self.publish_dir}',
            f'manifest_publish_dir: {self.root / "missing-public"}',
        )
        self.config.write_text(config, encoding='utf-8')
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            result = main(
                ['check', '--config', str(self.config), '--run-dir', str(self.root / 'check-runs')]
            )
        self.assertEqual(result, 0)

    def test_receipted_reused_manifest_fingerprint_can_be_published(self):
        output = self.root / 'generated-again'
        run = self._generate_and_push(output, self.root / 'second-runs')
        events = [json.loads(line) for line in (run / 'events.jsonl').read_text().splitlines()]
        self.assertTrue(
            any(
                event.get('role') == 'manifest'
                and event.get('stage') == 'destination-check'
                and event.get('status') == 'unchanged'
                for event in events
            )
        )
        second_publish_dir = self.root / 'public-again'
        second_publish_dir.mkdir()
        original = self.config.read_text(encoding='utf-8')
        self.config.write_text(
            original.replace(str(self.publish_dir), str(second_publish_dir)), encoding='utf-8'
        )
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                result = main(
                    [
                        'publish-manifests',
                        str(run),
                        '--config',
                        str(self.config),
                        '--generated-dir',
                        str(output),
                        '--execute',
                        '--confirm',
                        run.name,
                    ]
                )
        finally:
            self.config.write_text(original, encoding='utf-8')
        self.assertEqual(result, 0)
        self.assertTrue((second_publish_dir / 'manifests' / 'parent.json').is_file())

    def test_confirmation_errors_exit_two_before_reading_the_run(self):
        run = self.root / 'not-a-run'
        cases = [['--execute'], ['--execute', '--confirm', 'different'], ['--confirm', run.name]]
        for options in cases:
            with self.subTest(options=options), contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit) as caught:
                    main(
                        [
                            'publish-manifests',
                            str(run),
                            '--config',
                            str(self.config),
                            '--generated-dir',
                            str(self.output),
                            *options,
                        ]
                    )
            self.assertEqual(caught.exception.code, 2)

    def test_preflight_refusals_do_not_create_log_or_destination_files(self):
        cases = [
            'pending',
            'rollback',
            'incomplete',
            'damaged-journal',
            'missing-generated',
            'missing-source',
            'source-symlink',
            'changed-source',
            'missing-base',
            'changed-base',
            'missing-publish-dir',
            'relative-publish-dir',
            'destination-conflict',
            'parent-symlink',
            'final-symlink',
            'final-directory',
            'final-special',
        ]
        for case in cases:
            with self.subTest(case=case):
                cleanup = self._prepare_refusal(case)
                try:
                    output = io.StringIO()
                    with contextlib.redirect_stdout(output):
                        result = self.publish(
                            '--execute', '--confirm', self.run.name, '--format', 'json'
                        )
                    self.assertEqual(result, 3)
                    report = json.loads(output.getvalue())
                    self.assertTrue(report['validation_errors'])
                    if case in {'pending', 'rollback', 'incomplete', 'damaged-journal'}:
                        reason = inspect_run(self.run)['publication']['reason']
                        self.assertIn(reason, report['validation_errors'][0])
                    self.assertFalse((self.run / 'publication.jsonl').exists())
                    self.assertFalse((self.publish_dir / 'manifests' / 'parent.json').exists())
                finally:
                    cleanup()

    def _prepare_refusal(self, case):
        if case in {'pending', 'rollback', 'incomplete', 'damaged-journal'}:
            return self._prepare_run_refusal(case)
        if case in {'missing-generated', 'changed-source', 'missing-source', 'source-symlink'}:
            return self._prepare_source_refusal(case)
        if case in {'missing-base', 'changed-base', 'missing-publish-dir', 'relative-publish-dir'}:
            return self._prepare_config_refusal(case)
        return self._prepare_destination_refusal(case)

    def _prepare_run_refusal(self, case):
        journal = self.run / 'events.jsonl'
        original = journal.read_bytes()
        if case in {'pending', 'rollback'}:
            marker = self.run / (
                'publication-pending' if case == 'pending' else 'rollback-hold.json'
            )
            marker.touch()
            return marker.unlink
        if case == 'incomplete':
            RunJournal.open(self.run).start('synthetic-incomplete')
        else:
            journal.write_bytes(original + b'{')
        return lambda: journal.write_bytes(original)

    def _prepare_source_refusal(self, case):
        source = next(self.output.rglob('parent.json'))
        original = source.read_bytes()
        if case == 'missing-generated':
            moved = self.root / 'moved-generated'
            self.output.rename(moved)
            return lambda: moved.rename(self.output)
        if case == 'changed-source':
            source.write_bytes(original + b' ')
            return lambda: source.write_bytes(original)
        if case == 'missing-source':
            source.unlink()
            return lambda: source.write_bytes(original)
        backup = source.with_suffix('.backup')
        source.rename(backup)
        source.symlink_to(backup)
        return lambda: (source.unlink(), backup.rename(source))

    def _prepare_config_refusal(self, case):
        original = self.config.read_text(encoding='utf-8')
        if case in {'missing-publish-dir', 'relative-publish-dir'}:
            value = str(self.root / 'missing-public') if case == 'missing-publish-dir' else 'public'
            config = original.replace(
                f'manifest_publish_dir: {self.publish_dir}', f'manifest_publish_dir: {value}'
            )
            self.config.write_text(config, encoding='utf-8')
            return lambda: self.config.write_text(original, encoding='utf-8')
        line = 'manifest_base_url: https://manifests.example.org\n'
        replacement = (
            '' if case == 'missing-base' else 'manifest_base_url: https://other.example.org\n'
        )
        self.config.write_text(original.replace(line, replacement), encoding='utf-8')
        return lambda: self.config.write_text(original, encoding='utf-8')

    def _prepare_destination_refusal(self, case):
        target = self.publish_dir / 'manifests' / 'parent-two.json'
        target.parent.mkdir()
        if case == 'destination-conflict':
            target.write_bytes(b'conflict')
            remove = target.unlink
        elif case == 'final-symlink':
            target.symlink_to(self.root / 'outside-file')
            remove = target.unlink
        elif case == 'final-directory':
            target.mkdir()
            remove = target.rmdir
        elif case == 'final-special':
            os.mkfifo(target)
            remove = target.unlink
        else:
            outside = self.root / 'outside'
            outside.mkdir()
            target.parent.rmdir()
            target.parent.symlink_to(outside, target_is_directory=True)
            return lambda: (target.parent.unlink(), outside.rmdir())
        return lambda: (remove(), target.parent.rmdir())

    def test_copy_fault_points_report_and_rerun_safely(self):
        for fault, expected in [
            ('before-link', 'not_written'),
            ('after-link', 'uncertain'),
            ('result-fsync', 'uncertain'),
            ('source-change', 'not_written'),
            ('parent-swap', 'not_written'),
        ]:
            with self.subTest(fault=fault), tempfile.TemporaryDirectory() as temporary:
                report, run, item, root, outside_written = self._run_fault_case(
                    Path(temporary), fault
                )
                self.assertEqual(report['exit_code'], 1)
                self.assertEqual(report['results'][0]['status'], expected)
                self.assertEqual(
                    (root / item['destination_relative']).exists(), expected == 'uncertain'
                )
                self.assertFalse(outside_written)
                self.assertFalse(list(root.rglob('*.tmp')))
                retry = _copy_each(run, _preflight([item], root), root)
                status = 'created' if expected == 'not_written' else 'unchanged'
                self.assertEqual(retry['results'][0]['status'], status)
                lines = (run / 'publication.jsonl').read_text().splitlines()
                [json.loads(line) for line in lines]

    def test_copy_failure_stops_before_the_next_manifest(self):
        import workbench_lite.publish as publication

        original = publication._append_event

        def fail_first_result(run, event):
            if event['kind'] == 'result':
                raise OSError('synthetic record failure')
            original(run, event)

        output = io.StringIO()
        with patch('workbench_lite.publish._append_event', side_effect=fail_first_result):
            with contextlib.redirect_stdout(output):
                result = self.publish('--execute', '--confirm', self.run.name, '--format', 'json')
        report = json.loads(output.getvalue())
        self.assertEqual(result, 1)
        self.assertEqual([row['status'] for row in report['results']], ['uncertain', 'not_written'])
        self.assertTrue((self.publish_dir / 'manifests' / 'parent.json').is_file())
        self.assertFalse((self.publish_dir / 'manifests' / 'parent-two.json').exists())
        log = (self.run / 'publication.jsonl').read_text(encoding='utf-8').splitlines()
        self.assertEqual([json.loads(line)['kind'] for line in log], ['intent'])

    def _run_fault_case(self, root, fault):
        from workbench_lite.publish import _append_event

        publish_root = root / 'public'
        publish_root.mkdir()
        run = root / 'run'
        run.mkdir()
        source = root / 'generated.json'
        data = b'{"@id":"https://manifests.example.org/example.json"}'
        source.write_bytes(data)
        relative = Path('nested/example.json') if fault == 'parent-swap' else Path('example.json')
        item = {
            'source': source,
            'destination_relative': relative,
            'url': 'https://manifests.example.org/example.json',
            'sha256': hashlib.sha256(data).hexdigest(),
            'size_bytes': len(data),
            'status': 'created',
        }
        manager = self._fault_patch(fault, _append_event, root)
        preflight = _preflight([item], publish_root)
        if fault == 'source-change':
            source.write_bytes(data + b' ')
        with manager:
            report = _copy_each(run, preflight, publish_root)
        source.write_bytes(data)
        outside_written = False
        if fault == 'parent-swap':
            moved = root / 'moved-parent'
            outside_written = (moved / relative.name).exists()
            (publish_root / 'nested').unlink()
            moved.rename(publish_root / 'nested')
        return report, run, item, publish_root, outside_written

    def _fault_patch(self, fault, append_event, root):
        import workbench_lite.publish as publication

        if fault == 'before-link':
            return patch('workbench_lite.publish.os.link', side_effect=OSError('fault'))
        if fault == 'parent-swap':
            original_stage = publication._stage_file

            def stage_and_swap(item, directory_fd):
                temporary = original_stage(item, directory_fd)
                parent = root / 'public' / 'nested'
                moved = root / 'moved-parent'
                parent.rename(moved)
                parent.symlink_to(moved, target_is_directory=True)
                return temporary

            return patch('workbench_lite.publish._stage_file', side_effect=stage_and_swap)
        if fault == 'after-link':

            def fail_result(run, event):
                if event['kind'] == 'result':
                    raise OSError('fault')
                append_event(run, event)

            return patch('workbench_lite.publish._append_event', side_effect=fail_result)
        original = os.fsync
        calls = {'count': 0}

        def fail_result_sync(fd):
            calls['count'] += 1
            if calls['count'] == 6:
                raise OSError('fault')
            original(fd)

        return patch('workbench_lite.publish.os.fsync', side_effect=fail_result_sync)

    def test_raw_manifest_key_parts_remain_raw_filesystem_names(self):
        key = 'prefix/naïve page%2F.json'
        url = 'https://manifests.example.org/na%C3%AFve%20page%252F.json'
        source = self.output / key
        source.parent.mkdir(parents=True, exist_ok=True)
        data = json.dumps({'@id': url}, ensure_ascii=False).encode('utf-8')
        source.write_bytes(data)
        entry = {'bucket': 'synthetic-bucket', 'key': key}
        collected = _manifest_source(
            entry,
            self.output,
            'https://manifests.example.org',
            'prefix',
            {('synthetic-bucket', key): (hashlib.sha256(data).hexdigest(), len(data))},
        )
        planned = _plan_destinations([collected])
        run = self.root / 'raw-name-run'
        run.mkdir()
        result = _copy_each(run, _preflight(planned, self.publish_dir), self.publish_dir)
        target = self.publish_dir / 'naïve page%2F.json'
        self.assertEqual(result['exit_code'], 0)
        self.assertEqual(target.read_bytes(), data)
        self.assertFalse((self.publish_dir / 'naïve page%2F.json'.replace('%2F', '/')).exists())
        legacy = json.dumps(
            {'@id': 'https://manifests.example.org/prefix/na%C3%AFve%20page%252F.json'},
            ensure_ascii=False,
        ).encode('utf-8')
        source.write_bytes(legacy)
        with self.assertRaisesRegex(PublishError, 'Legacy full-key'):
            _manifest_source(
                entry,
                self.output,
                'https://manifests.example.org',
                'prefix',
                {('synthetic-bucket', key): (hashlib.sha256(legacy).hexdigest(), len(legacy))},
            )

    def test_run_and_resolved_publish_directory_locks_contend(self):
        lock_root = self.root / 'locks'
        lock_root.mkdir(mode=0o700)
        original_config = self.config.read_text(encoding='utf-8')
        aliases = [
            f'{self.publish_dir}/',
            f'{self.publish_dir}/../public',
            str(self.root / 'public-alias'),
        ]
        (self.root / 'public-alias').symlink_to(self.publish_dir, target_is_directory=True)
        for lock_key, paths in [
            ('run:' + str(self.run.resolve()), [str(self.publish_dir)]),
            ('publish-dir:' + str(self.publish_dir.resolve()), aliases),
        ]:
            with WriterLocks(lock_root) as held:
                held.acquire(lock_key)
                for alias in paths:
                    with self.subTest(lock=lock_key.split(':', 1)[0], alias=alias):
                        config = original_config.replace(
                            f'manifest_publish_dir: {self.publish_dir}',
                            f'manifest_publish_dir: {alias}',
                        )
                        self.config.write_text(config, encoding='utf-8')
                        with contextlib.redirect_stdout(io.StringIO()):
                            result = cli_main(
                                [
                                    'publish-manifests',
                                    str(self.run),
                                    '--config',
                                    str(self.config),
                                    '--generated-dir',
                                    str(self.output),
                                    '--execute',
                                    '--confirm',
                                    self.run.name,
                                ],
                                lock_factory=lambda: WriterLocks(lock_root),
                            )
                        self.assertEqual(result, 1)
        self.assertFalse((self.run / 'publication.jsonl').exists())

    def test_unreadable_manifest_source_is_refused(self):
        source = next(self.output.rglob('parent.json'))
        real_open = os.open

        def deny_source(path, flags, *args, **kwargs):
            if Path(path) == source:
                raise PermissionError('synthetic unreadable source')
            return real_open(path, flags, *args, **kwargs)

        with patch('workbench_lite.publish.os.open', side_effect=deny_source):
            with contextlib.redirect_stdout(io.StringIO()):
                result = self.publish()
        self.assertEqual(result, 3)
        self.assertFalse((self.run / 'publication.jsonl').exists())


if __name__ == '__main__':
    unittest.main()
