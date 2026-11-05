"""Coverage for public manifest URLs and related CLI boundaries."""

import contextlib
from dataclasses import replace
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch
from urllib.parse import quote

import yaml

from rehearsal_harness import make_synthetic_package
from test_outcomes import Storage
from workbench_lite import cli
from workbench_lite.check import run_check
from workbench_lite.locks import WriterLocks
from workbench_lite.manifest import generate_manifests
from workbench_lite.runs import RunJournal


MANIFEST_BASE = 'https://manifests.example.org'
IMAGE_BASE = 'https://images.example.org/iiif/2'


class ManifestPublicUrlTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.locks = self.root / 'locks'
        self.locks.mkdir(mode=0o700)

    def package(self, name='package', *, manifest_base=MANIFEST_BASE, image_base=IMAGE_BASE):
        package_root = self.root / name
        config = make_synthetic_package(package_root, pages=1)
        value = yaml.safe_load(config.read_text(encoding='utf-8'))
        if manifest_base is None:
            value.pop('manifest_base_url', None)
        else:
            value['manifest_base_url'] = manifest_base
        if image_base is None:
            value.pop('cantaloupe_base_url', None)
        else:
            value['cantaloupe_base_url'] = image_base
        config.write_text(yaml.safe_dump(value, sort_keys=False), encoding='utf-8')
        return config

    def invoke(self, arguments, *, client=None):
        output, errors = io.StringIO(), io.StringIO()
        factory = Mock(return_value=client)
        with (
            patch('workbench_lite.cli.create_s3_client', factory),
            contextlib.redirect_stdout(output),
            contextlib.redirect_stderr(errors),
        ):
            code = cli.main(
                [str(value) for value in arguments],
                lock_factory=lambda: WriterLocks(self.locks),
            )
        try:
            payload = json.loads(output.getvalue())
        except ValueError:
            payload = None
        return {
            'exit': code,
            'payload': payload,
            'stdout': output.getvalue(),
            'stderr': errors.getvalue(),
            'factory': factory,
        }

    def common(self, config, run_name):
        return [
            '--config',
            config,
            '--run-dir',
            self.root / f'{run_name}-runs',
            '--format',
            'json',
        ]

    def generate(self, config, output, run_name, *, manifests=True):
        arguments = ['generate', *self.common(config, run_name), '--output-dir', output]
        if manifests:
            arguments.extend(['--manifests', '--thumbnails'])
        result = self.invoke(arguments)
        return result

    def generated_manifest(self, output):
        return next(path for path in output.rglob('*.json') if 'manifests' in path.parts)

    def mutate_manifest(self, output, kind, value):
        path = self.generated_manifest(output)
        payload = json.loads(path.read_text(encoding='utf-8'))
        if kind.startswith('service-'):
            service = payload['sequences'][0]['canvases'][0]['images'][0]['resource']['service']
            service['@id'] = value
        else:
            payload['@id'] = value
        path.write_text(json.dumps(payload), encoding='utf-8')
        return path

    def push(self, config, output, run_name, mode, *, client=None, extra=()):
        return self.invoke(
            [
                'push',
                *self.common(config, run_name),
                mode,
                *extra,
                '--endpoint-url',
                'http://localhost:4566',
                '--generated-dir',
                output,
            ],
            client=client,
        )

    def test_public_base_url_normalizer_accepts_valid_forms(self):
        from workbench_lite.public_urls import normalize_public_base_url

        accepted = [
            ('https://example.org', 'https://example.org'),
            ('https://example.org/archive', 'https://example.org/archive'),
            ('https://example.org:8443/archive/', 'https://example.org:8443/archive'),
            ('https://example.org:65535/archive', 'https://example.org:65535/archive'),
            ('HTTPS://EXAMPLE.ORG/Archive////', 'https://example.org/Archive'),
            ('http://localhost', 'http://localhost'),
            ('http://LOCALHOST:8080/', 'http://localhost:8080'),
            ('http://127.0.0.1:8080/iiif/2/', 'http://127.0.0.1:8080/iiif/2'),
            ('HTTP://[::1]:8080/manifests///', 'http://[::1]:8080/manifests'),
            ('https://192.0.2.10/iiif', 'https://192.0.2.10/iiif'),
            ('https://[2001:db8::1]/iiif', 'https://[2001:db8::1]/iiif'),
        ]
        with (
            patch('socket.getaddrinfo', side_effect=AssertionError('unexpected DNS lookup')),
            patch('urllib.request.urlopen', side_effect=AssertionError('unexpected HTTP request')),
        ):
            for value, expected in accepted:
                with self.subTest(value=value):
                    self.assertEqual(normalize_public_base_url(value), expected)

    def test_public_base_url_normalizer_refuses_each_invalid_form(self):
        from workbench_lite.public_urls import normalize_public_base_url

        refused = [
            '',
            'manifests/archive',
            'ftp://example.org/archive',
            'https:///archive',
            'http://example.org',
            'http://localhost.example.org',
            'http://localhost./archive',
            'http://localhost@evil.example.org/archive',
            'https://user:pass@example.org/archive',
            'https://example.org/archive?x=1',
            'https://example.org/archive#section',
            'https://example.org/archive?',
            'https://example.org/archive#',
            'https://manifests.example.org/a@b',
            'https://example.org:/archive',
            'https://example.org:0/archive',
            'https://example.org:65536/archive',
            'https://example.org:port/archive',
            'https://example.org/archive path',
            'https://example.org/archive\tpath',
            'https://example.org/archive\x01path',
            'https://example.org\\archive',
            'https://example.org/%',
            'https://example.org/%2',
            'https://example.org/%GG',
            'https://exämple.org/archive',
            'https://K.example.org/archive',
            'https://bad_host.example/archive',
            'http://127.0.0.2/x',
            'http://[2001:db8::1]/x',
            'http://0.0.0.0/x',
        ]
        for value in refused:
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    normalize_public_base_url(value)

    def test_generate_manifest_ids_normalize_bases_and_join_full_keys(self):
        config = self.package()
        report = run_check(config)
        for index, base_case in enumerate(self._normalized_url_base_cases()):
            manifest_base, image_base, _, _ = base_case
            with self.subTest(manifest_base=manifest_base, image_base=image_base):
                self._assert_generated_ids_for_base_case(config, report, index, base_case)

    def test_manifest_and_service_keys_with_reserved_characters_are_encoded_without_key_changes(
        self,
    ):
        self._assert_reserved_key_urls(self._build_reserved_key_manifest())

    def _build_reserved_key_manifest(self):
        config = self.package('encoded')
        report = run_check(config)
        output = self.root / 'encoded-generated'
        service_report = self._generate_service_image(report, output, config)
        original_service = next(
            entry for entry in report.upload_plan if entry.role == 'service_jpg'
        )
        original_manifest = next(entry for entry in report.upload_plan if entry.role == 'manifest')
        service_key = 'sample/é/literal%2F with space?hash#/service.jpg'
        manifest_key = 'sample/é/literal%2F with space?hash#/manifest.json'
        service_path = output / service_key
        service_path.parent.mkdir(parents=True, exist_ok=True)
        service_path.write_bytes((output / original_service.key).read_bytes())
        service_entry = replace(original_service, key=service_key)
        manifest_entry = replace(original_manifest, key=manifest_key)
        plan = [
            manifest_entry
            if entry is original_manifest
            else service_entry
            if entry is original_service
            else entry
            for entry in report.upload_plan
        ]

        generated = generate_manifests(
            report.objects,
            plan,
            output,
            cantaloupe_base_url='HTTPS://IMAGES.EXAMPLE.ORG/iiif/2///',
            manifest_base_url='HTTPS://MANIFESTS.EXAMPLE.ORG/archive///',
            page_dimensions=service_report.page_dimensions,
        )
        payload = json.loads(Path(generated.results[0].output_path).read_text())
        return payload, manifest_entry, service_entry, manifest_key, service_key

    def _assert_reserved_key_urls(self, case):
        payload, manifest_entry, service_entry, manifest_key, service_key = case
        canvas = payload['sequences'][0]['canvases'][0]
        annotation = canvas['images'][0]

        self.assertEqual(
            payload['@id'],
            f'https://manifests.example.org/archive/{quote(manifest_key, safe="/")}',
        )
        self.assertEqual(
            annotation['resource']['service']['@id'],
            f'https://images.example.org/iiif/2/{quote(service_key, safe="")}',
        )
        self.assertIn('%252F', payload['@id'])
        self.assertEqual(manifest_entry.key, manifest_key)
        self.assertEqual(service_entry.key, service_key)
        self.assertEqual(canvas['@id'], 'page/page-1')
        self.assertEqual(annotation['on'], canvas['@id'])

    def test_library_generation_keeps_relative_base_override_support(self):
        config = self.package('relative-library')
        report = run_check(config)
        output = self.root / 'relative-library-generated'
        service_report = self._generate_service_image(report, output, config)

        generated = generate_manifests(
            report.objects,
            report.upload_plan,
            output,
            cantaloupe_base_url='/iiif/2',
            manifest_base_url='/manifests',
            page_dimensions=service_report.page_dimensions,
        )
        payload = json.loads(Path(generated.results[0].output_path).read_text())

        manifest_entry = next(entry for entry in report.upload_plan if entry.role == 'manifest')
        self.assertEqual(payload['@id'], f'/manifests/{manifest_entry.key}')
        self.assertTrue(
            payload['sequences'][0]['canvases'][0]['images'][0]['resource']['service'][
                '@id'
            ].startswith('/iiif/2/')
        )

    def test_generate_refuses_invalid_resolved_config_or_flag_bases_without_output(self):
        cases = [
            ('config-manifest', {'manifest_base_url': 'ftp://manifests.example.org'}, []),
            ('config-image', {'cantaloupe_base_url': 'http://images.example.org'}, []),
            (
                'flag-manifest',
                {'manifest_base_url': None},
                ['--manifest-base-url', 'ftp://manifests.example.org'],
            ),
            (
                'flag-image',
                {'cantaloupe_base_url': None},
                ['--cantaloupe-base-url', 'http://images.example.org'],
            ),
            ('empty-manifest-config', {'manifest_base_url': ''}, []),
            ('empty-image-config', {'cantaloupe_base_url': ''}, []),
        ]
        for name, settings, flags in cases:
            with self.subTest(name=name):
                config = self.package(name)
                self._update_config(config, **settings)
                output = self.root / f'{name}-generated'
                result = self.invoke(
                    [
                        'generate',
                        *self.common(config, name),
                        '--output-dir',
                        output,
                        '--manifests',
                        '--manifest-base-url',
                        MANIFEST_BASE,
                        '--cantaloupe-base-url',
                        IMAGE_BASE,
                        *flags,
                    ]
                )
                self.assertEqual(result['exit'], 3)
                self.assertFalse(output.exists())

    def test_generate_refuses_at_in_base_path_without_output(self):
        config = self.package('at-in-base-path', manifest_base='https://manifests.example.org/a@b')
        output = self.root / 'at-in-base-path-generated'

        result = self.generate(config, output, 'at-in-base-path')

        self.assertEqual(result['exit'], 3)
        self.assertFalse(output.exists())

    def test_generate_refusal_does_not_change_an_existing_output_folder(self):
        config = self.package('existing-output')
        self._update_config(config, cantaloupe_base_url='https://example.org/path?bad=1')
        output = self.root / 'already-generated'
        output.mkdir()
        marker = output / 'keep.txt'
        marker.write_text('synthetic marker', encoding='utf-8')
        before = {
            path.relative_to(output): path.read_bytes()
            for path in output.rglob('*')
            if path.is_file()
        }

        result = self.invoke(
            [
                'generate',
                *self.common(config, 'existing-output'),
                '--output-dir',
                output,
                '--manifests',
            ]
        )

        after = {
            path.relative_to(output): path.read_bytes()
            for path in output.rglob('*')
            if path.is_file()
        }
        self.assertEqual(result['exit'], 3)
        self.assertEqual(after, before)

    def test_config_bases_override_flags(self):
        config = self.package('precedence')
        output = self.root / 'precedence-generated'
        result = self.invoke(
            [
                'generate',
                *self.common(config, 'precedence'),
                '--output-dir',
                output,
                '--manifests',
                '--manifest-base-url',
                'ftp://invalid.example.org',
                '--cantaloupe-base-url',
                'ftp://invalid.example.org',
            ]
        )
        self.assertEqual(result['exit'], 0, result['stdout'] + result['stderr'])
        payload = json.loads(self.generated_manifest(output).read_text())
        manifest_entry = next(
            entry for entry in run_check(config).upload_plan if entry.role == 'manifest'
        )
        self.assertEqual(payload['@id'], f'{MANIFEST_BASE}/{manifest_entry.key}')

    def test_generate_without_manifests_and_check_do_not_require_bases(self):
        config = self.package('no-bases', manifest_base=None, image_base=None)
        check = self.invoke(['check', *self.common(config, 'check-no-bases')])
        output = self.root / 'no-manifests-generated'
        generated = self.generate(config, output, 'generate-no-bases', manifests=False)

        self.assertEqual(check['exit'], 0, check['stdout'] + check['stderr'])
        self.assertEqual(generated['exit'], 0, generated['stdout'] + generated['stderr'])
        self.assertFalse(list(output.rglob('manifests/*.json')))

    def test_generate_manifests_requires_both_bases_from_config_or_flags(self):
        cases = [
            ('missing-image-base', MANIFEST_BASE, None),
            ('missing-manifest-base', None, IMAGE_BASE),
        ]
        for name, manifest_base, image_base in cases:
            with self.subTest(name=name):
                config = self.package(name, manifest_base=manifest_base, image_base=image_base)
                output = self.root / f'{name}-generated'

                result = self.generate(config, output, name)

                self.assertEqual(result['exit'], 3)
                self.assertFalse(output.exists())

    def test_push_execute_refuses_each_invalid_manifest_or_service_id_before_writes(self):
        invalid_ids = [
            ('manifest-relative', 'relative/manifest.json'),
            ('manifest-invalid', 'ftp://manifests.example.org/manifest.json'),
            ('service-relative', '/iiif/2/relative-image'),
            ('service-invalid', 'ftp://images.example.org/iiif/2/image'),
        ]
        for kind, value in invalid_ids:
            with self.subTest(kind=kind):
                config = self.package(f'execute-{kind}')
                output = self.root / f'execute-{kind}-generated'
                generated = self.generate(config, output, f'generate-{kind}')
                self.assertEqual(generated['exit'], 0, generated['stdout'] + generated['stderr'])
                manifest_path = self.mutate_manifest(output, kind, value)
                storage = Storage()
                result = self.push(config, output, f'execute-{kind}', '--execute', client=storage)

                self.assert_manifest_id_error(result, output, manifest_path)
                self.assertEqual(result['exit'], 3)
                self.assertEqual(storage.claim_calls, [])
                self.assertEqual(storage.calls, [])

    def test_push_execute_checks_legacy_generated_folder_without_generation_record(self):
        config = self.package('legacy')
        output = self.root / 'legacy-generated'
        generated = self.generate(config, output, 'generate-legacy')
        self.assertEqual(generated['exit'], 0, generated['stdout'] + generated['stderr'])
        manifest_path = self.mutate_manifest(
            output, 'manifest-relative', 'workbench-lite/package/manifests/package.json'
        )
        (output / '.workbench-run.json').unlink()
        storage = Storage()

        result = self.push(config, output, 'legacy-push', '--execute', client=storage)

        self.assert_manifest_id_error(result, output, manifest_path)
        self.assertEqual(result['exit'], 3)
        self.assertEqual(storage.claim_calls, [])
        self.assertEqual(storage.calls, [])

    def test_push_execute_checks_stale_manifest_left_by_generate_without_manifests(self):
        config = self.package('stale')
        output = self.root / 'stale-generated'
        first = self.generate(config, output, 'generate-stale-first')
        self.assertEqual(first['exit'], 0, first['stdout'] + first['stderr'])
        stale_manifest = self.mutate_manifest(output, 'service-relative', '/iiif/2/relative-image')
        stale_contents = stale_manifest.read_bytes()
        second = self.generate(config, output, 'generate-stale-without-manifests', manifests=False)
        self.assertEqual(second['exit'], 0, second['stdout'] + second['stderr'])
        self.assertEqual(stale_manifest.read_bytes(), stale_contents)
        storage = Storage()

        result = self.push(config, output, 'stale-push', '--execute', client=storage)

        self.assert_manifest_id_error(result, output, stale_manifest)
        self.assertEqual(result['exit'], 3)
        self.assertEqual(storage.claim_calls, [])
        self.assertEqual(storage.calls, [])

    def test_interrupted_resume_refuses_invalid_manifest_before_reconcile_and_retains_hold(self):
        config, output, manifest_path, run_path = self._prepare_interrupted_resume()
        storage = Storage()
        result = self.push(
            config, output, 'resume', '--execute', client=storage, extra=['--resume']
        )
        inspection = self.invoke(['inspect-run', run_path, '--format', 'json'])

        self.assert_manifest_id_error(result, output, manifest_path)
        self.assertEqual(result['exit'], 3, result['stdout'] + result['stderr'])
        self.assertEqual(result['factory'].call_count, 0)
        self.assertEqual(storage.claim_calls, [])
        self.assertEqual(storage.calls, [])
        self.assertEqual(inspection['payload']['publication']['state'], 'held')
        self.assertEqual(len(inspection['payload']['unknown_operations']), 1)
        self.assertTrue(manifest_path.is_file())

    def _prepare_interrupted_resume(self):
        config = self.package('resume')
        output = self.root / 'resume-generated'
        generated = self.generate(config, output, 'generate-resume')
        self.assertEqual(generated['exit'], 0, generated['stdout'] + generated['stderr'])
        manifest_path = self.mutate_manifest(output, 'manifest-relative', 'relative/manifest.json')
        reference = json.loads((output / '.workbench-run.json').read_text())
        run_path = Path(reference['run_path'])
        report = run_check(config, run_id=reference['run_id'])
        manifest_entry = next(entry for entry in report.upload_plan if entry.role == 'manifest')
        journal = RunJournal.open(run_path)
        journal.start('push')
        journal.append(
            'context',
            stage='push',
            storage={
                'effective_endpoint_url': 'http://localhost:4566',
                'effective_region': 'us-east-1',
            },
        )
        journal.intent('push', manifest_entry.to_dict())
        journal.finish(143)
        recorded_events = [
            json.loads(line) for line in (run_path / 'events.jsonl').read_text().splitlines()
        ]
        self.assertTrue(
            any(
                event.get('stage') == 'push' and isinstance(event.get('storage'), dict)
                for event in recorded_events
            )
        )
        return config, output, manifest_path, run_path

    def test_online_and_offline_dry_runs_check_each_generated_manifest_id(self):
        invalid_ids = [
            ('manifest-relative', 'relative/manifest.json'),
            ('manifest-invalid', 'ftp://manifests.example.org/manifest.json'),
            ('service-relative', '/iiif/2/relative-image'),
            ('service-invalid', 'ftp://images.example.org/iiif/2/image'),
        ]
        for index, (kind, value) in enumerate(invalid_ids):
            for offline in (False, True):
                with self.subTest(kind=kind, offline=offline):
                    name = f'dry-{index}-{offline}'
                    config = self.package(name)
                    output = self.root / f'{name}-generated'
                    generated = self.generate(config, output, f'{name}-generate')
                    self.assertEqual(
                        generated['exit'], 0, generated['stdout'] + generated['stderr']
                    )
                    manifest_path = self.mutate_manifest(output, kind, value)
                    storage = Storage()
                    mode = '--dry-run'
                    arguments = ['push', *self.common(config, name), mode]
                    if offline:
                        arguments.append('--offline')
                    arguments.extend(
                        ['--generated-dir', output, '--endpoint-url', 'http://localhost:4566']
                    )
                    result = self.invoke(arguments, client=storage)

                    self.assert_manifest_id_error(result, output, manifest_path)
                    self.assertEqual(result['exit'], 3)
                    self.assertEqual(storage.calls, [])
                    self.assertEqual(storage.claim_calls, [])
                    if offline:
                        self.assertEqual(result['factory'].call_count, 0)

    def test_push_rejects_manifest_id_with_wrong_planned_key(self):
        self._assert_push_rejects_join_mismatch(
            'wrong-manifest-key', None, 'manifest', 'https://manifests.example.org/other.json'
        )

    def test_push_rejects_unencoded_manifest_key(self):
        self._assert_push_rejects_join_mismatch('unencoded-manifest-key', 'demo/literal%2F')

    def test_push_rejects_service_id_with_multiple_segments(self):
        self._assert_push_rejects_join_mismatch(
            'multi-segment-service-id',
            None,
            'service-multi-segment',
            'https://images.example.org/iiif/2/a/b',
        )

    def test_push_checks_each_service_in_a_service_list(self):
        config, output, manifest_path, payload = self._generated_manifest_case('service-list')
        resource = payload['sequences'][0]['canvases'][0]['images'][0]['resource']
        resource['service'] = [resource['service'], {'@id': '/iiif/2/relative-image'}]
        self._write_manifest(manifest_path, payload)
        result = self.push(config, output, 'service-list-push', '--dry-run', extra=['--offline'])

        self.assert_manifest_id_error(result, output, manifest_path)
        self.assertEqual(result['exit'], 3)
        self.assertEqual(result['factory'].call_count, 0)

    def test_push_checks_canvas_and_manifest_thumbnail_services(self):
        for scope in ('canvas', 'manifest'):
            with self.subTest(scope=scope):
                config, output, manifest_path, payload = self._generated_manifest_case(
                    f'thumbnail-service-{scope}'
                )
                owner = payload['sequences'][0]['canvases'][0] if scope == 'canvas' else payload
                owner['thumbnail'] = {
                    '@id': 'https://images.example.org/thumbnail.jpg',
                    'service': {'@id': '/iiif/2/relative-thumbnail'},
                }
                self._write_manifest(manifest_path, payload)
                result = self.push(
                    config,
                    output,
                    f'thumbnail-service-{scope}-push',
                    '--dry-run',
                    extra=['--offline'],
                )

                self.assert_manifest_id_error(result, output, manifest_path)
                self.assertEqual(result['exit'], 3)
                self.assertEqual(result['factory'].call_count, 0)

    def test_url_and_sequence_errors_are_reported_together(self):
        config, output, manifest_path, payload = self._generated_manifest_case(
            'url-and-sequences-error'
        )
        payload['sequences'] = None
        payload['thumbnail'] = {
            'service': {'@id': '/iiif/2/relative-thumbnail'},
        }
        self._write_manifest(manifest_path, payload)

        result = self.push(
            config, output, 'url-and-sequences-error-push', '--dry-run', extra=['--offline']
        )
        errors = result['payload']['preflight']['local']['errors']

        self.assertEqual(result['exit'], 3)
        self.assertTrue(any('Manifest sequences must be a list.' in error for error in errors))
        self.assertTrue(any('image service @id' in error for error in errors))

    def test_manifest_id_and_container_errors_are_reported_together(self):
        config, output, manifest_path, payload = self._generated_manifest_case(
            'manifest-id-and-container-error'
        )
        payload['@id'] = 'relative/manifest.json'
        self._set_manifest_container(payload, 'images', 'wrong-type')
        self._write_manifest(manifest_path, payload)

        result = self.push(
            config,
            output,
            'manifest-id-and-container-error-push',
            '--dry-run',
            extra=['--offline'],
        )
        errors = result['payload']['preflight']['local']['errors']

        self.assertEqual(result['exit'], 3)
        self.assertTrue(any('manifest @id' in error for error in errors))
        self.assertTrue(any('Manifest images must be a list.' in error for error in errors))

    def test_wrong_manifest_containers_are_reported_without_traceback(self):
        cases = [
            ('sequences', None),
            ('sequences', 'wrong-type'),
            ('canvases', None),
            ('canvases', 'wrong-type'),
            ('images', None),
            ('images', 'wrong-type'),
        ]
        for container, value in cases:
            name = f'malformed-{container}-{value}'
            with self.subTest(container=container, value=value):
                config, output, manifest_path, payload = self._generated_manifest_case(name)
                self._set_manifest_container(payload, container, value)
                self._write_manifest(manifest_path, payload)
                result = self.push(config, output, f'{name}-push', '--dry-run', extra=['--offline'])
                errors = result['payload']['preflight']['local']['errors']

                self.assertEqual(result['exit'], 1, result['stdout'] + result['stderr'])
                self.assertTrue(
                    any(manifest_path.relative_to(output).as_posix() in error for error in errors),
                    errors,
                )
                self.assertTrue(any(f'Manifest {container}' in error for error in errors), errors)
                self.assertNotIn('Traceback', result['stdout'] + result['stderr'])

    def test_non_object_manifest_with_url_marker_in_prefix_stays_exit_one(self):
        config, output, manifest_path, _ = self._generated_manifest_case(
            'marker-prefix', s3_prefix='Public URL validation failed: marker'
        )
        manifest_path.write_text('[]', encoding='utf-8')

        result = self.push(config, output, 'marker-prefix-push', '--dry-run', extra=['--offline'])
        errors = result['payload']['preflight']['local']['errors']

        self.assertEqual(result['exit'], 1, result['stdout'] + result['stderr'])
        self.assertTrue(any('Expected a JSON object.' in error for error in errors), errors)
        self.assertNotIn('Traceback', result['stdout'] + result['stderr'])

    def test_dry_run_without_generated_files_remains_planning_only(self):
        config = self.package(
            'planning-only',
            manifest_base='ftp://invalid.example.org',
            image_base='http://images.example.org',
        )
        result = self.invoke(
            ['push', *self.common(config, 'planning-only'), '--dry-run', '--offline']
        )
        self.assertEqual(result['exit'], 0, result['stdout'] + result['stderr'])
        self.assertEqual(result['factory'].call_count, 0)

    def test_push_accepts_manifest_urls_independent_of_current_config(self):
        config = self.package('alternate-base')
        output = self.root / 'alternate-base-generated'
        generated = self.generate(config, output, 'alternate-base-generate')
        self.assertEqual(generated['exit'], 0, generated['stdout'] + generated['stderr'])
        report = run_check(config)
        manifest_entry = next(entry for entry in report.upload_plan if entry.role == 'manifest')
        service_entry = next(entry for entry in report.upload_plan if entry.role == 'service_jpg')
        path = self.generated_manifest(output)
        payload = json.loads(path.read_text(encoding='utf-8'))
        payload['@id'] = 'https://alternate.example.org/public/' + quote(
            manifest_entry.key, safe='/'
        )
        service = payload['sequences'][0]['canvases'][0]['images'][0]['resource']['service']
        service['@id'] = 'https://images-alternate.example.org/iiif/v2/' + quote(
            service_entry.key, safe=''
        )
        path.write_text(json.dumps(payload), encoding='utf-8')
        (output / '.workbench-run.json').unlink()

        result = self.push(config, output, 'alternate-base-push', '--dry-run', extra=['--offline'])

        self.assertEqual(result['exit'], 0, result['stdout'] + result['stderr'])
        self.assertEqual(result['factory'].call_count, 0)

    def test_other_readiness_failures_stay_exit_one(self):
        for failure in ('missing', 'unreadable', 'non-object'):
            with self.subTest(failure=failure):
                config = self.package(f'readiness-{failure}')
                output = self.root / f'readiness-{failure}-generated'
                generated = self.generate(config, output, f'readiness-{failure}-generate')
                self.assertEqual(generated['exit'], 0, generated['stdout'] + generated['stderr'])
                manifest = self.generated_manifest(output)
                if failure == 'missing':
                    manifest.unlink()
                    result = self.push(
                        config, output, f'readiness-{failure}', '--dry-run', client=Storage()
                    )
                elif failure == 'non-object':
                    manifest.write_text('[]', encoding='utf-8')
                    result = self.push(
                        config, output, f'readiness-{failure}', '--dry-run', client=Storage()
                    )
                else:
                    original_open = Path.open

                    def unreadable(
                        path, *args, manifest_path=manifest, open_file=original_open, **kwargs
                    ):
                        if path == manifest_path:
                            raise PermissionError('synthetic unreadable manifest')
                        return open_file(path, *args, **kwargs)

                    with patch.object(Path, 'open', unreadable):
                        result = self.push(
                            config, output, f'readiness-{failure}', '--dry-run', client=Storage()
                        )
                self.assertEqual(result['exit'], 1, result['stdout'] + result['stderr'])

    def test_mixed_url_and_readiness_failures_report_both_and_exit_three(self):
        config = self.package('mixed-readiness')
        output = self.root / 'mixed-readiness-generated'
        generated = self.generate(config, output, 'mixed-readiness-generate')
        self.assertEqual(generated['exit'], 0, generated['stdout'] + generated['stderr'])
        manifest = self.mutate_manifest(output, 'manifest-relative', 'relative/manifest.json')
        missing_key = next(output.rglob('thumbnail.jpg'))
        missing_key.unlink()

        result = self.push(config, output, 'mixed-readiness', '--dry-run', client=Storage())
        errors = result['payload']['preflight']['local']['errors']

        self.assertEqual(result['exit'], 3)
        self.assertTrue(any(str(manifest.relative_to(output)) in error for error in errors))
        self.assertTrue(any('thumbnail' in error for error in errors))

    def test_special_manifest_key_is_encoded_and_passes_push_check(self):
        config = self.package('encoded-push')
        self._update_config(
            config,
            s3_prefix='demo/é/literal%2F space?hash#',
            manifest_base_url='HTTPS://MANIFESTS.EXAMPLE.ORG/archive///',
            cantaloupe_base_url='HTTPS://IMAGES.EXAMPLE.ORG/iiif/2///',
        )
        report = run_check(config)
        manifest_entry = next(entry for entry in report.upload_plan if entry.role == 'manifest')
        original_key = manifest_entry.key
        output = self.root / 'encoded-push-generated'
        generated = self.generate(config, output, 'encoded-push-generate')
        self.assertEqual(generated['exit'], 0, generated['stdout'] + generated['stderr'])
        payload = json.loads(self.generated_manifest(output).read_text())
        expected_id = f'https://manifests.example.org/archive/{quote(original_key, safe="/")}'
        canvas = payload['sequences'][0]['canvases'][0]
        push_result = self.push(config, output, 'encoded-push', '--dry-run', client=Storage())

        self.assertIn('literal%2F', original_key)
        self.assertEqual(payload['@id'], expected_id)
        self.assertIn('%252F', payload['@id'])
        self.assertEqual(canvas['@id'], 'page/page-1')
        self.assertEqual(canvas['images'][0]['on'], canvas['@id'])
        self.assertEqual(push_result['exit'], 0, push_result['stdout'] + push_result['stderr'])

    def test_rollback_inventory_uses_normalized_recorded_base_and_matches_manifest_id(self):
        config = self.package(
            'inventory-uppercase',
            manifest_base='HTTPS://MANIFESTS.EXAMPLE.ORG/archive///',
            image_base='HTTPS://IMAGES.EXAMPLE.ORG/iiif/2///',
        )
        output = self.root / 'inventory-uppercase-generated'
        generated = self.generate(config, output, 'inventory-uppercase-generate')
        self.assertEqual(generated['exit'], 0, generated['stdout'] + generated['stderr'])
        manifest_path = self.generated_manifest(output)
        manifest_payload = json.loads(manifest_path.read_text())
        reference = json.loads((output / '.workbench-run.json').read_text())
        run_path = Path(reference['run_path'])
        report = run_check(config, run_id=reference['run_id'])
        manifest_entry = next(entry for entry in report.upload_plan if entry.role == 'manifest')
        journal = RunJournal.open(run_path)
        journal.start('push')
        self._record_inventory_result(journal, manifest_entry, 'destination-check', 'absent')
        self._record_inventory_result(journal, manifest_entry, 'push', 'uploaded')
        journal.finish(1)

        from workbench_lite.rollback_inventory import build_inventory

        inventory = build_inventory(run_path)

        self.assertEqual(len(inventory['manifest_urls']), 1)
        self.assertEqual(inventory['manifest_urls'][0]['url'], manifest_payload['@id'])

    def test_rollback_inventory_omits_a_recorded_base_that_fails_the_url_rule(self):
        inventory = self._inventory_with_base('https://manifests.example.org/archive?query=1')
        self.assertEqual(inventory['manifest_urls'], [])

    def test_rollback_inventory_never_uses_an_unrecorded_current_config_base(self):
        from workbench_lite.rollback_inventory import build_inventory

        run = self._inventory_run(snapshot_base=None, current_base='https://current.example.org')
        inventory = build_inventory(run.path)
        self.assertEqual(inventory['manifest_urls'], [])

    def test_rehearsal_config_builders_supply_loopback_bases_and_override_sample_values(self):
        from scripts.rehearsal_storage import make_config

        source = self.root / 'sample-source'
        source.mkdir()
        (source / 'input.csv').write_text('title,id,field_model,parent_id,field_weight,file\n')
        (source / 'sample.yml').write_text(
            'input_dir: .\ninput_csv: input.csv\n'
            'manifest_base_url: https://sample.example.org\n'
            'cantaloupe_base_url: https://sample-images.example.org\n'
        )
        rehearsal_config = make_config(
            source,
            self.root / 'rehearsal',
            'synthetic-private',
            'synthetic-public',
            'test-prefix',
        )
        rehearsal_value = yaml.safe_load(rehearsal_config.read_text())
        shared_config = make_synthetic_package(self.root / 'shared-builder', pages=1)
        shared_value = yaml.safe_load(shared_config.read_text())

        self.assertEqual(rehearsal_value['manifest_base_url'], 'http://localhost:8080/manifests')
        self.assertEqual(rehearsal_value['cantaloupe_base_url'], 'http://localhost:8080/iiif/2')
        self.assertEqual(shared_value['manifest_base_url'], 'http://localhost:8080/manifests')
        self.assertEqual(shared_value['cantaloupe_base_url'], 'http://localhost:8080/iiif/2')

    def _normalized_url_base_cases(self):
        return [
            (
                'https://manifests.example.org',
                'https://images.example.org/iiif/2',
                'https://manifests.example.org',
                'https://images.example.org/iiif/2',
            ),
            (
                'https://manifests.example.org/',
                'https://images.example.org/iiif/2/',
                'https://manifests.example.org',
                'https://images.example.org/iiif/2',
            ),
            (
                'HTTPS://MANIFESTS.EXAMPLE.ORG/Archive////',
                'HTTPS://IMAGES.EXAMPLE.ORG/IIIF/2////',
                'https://manifests.example.org/Archive',
                'https://images.example.org/IIIF/2',
            ),
            (
                'HTTPS://MANIFESTS.EXAMPLE.ORG/archive///',
                'HTTPS://IMAGES.EXAMPLE.ORG/cantaloupe/iiif/2///',
                'https://manifests.example.org/archive',
                'https://images.example.org/cantaloupe/iiif/2',
            ),
        ]

    def _assert_generated_ids_for_base_case(self, config, report, index, base_case):
        manifest_base, image_base, expected_manifest_base, expected_image_base = base_case
        output = self.root / f'joined-{index}'
        service_report = self._generate_service_image(report, output, config)
        generated = generate_manifests(
            report.objects,
            report.upload_plan,
            output,
            cantaloupe_base_url=image_base,
            manifest_base_url=manifest_base,
            page_dimensions=service_report.page_dimensions,
        )
        self.assertEqual(generated.generated_count, 1)
        payload = json.loads(Path(generated.results[0].output_path).read_text())
        manifest_entry = next(entry for entry in report.upload_plan if entry.role == 'manifest')
        service_entry = next(entry for entry in report.upload_plan if entry.role == 'service_jpg')
        self.assertEqual(
            payload['@id'],
            f'{expected_manifest_base}/{quote(manifest_entry.key, safe="/")}',
        )
        canvas = payload['sequences'][0]['canvases'][0]
        annotation = canvas['images'][0]
        expected_service = f'{expected_image_base}/{quote(service_entry.key, safe="")}'
        self.assertEqual(annotation['resource']['service']['@id'], expected_service)
        self.assertEqual(canvas['@id'], 'page/page-1')
        self.assertEqual(annotation['on'], 'page/page-1')
        self.assertEqual(manifest_entry.key, 'run/package/manifests/package.json')

    def _assert_push_rejects_join_mismatch(self, name, prefix=None, kind='manifest', value=None):
        config, output, manifest_path = self._prepare_join_mismatch_case(name, prefix, kind, value)
        storage = Storage()
        result = self.push(config, output, name, '--execute', client=storage)
        self.assert_manifest_id_error(result, output, manifest_path)
        self.assertEqual(result['exit'], 3)
        self.assertEqual(storage.claim_calls, [])
        self.assertEqual(storage.calls, [])

    def _prepare_join_mismatch_case(self, name, prefix, kind, value):
        config = self.package(name)
        if prefix is not None:
            self._update_config(config, s3_prefix=prefix)
        output = self.root / f'{name}-generated'
        generated = self.generate(config, output, f'{name}-generate')
        self.assertEqual(generated['exit'], 0, generated['stdout'] + generated['stderr'])
        manifest_key = self._planned_manifest_key(config)
        generated_manifest_id = f'{MANIFEST_BASE}/{quote(manifest_key, safe="/")}'
        if kind.startswith('service-'):
            actual_id = json.loads(self.generated_manifest(output).read_text())['@id']
            self.assertEqual(actual_id, generated_manifest_id)
        if value is None:
            value = f'{MANIFEST_BASE}/{manifest_key}'
        manifest_path = self.mutate_manifest(output, kind, value)
        if kind.startswith('service-'):
            self._assert_service_mutation(manifest_path, generated_manifest_id, value)
        return config, output, manifest_path

    def _assert_service_mutation(self, manifest_path, expected_manifest_id, value):
        payload = json.loads(manifest_path.read_text(encoding='utf-8'))
        service = payload['sequences'][0]['canvases'][0]['images'][0]['resource']['service']
        self.assertEqual(payload['@id'], expected_manifest_id)
        self.assertEqual(service['@id'], value)

    def _planned_manifest_key(self, config):
        report = run_check(config)
        entry = next(entry for entry in report.upload_plan if entry.role == 'manifest')
        return entry.key

    def _generated_manifest_case(self, name, *, s3_prefix=None):
        config = self.package(name)
        if s3_prefix is not None:
            self._update_config(config, s3_prefix=s3_prefix)
        output = self.root / f'{name}-generated'
        generated = self.generate(config, output, f'{name}-generate')
        self.assertEqual(generated['exit'], 0, generated['stdout'] + generated['stderr'])
        manifest_path = self.generated_manifest(output)
        payload = json.loads(manifest_path.read_text(encoding='utf-8'))
        return config, output, manifest_path, payload

    def _write_manifest(self, path, payload):
        path.write_text(json.dumps(payload), encoding='utf-8')

    def _set_manifest_container(self, payload, container, value):
        if container == 'sequences':
            payload['sequences'] = value
            return
        sequence = payload['sequences'][0]
        if container == 'canvases':
            sequence['canvases'] = value
            return
        sequence['canvases'][0]['images'] = value

    def _generate_service_image(self, report, output, config):
        from workbench_lite.generate import generate_service_jpgs

        input_dir = Path(yaml.safe_load(config.read_text(encoding='utf-8'))['input_dir'])
        return generate_service_jpgs(report.upload_plan, input_dir, output)

    def assert_manifest_id_error(self, result, output, manifest_path):
        payload = result.get('payload') or {}
        preflight = payload.get('preflight') or {}
        local = preflight.get('local') or {}
        errors = local.get('errors') or []
        relative_path = manifest_path.relative_to(output).as_posix()
        self.assertTrue(
            any(relative_path in error and '@id' in error for error in errors),
            errors,
        )

    def _update_config(self, config, **updates):
        value = yaml.safe_load(config.read_text(encoding='utf-8'))
        for key, setting in updates.items():
            if setting is None:
                value.pop(key, None)
            else:
                value[key] = setting
        config.write_text(yaml.safe_dump(value, sort_keys=False), encoding='utf-8')

    def _inventory_with_base(self, base):
        from workbench_lite.rollback_inventory import build_inventory

        return build_inventory(self._inventory_run(snapshot_base=base).path)

    def _inventory_run(self, *, snapshot_base, current_base=None):
        config = self.root / f'inventory-{len(list(self.root.glob("inventory-config-*.yml")))}.yml'
        current_setting = f'manifest_base_url: {current_base}\n' if current_base is not None else ''
        config.write_text(f'batch_id: package\n{current_setting}', encoding='utf-8')
        csv_path = self.root / 'inventory.csv'
        csv_path.write_text('title\nsynthetic\n', encoding='utf-8')
        snapshot = {'batch_id': 'package'}
        if snapshot_base is not None:
            snapshot['manifest_base_url'] = snapshot_base
        run = RunJournal.create(self.root / 'inventory-runs')
        run.inputs(config, snapshot, csv_path)
        from workbench_lite.models import UploadPlanEntry

        entry = UploadPlanEntry(
            'manifest',
            '',
            'synthetic-public',
            'workbench-lite/package/manifests/package.json',
            True,
            'package',
            '',
            True,
            False,
            None,
        )
        run.plan([entry])
        run.start('generate', options={'config': str(config)})
        run.finish(0)
        if current_base is not None:
            config.write_text(
                f'batch_id: package\nmanifest_base_url: {current_base}\n', encoding='utf-8'
            )
        run.start('push')
        self._record_inventory_result(run, entry, 'destination-check', 'absent')
        self._record_inventory_result(run, entry, 'push', 'uploaded')
        run.finish(1)
        return run

    def _record_inventory_result(self, run, entry, stage, status):
        identity = entry.to_dict()
        operation = run.intent(stage, identity)
        run.result(
            operation,
            stage,
            {
                **identity,
                'status': status,
                'remote_outcome': 'written' if status == 'uploaded' else 'absent',
            },
        )


if __name__ == '__main__':
    unittest.main()
