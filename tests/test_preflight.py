from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from workbench_lite.check import run_check
from workbench_lite.config import ConfigError, load_config
from test_outcomes import package


class PreflightTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.config = package(self.root)

    def test_preflight_client_has_separate_bounded_config(self):
        from types import SimpleNamespace
        from unittest.mock import Mock
        from workbench_lite.s3_client import create_s3_client
        factory = Mock()
        with patch.dict(sys.modules, {'boto3': SimpleNamespace(client=factory)}):
            create_s3_client('http://localhost:4566', 'us-east-1', preflight=True)
            create_s3_client('http://localhost:4566', 'us-east-1')
        reader, writer = [call.kwargs for call in factory.call_args_list]
        config = reader.pop('config')
        self.assertEqual((config.connect_timeout, config.read_timeout), (5, 5))
        self.assertEqual(config.retries, {'total_max_attempts': 1})
        self.assertNotIn('config', writer)
        self.assertEqual(reader, writer)

    def test_execute_routes_reads_and_uploads_to_separate_clients(self):
        import contextlib
        import io
        from cli_test_runner import main
        from test_outcomes import Storage
        reader, writer = Storage(), Storage()
        reader.upload_file = lambda *args: self.fail('reader used for upload')
        writer.head_bucket = lambda **kwargs: self.fail('writer used for preflight')
        writer.list_objects_v2 = writer.head_bucket
        output = self.root/'generated'
        common = ['--config', str(self.config), '--run-dir', str(self.root/'runs')]
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(main(['generate', *common, '--output-dir', str(output), '--manifests', '--thumbnails']), 0)
            with patch('workbench_lite.cli.create_s3_client', side_effect=[reader, writer]) as factory:
                self.assertEqual(main(['push', *common, '--execute', '--generated-dir', str(output)]), 0)
        self.assertEqual(factory.call_args_list[0].kwargs, {'preflight': True})
        self.assertEqual(factory.call_args_list[1].kwargs, {'full_readback': False})
        self.assertTrue(writer.calls)

    def test_pixel_limit_is_distinct_from_corruption(self):
        from PIL import Image
        from workbench_lite.preflight import validate_content
        with patch.object(Image, 'MAX_IMAGE_PIXELS', 10):
            error = validate_content(self.root/'page.tif', 'master_tiff')
        self.assertIn('exceeds configured pixel limit', error)
        self.assertNotIn('Cannot read/decode', error)

    def test_generation_and_output_readiness_report_pixel_limit(self):
        from PIL import Image
        from workbench_lite.generate import generate_service_jpgs
        from workbench_lite.preflight import output_readiness
        plan = run_check(self.config).upload_plan
        with patch.object(Image, 'MAX_IMAGE_PIXELS', 10):
            readiness = output_readiness(plan, self.root, self.root/'generated', generating=True)
            generated = generate_service_jpgs(plan, self.root, self.root/'generated')
        self.assertTrue(any('exceeds configured pixel limit' in e for e in readiness['errors']))
        self.assertEqual(generated.failed_count, 1)
        self.assertIn('exceeds configured pixel limit', generated.results[0].message)

    def test_corrupt_tiff_blocks_before_processing(self):
        (self.root/'page.tif').write_bytes(b'not tiff')
        report = run_check(self.config)
        self.assertTrue(any('page' in e and 'decode' in e for e in report.validation_errors))

    def test_source_traversal_and_symlink_escape_rejected_even_with_dummy_files(self):
        outside = self.root.parent / (self.root.name + '-outside.tif')
        outside.write_bytes(b'not read')
        self.addCleanup(outside.unlink)
        for value in ['../' + outside.name, 'escape.tif']:
            with self.subTest(value=value):
                if value == 'escape.tif':
                    (self.root/value).symlink_to(outside)
                text = (self.root/'input.csv').read_text().replace('page.tif', value)
                (self.root/'input.csv').write_text(text)
                report = run_check(self.config, allow_dummy_files=True)
                self.assertTrue(report.validation_errors)
                (self.root/'input.csv').write_text(text.replace(value, 'page.tif'))

    def test_empty_ids_unsupported_models_and_duplicate_weights_block(self):
        original = (self.root/'input.csv').read_text()
        for text in [original.replace('Page,page,', 'Page,,'), original.replace(',Page,', ',Unknown,'),
                     original + 'Other,other,Page,parent,1,page.tif\n']:
            (self.root/'input.csv').write_text(text)
            self.assertTrue(run_check(self.config).validation_errors)

    def test_malformed_consumed_settings_and_duplicate_yaml_keys_rejected(self):
        for setting in ['additional_files: bad', 's3_prefix: a/../b', 'batch_id: ..',
                        's3_private_bucket: INVALID_BUCKET', 'input_dir: .\ninput_dir: elsewhere']:
            with self.subTest(setting=setting):
                self.config.write_text('input_csv: input.csv\n' + setting + '\n')
                with self.assertRaises(ConfigError):
                    load_config(self.config)

    def test_output_escape_and_low_disk_are_blocking(self):
        from workbench_lite.preflight import output_readiness
        from types import SimpleNamespace
        output = self.root/'generated'
        output.mkdir()
        (output/'workbench-lite').symlink_to(self.root.parent, target_is_directory=True)
        plan = run_check(self.config).upload_plan
        report = output_readiness(plan, self.root, output, generating=True)
        self.assertTrue(report['errors'])
        (output/'workbench-lite').unlink()
        with patch('workbench_lite.preflight.shutil.disk_usage', return_value=SimpleNamespace(free=0)):
            self.assertTrue(output_readiness(plan, self.root, output, generating=True)['errors'])

    def test_remote_preflight_is_bounded_read_only_and_never_claims_write_access(self):
        from workbench_lite.preflight import storage_readiness
        class Reader:
            def __init__(self): self.calls = []
            def head_bucket(self, **kwargs): self.calls.append(('head', kwargs))
            def list_objects_v2(self, **kwargs):
                self.calls.append(('list', kwargs))
                return {'KeyCount': 0}
        reader = Reader()
        report = storage_readiness(reader, run_check(self.config).upload_plan, 'http://localhost:4566')
        self.assertFalse(report['errors'])
        self.assertEqual(report['write_permission'], 'unverified')
        self.assertEqual(len(reader.calls), 4)
        self.assertTrue(all(kwargs['MaxKeys'] == 1 for operation, kwargs in reader.calls if operation == 'list'))

    def test_remote_denial_is_safe_and_blocking(self):
        from workbench_lite.preflight import storage_readiness
        class Denied:
            def head_bucket(self, **kwargs): raise PermissionError('SECRET')
        report = storage_readiness(Denied(), run_check(self.config).upload_plan, 'http://localhost:4566')
        self.assertTrue(report['errors'])
        self.assertNotIn('SECRET', str(report))

    def test_cli_online_dry_run_only_reads_and_reports_unverified_write_permission(self):
        import contextlib
        import io
        import json
        from cli_test_runner import main
        from test_outcomes import Storage
        class Reader(Storage):
            def __init__(self): super().__init__(); self.reads = []
            def head_bucket(self, **kwargs): self.reads.append(('head', kwargs)); return {}
            def list_objects_v2(self, **kwargs): self.reads.append(('list', kwargs)); return {'KeyCount': 0}
        reader = Reader()
        out = io.StringIO()
        with patch('workbench_lite.cli.create_s3_client', return_value=reader), contextlib.redirect_stdout(out):
            result = main(['push', '--dry-run', '--config', str(self.config), '--run-dir', str(self.root/'runs'), '--format', 'json'])
        self.assertEqual(result, 0)
        self.assertEqual(len(reader.reads), 4)
        self.assertEqual(reader.calls, [])
        report = json.loads(out.getvalue())
        self.assertEqual(report['preflight']['storage']['write_permission'], 'unverified')
        self.assertEqual(report['preflight']['storage']['credential_access'], 'read_access_verified')

    def test_cli_access_denial_blocks_uploads_and_offline_never_creates_client(self):
        import contextlib
        import io
        from cli_test_runner import main
        from test_outcomes import Storage
        output = self.root/'generated'
        common = ['--config', str(self.config), '--run-dir', str(self.root/'runs')]
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(main(['generate', *common, '--output-dir', str(output), '--manifests', '--thumbnails']), 0)
        with patch('workbench_lite.cli.create_s3_client', side_effect=AssertionError('offline created client')), contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(main(['push', *common, '--dry-run', '--offline', '--generated-dir', str(output)]), 0)
        storage = Storage()
        storage.head_bucket = lambda **kwargs: (_ for _ in ()).throw(PermissionError('SECRET'))
        with patch('workbench_lite.cli.create_s3_client', return_value=storage), contextlib.redirect_stdout(io.StringIO()) as out:
            self.assertEqual(main(['push', *common, '--execute', '--generated-dir', str(output)]), 1)
        self.assertEqual(storage.calls, [])
        self.assertNotIn('SECRET', out.getvalue())

    def test_undeclared_hocr_column_cannot_bypass_source_boundary(self):
        csv_path = self.root/'input.csv'
        csv_path.write_text('title,id,field_model,parent_id,field_weight,file,hocr\n'
                            'Parent,parent,Paged Content,,,parent.pdf,\n'
                            'Page,page,Page,parent,1,page.tif,../outside.hocr\n')
        report = run_check(self.config)
        self.assertTrue(any('unsafe' in error and ('page' in error or 'row 3' in error) for error in report.validation_errors))

    def test_unwritable_nested_output_blocks_before_generation(self):
        from workbench_lite.preflight import output_readiness
        plan = run_check(self.config).upload_plan
        output = self.root/'generated'
        target = output/next(e.key for e in plan if e.role == 'service_jpg')
        target.parent.mkdir(parents=True)
        with patch('workbench_lite.preflight.os.access', side_effect=lambda path, mode: Path(path).resolve() != target.parent.resolve()):
            report = output_readiness(plan, self.root, output, generating=True)
        self.assertTrue(report['errors'])
        target.write_bytes(b'existing')
        with patch('workbench_lite.preflight.os.access', side_effect=lambda path, mode: Path(path).resolve() != target.resolve()):
            report = output_readiness(plan, self.root, output, generating=True)
        self.assertTrue(report['errors'])

    def test_duplicate_weights_compare_numeric_values_and_invalid_digits_are_reported(self):
        csv_path = self.root/'input.csv'
        original = csv_path.read_text()
        csv_path.write_text(original + 'Other,other,Page,parent,01,page.tif\n')
        report = run_check(self.config)
        self.assertTrue(any('duplicate field_weight' in e for e in report.validation_errors))
        csv_path.write_text(original.replace('parent,1,', 'parent,²,'))
        self.assertTrue(run_check(self.config).validation_errors)

    def test_non_mapping_yaml_and_missing_parser_fail_closed(self):
        import builtins
        for value in ['[]', 'false', '0']:
            self.config.write_text(value)
            with self.subTest(value=value), self.assertRaises(ConfigError):
                load_config(self.config)
        self.config.write_text('input_dir: .\ninput_csv: input.csv\n')
        original = builtins.__import__
        def no_yaml(name, *args, **kwargs):
            if name == 'yaml':
                raise ImportError('not installed')
            return original(name, *args, **kwargs)
        with patch('builtins.__import__', side_effect=no_yaml), self.assertRaises(ConfigError):
            load_config(self.config)

    def test_page_without_required_image_is_blocked_before_generation(self):
        csv_path = self.root/'input.csv'
        csv_path.write_text(csv_path.read_text().replace('1,page.tif', '1,'))
        report = run_check(self.config)
        self.assertTrue(any('row 3' in error and 'file' in error for error in report.validation_errors))
