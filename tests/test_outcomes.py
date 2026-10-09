"""Regression coverage for command outcomes and storage boundaries."""
import contextlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from PIL import Image

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from workbench_lite.check import run_check
from cli_test_runner import main
from workbench_lite.generate import generate_service_jpgs, generate_thumbnails
from workbench_lite.manifest import generate_manifests
from workbench_lite.models import UploadPlanEntry
from workbench_lite.push import push_upload_plan


class Storage:
    def __init__(self, fail_key=None):
        self.written = {}
        self.calls = []
        self.fail_key = fail_key

    def head_bucket(self, **kwargs):
        return {}

    def list_objects_v2(self, **kwargs):
        return {'KeyCount': 0}

    def object_exists(self, bucket, key):
        return key in self.written

    def verify_file(self, bucket, key, checksum, size_bytes):
        import hashlib
        value = self.written[key]
        from workbench_lite.s3_client import VerificationResult
        observed = hashlib.sha256(value).hexdigest()
        return VerificationResult(len(value) == size_bytes and observed == checksum, observed, len(value), 's3-sha256+readback')

    def upload_file(self, source, bucket, key, checksum):
        self.calls.append(key)
        if key == self.fail_key:
            raise OSError('permission denied token=TOPSECRET https://user:PASS@host/x?X-Amz-Signature=SIGNED')
        self.written[key] = Path(source).read_bytes()


def package(root):
    (root / 'sample.yml').write_text('input_dir: .\ninput_csv: input.csv\n')
    (root / 'input.csv').write_text(
        'title,id,field_model,parent_id,field_weight,file\n'
        'Parent,parent,Paged Content,,,parent.pdf\n'
        'Page,page,Page,parent,1,page.tif\n')
    (root / 'parent.pdf').write_bytes(b'%PDF-1.4\n%%EOF\n')
    Image.new('RGB', (8, 8)).save(root / 'page.tif')
    return root / 'sample.yml'


class OutcomesTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.config = package(self.root)

    def cli(self, *args):
        env = {k:v for k,v in os.environ.items() if not k.startswith('AWS_')}
        env.update(PYTHONPATH=str(ROOT) + os.pathsep + str(ROOT / "tests"), AWS_EC2_METADATA_DISABLED='true', AWS_ENDPOINT_URL='http://127.0.0.1:1', AWS_ACCESS_KEY_ID='test', AWS_SECRET_ACCESS_KEY='test')
        return subprocess.run([sys.executable, '-m', 'cli_test_runner', *map(str,args)],
                              cwd=self.root, env=env, capture_output=True, text=True, timeout=15)

    def plan(self):
        entries=[]
        for oid in ['a', 'b']:
            for role, generated in [('master_tiff',False),('manifest',True)]:
                key=f'{oid}/{role}'
                target=self.root/key
                target.parent.mkdir(exist_ok=True)
                target.write_bytes(key.encode())
                if not (oid == 'b' and role == 'manifest'):
                    entries.append(UploadPlanEntry(role,key,'local',key,True,oid,'',generated,True,None))
        entries.append(UploadPlanEntry('audit','','local','audit/upload-plan.json',False,'','',True,False,None))
        (self.root/'audit').mkdir(exist_ok=True)
        (self.root/'audit/upload-plan.json').write_text('{}')
        return entries

    def test_all_content_and_required_audit_precede_manifests(self):
        store = Storage()
        report = push_upload_plan(self.plan(), self.root, store, False, self.root)
        self.assertEqual(store.calls, ['a/master_tiff', 'b/master_tiff', 'audit/upload-plan.json', 'a/manifest'])
        audit = next(r for r in report.results if r.role == 'audit')
        self.assertEqual(audit.status, 'uploaded')
        self.assertTrue(audit.required)

    def test_failure_retains_acknowledged_uploads_and_stops_before_any_manifest(self):
        plan=self.plan();store=Storage('b/master_tiff');rollback=self.root/'rollback.json'
        try:
            report=push_upload_plan(plan,self.root,store,False,self.root,rollback_path=rollback)
        except OSError as exc:
            self.fail(f'Expected a report preserving prior writes, got {type(exc).__name__}')
        self.assertEqual(store.calls,['a/master_tiff','b/master_tiff'])
        self.assertEqual(report.uploaded_count,1)
        states={r.key:r.status for r in report.results}
        self.assertEqual(states['b/master_tiff'],'failed')
        self.assertEqual(states['a/manifest'],'unattempted')
        self.assertEqual(report.to_dict()['failed_count'],1)
        self.assertEqual(json.loads(rollback.read_text())['count'],1)
        rendered=json.dumps(report.to_dict())
        for secret in ['TOPSECRET','PASS','SIGNED']:
            self.assertNotIn(secret,rendered)
        self.assertIn('unknown',rendered.lower())

    def test_missing_generated_directory_is_failure_not_successful_skip(self):
        report=push_upload_plan(self.plan(),self.root,Storage(),False)
        self.assertGreater(report.missing_generated_count,0)
        self.assertEqual(report.skipped_generated_count,0)

    def test_missing_client_cannot_claim_upload_success(self):
        with self.assertRaises(ValueError):
            push_upload_plan(self.plan(),self.root,dry_run=False,generated_dir=self.root)

    def test_programming_error_is_not_swallowed(self):
        store=Storage()
        with patch.object(store,'upload_file',side_effect=TypeError('bug')):
            with self.assertRaises(TypeError):
                push_upload_plan(self.plan(),self.root,store,False,self.root)

    def test_preflight_missing_source_returns_three_for_all_package_commands(self):
        (self.root/'page.tif').unlink()
        for cmd,extra in [('check',[]),('generate',[]),('push',['--execute'])]:
            with self.subTest(cmd=cmd):
                result=self.cli(cmd,'--config',self.config,'--format','json',*extra)
                self.assertEqual(result.returncode,3,result.stderr)
                self.assertTrue(json.loads(result.stdout)['validation_errors'])
        self.assertFalse((self.root/'generated').exists())

    def test_execute_dummy_mode_rejected_before_reading_config(self):
        result=self.cli('push','--execute','--allow-dummy-files','--config',self.root/'absent.yml')
        self.assertEqual(result.returncode,2,result.stderr)
        self.assertIn('--allow-dummy-files',result.stderr)

    def test_validation_blocks_client_creation(self):
        (self.root/'page.tif').unlink()
        with patch('workbench_lite.cli.create_s3_client',side_effect=AssertionError('client created')):
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(main(['push','--execute','--config',str(self.config)]),3)

    def test_corrupt_tiff_is_caught_during_preflight(self):
        (self.root/'page.tif').write_bytes(b'not an image')
        result=self.cli('generate','--config',self.config,'--format','json')
        self.assertEqual(result.returncode,3,result.stderr)
        self.assertTrue(json.loads(result.stdout)['validation_errors'])

    def test_bad_yaml_is_validation_error_without_secret_source_excerpt(self):
        self.config.write_text('password: [TOPSECRET\n')
        result=self.cli('check','--config',self.config,'--format','json')
        self.assertEqual(result.returncode,3,result.stderr)
        self.assertNotIn('TOPSECRET',result.stdout+result.stderr)
        self.assertTrue(json.loads(result.stdout)['validation_errors'])

    def test_invalid_manifest_is_validation_error(self):
        manifest=self.root/'bad.json'
        for content in ['{','{}','[]']:
            with self.subTest(content=content):
                manifest.write_text(content)
                result=self.cli('verify-cantaloupe','--manifest',manifest,'--format','json')
                self.assertEqual(result.returncode,3,result.stderr)
                self.assertTrue(json.loads(result.stdout)['validation_errors'])

    def test_failed_url_verification_returns_four(self):
        manifest=self.root/'manifest.json'
        manifest.write_text(json.dumps({'sequences':[{'canvases':[{'@id':'page','images':[{'resource':{'service':{'@id':'http://127.0.0.1:1/iiif/image'}}}]}]}]}))
        result=self.cli('verify-cantaloupe','--manifest',manifest,'--format','json')
        self.assertEqual(result.returncode,4,result.stderr)
        self.assertEqual(json.loads(result.stdout)['failed_count'],1)

    def test_push_cli_reports_success_missing_and_failed_uploads(self):
        report=run_check(self.config)
        generated=self.root/'generated'
        service_report=generate_service_jpgs(report.upload_plan,self.root,generated)
        generate_thumbnails(report.upload_plan,self.root,generated)
        generate_manifests(report.objects,report.upload_plan,generated,page_dimensions=service_report.page_dimensions)
        args=['push','--execute','--config',str(self.config),'--generated-dir',str(generated),'--format','json']
        for fail,want in [(None,0),(report.upload_plan[0].key,1)]:
            with self.subTest(fail=fail):
                (generated / '.workbench-run.json').unlink(missing_ok=True)
                store=Storage(fail);out=io.StringIO()
                with patch('workbench_lite.cli.create_s3_client',return_value=store), contextlib.redirect_stdout(out):
                    self.assertEqual(main(args),want)
                data=json.loads(out.getvalue())
                self.assertEqual(data['deferred_count'],0)
                if fail: self.assertEqual(data['uploaded_count'],0)
        (generated / '.workbench-run.json').unlink(missing_ok=True)
        for artifact in generated.rglob('*.jpg'): artifact.unlink()
        with patch('workbench_lite.cli.create_s3_client',return_value=Storage()), contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(main(args),1)

    def test_push_dry_run_checks_explicit_generated_directory(self):
        report=push_upload_plan(self.plan(),self.root,dry_run=True,generated_dir=self.root/'absent')
        self.assertEqual(report.missing_generated_count,2)

    def test_sdk_upload_failures_preserve_report_without_secret_payloads(self):
        from botocore.exceptions import ClientError, EndpointConnectionError
        from boto3.exceptions import S3UploadFailedError
        failures = [ClientError({'Error': {'Code': 'AccessDenied', 'Message': 'TOPSECRET'}}, 'PutObject'),
                    EndpointConnectionError(endpoint_url='https://host?token=TOPSECRET'),
                    S3UploadFailedError('TOPSECRET')]
        for failure in failures:
            with self.subTest(error=type(failure).__name__):
                store = Storage()
                with patch.object(store, 'upload_file', side_effect=failure):
                    report = push_upload_plan(self.plan(), self.root, store, False, self.root)
                self.assertEqual(report.uploaded_count, 0)
                self.assertEqual(report.failed_count, 1)
                self.assertNotIn('TOPSECRET', json.dumps(report.to_dict()))

    def test_no_network_checks_for_invalid_manifest_set(self):
        bad = self.root/'bad.json'
        bad.write_text('{}')
        from workbench_lite.verify import verify_manifest_service_urls
        from workbench_lite.config import ConfigError
        with patch('workbench_lite.verify.urlopen', side_effect=AssertionError('network reached')):
            with self.assertRaises(ConfigError):
                verify_manifest_service_urls([bad])

    def test_generate_programming_error_is_not_a_completed_file_failure(self):
        report=run_check(self.config)
        with patch('workbench_lite.generate._convert_tiff_to_jpeg', side_effect=TypeError('bug')):
            with self.assertRaises(TypeError):
                generate_service_jpgs(report.upload_plan, self.root, self.root/'generated')

    def test_text_report_matches_failed_json_and_shows_deferred_reason(self):
        store=Storage('workbench-lite/sample/originals/parent/parent.pdf')
        plan=run_check(self.config)
        generated=self.root/'generated'
        service_report=generate_service_jpgs(plan.upload_plan,self.root,generated)
        generate_thumbnails(plan.upload_plan,self.root,generated)
        generate_manifests(plan.objects,plan.upload_plan,generated,page_dimensions=service_report.page_dimensions)
        out=io.StringIO()
        with patch('workbench_lite.cli.create_s3_client',return_value=store), contextlib.redirect_stdout(out):
            code=main(['push','--execute','--config',str(self.config),'--generated-dir',str(generated)])
        self.assertEqual(code,1)
        for text in ['failed:', 'unattempted:', 'audit/', 'unknown']:
            self.assertIn(text,out.getvalue())
        self.assertNotIn('TOPSECRET',out.getvalue())

    def test_fatal_runtime_and_signals_are_nonzero_without_exception_secrets(self):
        runner = """import os, signal, sys
import workbench_lite.cli as cli
from cli_test_runner import main as test_main
cli.main = test_main
from pathlib import Path
def client(*args, **kwargs):
    mode = os.environ['FAULT']
    if mode == 'fatal': raise TypeError('TOPSECRET')
    os.kill(os.getpid(), getattr(signal, mode))
cli.create_s3_client = client
sys.exit(cli.entrypoint())
"""
        for fault,want in [('fatal',1),('SIGINT',130),('SIGTERM',143)]:
            (self.root / '.workbench-run.json').unlink(missing_ok=True)
            env={**os.environ,'PYTHONPATH':str(ROOT) + os.pathsep + str(ROOT / 'tests'),'FAULT':fault}
            result=subprocess.run([sys.executable,'-c',runner,'push','--execute','--config',str(self.config),'--generated-dir',str(self.root)],
                                  env=env,capture_output=True,text=True,timeout=10)
            self.assertEqual(result.returncode,want,result.stderr)
            self.assertNotIn('TOPSECRET',result.stdout+result.stderr)

    def test_clean_check_generate_dry_run_and_verify_return_zero(self):
        for cmd,extra in [('check',[]),('generate',['--manifests','--thumbnails']),('push',['--dry-run', '--offline'])]:
            result=self.cli(cmd,'--config',self.config,*extra,'--format','json')
            self.assertEqual(result.returncode,0,result.stderr)
            json.loads(result.stdout)
        manifest=next((self.root/'generated').rglob('manifests/*.json'))
        from unittest.mock import MagicMock
        response=MagicMock()
        response.__enter__.return_value=response
        response.status=200
        response.headers={'Content-Type':'application/json'}
        response.read.return_value=b'{"width":8,"height":8}'
        with patch('workbench_lite.verify.urlopen',return_value=response), contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(main(['verify-cantaloupe','--manifest',str(manifest),'--cantaloupe-base-url','http://localhost']),0)

    def test_rollback_write_failure_preserves_upload_report(self):
        target=self.root/'rollback-directory'
        target.mkdir()
        report=push_upload_plan(self.plan(), self.root, Storage(), False, self.root, rollback_path=target)
        self.assertEqual(report.uploaded_count,4)
        self.assertTrue(report.has_errors)
        self.assertTrue(report.to_dict()['errors'])


    def test_config_contract_errors_block_generated_files(self):
        for contents in ['{}', 'input_dir: .\ninput_csv: input.csv\nmanifest_base_url: 123\n']:
            with self.subTest(contents=contents):
                self.config.write_text(contents)
                result=self.cli('generate','--config',self.config,'--manifests','--format','json')
                self.assertEqual(result.returncode,3,result.stderr)
                self.assertTrue(json.loads(result.stdout)['validation_errors'])
                self.assertFalse((self.root/'generated').exists())

    def test_manifest_directory_collision_blocks_generation_at_readiness(self):
        (self.root/'generated/workbench-lite/sample/manifests/parent.json').mkdir(parents=True)
        result=self.cli('generate','--config',self.config,'--manifests','--format','json')
        self.assertEqual(result.returncode,3,result.stderr)
        report=json.loads(result.stdout)
        self.assertTrue(report['preflight']['local']['errors'])
        self.assertFalse(list((self.root/'generated').rglob('service.jpg')))

    def test_derivative_hash_failure_is_per_file_and_does_not_claim_success(self):
        plan=run_check(self.config).upload_plan
        with patch('workbench_lite.generate._checksum_for_file',side_effect=PermissionError('TOPSECRET')):
            report=generate_service_jpgs(plan,self.root,self.root/'generated')
        self.assertEqual(report.generated_count,0)
        self.assertEqual(report.failed_count,1)
        self.assertNotIn('TOPSECRET',json.dumps(report.to_dict()))

    def test_authorization_header_in_report_field_does_not_expose_bearer(self):
        from dataclasses import replace
        plan=self.plan()
        plan[0]=replace(plan[0],key='Authorization: Bearer TOPSECRET')
        report=push_upload_plan(plan,self.root,dry_run=True)
        from workbench_lite.diagnostics import sanitize
        self.assertIn('TOPSECRET', json.dumps(report.to_dict()))
        self.assertNotIn('TOPSECRET',json.dumps(sanitize(report.to_dict())))


    def test_single_manifest_failure_holds_batch(self):
        store=Storage('a/manifest')
        report=push_upload_plan(self.plan(),self.root,store,False,self.root)
        self.assertEqual(store.calls,['a/master_tiff','b/master_tiff','audit/upload-plan.json','a/manifest'])
        self.assertEqual(report.uploaded_count,3)
        self.assertTrue(report.has_errors)

    def test_source_disappearing_after_plan_is_not_uploaded(self):
        plan=self.plan()
        (self.root/'b/master_tiff').unlink()
        store=Storage()
        report=push_upload_plan(plan,self.root,store,False,self.root)
        self.assertEqual(store.calls,[])
        self.assertEqual(report.missing_source_count,1)
        self.assertTrue(report.has_errors)
        self.assertFalse(any(key.endswith('manifest') for key in store.written))


    def test_readiness_missing_generated_artifact_prevents_all_uploads(self):
        plan = self.plan()
        (self.root / 'a/manifest').unlink()
        store = Storage()
        report = push_upload_plan(plan, self.root, store, False, self.root)
        self.assertEqual(store.calls, [])
        self.assertEqual(report.uploaded_count, 0)
        self.assertEqual(report.missing_generated_count, 1)
        states = {r.key: r.status for r in report.results}
        self.assertEqual(states['a/manifest'], 'missing_generated')
        self.assertEqual(states['a/master_tiff'], 'unattempted')
        self.assertEqual(states['audit/upload-plan.json'], 'unattempted')

    def test_readiness_reports_every_missing_file_without_partial_upload(self):
        plan = self.plan()
        (self.root / 'b/master_tiff').unlink()
        (self.root / 'a/manifest').unlink()
        store = Storage()
        report = push_upload_plan(plan, self.root, store, False, self.root)
        self.assertEqual(store.calls, [])
        self.assertEqual(report.missing_source_count, 1)
        self.assertEqual(report.missing_generated_count, 1)
        self.assertTrue(report.has_errors)

    def test_execute_without_generated_directory_rejected_before_client_creation(self):
        with patch('workbench_lite.cli.create_s3_client', side_effect=AssertionError('client created')):
            with contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit) as caught:
                    main(['push', '--execute', '--config', str(self.config)])
        self.assertEqual(caught.exception.code, 2)
        result = self.cli('push', '--execute', '--config', self.config)
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertIn('--generated-dir', result.stderr)

    def test_source_removed_during_upload_stops_remaining_operations(self):
        plan = self.plan()
        store = Storage()
        upload = store.upload_file
        def remove_after_first(source, bucket, key, checksum):
            upload(source, bucket, key, checksum)
            if key == 'a/master_tiff':
                (self.root / 'b/master_tiff').unlink()
        store.upload_file = remove_after_first
        report = push_upload_plan(plan, self.root, store, False, self.root)
        self.assertEqual(store.calls, ['a/master_tiff'])
        self.assertEqual(report.uploaded_count, 1)
        self.assertEqual(report.missing_source_count, 1)
        self.assertFalse(any(r.status == 'uploaded' for r in report.results if r.role == 'manifest'))

    def test_service_error_codes_are_actionable_without_secret_payload(self):
        from botocore.exceptions import ClientError
        for code in ['AccessDenied', 'NoSuchBucket', 'RequestTimeout']:
            with self.subTest(code=code):
                store = Storage()
                exc = ClientError({'Error': {'Code': code, 'Message': 'TOPSECRET'},
                                   'ResponseMetadata': {'HTTPHeaders': {'Authorization': 'TOPSECRET'}}}, 'PutObject')
                with patch.object(store, 'upload_file', side_effect=exc):
                    report = push_upload_plan(self.plan(), self.root, store, False, self.root)
                message = next(r.message for r in report.results if r.status == 'failed')
                self.assertIn(code, message)
                self.assertNotIn('TOPSECRET', json.dumps(report.to_dict()))
                self.assertIn('unknown', message)


    def test_wrapped_transfer_failure_retains_s3_code(self):
        from botocore.exceptions import ClientError
        from boto3.exceptions import S3UploadFailedError
        store = Storage()
        def fail(*args):
            try:
                raise ClientError({'Error': {'Code': 'AccessDenied', 'Message': 'TOPSECRET'}}, 'PutObject')
            except ClientError as cause:
                raise S3UploadFailedError('TOPSECRET') from cause
        store.upload_file = fail
        report = push_upload_plan(self.plan(), self.root, store, False, self.root)
        self.assertIn('AccessDenied', report.results[0].message)
        self.assertNotIn('TOPSECRET', json.dumps(report.to_dict()))


if __name__=='__main__': unittest.main()
