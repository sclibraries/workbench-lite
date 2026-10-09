import json
import os
import subprocess
import sys
import tempfile
import unittest
from collections import Counter
from pathlib import Path

from PIL import Image


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PACKAGE_ROOT = PROJECT_ROOT
sys.path.insert(0, str(PACKAGE_ROOT))
FIXTURE_ROOT = Path(__file__).resolve().parent / "fixtures"
CONFIG_PATH = FIXTURE_ROOT / "synthetic_package.yml"
CSV_PATH = FIXTURE_ROOT / "synthetic_package.csv"


class FakeS3Client:
    def __init__(self):
        self.uploads = []
        self.written = {}

    def object_exists(self, bucket, key):
        return (bucket, key) in self.written

    def verify_file(self, bucket, key, checksum, size_bytes):
        import hashlib
        value = self.written[bucket, key]
        from workbench_lite.s3_client import VerificationResult
        observed = hashlib.sha256(value).hexdigest()
        return VerificationResult(len(value) == size_bytes and observed == checksum, observed, len(value), 's3-sha256+readback')

    def upload_file(self, source_path, bucket, key, checksum):
        self.uploads.append((Path(source_path), bucket, key))
        self.written[bucket, key] = Path(source_path).read_bytes()


class CheckCommandTest(unittest.TestCase):
    def write_tiny_package(self, package_dir):
        input_dir = package_dir / "files"
        input_dir.mkdir()
        (input_dir / "parent.pdf").write_bytes(b"%PDF-1.4\n%%EOF\n")
        Image.new("RGB", (16, 10), color=(12, 34, 56)).save(input_dir / "page.tif", format="TIFF")
        (input_dir / "page.hocr").write_bytes(b"page hocr")
        config_path = package_dir / "sample.yml"
        csv_path = package_dir / "input.csv"
        config_path.write_text(
            "input_dir: files\n"
            "input_csv: input.csv\n"
            "additional_files:\n"
            "  - hocr: 3507\n",
            encoding="utf-8",
        )
        csv_path.write_text(
            "title,id,field_model,parent_id,field_weight,file,hocr\n"
            "Parent,parent,Paged Content,,,parent.pdf,\n"
            "Page,page,Page,parent,1,page.tif,page.hocr\n",
            encoding="utf-8",
        )
        return config_path, csv_path, input_dir

    def run_check_with_csv(self, csv_text, config_text=None, allow_dummy_files=True):
        from workbench_lite.check import run_check

        with tempfile.TemporaryDirectory() as temp_dir:
            package_dir = Path(temp_dir)
            config_path = package_dir / "package.yml"
            csv_path = package_dir / "input.csv"
            config_path.write_text(
                config_text
                or "input_dir: .\ninput_csv: input.csv\nadditional_files:\n  - hocr: 3507\n",
                encoding="utf-8",
            )
            csv_path.write_text(csv_text, encoding="utf-8")

            return run_check(
                config_path=config_path,
                input_csv_override=csv_path,
                allow_dummy_files=allow_dummy_files,
            )

    def test_run_check_reports_duplicate_csv_headers(self):
        report = self.run_check_with_csv(
            "title,id,id,field_model,parent_id,field_weight,file,hocr\n"
            "Parent,parent,duplicate-parent,Paged Content,,,parent.pdf,\n"
        )

        self.assertIn("Duplicate CSV headers: id", report.validation_errors)

    def test_run_check_reports_csv_row_column_mismatch(self):
        report = self.run_check_with_csv(
            "title,id,field_model,parent_id,field_weight,file,hocr\n"
            "Parent,parent,Paged Content,,,parent.pdf,,extra-value\n"
        )

        self.assertIn(
            "CSV row 2 has 8 values but header has 7.",
            report.validation_errors,
        )

    def test_run_check_reports_unsupported_file_extensions(self):
        report = self.run_check_with_csv(
            "title,id,field_model,parent_id,field_weight,file,hocr\n"
            "Bad parent,parent,Paged Content,,,parent.tif,\n"
            "Bad page,page,Page,parent,1,page.pdf,page.txt\n"
        )

        self.assertIn(
            "Parent row parent column file uses unsupported extension .tif; expected .pdf.",
            report.validation_errors,
        )
        self.assertIn(
            "Page row page column file uses unsupported extension .pdf; expected .tif or .tiff.",
            report.validation_errors,
        )
        self.assertIn(
            "Page row page column hocr uses unsupported extension .txt; expected .hocr, .html, or .htm.",
            report.validation_errors,
        )

    def test_run_check_reports_upload_plan_collisions(self):
        report = self.run_check_with_csv(
            "title,id,field_model,parent_id,field_weight,file,hocr\n"
            "Parent,obj one,Paged Content,,,A one.pdf,\n"
            "Parent,obj/one,Paged Content,,,A_one.pdf,\n",
            allow_dummy_files=True,
        )

        self.assertEqual(len(report.validation_errors), 2)
        self.assertIn(
            "Upload plan collision for bucket 'compass-prod-i2-files', key 'workbench-lite/package/originals/obj_one/A_one.pdf': "
            "role=pdf, object_id=obj/one, page_id=; collides with role=pdf, object_id=obj one, page_id=.",
            report.validation_errors,
        )
        self.assertIn(
            "Upload plan collision for bucket 'islandora-derivatives', key 'workbench-lite/package/manifests/obj_one.json': "
            "role=manifest, object_id=obj/one, page_id=; collides with role=manifest, object_id=obj one, page_id=.",
            report.validation_errors,
        )

    def test_run_check_reports_allowed_missing_files_as_warnings(self):
        report = self.run_check_with_csv(
            "title,id,field_model,parent_id,field_weight,file,hocr\n"
            "Parent,parent,Paged Content,,,missing-parent.pdf,\n"
            "Page,page,Page,parent,1,missing-page.tif,missing-page.hocr\n",
            config_text=(
                "input_dir: .\n"
                "input_csv: input.csv\n"
                "allow_missing_files: true\n"
                "additional_files:\n"
                "  - hocr: 3507\n"
            ),
            allow_dummy_files=False,
        )

        self.assertEqual(report.validation_errors, [])
        self.assertIn(
            "File not found for row parent column file: missing-parent.pdf",
            report.validation_warnings,
        )
        self.assertIn(
            "File not found for row page column hocr: missing-page.hocr",
            report.validation_warnings,
        )

    def test_run_check_summarizes_synthetic_package(self):
        from workbench_lite.check import run_check

        report = run_check(
            config_path=CONFIG_PATH,
            input_csv_override=CSV_PATH,
            allow_dummy_files=True,
        )

        self.assertEqual(report.row_count, 5)
        self.assertEqual(report.column_count, 31)
        self.assertEqual(report.parent_count, 2)
        self.assertEqual(report.page_count, 3)
        self.assertEqual(report.unique_child_parent_count, 2)
        self.assertEqual(report.bad_child_weight_count, 0)
        self.assertEqual(report.validation_errors, [])
        self.assertEqual(report.file_counts["pdf"], 2)
        self.assertEqual(report.file_counts["tif"], 3)
        self.assertEqual(report.file_counts["hocr"], 3)

    def test_run_check_builds_ordered_parent_page_model(self):
        from workbench_lite.check import run_check

        report = run_check(
            config_path=CONFIG_PATH,
            input_csv_override=CSV_PATH,
            allow_dummy_files=True,
        )

        first_object = report.objects[0]
        self.assertEqual(first_object.object_id, "collection-north")
        self.assertEqual(first_object.title, "North Star Collection")
        self.assertEqual(first_object.pdf_path, "originals/north-star.pdf")
        self.assertEqual(len(first_object.pages), 2)
        self.assertEqual(first_object.pages[0].page_id, "north-page-001")
        self.assertEqual(first_object.pages[0].weight, 1)
        self.assertEqual(first_object.pages[-1].weight, 2)

    def test_run_check_builds_deterministic_upload_plan_for_synthetic_package(self):
        from workbench_lite.check import run_check

        report = run_check(
            config_path=CONFIG_PATH,
            input_csv_override=CSV_PATH,
            allow_dummy_files=True,
            run_id="test-run-synthetic",
        )

        role_counts = Counter(entry.role for entry in report.upload_plan)
        self.assertEqual(role_counts["pdf"], 2)
        self.assertEqual(role_counts["master_tiff"], 3)
        self.assertEqual(role_counts["hocr"], 3)
        self.assertEqual(role_counts["service_jpg"], 3)
        self.assertEqual(role_counts["thumbnail"], 3)
        self.assertEqual(role_counts["manifest"], 2)
        self.assertEqual(role_counts["audit"], 1)

        pdf_entry = next(entry for entry in report.upload_plan if entry.role == "pdf")
        self.assertEqual(pdf_entry.bucket, "compass-prod-i2-files")
        self.assertEqual(
            pdf_entry.key,
            "workbench-lite/synthetic_package/originals/"
            "collection-north/north-star.pdf",
        )
        self.assertFalse(pdf_entry.public)
        self.assertFalse(pdf_entry.generated)

        first_page_id = "north-page-001"
        first_page_tiff = next(
            entry
            for entry in report.upload_plan
            if entry.role == "master_tiff" and entry.page_id == first_page_id
        )
        self.assertEqual(
            first_page_tiff.key,
            "workbench-lite/synthetic_package/originals/"
            "collection-north/pages/north-page-001.tif",
        )

        first_page_hocr = next(
            entry
            for entry in report.upload_plan
            if entry.role == "hocr" and entry.page_id == first_page_id
        )
        self.assertEqual(
            first_page_hocr.key,
            "workbench-lite/synthetic_package/ocr/"
            "collection-north/north-page-001.html",
        )
        self.assertTrue(first_page_hocr.public)

        first_service_jpg = next(
            entry
            for entry in report.upload_plan
            if entry.role == "service_jpg" and entry.page_id == first_page_id
        )
        self.assertEqual(
            first_service_jpg.key,
            "workbench-lite/synthetic_package/derivatives/"
            "collection-north/pages/north-page-001/service.jpg",
        )
        self.assertTrue(first_service_jpg.generated)

        manifest_entry = next(entry for entry in report.upload_plan if entry.role == "manifest")
        self.assertEqual(
            manifest_entry.key,
            "workbench-lite/synthetic_package/manifests/collection-north.json",
        )

        audit_entry = next(entry for entry in report.upload_plan if entry.role == "audit")
        self.assertEqual(
            audit_entry.key,
            "workbench-lite/synthetic_package/audit/test-run-synthetic/upload-plan.json",
        )

    def test_cli_check_outputs_json_summary(self):
        env = os.environ.copy()
        env["PYTHONPATH"] = str(PACKAGE_ROOT) + os.pathsep + str(PACKAGE_ROOT / "tests")

        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "cli_test_runner",
                "check",
                "--config",
                str(CONFIG_PATH),
                "--input-csv",
                str(CSV_PATH),
                "--allow-dummy-files",
                "--format",
                "json",
            ],
            cwd=PROJECT_ROOT,
            env=env,
            text=True,
            capture_output=True,
            check=False,
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        payload = json.loads(result.stdout)
        self.assertEqual(payload["row_count"], 5)
        self.assertEqual(payload["parent_count"], 2)
        self.assertEqual(payload["page_count"], 3)
        self.assertEqual(payload["validation_errors"], [])

    def test_cli_push_dry_run_outputs_json_summary(self):
        env = os.environ.copy()
        env["PYTHONPATH"] = str(PACKAGE_ROOT) + os.pathsep + str(PACKAGE_ROOT / "tests")

        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "cli_test_runner",
                "push",
                "--config",
                str(CONFIG_PATH),
                "--input-csv",
                str(CSV_PATH),
                "--allow-dummy-files",
                "--dry-run", "--offline",
                "--endpoint-url",
                "http://localhost:4566",
                "--format",
                "json",
            ],
            cwd=PROJECT_ROOT,
            env=env,
            text=True,
            capture_output=True,
            check=False,
        )

        self.assertEqual(result.returncode, 1, result.stderr)
        payload = json.loads(result.stdout)
        self.assertTrue(payload["dry_run"])
        self.assertEqual(payload["endpoint_url"], "http://localhost:4566")
        self.assertEqual(payload["planned_count"], 17)
        self.assertEqual(payload["source_backed_count"], 8)
        self.assertEqual(payload["generated_count"], 9)
        self.assertEqual(payload["missing_source_count"], 8)
        self.assertEqual(payload["uploaded_count"], 0)
        self.assertEqual(payload["role_counts"]["pdf"], 2)
        self.assertEqual(payload["role_counts"]["master_tiff"], 3)
        self.assertEqual(payload["role_counts"]["manifest"], 2)

    def test_cli_generate_outputs_json_summary(self):
        env = os.environ.copy()
        env["PYTHONPATH"] = str(PACKAGE_ROOT) + os.pathsep + str(PACKAGE_ROOT / "tests")

        with tempfile.TemporaryDirectory() as temp_dir:
            package_dir = Path(temp_dir)
            config_path, csv_path, input_dir = self.write_tiny_package(package_dir)

            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "cli_test_runner",
                    "generate",
                    "--config",
                    str(config_path),
                    "--input-csv",
                    str(csv_path),
                    "--allow-dummy-files",
                    "--output-dir",
                    str(package_dir / "generated"),
                    "--format",
                    "json",
                ],
                cwd=str(PACKAGE_ROOT),
                env=env,
                text=True,
                capture_output=True,
                check=False,
            )

        self.assertEqual(result.returncode, 0, result.stderr)
        payload = json.loads(result.stdout)
        self.assertEqual(payload["target_count"], 1)
        self.assertEqual(payload["generated_count"], 1)
        self.assertEqual(payload["failed_count"], 0)
        self.assertEqual(payload["missing_source_count"], 0)

    def test_cli_generate_outputs_json_summary_with_thumbnails(self):
        env = os.environ.copy()
        env["PYTHONPATH"] = str(PACKAGE_ROOT) + os.pathsep + str(PACKAGE_ROOT / "tests")

        with tempfile.TemporaryDirectory() as temp_dir:
            package_dir = Path(temp_dir)
            config_path, csv_path, input_dir = self.write_tiny_package(package_dir)

            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "cli_test_runner",
                    "generate",
                    "--config",
                    str(config_path),
                    "--input-csv",
                    str(csv_path),
                    "--output-dir",
                    str(package_dir / "generated"),
                    "--thumbnails",
                    "--format",
                    "json",
                ],
                cwd=str(PACKAGE_ROOT),
                env=env,
                text=True,
                capture_output=True,
                check=False,
            )

        self.assertEqual(result.returncode, 0, result.stderr)
        payload = json.loads(result.stdout)
        self.assertEqual(payload["target_count"], 2)
        self.assertEqual(payload["generated_count"], 2)
        self.assertEqual(payload["failed_count"], 0)
        self.assertEqual(payload["missing_source_count"], 0)
        roles = {entry["role"] for entry in payload["results"]}
        self.assertIn("service_jpg", roles)
        self.assertIn("thumbnail", roles)

    def test_cli_push_requires_explicit_mode(self):
        env = os.environ.copy()
        env["PYTHONPATH"] = str(PACKAGE_ROOT) + os.pathsep + str(PACKAGE_ROOT / "tests")

        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "cli_test_runner",
                "push",
                "--config",
                str(CONFIG_PATH),
                "--input-csv",
                str(CSV_PATH),
                "--allow-dummy-files",
                "--format",
                "json",
            ],
            cwd=PROJECT_ROOT,
            env=env,
            text=True,
            capture_output=True,
            check=False,
        )

        self.assertEqual(result.returncode, 2)
        self.assertIn("push currently requires --dry-run", result.stderr)

    def test_create_s3_client_uses_boto3_with_localstack_endpoint(self):
        import types

        from workbench_lite.s3_client import create_s3_client

        calls = []
        fake_boto3 = types.SimpleNamespace()

        def client(service_name, **kwargs):
            calls.append((service_name, kwargs))

            class FakeBotoClient:
                def __init__(self):
                    self.uploads = []

                def upload_file(self, source_path, bucket, key, checksum):
                    self.uploads.append((source_path, bucket, key))

            return FakeBotoClient()

        fake_boto3.client = client
        original_boto3 = sys.modules.get("boto3")
        sys.modules["boto3"] = fake_boto3
        try:
            s3_client = create_s3_client(
                endpoint_url="http://localhost:4566",
                region="us-east-1",
            )
        finally:
            if original_boto3 is None:
                sys.modules.pop("boto3", None)
            else:
                sys.modules["boto3"] = original_boto3

        self.assertNotIn("config", calls[0][1])
        self.assertEqual(calls, [("s3", {"endpoint_url": "http://localhost:4566", "region_name": "us-east-1"})])
        self.assertEqual(s3_client.client.uploads, [])

    def test_create_s3_client_supports_profile(self):
        import types

        from workbench_lite.s3_client import create_s3_client

        session_calls = []
        client_calls = []
        fake_boto3 = types.SimpleNamespace()

        class FakeBotoClient:
            def __init__(self):
                self.uploads = []

            def upload_file(self, source_path, bucket, key, checksum):
                self.uploads.append((source_path, bucket, key))

        class FakeSession:
            def __init__(self, profile_name=None):
                session_calls.append(profile_name)

            def client(self, service_name, **kwargs):
                client_calls.append((service_name, kwargs))
                return FakeBotoClient()

        fake_boto3.Session = FakeSession
        fake_boto3.client = lambda service_name, **kwargs: (_ for _ in ()).throw(
            RuntimeError("client() should not be used when profile is set")
        )

        original_boto3 = sys.modules.get("boto3")
        sys.modules["boto3"] = fake_boto3
        try:
            s3_client = create_s3_client(
                endpoint_url="http://localhost:4566",
                region="us-east-1",
                profile="staff-profile",
            )
        finally:
            if original_boto3 is None:
                sys.modules.pop("boto3", None)
            else:
                sys.modules["boto3"] = original_boto3

        self.assertEqual(session_calls, ["staff-profile"])
        self.assertNotIn("config", client_calls[0][1])
        self.assertEqual(
            client_calls,
            [("s3", {"endpoint_url": "http://localhost:4566", "region_name": "us-east-1"})],
        )
        self.assertEqual(s3_client.client.uploads, [])

    def test_cli_execute_requires_generated_directory(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            config, csv_path, _ = self.write_tiny_package(Path(temp_dir))
            env = os.environ.copy()
            env["PYTHONPATH"] = str(PACKAGE_ROOT) + os.pathsep + str(PACKAGE_ROOT / "tests")
            result = subprocess.run(
                [sys.executable, "-m", "cli_test_runner", "push", "--execute",
                 "--config", str(config), "--input-csv", str(csv_path)],
                env=env, capture_output=True, text=True, check=False,
            )
        self.assertEqual(result.returncode, 2)
        self.assertIn("--generated-dir", result.stderr)

    def test_cli_push_execute_uploads_generated_artifacts_with_fake_boto3(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            package_dir = Path(temp_dir) / "package"
            package_dir.mkdir()
            config_path, csv_path, _input_dir = self.write_tiny_package(package_dir)
            generated_dir = package_dir / "generated"
            generated_paths = [
                generated_dir / "workbench-lite/sample/derivatives/parent/pages/page/service.jpg",
                generated_dir / "workbench-lite/sample/derivatives/parent/pages/page/thumbnail.jpg",
                generated_dir / "workbench-lite/sample/manifests/parent.json",
                generated_dir / "workbench-lite/sample/audit/upload-plan.json",
            ]
            for path in generated_paths:
                path.parent.mkdir(parents=True, exist_ok=True)
                if path.suffix == '.jpg':
                    Image.new('RGB', (8, 8)).save(path)
                else:
                    path.write_text('{}')

            fake_module_dir = Path(temp_dir) / "fake_boto3"
            fake_module_dir.mkdir()
            upload_log = Path(temp_dir) / "uploads.jsonl"
            (fake_module_dir / "boto3.py").write_text(
                r"""import io, json, os, hashlib, base64
from types import SimpleNamespace
from botocore.exceptions import ClientError
def client(service_name, **kwargs):
    return FakeClient()
class FakeClient:
    def __init__(self):
        self.written = {}
        self.meta = SimpleNamespace(service_model=SimpleNamespace(operation_model=lambda name: SimpleNamespace(input_shape=SimpleNamespace(members={'IfNoneMatch': None}))))
    def head_bucket(self, **kwargs): return {}
    def list_objects_v2(self, **kwargs): return {'KeyCount': 0}
    def head_object(self, Bucket, Key, **kwargs):
        if (Bucket, Key) not in self.written:
            raise ClientError({'Error': {'Code': '404'}}, 'HeadObject')
        value = self.written[Bucket, Key]
        return {'ContentLength': len(value), 'ChecksumSHA256': base64.b64encode(hashlib.sha256(value).digest()).decode()}
    def get_object(self, Bucket, Key): return {'Body': io.BytesIO(self.written[Bucket, Key])}
    def put_object(self, Bucket, Key, Body, IfNoneMatch, ChecksumAlgorithm, ChecksumSHA256):
        assert IfNoneMatch == '*'
        self.written[Bucket, Key] = Body.read()
        with open(os.environ['FAKE_BOTO3_UPLOAD_LOG'], 'a') as handle:
            handle.write(json.dumps({'source_path': Body.name, 'bucket': Bucket, 'key': Key}) + '\n')
""", encoding="utf-8")

            env = os.environ.copy()
            env["PYTHONPATH"] = f"{PACKAGE_ROOT}{os.pathsep}{PACKAGE_ROOT / 'tests'}{os.pathsep}{fake_module_dir}"
            env["FAKE_BOTO3_UPLOAD_LOG"] = str(upload_log)

            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "cli_test_runner",
                    "push",
                    "--config",
                    str(config_path),
                    "--input-csv",
                    str(csv_path),
                    "--execute",
                    "--generated-dir",
                    str(generated_dir),
                    "--endpoint-url",
                    "http://localhost:4566",
                    "--region",
                    "us-east-1",
                    "--format",
                    "json",
                ],
                cwd=PROJECT_ROOT,
                env=env,
                text=True,
                capture_output=True,
                check=False,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            payload = json.loads(result.stdout)
            upload_rows = [json.loads(line) for line in upload_log.read_text(encoding="utf-8").splitlines()]

        self.assertEqual(payload["planned_count"], 7)
        self.assertEqual(payload["uploaded_count"], 7)
        self.assertEqual(payload["missing_generated_count"], 0)
        self.assertEqual(len(upload_rows), 7)
        self.assertIn(
            "workbench-lite/sample/manifests/parent.json",
            [row["key"] for row in upload_rows],
        )

    def test_push_uploads_existing_source_entries_with_s3_client(self):
        from workbench_lite.check import run_check
        from workbench_lite.push import push_upload_plan

        with tempfile.TemporaryDirectory() as temp_dir:
            package_dir = Path(temp_dir)
            config_path, csv_path, input_dir = self.write_tiny_package(package_dir)
            check_report = run_check(config_path=config_path, input_csv_override=csv_path)
            fake_client = FakeS3Client()

            push_report = push_upload_plan(
                [entry for entry in check_report.upload_plan if not entry.generated],
                input_dir=input_dir,
                s3_client=fake_client,
                dry_run=False,
            )

        self.assertEqual(check_report.validation_errors, [])
        self.assertEqual(push_report.planned_count, 3)
        self.assertEqual(push_report.uploaded_count, 3)
        self.assertEqual(push_report.skipped_generated_count, 0)
        self.assertEqual(push_report.missing_source_count, 0)
        self.assertEqual(len(fake_client.uploads), 3)
        uploaded_keys = [upload[2] for upload in fake_client.uploads]
        self.assertIn("workbench-lite/sample/originals/parent/parent.pdf", uploaded_keys)
        self.assertIn("workbench-lite/sample/originals/parent/pages/page.tif", uploaded_keys)
        self.assertIn("workbench-lite/sample/ocr/parent/page.hocr", uploaded_keys)
        self.assertTrue(all(result.status == "uploaded" for result in push_report.results[:3]))

    def test_push_writes_uploaded_entries_to_rollback_file(self):
        from workbench_lite.check import run_check
        from workbench_lite.push import push_upload_plan

        with tempfile.TemporaryDirectory() as temp_dir:
            package_dir = Path(temp_dir)
            config_path, csv_path, input_dir = self.write_tiny_package(package_dir)
            check_report = run_check(config_path=config_path, input_csv_override=csv_path)
            rollback_file = Path(temp_dir) / "rollback.json"
            fake_client = FakeS3Client()

            push_report = push_upload_plan(
                [entry for entry in check_report.upload_plan if not entry.generated],
                input_dir=input_dir,
                s3_client=fake_client,
                dry_run=False,
                rollback_path=rollback_file,
            )

            self.assertTrue(rollback_file.exists())
            payload = json.loads(rollback_file.read_text(encoding="utf-8"))
            self.assertEqual(payload["count"], 3)
            written = payload["written"]
            self.assertEqual(len(written), 3)
            self.assertEqual(written[0]["status"], "uploaded")
            self.assertEqual(payload["written"][1]["role"], "master_tiff")
            self.assertEqual(payload["written"][2]["role"], "hocr")
            self.assertIn("size_bytes", payload["written"][0])
            self.assertIn("size_bytes", payload["written"][1])
            self.assertIsInstance(payload["written"][0]["size_bytes"], int)
            self.assertIsInstance(payload["written"][1]["size_bytes"], int)
            self.assertIn("checksum", payload["written"][0])
            self.assertIn("checksum", payload["written"][1])
            self.assertIn("checksum", payload["written"][2])
            self.assertEqual(push_report.uploaded_count, 3)

    def test_push_uploads_existing_generated_artifacts_from_generated_dir(self):
        from workbench_lite.check import run_check
        from workbench_lite.generate import generate_service_jpgs, generate_thumbnails
        from workbench_lite.manifest import generate_manifests
        from workbench_lite.push import push_upload_plan

        with tempfile.TemporaryDirectory() as temp_dir:
            package_dir = Path(temp_dir)
            config_path, csv_path, input_dir = self.write_tiny_package(package_dir)
            check_report = run_check(config_path=config_path, input_csv_override=csv_path)
            generated_dir = package_dir / "generated"
            service_report = generate_service_jpgs(
                upload_plan=check_report.upload_plan,
                input_dir=input_dir,
                output_dir=generated_dir,
                max_long_side=8,
                quality=75,
            )
            generate_thumbnails(
                upload_plan=check_report.upload_plan,
                input_dir=input_dir,
                output_dir=generated_dir,
                max_long_side=8,
                quality=75,
            )
            generate_manifests(
                objects=check_report.objects,
                upload_plan=check_report.upload_plan,
                output_dir=generated_dir,
                cantaloupe_base_url="http://localhost:8080/iiif/2",
                page_dimensions=service_report.page_dimensions,
            )
            audit_path = generated_dir / next(e.key for e in check_report.upload_plan if e.role == 'audit')
            audit_path.parent.mkdir(parents=True)
            audit_path.write_text(
                "{\"ok\": true}",
                encoding="utf-8",
            )
            fake_client = FakeS3Client()

            push_report = push_upload_plan(
                check_report.upload_plan,
                input_dir=input_dir,
                generated_dir=generated_dir,
                s3_client=fake_client,
                dry_run=False,
            )

        self.assertEqual(push_report.planned_count, 7)
        self.assertEqual(push_report.uploaded_count, 7)
        self.assertEqual(push_report.skipped_generated_count, 0)
        self.assertEqual(push_report.missing_generated_count, 0)
        self.assertEqual(len(fake_client.uploads), 7)
        uploaded_keys = [upload[2] for upload in fake_client.uploads]
        self.assertIn("workbench-lite/sample/derivatives/parent/pages/page/service.jpg", uploaded_keys)
        self.assertIn("workbench-lite/sample/derivatives/parent/pages/page/thumbnail.jpg", uploaded_keys)
        self.assertIn("workbench-lite/sample/manifests/parent.json", uploaded_keys)
        self.assertIn(next(e.key for e in check_report.upload_plan if e.role == "audit"), uploaded_keys)
        generated_results = [result for result in push_report.results if result.role in {"service_jpg", "thumbnail", "manifest"}]
        self.assertTrue(all(result.status == "uploaded" for result in generated_results))
        self.assertTrue(all(isinstance(result.checksum, str) for result in generated_results))
        self.assertTrue(all(isinstance(result.size_bytes, int) for result in generated_results))

    def test_push_reports_missing_generated_artifacts_in_execute_mode(self):
        from workbench_lite.check import run_check
        from workbench_lite.push import push_upload_plan

        with tempfile.TemporaryDirectory() as temp_dir:
            package_dir = Path(temp_dir)
            config_path, csv_path, input_dir = self.write_tiny_package(package_dir)
            check_report = run_check(config_path=config_path, input_csv_override=csv_path)
            fake_client = FakeS3Client()

            push_report = push_upload_plan(
                check_report.upload_plan,
                input_dir=input_dir,
                generated_dir=package_dir / "generated",
                s3_client=fake_client,
                dry_run=False,
            )

        self.assertEqual(push_report.uploaded_count, 0)
        self.assertEqual(push_report.missing_generated_count, 4)
        self.assertEqual(push_report.skipped_generated_count, 0)
        self.assertEqual(len(fake_client.uploads), 0)
        missing_generated = [result for result in push_report.results if result.status == "missing_generated"]
        self.assertEqual(
            sorted(result.role for result in missing_generated),
            ["audit", "manifest", "service_jpg", "thumbnail"],
        )

    def test_generate_service_jpgs_creates_outputs(self):
        from workbench_lite.check import run_check
        from workbench_lite.generate import generate_service_jpgs

        with tempfile.TemporaryDirectory() as temp_dir:
            package_dir = Path(temp_dir)
            config_path, csv_path, input_dir = self.write_tiny_package(package_dir)
            check_report = run_check(config_path=config_path, input_csv_override=csv_path)
            output_dir = package_dir / "generated"

            generate_report = generate_service_jpgs(
                upload_plan=check_report.upload_plan,
                input_dir=input_dir,
                output_dir=output_dir,
                max_long_side=8,
                quality=75,
            )
            self.assertEqual(generate_report.target_count, 1)
            self.assertEqual(generate_report.generated_count, 1)
            self.assertEqual(generate_report.failed_count, 0)
            self.assertEqual(generate_report.missing_source_count, 0)
            artifact = generate_report.results[0]
            self.assertEqual(artifact.role, "service_jpg")
            self.assertEqual(artifact.status, "generated")
            self.assertTrue(Path(artifact.output_path).exists())
            self.assertGreater(artifact.size_bytes or 0, 0)
            self.assertIsInstance(artifact.checksum, str)
        self.assertEqual(
            getattr(generate_report, "page_dimensions", {}),
            {("parent", "page"): (16, 10)},
        )
        self.assertNotIn("page_dimensions", generate_report.to_dict())

    def test_generate_thumbnails_creates_outputs(self):
        from workbench_lite.check import run_check
        from workbench_lite.generate import generate_thumbnails

        with tempfile.TemporaryDirectory() as temp_dir:
            package_dir = Path(temp_dir)
            config_path, csv_path, input_dir = self.write_tiny_package(package_dir)
            check_report = run_check(config_path=config_path, input_csv_override=csv_path)
            output_dir = package_dir / "generated"

            generate_report = generate_thumbnails(
                upload_plan=check_report.upload_plan,
                input_dir=input_dir,
                output_dir=output_dir,
                max_long_side=64,
                quality=70,
            )

            self.assertEqual(generate_report.target_count, 1)
            self.assertEqual(generate_report.generated_count, 1)
            self.assertEqual(generate_report.failed_count, 0)
            self.assertEqual(generate_report.missing_source_count, 0)
            artifact = generate_report.results[0]
            self.assertEqual(artifact.role, "thumbnail")
            self.assertEqual(artifact.status, "generated")
            self.assertTrue(Path(artifact.output_path).exists())

    def test_generate_manifests_fails_if_service_image_missing(self):
        from workbench_lite.check import run_check
        from workbench_lite.manifest import generate_manifests

        with tempfile.TemporaryDirectory() as temp_dir:
            package_dir = Path(temp_dir)
            config_path, csv_path, input_dir = self.write_tiny_package(package_dir)
            check_report = run_check(config_path=config_path, input_csv_override=csv_path)

            output_dir = package_dir / "generated"
            manifest_report = generate_manifests(
                objects=check_report.objects,
                upload_plan=check_report.upload_plan,
                output_dir=output_dir,
                cantaloupe_base_url="http://localhost:8080/iiif/2",
            )

        self.assertEqual(manifest_report.target_count, 1)
        self.assertEqual(manifest_report.generated_count, 0)
        self.assertEqual(manifest_report.failed_count, 1)
        self.assertEqual(manifest_report.missing_source_count, 1)
        self.assertEqual(manifest_report.results[0].status, "failed")

    def test_generate_manifests_fails_when_source_dimensions_are_unavailable(self):
        from workbench_lite.check import run_check
        from workbench_lite.generate import generate_service_jpgs
        from workbench_lite.manifest import generate_manifests

        with tempfile.TemporaryDirectory() as temp_dir:
            package_dir = Path(temp_dir)
            config_path, csv_path, input_dir = self.write_tiny_package(package_dir)
            check_report = run_check(config_path=config_path, input_csv_override=csv_path)
            output_dir = package_dir / "generated"
            generate_service_jpgs(
                upload_plan=check_report.upload_plan,
                input_dir=input_dir,
                output_dir=output_dir,
                max_long_side=8,
                quality=75,
            )

            manifest_report = generate_manifests(
                objects=check_report.objects,
                upload_plan=check_report.upload_plan,
                output_dir=output_dir,
                cantaloupe_base_url="http://localhost:8080/iiif/2",
            )
            manifest_path = Path(manifest_report.results[0].output_path)

        self.assertEqual(manifest_report.generated_count, 0)
        self.assertEqual(manifest_report.failed_count, 1)
        self.assertIn(
            "source pixel dimensions are unavailable",
            manifest_report.results[0].message.lower(),
        )
        self.assertFalse(manifest_path.exists())

    def test_generate_manifests_creates_outputs(self):
        from workbench_lite.check import run_check
        from workbench_lite.generate import generate_service_jpgs
        from workbench_lite.manifest import generate_manifests

        with tempfile.TemporaryDirectory() as temp_dir:
            package_dir = Path(temp_dir)
            config_path, csv_path, input_dir = self.write_tiny_package(package_dir)
            check_report = run_check(config_path=config_path, input_csv_override=csv_path)
            output_dir = package_dir / "generated"

            service_report = generate_service_jpgs(
                upload_plan=check_report.upload_plan,
                input_dir=input_dir,
                output_dir=output_dir,
                max_long_side=8,
                quality=75,
            )

            manifest_report = generate_manifests(
                objects=check_report.objects,
                upload_plan=check_report.upload_plan,
                output_dir=output_dir,
                cantaloupe_base_url="http://localhost:8080/iiif/2",
                page_dimensions=service_report.page_dimensions,
            )
            payload = json.loads(Path(manifest_report.results[0].output_path).read_text(encoding="utf-8"))

        self.assertEqual(manifest_report.target_count, 1)
        self.assertEqual(manifest_report.generated_count, 1)
        self.assertEqual(manifest_report.failed_count, 0)
        self.assertEqual(manifest_report.missing_source_count, 0)
        self.assertEqual(payload["@context"], "http://iiif.io/api/presentation/2/context.json")
        self.assertEqual(len(payload["sequences"][0]["canvases"]), 1)
        canvas = payload["sequences"][0]["canvases"][0]
        resource = canvas["images"][0]["resource"]
        self.assertEqual((canvas["width"], canvas["height"]), (16, 10))
        self.assertNotIn("width", resource)
        self.assertNotIn("height", resource)
        self.assertIs(type(canvas["width"]), int)
        self.assertIs(type(canvas["height"]), int)
        self.assertIn("service", resource)
        self.assertEqual(resource["service"]["@context"], "http://iiif.io/api/image/2/context.json")
        self.assertTrue(resource["service"]["@id"].startswith("http://localhost:8080/iiif/2/"))

    def test_generate_manifests_bounds_canvas_image_requests(self):
        from workbench_lite.check import run_check
        from workbench_lite.generate import generate_service_jpgs
        from workbench_lite.manifest import generate_manifests

        with tempfile.TemporaryDirectory() as temp_dir:
            package_dir = Path(temp_dir)
            config_path, csv_path, input_dir = self.write_tiny_package(package_dir)
            check_report = run_check(config_path=config_path, input_csv_override=csv_path)
            output_dir = package_dir / "generated"

            service_report = generate_service_jpgs(
                upload_plan=check_report.upload_plan,
                input_dir=input_dir,
                output_dir=output_dir,
                max_long_side=8,
                quality=75,
            )
            manifest_report = generate_manifests(
                objects=check_report.objects,
                upload_plan=check_report.upload_plan,
                output_dir=output_dir,
                cantaloupe_base_url="http://localhost:8080/iiif/2",
                page_dimensions=service_report.page_dimensions,
            )
            manifest_path = Path(manifest_report.results[0].output_path)
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))

        canvas = payload["sequences"][0]["canvases"][0]
        resource = canvas["images"][0]["resource"]
        service_id = (
            "http://localhost:8080/iiif/2/workbench-lite%2Fsample%2Fderivatives%2Fparent"
            "%2Fpages%2Fpage%2Fservice.jpg"
        )

        self.assertEqual(canvas["@id"], "page/page")
        self.assertEqual((canvas["width"], canvas["height"]), (16, 10))
        self.assertNotIn("width", resource)
        self.assertEqual(resource["@id"], f"{service_id}/full/!2000,2000/0/default.jpg")
        self.assertEqual(canvas["thumbnail"]["@id"], f"{service_id}/full/!160,160/0/default.jpg")
        self.assertEqual(
            resource["service"],
            {
                "@context": "http://iiif.io/api/image/2/context.json",
                "@id": service_id,
                "profile": "http://iiif.io/api/image/2/level2.json",
            },
        )
        self.assertNotIn("full/full", json.dumps(payload))

    def test_cli_generate_outputs_json_summary_with_manifests(self):
        env = os.environ.copy()
        env["PYTHONPATH"] = str(PACKAGE_ROOT) + os.pathsep + str(PACKAGE_ROOT / "tests")

        with tempfile.TemporaryDirectory() as temp_dir:
            package_dir = Path(temp_dir)
            config_path, csv_path, _input_dir = self.write_tiny_package(package_dir)

            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "cli_test_runner",
                    "generate",
                    "--config",
                    str(config_path),
                    "--input-csv",
                    str(csv_path),
                    "--output-dir",
                    str(package_dir / "generated"),
                    "--manifests",
                    "--format",
                    "json",
                ],
                cwd=str(PACKAGE_ROOT),
                env=env,
                text=True,
                capture_output=True,
                check=False,
            )
            manifest_path = package_dir / "generated/workbench-lite/sample/manifests/parent.json"
            manifest_payload = json.loads(manifest_path.read_text(encoding="utf-8"))

        self.assertEqual(result.returncode, 0, result.stderr)
        payload = json.loads(result.stdout)
        self.assertEqual(payload["target_count"], 2)
        self.assertEqual(payload["generated_count"], 2)
        self.assertEqual(payload["failed_count"], 0)
        roles = {entry["role"] for entry in payload["results"]}
        self.assertIn("manifest", roles)
        self.assertIn("service_jpg", roles)
        self.assertNotIn("page_dimensions", payload)
        canvas = manifest_payload["sequences"][0]["canvases"][0]
        resource = canvas["images"][0]["resource"]
        self.assertEqual((canvas.get("width"), canvas.get("height")), (16, 10))
        self.assertEqual((resource.get("width"), resource.get("height")), (None, None))
        self.assertEqual(canvas["@id"], "page/page")
        self.assertTrue(resource["@id"].endswith("/full/!2000,2000/0/default.jpg"))
        self.assertTrue(canvas["thumbnail"]["@id"].endswith("/full/!160,160/0/default.jpg"))

    def test_build_cantaloupe_service_id_encodes_path_separators_and_spaces(self):
        from workbench_lite.manifest import build_cantaloupe_service_id

        service_id = build_cantaloupe_service_id(
            base_url="http://localhost:8182/iiif/2/",
            image_key="workbench-lite/package dir/objects/obj name/page_001.tif",
        )
        self.assertEqual(
            service_id,
            "http://localhost:8182/iiif/2/workbench-lite%2Fpackage%20dir%2Fobjects%2Fobj%20name%2Fpage_001.tif",
        )

    def test_verify_manifest_service_urls_reports_successful_info_json(self):
        import workbench_lite.verify as verify

        with tempfile.TemporaryDirectory() as temp_dir:
            manifest_dir = Path(temp_dir)
            manifest_path = manifest_dir / "manifest.json"
            service_id = "http://localhost:8182/iiif/2/workbench-lite%2Fsample%2Fmanifests%2Fobj%2F1"
            manifest_path.write_text(
                json.dumps(
                    {
                        "@id": "obj-1",
                        "@context": "http://iiif.io/api/presentation/2/context.json",
                        "@type": "sc:Manifest",
                        "sequences": [
                            {
                                "@id": "seq/1",
                                "@type": "sc:Sequence",
                                "canvases": [
                                    {
                                        "@id": "page/1",
                                        "label": "1",
                                        "images": [
                                            {
                                                "@type": "oa:Annotation",
                                                "resource": {
                                                    "@type": "dctypes:Image",
                                                    "service": {
                                                        "@context": "http://iiif.io/api/image/2/context.json",
                                                        "@id": service_id,
                                                        "profile": "http://iiif.io/api/image/2/level2.json",
                                                    },
                                                },
                                            },
                                        ],
                                    }
                                ],
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            class FakeResponse:
                def __init__(self):
                    self.headers = {"Content-Type": "application/json"}
                    self.status = 200
                    self.body = b"{\"width\": 2560, \"height\": 1700, \"identifier\": \"sample\"}"

                def __enter__(self):
                    return self

                def __exit__(self, exc_type, exc, tb):
                    return None

                def getcode(self):
                    return self.status

                def read(self, amt=-1):
                    return self.body

            original_urlopen = verify.urlopen

            def fake_urlopen(request, timeout=5):
                return FakeResponse()

            verify.urlopen = fake_urlopen
            try:
                report = verify.verify_manifest_service_urls([manifest_path], timeout_seconds=5)
            finally:
                verify.urlopen = original_urlopen

        self.assertEqual(report.tested_count, 1)
        self.assertEqual(report.passed_count, 1)
        self.assertEqual(report.failed_count, 0)
        self.assertEqual(report.results[0].status_code, 200)
        self.assertTrue(report.results[0].ok)
        self.assertEqual(report.results[0].info_url, f"{service_id}/info.json")
        self.assertEqual(report.results[0].width, 2560)
        self.assertEqual(report.results[0].height, 1700)
        self.assertEqual(report.results[0].content_type, "application/json")

    def test_verify_manifest_service_urls_reports_failed_info_json(self):
        import workbench_lite.verify as verify
        from urllib.error import URLError

        with tempfile.TemporaryDirectory() as temp_dir:
            manifest_dir = Path(temp_dir)
            manifest_path = manifest_dir / "manifest.json"
            service_id = "http://localhost:8182/iiif/2/workbench-lite%2Fmissing%2Fobj%2F2"
            manifest_path.write_text(
                json.dumps(
                    {
                        "@id": "obj-2",
                        "@context": "http://iiif.io/api/presentation/2/context.json",
                        "@type": "sc:Manifest",
                        "sequences": [
                            {
                                "@id": "seq/2",
                                "@type": "sc:Sequence",
                                "canvases": [
                                    {
                                        "@id": "page/2",
                                        "images": [
                                            {
                                                "@type": "oa:Annotation",
                                                "resource": {
                                                    "@type": "dctypes:Image",
                                                    "service": {"@id": service_id},
                                                },
                                            }
                                        ],
                                    }
                                ],
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            def fake_urlopen(request, timeout=5):
                raise URLError("not found")

            original_urlopen = verify.urlopen
            verify.urlopen = fake_urlopen
            try:
                report = verify.verify_manifest_service_urls([manifest_path], timeout_seconds=5)
            finally:
                verify.urlopen = original_urlopen

        self.assertEqual(report.tested_count, 1)
        self.assertEqual(report.passed_count, 0)
        self.assertEqual(report.failed_count, 1)
        self.assertIsNone(report.results[0].status_code)
        self.assertFalse(report.results[0].ok)

    def test_verify_manifest_service_urls_uses_base_url_override_for_relative_service(self):
        import workbench_lite.verify as verify

        with tempfile.TemporaryDirectory() as temp_dir:
            manifest_dir = Path(temp_dir)
            manifest_path = manifest_dir / "manifest.json"
            manifest_path.write_text(
                json.dumps(
                    {
                        "@id": "obj-3",
                        "@context": "http://iiif.io/api/presentation/2/context.json",
                        "@type": "sc:Manifest",
                        "sequences": [
                            {
                                "@id": "seq/3",
                                "@type": "sc:Sequence",
                                "canvases": [
                                    {
                                        "@id": "page/3",
                                        "images": [
                                            {
                                                "@type": "oa:Annotation",
                                                "resource": {
                                                    "@type": "dctypes:Image",
                                                    "service": {
                                                        "@id": "/iiif/2/workbench-lite%2Fsample%2Fpage.tif",
                                                        "profile": "http://iiif.io/api/image/2/level2.json",
                                                    },
                                                },
                                            },
                                        ],
                                    }
                                ],
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            class FakeResponse:
                def __init__(self):
                    self.headers = {"Content-Type": "application/json"}
                    self.status = 200
                    self.body = b"{\"width\": 100, \"height\": 100}"

                def __enter__(self):
                    return self

                def __exit__(self, exc_type, exc, tb):
                    return None

                def getcode(self):
                    return self.status

                def read(self, amt=-1):
                    return self.body

            original_urlopen = verify.urlopen

            def fake_urlopen(request, timeout=5):
                self.assertEqual(
                    request.full_url,
                    "https://cantaloupe.example/iiif/2/workbench-lite%2Fsample%2Fpage.tif/info.json",
                )
                return FakeResponse()

            verify.urlopen = fake_urlopen
            try:
                report = verify.verify_manifest_service_urls(
                    [manifest_path],
                    base_url_override="https://cantaloupe.example",
                    timeout_seconds=5,
                )
            finally:
                verify.urlopen = original_urlopen

        self.assertEqual(report.tested_count, 1)
        self.assertEqual(report.passed_count, 1)
        self.assertEqual(report.failed_count, 0)

    def test_cli_verify_cantaloupe_requires_manifest_input(self):
        env = os.environ.copy()
        env["PYTHONPATH"] = str(PACKAGE_ROOT) + os.pathsep + str(PACKAGE_ROOT / "tests")
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "cli_test_runner",
                "verify-cantaloupe",
                "--format",
                "json",
            ],
            cwd=PROJECT_ROOT,
            env=env,
            text=True,
            capture_output=True,
            check=False,
        )

        self.assertEqual(result.returncode, 2)
        self.assertIn("requires --manifest or --manifest-dir", result.stderr)


if __name__ == "__main__":
    unittest.main()
