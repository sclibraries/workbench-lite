import importlib.util
import tempfile
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = PROJECT_ROOT / "scripts" / "export_compass_s3_package.py"


def load_script_module():
    spec = importlib.util.spec_from_file_location("export_compass_s3_package", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class CompassS3ExportScriptTest(unittest.TestCase):
    def test_extract_s3_references_finds_direct_and_nested_manifest_urls(self):
        script = load_script_module()
        manifest = {
            "@id": "https://compass.fivecolleges.edu/node/100001/manifest",
            "sequences": [
                {
                    "canvases": [
                        {
                            "images": [
                                {
                                    "resource": {
                                        "@id": (
                                            "https://compass.fivecolleges.edu/cantaloupe/iiif/2/"
                                            "https%3A%2F%2Fcompass-prod-i2-files.s3.amazonaws.com%2F"
                                            "s3fs-public%2F2025-10%2Fexample-page-0001.tif"
                                            "/full/full/0/default.jpg"
                                        ),
                                        "service": {
                                            "@id": (
                                                "https://compass.fivecolleges.edu/cantaloupe/iiif/2/"
                                                "https%3A%2F%2Fcompass-prod-i2-files.s3.amazonaws.com%2F"
                                                "s3fs-public%2F2025-10%2Fexample-page-0001.tif"
                                            )
                                        },
                                    }
                                }
                            ],
                            "thumbnail": {
                                "@id": "https://compass-prod-i2-files.s3.amazonaws.com/s3fs-public/2025-10/example-thumbnail.jpg"
                            },
                            "metadata": [
                                {
                                    "label": "hOCR URL",
                                    "value": "https://compass-prod-i2-files.s3.amazonaws.com/s3fs-public/2025-10/example-page-0001.hocr",
                                }
                            ],
                            "seeAlso": {
                                "@id": "https://compass.fivecolleges.edu/system/files/2025-10/example-page-0001.html",
                                "format": "text/vnd.hocr+html",
                            },
                        }
                    ]
                }
            ],
        }

        references = script.extract_s3_references(manifest)

        self.assertEqual(
            sorted((reference.bucket, reference.key) for reference in references),
            [
                ("compass-prod-i2-files", "s3fs-public/2025-10/example-page-0001.hocr"),
                ("compass-prod-i2-files", "s3fs-public/2025-10/example-page-0001.html"),
                ("compass-prod-i2-files", "s3fs-public/2025-10/example-page-0001.tif"),
                ("compass-prod-i2-files", "s3fs-public/2025-10/example-thumbnail.jpg"),
            ],
        )

    def test_read_node_requests_accepts_csv_and_plain_text_inputs(self):
        script = load_script_module()

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            csv_path = temp_path / "nodes.csv"
            txt_path = temp_path / "nodes.txt"
            csv_path.write_text(
                "node_id,ingest_date,label\n"
                "100001,2025-10-15,First example object\n"
                "100002,2024-02-01,Second example object\n",
                encoding="utf-8",
            )
            txt_path.write_text("100001\n# comment\n100002\n", encoding="utf-8")

            csv_requests = script.read_node_requests(csv_path)
            txt_requests = script.read_node_requests(txt_path)

        self.assertEqual(
            [(request.node_id, request.ingest_date, request.label) for request in csv_requests],
            [("100001", "2025-10-15", "First example object"), ("100002", "2024-02-01", "Second example object")],
        )
        self.assertEqual(
            [(request.node_id, request.ingest_date, request.label) for request in txt_requests],
            [("100001", "", ""), ("100002", "", "")],
        )

    def test_read_node_requests_csv_supports_supplemental_sources(self):
        script = load_script_module()

        with tempfile.TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "nodes_with_sources.csv"
            csv_path.write_text(
                "node_id,ingest_date,label,source_urls\n"
                "100003,2023-07,Example collection,https://compass.fivecolleges.edu/system/files/2023-07/example-master.tif\n"
                "100004,2026-01,Example collection,s3://compass-prod-i2-files/s3fs-private/2026-01/100004-FITS File.xml|"
                "https://compass.fivecolleges.edu/system/files/2026-01/example.jpg\n",
                encoding="utf-8",
            )

            requests = script.read_node_requests(csv_path)

        self.assertEqual(requests[0].supplemental_sources, ("https://compass.fivecolleges.edu/system/files/2023-07/example-master.tif",))
        self.assertEqual(
            requests[1].supplemental_sources,
            (
                "s3://compass-prod-i2-files/s3fs-private/2026-01/100004-FITS File.xml",
                "https://compass.fivecolleges.edu/system/files/2026-01/example.jpg",
            ),
        )

    def test_read_node_requests_accepts_export_csv_headers(self):
        script = load_script_module()

        with tempfile.TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "export_example.csv"
            csv_path.write_text(
                "Node ID,Type,Original Files,Ingest Date,Compass Link\n"
                "100005,PDF,1,Jul-23,https://compass.fivecolleges.edu/node/100005\n"
                "100006,Paged Content,2,Dec 2023,https://compass.fivecolleges.edu/node/100006\n",
                encoding="utf-8",
            )

            requests = script.read_node_requests(csv_path)

        self.assertEqual(
            [(request.node_id, request.ingest_date, request.label) for request in requests],
            [("100005", "Jul-23", "PDF"), ("100006", "Dec 2023", "Paged Content")],
        )

    def test_extract_ingest_month_accepts_human_readable_dates(self):
        script = load_script_module()

        self.assertEqual(script._extract_ingest_month("Jul-23"), "2023-07")
        self.assertEqual(script._extract_ingest_month("Dec 2023"), "2023-12")
        self.assertEqual(script._extract_ingest_month("August 2023"), "2023-08")
        self.assertEqual(script._extract_ingest_month("2026-01"), "2026-01")

    def test_extract_s3_references_reads_embedded_source_from_iiif_info_json_ids(self):
        script = load_script_module()
        payload = {
            "items": [
                {
                    "node_id": "100007",
                    "info": {
                        "@id": (
                            "https://compass.fivecolleges.edu/cantaloupe/iiif/2/"
                            "https%3A%2F%2Fcompass.fivecolleges.edu%2Fsystem%2Ffiles%2F2023-07%2Fexample-master.tif"
                        )
                    },
                },
                {
                    "node_id": "100008",
                    "info": {
                        "@id": (
                            "https://compass.fivecolleges.edu/cantaloupe/iiif/2/"
                            "https%3A%2F%2Fcompass.fivecolleges.edu%2Fsystem%2Ffiles%2F2023-12%2F"
                            "example-thumbnail.jpg"
                        )
                    },
                },
            ]
        }

        references = script.extract_s3_references(payload)

        self.assertEqual(
            sorted((reference.bucket, reference.key) for reference in references),
            [
                ("compass-prod-i2-files", "s3fs-public/2023-07/example-master.tif"),
                ("compass-prod-i2-files", "s3fs-public/2023-12/example-thumbnail.jpg"),
            ],
        )

    def test_list_s3_references_for_prefix_handles_pagination(self):
        script = load_script_module()

        class FakeS3Client:
            def __init__(self):
                self.calls = []

            def list_objects_v2(self, **kwargs):
                self.calls.append(kwargs)
                token = kwargs.get("ContinuationToken")
                if token is None:
                    return {
                        "IsTruncated": True,
                        "NextContinuationToken": "page2",
                        "Contents": [{"Key": "s3fs-private/2023-07/100009-FITS File.xml"}],
                    }
                return {
                    "IsTruncated": False,
                    "Contents": [{"Key": "s3fs-private/2023-07/100009-Extracted Text.txt"}],
                }

        fake_client = FakeS3Client()
        references = script._list_s3_references_for_prefix(
            fake_client,
            "compass-prod-i2-files",
            "s3fs-private/2023-07/100009-",
        )

        self.assertEqual(len(fake_client.calls), 2)
        self.assertEqual(
            [(reference.bucket, reference.key) for reference in references],
            [
                ("compass-prod-i2-files", "s3fs-private/2023-07/100009-FITS File.xml"),
                ("compass-prod-i2-files", "s3fs-private/2023-07/100009-Extracted Text.txt"),
            ],
        )

    def test_build_private_sweep_inventory_rows_uses_ingest_month_and_node_prefix(self):
        script = load_script_module()

        class FakeS3Client:
            def __init__(self):
                self.kwargs = None

            def list_objects_v2(self, **kwargs):
                self.kwargs = kwargs
                return {
                    "IsTruncated": False,
                    "Contents": [{"Key": "s3fs-private/2026-01/100010-Extracted Text.txt"}],
                }

        node = script.NodeRequest(node_id="100010", ingest_date="2026-01", label="Example collection")
        fake_client = FakeS3Client()
        rows = script.build_private_sweep_inventory_rows(
            node,
            "https://compass.fivecolleges.edu",
            fake_client,
            "compass-prod-i2-files",
            "s3fs-private/",
        )

        self.assertEqual(fake_client.kwargs["Prefix"], "s3fs-private/2026-01/100010-")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].key, "s3fs-private/2026-01/100010-Extracted Text.txt")
        self.assertEqual(rows[0].source_url, "s3://compass-prod-i2-files/s3fs-private/2026-01/100010-Extracted Text.txt")

    def test_build_inventory_rows_from_node_page_extracts_html_escaped_system_files_urls(self):
        script = load_script_module()
        node = script.NodeRequest(node_id="100011", ingest_date="2023-07", label="Example collection")
        html = (
            '<iframe data-src="https://compass.fivecolleges.edu/system/files/2023-07/example-document.pdf" '
            'src="/libraries/pdf.js/web/viewer.html?file=https%3A%2F%2Fcompass.fivecolleges.edu%2Fsystem%2Ffiles%2F'
            'submitted-files%2Fexample-record.pdf"></iframe>'
            '<link rel="icon" href="https://compass-prod-i2-files.s3.amazonaws.com/compass_favicon_large.png">'
        )

        rows = script.build_inventory_rows_from_node_page(
            node,
            html,
            "https://compass.fivecolleges.edu",
            "compass-prod-i2-files",
            "s3fs-public/",
        )

        self.assertEqual(
            sorted(row.key for row in rows),
            [
                "s3fs-public/2023-07/example-document.pdf",
                "s3fs-public/submitted-files/example-record.pdf",
            ],
        )
        self.assertEqual({row.role for row in rows}, {"pdf"})
        self.assertEqual({row.manifest_url for row in rows}, {"https://compass.fivecolleges.edu/node/100011"})

    def test_resolve_system_file_rows_with_s3_checks_all_rows_and_private_fallback(self):
        script = load_script_module()

        class NotFoundError(Exception):
            response = {"Error": {"Code": "404"}}

        class FakeS3Client:
            existing_keys = {
                "s3fs-private/2023-07/example-master.tif",
                "s3fs-private/2023-12/example-thumbnail.jpg",
            }

            def head_object(self, **kwargs):
                if kwargs["Key"] not in self.existing_keys:
                    raise NotFoundError()
                return {}

        rows = [
            script.InventoryRow(
                node_id="100012",
                ingest_date="2023-07",
                label="Example collection",
                manifest_url="https://compass.fivecolleges.edu/node/100012/manifest",
                bucket="compass-prod-i2-files",
                key="s3fs-public/2023-07/example-master.tif",
                role="master_image",
                source_url="https://compass.fivecolleges.edu/system/files/2023-07/example-master.tif",
            ),
            script.InventoryRow(
                node_id="100013",
                ingest_date="2023-07",
                label="Example collection",
                manifest_url="https://compass.fivecolleges.edu/node/100013/manifest",
                bucket="compass-prod-i2-files",
                key="s3fs-public/2023-12/example-thumbnail.jpg",
                role="image_derivative",
                source_url="https://compass.fivecolleges.edu/system/files/2023-12/example-thumbnail.jpg",
            ),
        ]

        script.resolve_system_file_rows_with_s3(rows, FakeS3Client(), "s3fs-public/", ("s3fs-private/",))

        self.assertEqual(rows[0].key, "s3fs-private/2023-07/example-master.tif")
        self.assertEqual(rows[1].key, "s3fs-private/2023-12/example-thumbnail.jpg")

    def test_build_ssl_context_defaults_to_verified_and_can_be_explicitly_disabled(self):
        script = load_script_module()

        self.assertIsNone(script.build_ssl_context(skip_tls_verify=False))
        context = script.build_ssl_context(skip_tls_verify=True)

        self.assertFalse(context.check_hostname)
        self.assertEqual(context.verify_mode, script.ssl.CERT_NONE)


if __name__ == "__main__":
    unittest.main()
