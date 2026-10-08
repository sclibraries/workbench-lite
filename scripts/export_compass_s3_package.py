#!/usr/bin/env python3
"""Build and optionally download an S3 export package from Compass node IDs."""

import argparse
import csv
import hashlib
import html
import json
import re
import ssl
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence
from urllib.parse import unquote, urlparse
from urllib.request import Request, urlopen


S3_SCHEME_URL_RE = re.compile(r"s3://[^\s\"'<>`)]+", re.IGNORECASE)
S3_HTTP_URL_RE = re.compile(
    r"https?://[A-Za-z0-9.-]*s3[.-][A-Za-z0-9.-]*amazonaws\.com/"
    r"[^\s\"'<>`)]*?\.(?:tif|tiff|jp2|jpg|jpeg|png|pdf|hocr|html|htm|alto|xml|txt)"
    r"(?=$|[/?#\s\"'<>`)])",
    re.IGNORECASE,
)
SYSTEM_FILES_URL_RE = re.compile(
    r"https?://[^\s\"'<>`)]+/system/files/"
    r"[^\s\"'<>`)]*?\.(?:tif|tiff|jp2|jpg|jpeg|png|pdf|hocr|html|htm|alto|xml|txt)"
    r"(?=$|[/?#\s\"'<>`)])",
    re.IGNORECASE,
)
IIIF_EMBEDDED_SOURCE_RE = re.compile(r"/iiif/[23]/([^\s\"'<>`)]+)", re.IGNORECASE)
DEFAULT_SYSTEM_FILES_BUCKET = "compass-prod-i2-files"
DEFAULT_SYSTEM_FILES_PREFIX = "s3fs-public/"
DEFAULT_PRIVATE_SWEEP_BUCKET = "compass-prod-i2-files"
DEFAULT_PRIVATE_SWEEP_PREFIX = "s3fs-private/"
NODE_ID_COLUMNS = ("node_id", "node id", "node", "nid", "drupal_node_id", "compass_node_id")
INGEST_DATE_COLUMNS = ("ingest_date", "ingest date", "date", "created", "created_date", "ingested")
LABEL_COLUMNS = ("label", "title", "name", "type")
SUPPLEMENTAL_SOURCE_COLUMNS = (
    "source_url",
    "source_urls",
    "supplemental_source",
    "supplemental_sources",
    "iiif_source_url",
)
CSV_FIELDNAMES = [
    "node_id",
    "ingest_date",
    "label",
    "manifest_url",
    "bucket",
    "key",
    "role",
    "source_url",
    "status",
    "local_path",
    "sha256",
    "message",
]


@dataclass(frozen=True)
class NodeRequest:
    node_id: str
    ingest_date: str = ""
    label: str = ""
    supplemental_sources: tuple[str, ...] = ()


@dataclass(frozen=True)
class S3Reference:
    bucket: str
    key: str
    source_url: str
    role: str


@dataclass
class InventoryRow:
    node_id: str
    ingest_date: str
    label: str
    manifest_url: str
    bucket: str
    key: str
    role: str
    source_url: str
    status: str = "planned"
    local_path: str = ""
    sha256: str = ""
    message: str = ""


def read_node_requests(path: Path) -> List[NodeRequest]:
    text = path.read_text(encoding="utf-8-sig")
    first_content_line = next(
        (line for line in text.splitlines() if line.strip() and not line.lstrip().startswith("#")),
        "",
    )

    if path.suffix.lower() == ".csv" or "," in first_content_line:
        return _read_node_requests_csv(text)

    requests = []
    for line in text.splitlines():
        value = line.strip()
        if not value or value.startswith("#"):
            continue
        requests.append(NodeRequest(node_id=value))
    return requests


def extract_s3_references(
    payload,
    system_files_bucket: str = DEFAULT_SYSTEM_FILES_BUCKET,
    system_files_prefix: str = DEFAULT_SYSTEM_FILES_PREFIX,
) -> List[S3Reference]:
    references: Dict[tuple, S3Reference] = {}
    for value in _walk_strings(payload):
        for url in _extract_s3_urls(value):
            reference = s3_url_to_object_ref(url, system_files_bucket, system_files_prefix)
            if reference is None:
                continue
            references[(reference.bucket, reference.key)] = reference
    return list(references.values())


def s3_url_to_object_ref(
    url: str,
    system_files_bucket: str = DEFAULT_SYSTEM_FILES_BUCKET,
    system_files_prefix: str = DEFAULT_SYSTEM_FILES_PREFIX,
) -> Optional[S3Reference]:
    parsed = urlparse(url)
    bucket = ""
    key = ""

    if parsed.scheme.lower() == "s3":
        bucket = parsed.netloc
        key = parsed.path.lstrip("/")
    elif parsed.scheme.lower() in {"http", "https"}:
        host = (parsed.hostname or "").lower()
        path = parsed.path.lstrip("/")
        if host.endswith(".s3.amazonaws.com"):
            bucket = host[: -len(".s3.amazonaws.com")]
            key = path
        elif ".s3." in host and host.endswith(".amazonaws.com"):
            bucket = host.split(".s3.", 1)[0]
            key = path
        elif host == "s3.amazonaws.com" or host.startswith("s3."):
            parts = path.split("/", 1)
            if len(parts) == 2:
                bucket, key = parts
        elif "/system/files/" in parsed.path:
            system_key = parsed.path.split("/system/files/", 1)[1]
            bucket = system_files_bucket
            key = _join_s3_key(system_files_prefix, system_key)

    bucket = unquote(bucket).strip()
    key = unquote(key).strip().lstrip("/")
    if not bucket or not key:
        return None

    return S3Reference(bucket=bucket, key=key, source_url=url, role=_infer_role(key))


def fetch_manifest(node_id: str, compass_base_url: str, timeout: int, ssl_context=None) -> Dict[str, object]:
    manifest_url = build_manifest_url(node_id, compass_base_url)
    request = Request(
        manifest_url,
        headers={
            "Accept": "application/json,application/ld+json;q=0.9,*/*;q=0.8",
            "User-Agent": "preservica-compass-s3-export/1.0",
        },
    )
    with urlopen(request, timeout=timeout, context=ssl_context) as response:
        return json.loads(response.read().decode("utf-8"))


def fetch_node_page(node_id: str, compass_base_url: str, timeout: int, ssl_context=None) -> str:
    node_url = build_node_url(node_id, compass_base_url)
    request = Request(
        node_url,
        headers={
            "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
            "User-Agent": "preservica-compass-s3-export/1.0",
        },
    )
    with urlopen(request, timeout=timeout, context=ssl_context) as response:
        return response.read().decode("utf-8", errors="replace")


def build_ssl_context(skip_tls_verify: bool):
    if not skip_tls_verify:
        return None
    return ssl._create_unverified_context()


def build_manifest_url(node_id: str, compass_base_url: str) -> str:
    return f"{compass_base_url.rstrip('/')}/node/{node_id}/manifest"


def build_node_url(node_id: str, compass_base_url: str) -> str:
    return f"{compass_base_url.rstrip('/')}/node/{node_id}"


def build_inventory_rows(
    node_request: NodeRequest,
    manifest: Dict[str, object],
    compass_base_url: str,
    system_files_bucket: str = DEFAULT_SYSTEM_FILES_BUCKET,
    system_files_prefix: str = DEFAULT_SYSTEM_FILES_PREFIX,
) -> List[InventoryRow]:
    manifest_url = str(manifest.get("@id") or manifest.get("id") or build_manifest_url(node_request.node_id, compass_base_url))
    return [
        InventoryRow(
            node_id=node_request.node_id,
            ingest_date=node_request.ingest_date,
            label=node_request.label,
            manifest_url=manifest_url,
            bucket=reference.bucket,
            key=reference.key,
            role=reference.role,
            source_url=reference.source_url,
        )
        for reference in sorted(
            extract_s3_references(manifest, system_files_bucket, system_files_prefix),
            key=lambda item: (item.bucket, item.key),
        )
    ]


def download_inventory_rows(rows: Iterable[InventoryRow], output_dir: Path, s3_client) -> None:
    for row in rows:
        local_path = output_dir / row.bucket / row.key
        local_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            s3_client.download_file(row.bucket, row.key, str(local_path))
            row.local_path = str(local_path)
            row.sha256 = _sha256_file(local_path)
            row.status = "downloaded"
        except Exception as error:  # pragma: no cover - exercised against real AWS.
            row.status = "download_failed"
            row.message = str(error)


def write_inventory_json(rows: Sequence[InventoryRow], path: Path, nodes: Sequence[NodeRequest]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "nodes": [asdict(node) for node in nodes],
        "rows": [asdict(row) for row in rows],
    }
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def write_inventory_csv(rows: Sequence[InventoryRow], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDNAMES)
        writer.writeheader()
        for row in rows:
            writer.writerow(asdict(row))


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Inventory and optionally download S3 objects referenced by Compass IIIF manifests."
    )
    parser.add_argument("--nodes", required=True, type=Path, help="CSV or text file containing Compass node IDs.")
    parser.add_argument("--compass-base-url", default="https://compass.fivecolleges.edu")
    parser.add_argument("--timeout", default=30, type=int, help="Manifest request timeout in seconds.")
    parser.add_argument(
        "--insecure-skip-tls-verify",
        action="store_true",
        help="Fetch Compass manifests without TLS certificate verification. Use only for controlled exports.",
    )
    parser.add_argument("--inventory-json", type=Path, default=None)
    parser.add_argument("--inventory-csv", type=Path, default=None)
    parser.add_argument("--download-dir", type=Path, default=None, help="Destination root for downloaded files.")
    parser.add_argument("--execute", action="store_true", help="Actually download S3 objects. Default is inventory-only.")
    parser.add_argument("--system-files-bucket", default=DEFAULT_SYSTEM_FILES_BUCKET)
    parser.add_argument("--system-files-prefix", default=DEFAULT_SYSTEM_FILES_PREFIX)
    parser.add_argument(
        "--system-files-private-prefix",
        default=DEFAULT_PRIVATE_SWEEP_PREFIX,
        help="Fallback S3 prefix for Drupal /system/files/ URLs backed by private S3 files.",
    )
    parser.add_argument(
        "--include-private-prefix-sweep",
        action="store_true",
        help=(
            "List S3 objects under private prefix using node_id and ingest month pattern "
            "(prefix/YYYY-MM/{node_id}-*)."
        ),
    )
    parser.add_argument(
        "--include-node-page-sources",
        action="store_true",
        help="Fetch Compass node pages and inventory raw file URLs exposed in the HTML.",
    )
    parser.add_argument("--private-sweep-bucket", default=DEFAULT_PRIVATE_SWEEP_BUCKET)
    parser.add_argument("--private-sweep-prefix", default=DEFAULT_PRIVATE_SWEEP_PREFIX)
    parser.add_argument("--endpoint-url", default=None, help="Optional S3 endpoint URL, useful for LocalStack tests.")
    parser.add_argument("--region", default=None, help="Optional AWS region for the boto3 client.")
    args = parser.parse_args(argv)

    if args.execute and args.download_dir is None:
        parser.error("--execute requires --download-dir")

    nodes = read_node_requests(args.nodes)
    ssl_context = build_ssl_context(args.insecure_skip_tls_verify)
    s3_client = None
    if args.execute or args.include_private_prefix_sweep:
        s3_client = _create_s3_client(args.endpoint_url, args.region)

    rows: List[InventoryRow] = []
    errors = []
    for node_request in nodes:
        node_rows: List[InventoryRow] = []
        try:
            manifest = fetch_manifest(node_request.node_id, args.compass_base_url, args.timeout, ssl_context)
            node_rows.extend(
                build_inventory_rows(
                    node_request,
                    manifest,
                    args.compass_base_url,
                    args.system_files_bucket,
                    args.system_files_prefix,
                )
            )
        except Exception as error:
            errors.append(f"node {node_request.node_id}: {error}")

        node_rows.extend(
            build_inventory_rows_from_sources(
                node_request,
                node_request.supplemental_sources,
                args.compass_base_url,
                args.system_files_bucket,
                args.system_files_prefix,
            )
        )

        if args.include_node_page_sources:
            try:
                node_page = fetch_node_page(node_request.node_id, args.compass_base_url, args.timeout, ssl_context)
                node_rows.extend(
                    build_inventory_rows_from_node_page(
                        node_request,
                        node_page,
                        args.compass_base_url,
                        args.system_files_bucket,
                        args.system_files_prefix,
                    )
                )
            except Exception as error:
                errors.append(f"node {node_request.node_id} page sources: {error}")

        if args.include_private_prefix_sweep:
            try:
                node_rows.extend(
                    build_private_sweep_inventory_rows(
                        node_request,
                        args.compass_base_url,
                        s3_client,
                        args.private_sweep_bucket,
                        args.private_sweep_prefix,
                    )
                )
            except Exception as error:
                errors.append(f"node {node_request.node_id} private sweep: {error}")

        rows.extend(_dedupe_inventory_rows(node_rows))

    if s3_client is not None:
        resolve_system_file_rows_with_s3(
            rows,
            s3_client,
            args.system_files_prefix,
            (args.system_files_private_prefix,),
        )
        rows = _dedupe_inventory_rows(rows)

    if args.execute:
        download_inventory_rows(rows, args.download_dir, s3_client)

    if args.inventory_json:
        write_inventory_json(rows, args.inventory_json, nodes)
    if args.inventory_csv:
        write_inventory_csv(rows, args.inventory_csv)
    if not args.inventory_json and not args.inventory_csv:
        json.dump([asdict(row) for row in rows], sys.stdout, indent=2)
        print()

    for error in errors:
        print(f"ERROR: {error}", file=sys.stderr)
    return 1 if errors or any(row.status.endswith("failed") for row in rows) else 0


def _read_node_requests_csv(text: str) -> List[NodeRequest]:
    rows = csv.DictReader(line for line in text.splitlines() if line.strip() and not line.lstrip().startswith("#"))
    if rows.fieldnames is None:
        return []

    normalized_names = {name.lower().strip(): name for name in rows.fieldnames if name is not None}
    node_column = _pick_column(normalized_names, NODE_ID_COLUMNS)
    if node_column is None:
        raise ValueError("Node CSV requires one of these columns: " + ", ".join(NODE_ID_COLUMNS))

    date_column = _pick_column(normalized_names, INGEST_DATE_COLUMNS)
    label_column = _pick_column(normalized_names, LABEL_COLUMNS)
    supplemental_column = _pick_column(normalized_names, SUPPLEMENTAL_SOURCE_COLUMNS)
    requests = []
    for row in rows:
        node_id = (row.get(node_column) or "").strip()
        if not node_id:
            continue
        supplemental_sources = ()
        if supplemental_column:
            supplemental_sources = tuple(_split_source_urls(row.get(supplemental_column) or ""))
        requests.append(
            NodeRequest(
                node_id=node_id,
                ingest_date=(row.get(date_column) or "").strip() if date_column else "",
                label=(row.get(label_column) or "").strip() if label_column else "",
                supplemental_sources=supplemental_sources,
            )
        )
    return requests


def build_inventory_rows_from_sources(
    node_request: NodeRequest,
    source_urls: Sequence[str],
    compass_base_url: str,
    system_files_bucket: str,
    system_files_prefix: str,
) -> List[InventoryRow]:
    references: Dict[tuple, S3Reference] = {}
    for source_url in source_urls:
        reference = s3_url_to_object_ref(source_url, system_files_bucket, system_files_prefix)
        if reference is not None:
            references[(reference.bucket, reference.key)] = reference

    manifest_url = build_manifest_url(node_request.node_id, compass_base_url)
    return [
        InventoryRow(
            node_id=node_request.node_id,
            ingest_date=node_request.ingest_date,
            label=node_request.label,
            manifest_url=manifest_url,
            bucket=reference.bucket,
            key=reference.key,
            role=reference.role,
            source_url=reference.source_url,
        )
        for reference in sorted(references.values(), key=lambda item: (item.bucket, item.key))
    ]


def build_inventory_rows_from_node_page(
    node_request: NodeRequest,
    node_page: str,
    compass_base_url: str,
    system_files_bucket: str,
    system_files_prefix: str,
) -> List[InventoryRow]:
    node_url = build_node_url(node_request.node_id, compass_base_url)
    return [
        InventoryRow(
            node_id=node_request.node_id,
            ingest_date=node_request.ingest_date,
            label=node_request.label,
            manifest_url=node_url,
            bucket=reference.bucket,
            key=reference.key,
            role=reference.role,
            source_url=reference.source_url,
        )
        for reference in sorted(
            (
                reference
                for reference in extract_s3_references(html.unescape(node_page), system_files_bucket, system_files_prefix)
                if not _is_compass_page_chrome_reference(reference)
            ),
            key=lambda item: (item.bucket, item.key),
        )
    ]


def build_private_sweep_inventory_rows(
    node_request: NodeRequest,
    compass_base_url: str,
    s3_client,
    private_sweep_bucket: str,
    private_sweep_prefix: str,
) -> List[InventoryRow]:
    if s3_client is None:
        return []

    ingest_month = _extract_ingest_month(node_request.ingest_date)
    if ingest_month is None:
        return []

    sweep_prefix = _build_private_sweep_prefix(private_sweep_prefix, ingest_month, node_request.node_id)
    references = []
    for reference in _list_s3_references_for_prefix(s3_client, private_sweep_bucket, sweep_prefix):
        references.append(reference)

    manifest_url = build_manifest_url(node_request.node_id, compass_base_url)
    return [
        InventoryRow(
            node_id=node_request.node_id,
            ingest_date=node_request.ingest_date,
            label=node_request.label,
            manifest_url=manifest_url,
            bucket=reference.bucket,
            key=reference.key,
            role=reference.role,
            source_url=reference.source_url,
        )
        for reference in sorted(references, key=lambda item: (item.bucket, item.key))
    ]


def resolve_system_file_rows_with_s3(
    rows: Sequence[InventoryRow],
    s3_client,
    system_files_prefix: str,
    fallback_prefixes: Sequence[str],
) -> None:
    for row in rows:
        if not _is_system_files_source(row.source_url):
            continue

        suffix = _strip_known_s3_prefix(row.key, (system_files_prefix, *fallback_prefixes))
        if suffix is None:
            continue

        for candidate_key in _system_file_candidate_keys(suffix, system_files_prefix, fallback_prefixes):
            if _s3_object_exists(s3_client, row.bucket, candidate_key):
                row.key = candidate_key
                row.role = _infer_role(candidate_key)
                break


def _pick_column(normalized_names: Dict[str, str], candidates: Sequence[str]) -> Optional[str]:
    for candidate in candidates:
        if candidate in normalized_names:
            return normalized_names[candidate]
    return None


def _split_source_urls(raw_value: str) -> List[str]:
    parts = re.split(r"[|;,\n\r]+", raw_value)
    return [part.strip() for part in parts if part.strip()]


def _is_system_files_source(source_url: str) -> bool:
    return "/system/files/" in unquote(source_url)


def _strip_known_s3_prefix(key: str, prefixes: Sequence[str]) -> Optional[str]:
    normalized_key = key.strip("/")
    for prefix in prefixes:
        normalized_prefix = prefix.strip("/")
        if normalized_key.startswith(normalized_prefix + "/"):
            return normalized_key[len(normalized_prefix) + 1 :]
    return None


def _system_file_candidate_keys(
    suffix: str,
    system_files_prefix: str,
    fallback_prefixes: Sequence[str],
) -> List[str]:
    keys = []
    seen = set()
    for prefix in (system_files_prefix, *fallback_prefixes):
        key = _join_s3_key(prefix, suffix)
        if key not in seen:
            keys.append(key)
            seen.add(key)
    return keys


def _s3_object_exists(s3_client, bucket: str, key: str) -> bool:
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except Exception as error:
        response = getattr(error, "response", {})
        error_code = str(response.get("Error", {}).get("Code", ""))
        if error_code in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise


def _is_compass_page_chrome_reference(reference: S3Reference) -> bool:
    return reference.key.lower().endswith("/compass_favicon_large.png") or reference.key.lower() == "compass_favicon_large.png"


def _extract_ingest_month(value: str) -> Optional[str]:
    cleaned = re.sub(r"\s+", " ", value.strip())
    match = re.search(r"(\d{4})[-/](\d{1,2})", cleaned)
    if match:
        return f"{match.group(1)}-{int(match.group(2)):02d}"

    for date_format in ("%b-%y", "%b %y", "%B-%y", "%B %y", "%b %Y", "%B %Y"):
        try:
            return datetime.strptime(cleaned, date_format).strftime("%Y-%m")
        except ValueError:
            continue
    return None


def _build_private_sweep_prefix(base_prefix: str, ingest_month: str, node_id: str) -> str:
    return "/".join(
        part.strip("/")
        for part in (base_prefix, ingest_month, f"{node_id}-")
        if part.strip("/")
    )


def _list_s3_references_for_prefix(s3_client, bucket: str, prefix: str) -> List[S3Reference]:
    continuation_token = None
    references = []
    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if continuation_token:
            kwargs["ContinuationToken"] = continuation_token

        response = s3_client.list_objects_v2(**kwargs)
        for item in response.get("Contents", []):
            key = item.get("Key")
            if not key:
                continue
            references.append(
                S3Reference(
                    bucket=bucket,
                    key=key,
                    source_url=f"s3://{bucket}/{key}",
                    role=_infer_role(key),
                )
            )

        if not response.get("IsTruncated"):
            break
        continuation_token = response.get("NextContinuationToken")
        if not continuation_token:
            break
    return references


def _dedupe_inventory_rows(rows: Sequence[InventoryRow]) -> List[InventoryRow]:
    deduped: Dict[tuple, InventoryRow] = {}
    for row in rows:
        key = (row.node_id, row.bucket, row.key)
        existing = deduped.get(key)
        if existing is None:
            deduped[key] = row
            continue
        if row.source_url.startswith("s3://") and not existing.source_url.startswith("s3://"):
            deduped[key] = row
    return sorted(deduped.values(), key=lambda item: (item.node_id, item.bucket, item.key))


def _walk_strings(value):
    if isinstance(value, str):
        yield value
    elif isinstance(value, dict):
        for item in value.values():
            yield from _walk_strings(item)
    elif isinstance(value, list):
        for item in value:
            yield from _walk_strings(item)


def _extract_s3_urls(value: str) -> List[str]:
    urls = []
    seen = set()
    for candidate in _decoded_candidates(value):
        for embedded in _extract_iiif_embedded_source_urls(candidate):
            if embedded not in seen:
                urls.append(embedded)
                seen.add(embedded)
        for regex in (S3_SCHEME_URL_RE, S3_HTTP_URL_RE, SYSTEM_FILES_URL_RE):
            for match in regex.finditer(candidate):
                url = match.group(0)
                if url not in seen:
                    urls.append(url)
                    seen.add(url)
    return urls


def _decoded_candidates(value: str) -> List[str]:
    candidates = [value]
    decoded = value
    for _index in range(3):
        next_decoded = unquote(decoded)
        if next_decoded == decoded:
            break
        candidates.append(next_decoded)
        decoded = next_decoded
    return candidates


def _extract_iiif_embedded_source_urls(value: str) -> List[str]:
    extracted = []
    seen = set()
    for match in IIIF_EMBEDDED_SOURCE_RE.finditer(value):
        raw_token = match.group(1)
        token = _trim_iiif_suffix(raw_token)
        for decoded in _decoded_candidates(token):
            candidate = decoded.strip()
            if candidate.startswith(("http://", "https://", "s3://")) and candidate not in seen:
                extracted.append(candidate)
                seen.add(candidate)
    return extracted


def _trim_iiif_suffix(value: str) -> str:
    token = value
    for separator in ("/full/", "/max/", "/pct:", "/info.json", "/region/", "/size/", "/rotation/"):
        if separator in token:
            token = token.split(separator, 1)[0]
    return token.rstrip("/")


def _infer_role(key: str) -> str:
    lowered = key.lower()
    if lowered.endswith((".tif", ".tiff", ".jp2")):
        return "master_image"
    if lowered.endswith((".jpg", ".jpeg", ".png")):
        if "thumb" in lowered or lowered.endswith(("_tn.jpg", "_tn.jpeg")):
            return "thumbnail"
        return "image_derivative"
    if lowered.endswith(".pdf"):
        return "pdf"
    if lowered.endswith((".hocr", ".html", ".htm", ".alto", ".xml", ".txt")):
        return "text_sidecar"
    return "unknown"


def _join_s3_key(prefix: str, key: str) -> str:
    return "/".join(part.strip("/") for part in (prefix, key) if part.strip("/"))


def _create_s3_client(endpoint_url: Optional[str], region: Optional[str]):
    try:
        import boto3  # type: ignore
    except ImportError as error:  # pragma: no cover - depends on runtime environment.
        raise RuntimeError("boto3 is required for --execute downloads") from error

    client_kwargs = {}
    if endpoint_url:
        client_kwargs["endpoint_url"] = endpoint_url
    if region:
        client_kwargs["region_name"] = region
    return boto3.client("s3", **client_kwargs)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


if __name__ == "__main__":
    sys.exit(main())