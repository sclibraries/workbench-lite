from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from pathlib import Path
from urllib.parse import urlsplit
from .config import ConfigError
from .diagnostics import operation_error
from .runs import RecordedResults


@dataclass(frozen=True)
class ServiceVerificationResult:
    manifest_path: str
    object_id: str
    canvas_id: str
    service_id: str
    info_url: str
    ok: bool
    status_code: Optional[int]
    content_type: Optional[str]
    width: Optional[int]
    height: Optional[int]
    message: str = ""

    def to_dict(self) -> Dict[str, object]:
        return {
            "manifest_path": self.manifest_path,
            "object_id": self.object_id,
            "canvas_id": self.canvas_id,
            "service_id": self.service_id,
            "info_url": self.info_url,
            "ok": self.ok,
            "status_code": self.status_code,
            "content_type": self.content_type,
            "width": self.width,
            "height": self.height,
            "message": self.message,
        }


@dataclass(frozen=True)
class CantaloupeVerifyReport:
    tested_count: int
    passed_count: int
    failed_count: int
    results: List[ServiceVerificationResult]

    def to_dict(self) -> Dict[str, object]:
        return {
            "tested_count": self.tested_count,
            "passed_count": self.passed_count,
            "failed_count": self.failed_count,
            "results": [result.to_dict() for result in self.results],
        }


def verify_manifest_service_urls(
    manifest_paths: Sequence[str | Path],
    base_url_override: str | None = None,
    timeout_seconds: int = 5,
    journal=None,
) -> CantaloupeVerifyReport:
    # Validate the complete input set before making any HTTP requests.
    prepared = []
    for manifest_path in manifest_paths:
        path = Path(manifest_path)
        payload = _load_manifest_json(path)
        service_entries = _iter_service_entries(payload)
        if not service_entries:
            raise ConfigError(f"Manifest has no canvas service IDs: {path}")
        for _, service_id in service_entries:
            resolved = _resolve_service_id(service_id, base_url_override)
            try:
                url = urlsplit(resolved)
                valid = url.scheme in {"http", "https"} and bool(url.hostname)
                url.port
            except ValueError:
                valid = False
            if not valid:
                raise ConfigError(f"Manifest service IDs need valid HTTP(S) URLs or a base override: {path}")
        prepared.append((path, str(payload.get("@id", path.stem)), service_entries))

    results = RecordedResults(journal, "verify-cantaloupe")
    for path, object_id, service_entries in prepared:
        for canvas_id, service_id in service_entries:
            resolved_service_id = _resolve_service_id(service_id, base_url_override=base_url_override)
            info_url = _info_url(resolved_service_id)
            results.begin({'manifest_path': str(path), 'object_id': object_id,
                           'canvas_id': canvas_id, 'info_url': info_url})
            is_ok, status_code, content_type, width, height, message = _fetch_info_json(
                info_url,
                timeout_seconds,
            )
            results.append(
                ServiceVerificationResult(
                    manifest_path=str(path),
                    object_id=object_id,
                    canvas_id=canvas_id,
                    service_id=resolved_service_id,
                    info_url=info_url,
                    ok=is_ok,
                    status_code=status_code,
                    content_type=content_type,
                    width=width,
                    height=height,
                    message=message,
                )
            )

    tested_count = len(results)
    passed_count = sum(1 for result in results if result.ok)
    failed_count = tested_count - passed_count
    return CantaloupeVerifyReport(
        tested_count=tested_count,
        passed_count=passed_count,
        failed_count=failed_count,
        results=results,
    )


def _load_manifest_json(path: Path) -> dict:
    if not path.is_file():
        raise ConfigError(f"Manifest file not found: {path}")
    try:
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ConfigError(f"Manifest did not contain valid JSON: {path}") from exc
    if not isinstance(payload, dict):
        raise ConfigError(f"Manifest must be a JSON object: {path}")
    return payload


def _iter_service_entries(payload: dict) -> List[tuple[str, str]]:
    sequences = payload.get("sequences", [])
    if not isinstance(sequences, list):
        return []

    entries: List[tuple[str, str]] = []
    for sequence in sequences:
        if not isinstance(sequence, dict):
            continue
        canvases = sequence.get("canvases", [])
        if not isinstance(canvases, list):
            continue

        for canvas in canvases:
            if not isinstance(canvas, dict):
                continue

            canvas_id = str(canvas.get("@id", "unknown"))
            images = canvas.get("images", [])
            if not isinstance(images, list):
                continue
            for image in images:
                if not isinstance(image, dict):
                    continue
                resource = image.get("resource", {})
                if not isinstance(resource, dict):
                    continue
                service = resource.get("service")
                if not isinstance(service, dict):
                    continue

                service_id = service.get("@id")
                if isinstance(service_id, str) and service_id:
                    entries.append((canvas_id, service_id))
    return entries


def _fetch_info_json(
    info_url: str,
    timeout_seconds: int,
) -> tuple[bool, Optional[int], Optional[str], Optional[int], Optional[int], str]:
    try:
        request = Request(info_url, method="GET")
        with urlopen(request, timeout=timeout_seconds) as response:
            status_code = _get_status_code(response)
            content_type = _get_content_type(response)
            width, height = _extract_dimensions(response)
            if status_code == 200:
                return True, status_code, content_type, width, height, ""
            return (
                False,
                status_code,
                content_type,
                width,
                height,
                f"Unexpected status {status_code}",
            )
    except HTTPError as exc:
        return (
            False,
            exc.code,
            _get_content_type(exc),
            None,
            None,
            f"HTTP status {exc.code}",
        )
    except URLError as exc:
        return False, None, None, None, None, operation_error(exc)

    except (OSError, ValueError) as exc:
        return False, None, None, None, None, operation_error(exc)


def _resolve_service_id(service_id: str, base_url_override: str | None = None) -> str:
    if service_id.startswith("http://") or service_id.startswith("https://"):
        return service_id

    if base_url_override is None:
        return service_id

    normalized_base = base_url_override.rstrip("/")
    if service_id.startswith("/"):
        return f"{normalized_base}{service_id}"

    return f"{normalized_base}/{service_id}"


def _extract_dimensions(response) -> tuple[Optional[int], Optional[int]]:
    try:
        raw = response.read()
    except Exception:
        return None, None
    if not isinstance(raw, (bytes, bytearray)):
        return None, None

    content = raw.strip()
    if not content:
        return None, None

    try:
        payload = json.loads(content.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return None, None

    if not isinstance(payload, dict):
        return None, None

    width = payload.get("width")
    height = payload.get("height")
    if isinstance(width, int) and isinstance(height, int):
        return width, height
    return None, None


def _get_status_code(response) -> Optional[int]:
    if hasattr(response, "status"):
        return int(response.status)
    return response.getcode()


def _get_content_type(response) -> Optional[str]:
    if hasattr(response, "headers"):
        headers = response.headers
        if isinstance(headers, dict):
            return headers.get("Content-Type") or headers.get("content-type")
        if hasattr(headers, "get_content_type"):
            return headers.get_content_type()
        return headers.get("Content-Type") if hasattr(headers, "get") else None
    return None


def _info_url(service_id: str) -> str:
    return f"{service_id.rstrip('/')}/info.json"
