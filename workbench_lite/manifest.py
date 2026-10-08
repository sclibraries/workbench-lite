from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import quote
import hashlib
import json

from .generate import GenerateResult, GenerateResultEntry
from .diagnostics import operation_error
from .runs import RecordedResults
from .models import UploadPlanEntry, WorkbenchObject


def generate_manifests(
    objects: List[WorkbenchObject],
    upload_plan: List[UploadPlanEntry],
    output_dir: Path,
    cantaloupe_base_url: str = "/iiif/2",
    manifest_base_url: Optional[str] = None,
    journal=None,
) -> GenerateResult:
    results = RecordedResults(journal, "generate")
    generated_count = 0
    failed_count = 0
    missing_source_count = 0
    target_count = len(objects)

    entries_by_key: Dict[tuple[str, str, str], UploadPlanEntry] = {
        (entry.role, entry.object_id, entry.page_id): entry for entry in upload_plan
    }

    for obj in objects:
        manifest_entry = entries_by_key.get(("manifest", obj.object_id, ""))
        results.begin(manifest_entry.to_dict() if manifest_entry else {"object_id": obj.object_id})
        manifest_key = (
            manifest_entry.key
            if manifest_entry
            else f"manifests/{_normalize_segment(obj.object_id)}.json"
        )
        manifest_path = _output_path_for_entry(manifest_key, output_dir)

        manifest_pages: List[Dict[str, object]] = []
        missing_reasons: List[str] = []
        service_url_prefix = cantaloupe_base_url.rstrip("/")

        for page in obj.pages:
            page_canvas, page_missing = _build_canvas_entry(
                page,
                obj,
                entries_by_key,
                output_dir,
                service_url_prefix,
            )
            if page_canvas is None:
                missing_reasons.append(page_missing or f"Missing image source for page {page.page_id}")
                continue

            manifest_pages.append(page_canvas)

        if missing_reasons:
            failed_count += 1
            missing_source_count += len(missing_reasons)
            results.append(
                GenerateResultEntry(
                    role="manifest",
                    source_path=manifest_entry.source_path if manifest_entry else "",
                    output_path=str(manifest_path),
                    status="failed",
                    checksum=None,
                    size_bytes=None,
                    message="; ".join(sorted(set(missing_reasons))),
                )
            )
            continue

        manifest_payload = _build_manifest_payload(
            obj,
            manifest_pages=manifest_pages,
            manifest_id=_manifest_id(manifest_entry, manifest_key, manifest_base_url),
            sequence_id=None,
            pdf_entry=entries_by_key.get(("pdf", obj.object_id, "")),
        )

        try:
            manifest_path.parent.mkdir(parents=True, exist_ok=True)
            manifest_path.write_text(_dump_manifest(manifest_payload), encoding="utf-8")
            checksum = _checksum_for_file(manifest_path)
            size_bytes = manifest_path.stat().st_size
        except OSError as exc:
            failed_count += 1
            results.append(GenerateResultEntry(
                role="manifest",
                source_path=manifest_entry.source_path if manifest_entry else "",
                output_path=str(manifest_path),
                status="failed",
                checksum=None,
                size_bytes=None,
                message=operation_error(exc),
            ))
            continue

        generated_count += 1
        results.append(
            GenerateResultEntry(
                role="manifest",
                source_path=manifest_entry.source_path if manifest_entry else "",
                output_path=str(manifest_path),
                status="generated",
                checksum=checksum,
                size_bytes=size_bytes,
                message=f"Manifest generated for {obj.object_id}.",
            )
        )

    return GenerateResult(
        generated_count=generated_count,
        missing_source_count=missing_source_count,
        failed_count=failed_count,
        target_count=target_count,
        results=results,
    )


def _build_canvas_entry(
    page,
    obj: WorkbenchObject,
    entries_by_key: Dict[tuple[str, str, str], UploadPlanEntry],
    output_dir: Path,
    cantaloupe_base_url: str,
) -> tuple[Optional[Dict[str, object]], Optional[str]]:
    service_entry = entries_by_key.get(("service_jpg", obj.object_id, page.page_id))
    master_entry = entries_by_key.get(("master_tiff", obj.object_id, page.page_id))

    if service_entry is not None:
        service_key = service_entry.key
    elif master_entry is not None:
        service_key = master_entry.key
    else:
        return None, f"Page {page.page_id} has no generated service_jpg or master_tiff entry"

    if service_entry is not None:
        generated_output = _output_path_for_entry(service_key, output_dir)
        if not generated_output.exists():
            return None, f"Generated image not found for page {page.page_id} at {generated_output}"

    service_id = build_cantaloupe_service_id(
        base_url=cantaloupe_base_url,
        image_key=service_key,
    )
    image_id = f"{service_id}/full/full/0/default.jpg"

    hocr_entry = entries_by_key.get(("hocr", obj.object_id, page.page_id))

    canvas_id = f"page/{_normalize_segment(page.page_id)}"
    canvas_payload: Dict[str, object] = {
        "@id": canvas_id,
        "@type": "sc:Canvas",
        "label": page.title or page.page_id,
        "images": [
            {
                "@type": "oa:Annotation",
                "motivation": "sc:painting",
                "on": canvas_id,
                "resource": {
                    "@type": "dctypes:Image",
                    "@id": image_id,
                    "format": "image/jpeg",
                    "service": {
                        "@context": "http://iiif.io/api/image/2/context.json",
                        "@id": service_id,
                        "profile": "http://iiif.io/api/image/2/level2.json",
                    },
                },
            }
        ],
        "thumbnail": {
            "@id": image_id,
        },
    }

    if hocr_entry is not None:
        canvas_payload["seeAlso"] = {
            "@id": _manifest_asset_url(hocr_entry),
            "format": "text/vnd.hocr+html",
            "label": "hOCR",
        }

    if page.title:
        canvas_payload["metadata"] = [
            {
                "label": "Order",
                "value": str(page.weight),
            },
            {
                "label": "Identifier",
                "value": page.page_id,
            },
        ]

    return canvas_payload, None


def _build_manifest_payload(
    obj: WorkbenchObject,
    manifest_pages: List[Dict[str, object]],
    manifest_id: str,
    sequence_id: Optional[str],
    pdf_entry: Optional[UploadPlanEntry],
) -> Dict[str, object]:
    sequence_id = sequence_id or f"{manifest_id}/sequence/normal"

    payload: Dict[str, object] = {
        "@context": "http://iiif.io/api/presentation/2/context.json",
        "@id": manifest_id,
        "@type": "sc:Manifest",
        "label": obj.title or obj.object_id,
        "sequences": [
            {
                "@id": sequence_id,
                "@type": "sc:Sequence",
                "canvases": manifest_pages,
            }
        ],
    }

    metadata: List[Dict[str, str]] = [
        {"label": "Object ID", "value": obj.object_id},
    ]
    if pdf_entry:
        metadata.append({"label": "Download PDF", "value": _manifest_asset_url(pdf_entry)})
    payload["metadata"] = metadata

    return payload


def _manifest_id(
    manifest_entry: Optional[UploadPlanEntry],
    fallback_key: str,
    manifest_base_url: Optional[str],
) -> str:
    manifest_path = manifest_entry.key if manifest_entry else fallback_key

    if manifest_base_url:
        return manifest_base_url.rstrip("/") + "/" + manifest_path.lstrip("/")

    return manifest_path


def _manifest_asset_url(entry: UploadPlanEntry) -> str:
    return entry.key


def _output_path_for_entry(entry_key: str, output_dir: Path) -> Path:
    normalized = str(Path(entry_key).as_posix()).lstrip("/")
    return output_dir / normalized


def build_cantaloupe_service_id(base_url: str, image_key: str) -> str:
    return f"{base_url.rstrip('/')}/{quote(image_key, safe='')}"


def _normalize_segment(value: str) -> str:
    characters: List[str] = []
    previous_was_separator = False
    for character in value.strip():
        if character.isalnum() or character in {"-", "_", "."}:
            characters.append(character)
            previous_was_separator = False
        else:
            if not previous_was_separator:
                characters.append("_")
            previous_was_separator = True

    normalized = "".join(characters).strip("._-")
    return normalized or "unnamed"


def _checksum_for_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _dump_manifest(payload: Dict[str, object]) -> str:
    return json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
