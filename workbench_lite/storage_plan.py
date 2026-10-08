from pathlib import Path
from uuid import uuid4
from typing import Dict, Iterable, List
from urllib.parse import urlparse

from .models import UploadPlanEntry, WorkbenchObject


DEFAULT_PRIVATE_BUCKET = "compass-prod-i2-files"
DEFAULT_PUBLIC_BUCKET = "islandora-derivatives"
DEFAULT_PREFIX = "workbench-lite"


def build_upload_plan(
    objects: Iterable[WorkbenchObject],
    config: Dict[str, object],
    config_path: Path,
    input_dir: Path,
    run_id: str = None,
) -> List[UploadPlanEntry]:
    run_id = run_id or uuid4().hex
    private_bucket = str(config.get("s3_private_bucket") or DEFAULT_PRIVATE_BUCKET)
    public_bucket = str(config.get("s3_public_bucket") or DEFAULT_PUBLIC_BUCKET)
    prefix = str(config.get("s3_prefix") or DEFAULT_PREFIX)
    batch_id = str(config.get("batch_id") or config_path.stem)
    batch_prefix = _join_key(prefix, _normalize_segment(batch_id))

    plan: List[UploadPlanEntry] = []
    for workbench_object in objects:
        object_segment = _normalize_segment(workbench_object.object_id)
        if workbench_object.pdf_path:
            plan.append(
                _entry(
                    role="pdf",
                    source_path=workbench_object.pdf_path,
                    bucket=private_bucket,
                    key=_join_key(
                        batch_prefix,
                        "originals",
                        object_segment,
                        _source_basename(workbench_object.pdf_path),
                    ),
                    public=False,
                    object_id=workbench_object.object_id,
                    page_id="",
                    generated=False,
                    input_dir=input_dir,
                )
            )

        for page in workbench_object.pages:
            page_segment = _normalize_segment(page.page_id)
            if page.file_path:
                plan.append(
                    _entry(
                        role="master_tiff",
                        source_path=page.file_path,
                        bucket=private_bucket,
                        key=_join_key(
                            batch_prefix,
                            "originals",
                            object_segment,
                            "pages",
                            _source_basename(page.file_path),
                        ),
                        public=False,
                        object_id=workbench_object.object_id,
                        page_id=page.page_id,
                        generated=False,
                        input_dir=input_dir,
                    )
                )

            if page.hocr_path:
                plan.append(
                    _entry(
                        role="hocr",
                        source_path=page.hocr_path,
                        bucket=public_bucket,
                        key=_join_key(
                            batch_prefix,
                            "ocr",
                            object_segment,
                            _source_basename(page.hocr_path),
                        ),
                        public=True,
                        object_id=workbench_object.object_id,
                        page_id=page.page_id,
                        generated=False,
                        input_dir=input_dir,
                    )
                )

            if page.file_path:
                plan.append(
                    _entry(
                        role="service_jpg",
                        source_path=page.file_path,
                        bucket=public_bucket,
                        key=_join_key(
                            batch_prefix,
                            "derivatives",
                            object_segment,
                            "pages",
                            page_segment,
                            "service.jpg",
                        ),
                        public=True,
                        object_id=workbench_object.object_id,
                        page_id=page.page_id,
                        generated=True,
                        input_dir=input_dir,
                    )
                )
                plan.append(
                    _entry(
                        role="thumbnail",
                        source_path=page.file_path,
                        bucket=public_bucket,
                        key=_join_key(
                            batch_prefix,
                            "derivatives",
                            object_segment,
                            "pages",
                            page_segment,
                            "thumbnail.jpg",
                        ),
                        public=True,
                        object_id=workbench_object.object_id,
                        page_id=page.page_id,
                        generated=True,
                        input_dir=input_dir,
                    )
                )

        plan.append(
            UploadPlanEntry(
                role="manifest",
                source_path="",
                bucket=public_bucket,
                key=_join_key(batch_prefix, "manifests", f"{object_segment}.json"),
                public=True,
                object_id=workbench_object.object_id,
                page_id="",
                generated=True,
                source_exists=False,
                checksum=None,
            )
        )

    plan.append(
        UploadPlanEntry(
            role="audit",
            source_path="",
            bucket=private_bucket,
            key=_join_key(batch_prefix, "audit", run_id, "upload-plan.json"),
            public=False,
            object_id="",
            page_id="",
            generated=True,
            source_exists=False,
            checksum=None,
        )
    )
    return plan


def _entry(
    role: str,
    source_path: str,
    bucket: str,
    key: str,
    public: bool,
    object_id: str,
    page_id: str,
    generated: bool,
    input_dir: Path,
) -> UploadPlanEntry:
    return UploadPlanEntry(
        role=role,
        source_path=source_path,
        bucket=bucket,
        key=key,
        public=public,
        object_id=object_id,
        page_id=page_id,
        generated=generated,
        source_exists=_source_exists(source_path, input_dir),
        checksum=None,
    )


def _source_exists(source_path: str, input_dir: Path) -> bool:
    if not source_path:
        return False
    if source_path.startswith("http://") or source_path.startswith("https://"):
        return True
    path = Path(source_path)
    if not path.is_absolute():
        path = input_dir / path
    return path.is_file()


def _source_basename(source_path: str) -> str:
    parsed = urlparse(source_path)
    path = parsed.path if parsed.scheme else source_path
    return _normalize_segment(Path(path).name)


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


def _join_key(*parts: str) -> str:
    return "/".join(part.strip("/") for part in parts if part.strip("/"))
