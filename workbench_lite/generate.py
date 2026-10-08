from dataclasses import dataclass, field
import hashlib
from pathlib import Path
from .diagnostics import operation_error
from .runs import RecordedResults
from typing import List, Optional

from .image_policy import Image, pixel_limit_error

from .models import UploadPlanEntry


@dataclass(frozen=True)
class GenerateResult:
    generated_count: int
    missing_source_count: int
    failed_count: int
    target_count: int
    results: List["GenerateResultEntry"] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "generated_count": self.generated_count,
            "missing_source_count": self.missing_source_count,
            "failed_count": self.failed_count,
            "target_count": self.target_count,
            "results": [entry.to_dict() for entry in self.results],
        }


@dataclass(frozen=True)
class GenerateResultEntry:
    role: str
    source_path: str
    output_path: str
    status: str
    checksum: Optional[str]
    size_bytes: Optional[int]
    message: str = ""

    def to_dict(self) -> dict:
        return {
            "role": self.role,
            "source_path": self.source_path,
            "output_path": self.output_path,
            "status": self.status,
            "checksum": self.checksum,
            "size_bytes": self.size_bytes,
            "message": self.message,
        }


def generate_service_jpgs(
    upload_plan: List[UploadPlanEntry],
    input_dir: Path,
    output_dir: Path,
    max_long_side: int = 2000,
    quality: int = 82,
    journal=None,
) -> GenerateResult:
    return _generate_derivatives(
        upload_plan=upload_plan,
        input_dir=input_dir,
        output_dir=output_dir,
        role="service_jpg",
        max_long_side=max_long_side,
        quality=quality,
        journal=journal,
    )


def generate_thumbnails(
    upload_plan: List[UploadPlanEntry],
    input_dir: Path,
    output_dir: Path,
    max_long_side: int = 300,
    quality: int = 75,
    journal=None,
) -> GenerateResult:
    return _generate_derivatives(
        upload_plan=upload_plan,
        input_dir=input_dir,
        output_dir=output_dir,
        role="thumbnail",
        max_long_side=max_long_side,
        quality=quality,
        journal=journal,
    )


def _generate_derivatives(
    upload_plan: List[UploadPlanEntry],
    input_dir: Path,
    output_dir: Path,
    role: str,
    max_long_side: int,
    quality: int,
    journal=None,
) -> GenerateResult:
    entries = [entry for entry in upload_plan if entry.role == role]
    results = RecordedResults(journal, "generate")
    generated_count = 0
    missing_source_count = 0
    failed_count = 0
    success_message = "Service JPEG generated." if role == "service_jpg" else "Thumbnail JPEG generated."

    for entry in entries:
        results.begin(entry.to_dict())
        source_path = _resolve_source_path(entry.source_path, input_dir)
        if not source_path.is_file():
            missing_source_count += 1
            results.append(
                GenerateResultEntry(
                    role=entry.role,
                    source_path=str(source_path),
                    output_path=str(_output_path_for_entry(entry, output_dir)),
                    status="missing_source",
                    checksum=None,
                    size_bytes=None,
                    message="Source image file is missing.",
                )
            )
            continue

        target = _output_path_for_entry(entry, output_dir)
        try:
            _convert_tiff_to_jpeg(source_path, target, max_long_side=max_long_side, quality=quality)
            checksum = _checksum_for_file(target)
            size_bytes = target.stat().st_size
        except (OSError, ValueError, Image.DecompressionBombError) as exc:
            failed_count += 1
            results.append(
                GenerateResultEntry(
                    role=entry.role,
                    source_path=str(source_path),
                    output_path=str(target),
                    status="failed",
                    checksum=None,
                    size_bytes=None,
                    message=pixel_limit_error() if isinstance(exc, Image.DecompressionBombError) else operation_error(exc),
                )
            )
            continue

        generated_count += 1
        results.append(
            GenerateResultEntry(
                role=entry.role,
                source_path=str(source_path),
                output_path=str(target),
                status="generated",
                checksum=checksum,
                size_bytes=size_bytes,
                message=success_message,
            )
        )

    return GenerateResult(
        generated_count=generated_count,
        missing_source_count=missing_source_count,
        failed_count=failed_count,
        target_count=len(entries),
        results=results,
    )


def _convert_tiff_to_jpeg(source_path: Path, output_path: Path, max_long_side: int, quality: int) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with Image.open(source_path) as image:
        width, height = image.size
        if width == 0 or height == 0:
            raise ValueError("Source image has invalid dimensions.")

        image = image.convert("RGB")
        if max_long_side > 0:
            image.thumbnail((max_long_side, max_long_side), resample=Image.Resampling.LANCZOS)

        image.save(output_path, format="JPEG", quality=quality, optimize=True)


def _resolve_source_path(source_path: str, input_dir: Path) -> Path:
    path = Path(source_path)
    if path.is_absolute():
        return path
    return input_dir / path


def _output_path_for_entry(entry: UploadPlanEntry, output_dir: Path) -> Path:
    normalized = str(Path(entry.key).as_posix()).lstrip("/")
    return output_dir / normalized


def _checksum_for_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
