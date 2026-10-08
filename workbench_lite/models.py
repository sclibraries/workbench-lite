from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass(frozen=True)
class PageRecord:
    page_id: str
    title: str
    weight: int
    file_path: str
    hocr_path: str
    source_row: Dict[str, str]

    def to_dict(self) -> Dict[str, object]:
        return {
            "page_id": self.page_id,
            "title": self.title,
            "weight": self.weight,
            "file_path": self.file_path,
            "hocr_path": self.hocr_path,
        }


@dataclass(frozen=True)
class WorkbenchObject:
    object_id: str
    title: str
    pdf_path: str
    source_row: Dict[str, str]
    pages: List[PageRecord] = field(default_factory=list)

    def to_dict(self) -> Dict[str, object]:
        return {
            "object_id": self.object_id,
            "title": self.title,
            "pdf_path": self.pdf_path,
            "page_count": len(self.pages),
            "pages": [page.to_dict() for page in self.pages],
        }


@dataclass(frozen=True)
class UploadPlanEntry:
    role: str
    source_path: str
    bucket: str
    key: str
    public: bool
    object_id: str
    page_id: str
    generated: bool
    source_exists: bool
    checksum: Optional[str]

    def to_dict(self) -> Dict[str, object]:
        return {
            "role": self.role,
            "source_path": self.source_path,
            "bucket": self.bucket,
            "key": self.key,
            "public": self.public,
            "object_id": self.object_id,
            "page_id": self.page_id,
            "generated": self.generated,
            "source_exists": self.source_exists,
            "checksum": self.checksum,
        }


@dataclass(frozen=True)
class CheckReport:
    row_count: int
    column_count: int
    parent_count: int
    page_count: int
    unique_child_parent_count: int
    bad_child_weight_count: int
    file_counts: Dict[str, int]
    validation_errors: List[str]
    validation_warnings: List[str]
    objects: List[WorkbenchObject]
    upload_plan: List[UploadPlanEntry]

    def to_dict(self) -> Dict[str, object]:
        return {
            "row_count": self.row_count,
            "column_count": self.column_count,
            "parent_count": self.parent_count,
            "page_count": self.page_count,
            "unique_child_parent_count": self.unique_child_parent_count,
            "bad_child_weight_count": self.bad_child_weight_count,
            "file_counts": self.file_counts,
            "validation_errors": self.validation_errors,
            "validation_warnings": self.validation_warnings,
            "objects": [obj.to_dict() for obj in self.objects],
            "upload_plan": [entry.to_dict() for entry in self.upload_plan],
        }
