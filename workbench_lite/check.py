import csv
from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Optional
from urllib.parse import urlparse

from .config import ConfigError, load_config, normalize_additional_files
from .models import CheckReport, PageRecord, UploadPlanEntry, WorkbenchObject
from .storage_plan import build_upload_plan
from .preflight import safe_source, validate_content


REQUIRED_COLUMNS = ["title", "id", "field_model", "parent_id", "field_weight", "file"]


def run_check(
    config_path: Path,
    input_csv_override: Optional[Path] = None,
    allow_dummy_files: bool = False,
    run_id: Optional[str] = None,
) -> CheckReport:
    config_path = Path(config_path)
    config = load_config(config_path)
    csv_path = _resolve_csv_path(config_path, config, input_csv_override)
    rows, fieldnames, csv_errors = _read_csv(csv_path)

    validation_errors: List[str] = []
    validation_warnings: List[str] = []
    validation_errors.extend(csv_errors)

    _validate_duplicate_headers(fieldnames, validation_errors)
    _validate_required_columns(fieldnames, validation_errors)
    _validate_duplicate_ids(rows, validation_errors)
    for row_number, row in enumerate(rows, 2):
        if not row.get('id', '').strip():
            validation_errors.append(f'CSV row {row_number} needs a nonempty id.')
        if row.get('field_model') == 'Page' and not row.get('file', '').strip():
            validation_errors.append(f'CSV row {row_number} ({row.get("id", "")}) needs a page image file.')
        if row.get('field_model') not in {'Page', 'Paged Content'}:
            validation_errors.append(f'CSV row {row_number} has an unsupported field_model.')

    parent_rows = [row for row in rows if row.get("field_model") == "Paged Content"]
    page_rows = [row for row in rows if row.get("field_model") == "Page"]
    parent_ids = {row.get("id", "") for row in parent_rows}
    child_parent_ids = {row.get("parent_id", "") for row in page_rows if row.get("parent_id", "")}

    _validate_child_parent_ids(page_rows, parent_ids, validation_errors)
    bad_child_weight_count = _validate_child_weights(page_rows, validation_errors)
    _validate_duplicate_weights(page_rows, validation_errors)

    additional_files = normalize_additional_files(config)
    # hOCR is consumed by the object builder even without legacy YAML mapping.
    if "hocr" in fieldnames:
        additional_files.setdefault("hocr", None)
    _validate_file_extensions(parent_rows, page_rows, additional_files.keys(), validation_errors)
    input_dir = _resolve_input_dir(config_path, config)
    for row_number, row in enumerate(rows, 2):
        for column in ['file', *additional_files]:
            value = row.get(column, '').strip()
            if value and not safe_source(value, input_dir):
                validation_errors.append(f'CSV row {row_number} ({row.get("id", "")}) column {column}: unsafe source path; must remain within input_dir.')
    if not allow_dummy_files:
        _validate_file_paths(
            rows=rows,
            input_dir=input_dir,
            additional_file_columns=additional_files.keys(),
            errors=validation_errors,
            warnings=validation_warnings,
            allow_missing_files=bool(config.get("allow_missing_files", False)),
        )

    objects = _build_objects(parent_rows, page_rows)
    file_counts = _count_file_roles(rows, additional_files.keys())
    upload_plan = build_upload_plan(objects, config, config_path, input_dir, run_id=run_id)
    _validate_upload_plan_collisions(upload_plan, validation_errors)
    if not allow_dummy_files:
        seen_files = set()
        for entry in upload_plan:
            if entry.generated:
                continue
            if not safe_source(entry.source_path, input_dir):
                validation_errors.append(f'{entry.object_id}/{entry.page_id}: unsafe planned source path.')
                continue
            path = Path(entry.source_path)
            path = path if path.is_absolute() else input_dir/path
            identity = (path, entry.role)
            if path.is_file() and identity not in seen_files:
                seen_files.add(identity)
                error = validate_content(path, entry.role)
                if error:
                    validation_errors.append(f'{entry.object_id}/{entry.page_id}: {entry.source_path}: {error}')

    return CheckReport(
        row_count=len(rows),
        column_count=len(fieldnames),
        parent_count=len(parent_rows),
        page_count=len(page_rows),
        unique_child_parent_count=len(child_parent_ids),
        bad_child_weight_count=bad_child_weight_count,
        file_counts=file_counts,
        validation_errors=validation_errors,
        validation_warnings=validation_warnings,
        objects=objects,
        upload_plan=upload_plan,
    )


def _resolve_csv_path(
    config_path: Path, config: Dict[str, object], input_csv_override: Optional[Path]
) -> Path:
    if input_csv_override is not None:
        return Path(input_csv_override)

    input_csv = str(config.get("input_csv", ""))
    if not input_csv.strip():
        raise ConfigError("input_csv or --input-csv is required.")
    if input_csv.startswith("http://") or input_csv.startswith("https://"):
        raise ConfigError("Remote input_csv values require --input-csv for the MVP check command.")

    path = Path(input_csv)
    if path.is_absolute():
        return path
    return _resolve_input_dir(config_path, config) / path


def _resolve_input_dir(config_path: Path, config: Dict[str, object]) -> Path:
    input_dir = Path(str(config.get("input_dir", config_path.parent)))
    if input_dir.is_absolute():
        return input_dir
    return config_path.parent / input_dir


def _read_csv(csv_path: Path) -> tuple[List[Dict[str, str]], List[str], List[str]]:
    if not csv_path.is_file():
        raise ConfigError(f"CSV file not found: {csv_path}")

    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        try:
            fieldnames = next(reader)
        except StopIteration:
            return [], [], ["CSV file is empty."]

        rows: List[Dict[str, str]] = []
        errors: List[str] = []
        for line_number, values in enumerate(reader, start=2):
            if len(values) != len(fieldnames):
                errors.append(
                    f"CSV row {line_number} has {len(values)} values but header has {len(fieldnames)}."
                )
            rows.append(
                {
                    fieldname: values[index] if index < len(values) else ""
                    for index, fieldname in enumerate(fieldnames)
                }
            )

    return rows, fieldnames, errors


def _validate_required_columns(fieldnames: Iterable[str], errors: List[str]) -> None:
    present = set(fieldnames)
    missing = [column for column in REQUIRED_COLUMNS if column not in present]
    if missing:
        errors.append("Missing required columns: " + ", ".join(missing))


def _validate_duplicate_headers(fieldnames: Iterable[str], errors: List[str]) -> None:
    duplicates = sorted(header for header, count in Counter(fieldnames).items() if count > 1)
    if duplicates:
        errors.append("Duplicate CSV headers: " + ", ".join(duplicates))


def _validate_duplicate_ids(rows: List[Dict[str, str]], errors: List[str]) -> None:
    ids = [row.get("id", "") for row in rows if row.get("id", "")]
    duplicates = sorted(identifier for identifier, count in Counter(ids).items() if count > 1)
    if duplicates:
        errors.append("Duplicate id values: " + ", ".join(duplicates))


def _validate_child_parent_ids(
    page_rows: List[Dict[str, str]], parent_ids: set[str], errors: List[str]
) -> None:
    for row in page_rows:
        parent_id = row.get("parent_id", "")
        if not parent_id:
            errors.append(f"Page row {row.get('id', '<unknown>')} is missing parent_id.")
        elif parent_id not in parent_ids:
            errors.append(
                f"Page row {row.get('id', '<unknown>')} references missing parent_id {parent_id}."
            )


def _validate_child_weights(page_rows: List[Dict[str, str]], errors: List[str]) -> int:
    bad_count = 0
    for row in page_rows:
        weight = row.get("field_weight", "")
        if not weight.isdecimal():
            bad_count += 1
            errors.append(
                f"Page row {row.get('id', '<unknown>')} has non-numeric field_weight {weight!r}."
            )
    return bad_count


def _validate_duplicate_weights(
    page_rows: List[Dict[str, str]], warnings: List[str]
) -> None:
    weights_by_parent: Dict[str, List[str]] = defaultdict(list)
    for row in page_rows:
        weight = row.get("field_weight", "")
        if weight.isdecimal():
            weights_by_parent[row.get("parent_id", "")].append(str(int(weight)))

    for parent_id, weights in weights_by_parent.items():
        duplicates = sorted(weight for weight, count in Counter(weights).items() if count > 1)
        if duplicates:
            warnings.append(
                f"Parent {parent_id} has duplicate field_weight values: {', '.join(duplicates)}"
            )


def _validate_file_extensions(
    parent_rows: List[Dict[str, str]],
    page_rows: List[Dict[str, str]],
    additional_file_columns: Iterable[str],
    errors: List[str],
) -> None:
    for row in parent_rows:
        _validate_extension(
            row=row,
            row_label="Parent",
            column="file",
            allowed_extensions=[".pdf"],
            expected_text=".pdf",
            errors=errors,
        )

    for row in page_rows:
        _validate_extension(
            row=row,
            row_label="Page",
            column="file",
            allowed_extensions=[".tif", ".tiff"],
            expected_text=".tif or .tiff",
            errors=errors,
        )
        for column in additional_file_columns:
            if column == "hocr":
                _validate_extension(
                    row=row,
                    row_label="Page",
                    column=column,
                    allowed_extensions=[".hocr", ".html", ".htm"],
                    expected_text=".hocr, .html, or .htm",
                    errors=errors,
                )


def _validate_extension(
    row: Dict[str, str],
    row_label: str,
    column: str,
    allowed_extensions: List[str],
    expected_text: str,
    errors: List[str],
) -> None:
    value = row.get(column, "").strip()
    if not value:
        return

    extension = _extract_extension(value)
    if extension not in allowed_extensions:
        display_extension = extension or "<none>"
        errors.append(
            f"{row_label} row {row.get('id', '<unknown>')} column {column} uses unsupported "
            f"extension {display_extension}; expected {expected_text}."
        )


def _extract_extension(value: str) -> str:
    parsed = urlparse(value)
    path = parsed.path if parsed.scheme else value
    return Path(path).suffix.lower()


def _validate_file_paths(
    rows: List[Dict[str, str]],
    input_dir: Path,
    additional_file_columns: Iterable[str],
    errors: List[str],
    warnings: List[str],
    allow_missing_files: bool,
) -> None:
    missing_file_reports = warnings if allow_missing_files else errors
    columns = ["file", *additional_file_columns]
    for row in rows:
        for column in columns:
            value = row.get(column, "").strip()
            if not value:
                continue
            if value.startswith("http://") or value.startswith("https://"):
                continue
            candidate = Path(value)
            if not candidate.is_absolute():
                candidate = input_dir / candidate
            if not candidate.is_file():
                missing_file_reports.append(
                    f"File not found for row {row.get('id', '<unknown>')} column {column}: {value}"
                )


def _count_file_roles(
    rows: List[Dict[str, str]], additional_file_columns: Iterable[str]
) -> Dict[str, int]:
    counts: Counter[str] = Counter()
    for row in rows:
        file_value = row.get("file", "").strip()
        if file_value:
            suffix = Path(file_value).suffix.lower().lstrip(".") or "file"
            counts[suffix] += 1
        for column in additional_file_columns:
            if row.get(column, "").strip():
                counts[column] += 1
    return dict(counts)


def _build_objects(
    parent_rows: List[Dict[str, str]], page_rows: List[Dict[str, str]]
) -> List[WorkbenchObject]:
    pages_by_parent: Dict[str, List[PageRecord]] = defaultdict(list)
    for row in page_rows:
        weight_text = row.get("field_weight", "")
        weight = int(weight_text) if weight_text.isdecimal() else 0
        pages_by_parent[row.get("parent_id", "")].append(
            PageRecord(
                page_id=row.get("id", ""),
                title=row.get("title", ""),
                weight=weight,
                file_path=row.get("file", ""),
                hocr_path=row.get("hocr", ""),
                source_row=row,
            )
        )

    objects: List[WorkbenchObject] = []
    for row in parent_rows:
        object_id = row.get("id", "")
        pages = sorted(pages_by_parent.get(object_id, []), key=lambda page: page.weight)
        objects.append(
            WorkbenchObject(
                object_id=object_id,
                title=row.get("title", ""),
                pdf_path=row.get("file", ""),
                source_row=row,
                pages=pages,
            )
        )
    return objects


def _validate_upload_plan_collisions(
    upload_plan: List[UploadPlanEntry], validation_errors: List[str]
) -> None:
    seen: Dict[tuple[str, str], UploadPlanEntry] = {}
    for entry in upload_plan:
        key = (entry.bucket, entry.key)
        if key not in seen:
            seen[key] = entry
            continue

        previous = seen[key]
        validation_errors.append(
            "Upload plan collision for bucket "
            f"{entry.bucket!r}, key {entry.key!r}: "
            f"role={entry.role}, object_id={entry.object_id}, page_id={entry.page_id}; "
            f"collides with role={previous.role}, object_id={previous.object_id}, "
            f"page_id={previous.page_id}."
        )
