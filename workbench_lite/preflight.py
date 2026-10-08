"""Local readiness and bounded read-only storage checks; never probe-write."""
import json
import os
from pathlib import Path
import shutil

from .image_policy import Image, pixel_limit_error

from .diagnostics import operation_error


def safe_source(value, input_dir):
    path = Path(value)
    root = Path(input_dir).resolve()
    if ('..' in path.parts or '\\' in value or '\x00' in value
            or value.startswith(('http://', 'https://'))):
        return False
    candidate = path if path.is_absolute() else root/path
    return candidate.resolve().is_relative_to(root)


def validate_content(path, role):
    """Return a safe diagnostic; PDF checking is framing, not full parsing."""
    path = Path(path)
    try:
        if role in {'master_tiff', 'service_jpg', 'thumbnail'}:
            with Image.open(path) as image:
                expected = 'TIFF' if role == 'master_tiff' else 'JPEG'
                if image.format != expected or getattr(image, 'n_frames', 1) != 1:
                    return f'Expected a single-frame {expected} image.'
                image.load()
        elif role == 'pdf':
            with path.open('rb') as handle:
                header = handle.read(8)
                handle.seek(max(0, path.stat().st_size - 1024))
                trailer = handle.read()
            if not header.startswith(b'%PDF-') or b'%%EOF' not in trailer:
                return 'PDF framing is invalid; expected PDF header and EOF marker.'
        elif role == 'hocr':
            with path.open(encoding='utf-8-sig') as handle:
                for _ in handle:
                    pass
        elif role in {'manifest', 'audit'}:
            value = json.loads(path.read_text(encoding='utf-8'))
            if not isinstance(value, dict):
                return 'Expected a JSON object.'
        else:
            with path.open('rb') as handle:
                handle.read(1)
    except Image.DecompressionBombError:
        return pixel_limit_error()
    except (OSError, ValueError) as exc:
        return 'Cannot read/decode required file: ' + operation_error(exc)
    return None


def output_readiness(entries, input_dir, output_dir, *, generating=False, new_audit=False):
    errors, planned, missing = [], [], []
    required_bytes = 0
    root = Path(output_dir).resolve() if output_dir is not None else None
    for entry in entries:
        if not entry.generated:
            continue
        if '..' in Path(entry.key).parts or Path(entry.key).is_absolute() or '\\' in entry.key:
            errors.append(f'Unsafe destination key for {entry.object_id}/{entry.page_id}.')
            continue
        target = root/entry.key if root else None
        if target and not target.resolve().is_relative_to(root):
            errors.append(f'Output escapes generated directory for {entry.object_id}/{entry.page_id}: {entry.key}')
            continue
        if target:
            parent = target.parent
            while not parent.exists():
                parent = parent.parent
            if not parent.is_dir() or (target.exists() and not target.is_file()):
                errors.append(f'Generated artifact path is not a writable file location: {entry.key}')
                continue
            if generating or (new_audit and entry.role == 'audit'):
                if not os.access(parent, os.W_OK | os.X_OK):
                    errors.append(f'Generated artifact parent is not writable/searchable: {entry.key}')
                if target.is_file() and entry.role != 'audit' and not os.access(target, os.W_OK):
                    errors.append(f'Generated artifact cannot be overwritten: {entry.key}')
        if generating or (new_audit and entry.role == 'audit') or root is None:
            planned.append(entry.key)
            required_bytes += 65536  # Per-file encoding, manifest and journal allowance.
            if generating and entry.role in {'service_jpg', 'thumbnail'}:
                source = Path(entry.source_path)
                source = source if source.is_absolute() else Path(input_dir)/source
                if source.is_file():
                    try:
                        with Image.open(source) as image:
                            required_bytes += image.width * image.height * 3
                    except Image.DecompressionBombError:
                        errors.append(f'{entry.object_id}/{entry.page_id}: {pixel_limit_error()}')
        elif not target.is_file():
            missing.append(entry.key)
        else:
            error = validate_content(target, entry.role)
            if error:
                errors.append(f'{entry.object_id}/{entry.page_id}: {entry.key}: {error}')
    free = None
    if root:
        ancestor = root
        while not ancestor.exists():
            ancestor = ancestor.parent
        free = shutil.disk_usage(ancestor).free
        if not ancestor.is_dir() or not os.access(ancestor, os.W_OK | os.X_OK):
            errors.append('Output directory is not writable/searchable.')
        if free < required_bytes:
            errors.append('Insufficient free disk for conservative generation/evidence estimate.')
    return {'errors': errors, 'missing_generated': missing, 'planned_generation': planned,
            'estimated_additional_bytes': required_bytes, 'available_bytes': free,
            'estimate_basis': 'source RGB upper estimate plus 64 KiB per generated entry; no disk reservation'}


def storage_readiness(client, entries, endpoint, *, offline=False, journal=None):
    from .push import STORAGE_ERRORS
    targets = []
    checks, errors = [], []
    for bucket in sorted({entry.bucket for entry in entries}):
        parents = [str(Path(entry.key).parent) for entry in entries if entry.bucket == bucket]
        prefix = os.path.commonpath(parents).rstrip('/') + '/'
        if prefix == '/':
            prefix = ''
        targets.append({'endpoint': endpoint, 'bucket': bucket, 'prefix': prefix})
        if offline:
            continue
        operations = [('HeadBucket', client.head_bucket, {'Bucket': bucket}),
                      ('ListObjectsV2', None, {'Bucket': bucket, 'Prefix': prefix, 'MaxKeys': 1})]
        for operation, method, parameters in operations:
            identity = {'operation': operation, 'bucket': bucket, 'prefix': prefix}
            op = journal.intent('preflight', identity) if journal else None
            try:
                if method is None:
                    method = client.list_objects_v2
                response = method(**parameters)
                result = {**identity, 'status': 'verified', 'outcome': 'accessible'}
                if operation == 'ListObjectsV2':
                    result.update(max_keys=1, sampled_objects=response.get('KeyCount', 0))
            except STORAGE_ERRORS as exc:
                message = f'{bucket}: {operation_error(exc)}'
                errors.append(message)
                result = {**identity, 'status': 'failed', 'message': message}
            checks.append(result)
            if journal:
                journal.result(op, 'preflight', result)
            if result['status'] == 'failed':
                break
    return {'targets': targets, 'checks': checks, 'errors': errors,
            'credential_access': 'not_checked' if offline else 'failed' if errors else 'read_access_verified',
            'write_permission': 'unverified', 'offline': offline,
            'limitations': 'Read-only access does not prove PutObject permission or absence of conflicting remote objects.'}
