"""Publish receipted manifests into their configured local serving directory."""

import hashlib
import json
import os
from pathlib import Path
import stat
from uuid import uuid4

from .config import manifest_publish_dir
from .locks import WriterLocks
from .public_urls import join_manifest_url, strip_key_prefix
from .runs import push_stage_results, inspect_run, now, read_events


class PublishError(ValueError):
    """Publication was refused before writing a destination."""


def publish_manifests(run_path, generated_dir, config, *, execute=False, lock_factory=WriterLocks):
    run = Path(run_path).resolve()
    root = manifest_publish_dir(config)
    if not execute:
        return _publish(run, Path(generated_dir), root, config, execute=False)
    with lock_factory() as locks:
        locks.acquire('run:' + str(run))
        locks.acquire('publish-dir:' + str(root))
        return _publish(run, Path(generated_dir), root, config, execute=True)


def _publish(run, generated_dir, root, config, *, execute):
    summary = inspect_run(run)
    publication = summary['publication']
    if publication['state'] != 'ready_for_review':
        raise PublishError(f'Publication refused: {publication["reason"]}')
    sources = _collect_sources(run, generated_dir, config)
    planned = _plan_destinations(sources)
    preflight = _preflight(planned, root)
    if not execute:
        results = [_result(item, item['status']) for item in preflight]
        return _report(run, 0, results, 'Preview only; no files or publication log written.')
    return _copy_each(run, preflight, root)


def _collect_sources(run, generated_dir, config):
    try:
        plan = json.loads((run / 'upload-plan.json').read_text(encoding='utf-8'))['entries']
        run_config = json.loads((run / 'config.json').read_text(encoding='utf-8'))
        events, error = read_events(run)
        if error:
            raise PublishError('Run journal is damaged; manifest publication is held.')
    except (OSError, ValueError, KeyError, TypeError) as exc:
        raise PublishError('Run evidence is unavailable; manifest publication is held.') from exc
    prefix = run_config.get('manifest_key_prefix') or None
    if prefix is not None and not isinstance(prefix, str):
        raise PublishError('The run has an invalid recorded manifest key prefix.')
    fingerprints = _receipted_fingerprints(push_stage_results(events))
    base = config.get('manifest_base_url')
    if not isinstance(base, str) or not base:
        raise PublishError('Configuration manifest_base_url is required for publication.')
    sources = []
    for entry in plan:
        if entry.get('role') == 'manifest':
            sources.append(_manifest_source(entry, generated_dir, base, prefix, fingerprints))
    if not sources:
        raise PublishError('The receipted upload plan contains no manifests.')
    return sources


def _receipted_fingerprints(results):
    fingerprints = {}
    for event in results:
        stage, status = event.get('stage'), event.get('status')
        uploaded = stage == 'push' and status == 'uploaded'
        reused = stage == 'destination-check' and status == 'unchanged'
        if not (uploaded or reused):
            continue
        identity = event.get('bucket'), event.get('key')
        checksum, size = event.get('checksum'), event.get('size_bytes')
        if isinstance(checksum, str) and isinstance(size, int) and not isinstance(size, bool):
            fingerprints[identity] = (checksum, size)
    return fingerprints


def _manifest_source(entry, generated_dir, base, prefix, fingerprints):
    identity = entry.get('bucket'), entry.get('key')
    fingerprint = fingerprints.get(identity)
    key = entry.get('key')
    if fingerprint is None or not isinstance(key, str):
        raise PublishError('A manifest has no verified push fingerprint.')
    source_relative = _safe_relative_key(key)
    url = join_manifest_url(base, key, prefix)
    legacy_url = join_manifest_url(base, key) if prefix else None
    source = generated_dir / source_relative
    _verify_source(source, fingerprint, url, legacy_url)
    return {
        'source': source,
        'key': key,
        'source_relative': source_relative,
        'prefix': prefix,
        'url': url,
        'legacy_url': legacy_url,
        'sha256': fingerprint[0],
        'size_bytes': fingerprint[1],
    }


def _safe_relative_key(key):
    path = Path(key)
    if (
        not key
        or path.is_absolute()
        or '\\' in key
        or any(part in {'', '.', '..'} for part in key.split('/'))
    ):
        raise PublishError('A manifest key is not a safe relative path.')
    return path


def _verify_source(source, fingerprint, url, legacy_url):
    data = _read_verified_source(source, fingerprint)
    _verify_manifest_bytes(data, fingerprint, url, legacy_url)


def _read_verified_source(source, fingerprint):
    try:
        fd = os.open(source, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(fd, 'rb') as handle:
            info = os.fstat(handle.fileno())
            data = handle.read()
        if not stat.S_ISREG(info.st_mode) or len(data) != fingerprint[1]:
            raise PublishError(f'Manifest source is missing or not a regular file: {source.name}')
        if hashlib.sha256(data).hexdigest() != fingerprint[0]:
            raise PublishError(f'Manifest source changed after push: {source.name}')
        return data
    except OSError as exc:
        raise PublishError(f'Manifest source is unavailable: {source.name}') from exc


def _verify_manifest_bytes(data, fingerprint, url, legacy_url=None):
    if len(data) != fingerprint[1] or hashlib.sha256(data).hexdigest() != fingerprint[0]:
        raise PublishError('Manifest bytes do not match the verified push fingerprint.')
    try:
        manifest = json.loads(data)
    except (ValueError, UnicodeDecodeError) as exc:
        raise PublishError('Manifest source is not valid UTF-8 JSON.') from exc
    if isinstance(manifest, dict) and legacy_url and manifest.get('@id') == legacy_url:
        raise PublishError(
            'Legacy full-key manifest @id is incompatible with this prefix; '
            'regenerate the manifests.'
        )
    if not isinstance(manifest, dict) or manifest.get('@id') != url:
        raise PublishError('Manifest @id does not match the configured serving URL.')


def _plan_destinations(sources):
    planned = []
    for source in sources:
        try:
            destination_relative = Path(strip_key_prefix(source['key'], source['prefix']))
        except (KeyError, ValueError) as exc:
            raise PublishError('Manifest key is outside its recorded prefix.') from exc
        planned.append({**source, 'destination_relative': destination_relative})
    return planned


def _preflight(planned, root):
    ready = []
    for item in planned:
        status = _destination_status(
            root, item['destination_relative'], item['sha256'], item['size_bytes']
        )
        ready.append({**item, 'status': status})
    return ready


def _destination_status(root, relative, checksum, size):
    if relative.is_absolute() or any(part in {'', '.', '..'} for part in relative.parts):
        raise PublishError('Manifest destination is not contained by the publish directory.')
    parent_fd = _open_parent(root, relative, create=False)
    if parent_fd is None:
        return 'created'
    try:
        try:
            fd = os.open(
                relative.name,
                os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
                dir_fd=parent_fd,
            )
        except FileNotFoundError:
            return 'created'
        except OSError as exc:
            raise PublishError(
                f'Manifest destination is not a regular file: {relative.as_posix()}'
            ) from exc
        with os.fdopen(fd, 'rb') as handle:
            info = os.fstat(handle.fileno())
            if not stat.S_ISREG(info.st_mode):
                raise PublishError(
                    f'Manifest destination is not a regular file: {relative.as_posix()}'
                )
            data = handle.read()
    finally:
        os.close(parent_fd)
    if len(data) != size or not stat.S_ISREG(info.st_mode):
        raise PublishError(f'Manifest destination is not a regular file: {relative.as_posix()}')
    if info.st_size == size and hashlib.sha256(data).hexdigest() == checksum:
        return 'unchanged'
    raise PublishError(
        f'Manifest destination already exists with different bytes: {relative.as_posix()}'
    )


def _open_parent(root, relative, *, create):
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    current_fd = os.open(root, flags)
    try:
        for part in relative.parts[:-1]:
            next_fd, created = _open_directory(current_fd, part, create, flags)
            if next_fd is None:
                return None
            if created:
                try:
                    os.fchmod(next_fd, 0o755)
                    os.fsync(current_fd)
                except OSError:
                    os.close(next_fd)
                    raise
            os.close(current_fd)
            current_fd = next_fd
        result, current_fd = current_fd, None
        return result
    finally:
        if current_fd is not None:
            os.close(current_fd)


def _open_directory(parent_fd, name, create, flags):
    try:
        return os.open(name, flags, dir_fd=parent_fd), False
    except FileNotFoundError:
        if not create:
            return None, False
    try:
        os.mkdir(name, mode=0o755, dir_fd=parent_fd)
        created = True
    except FileExistsError:
        created = False
    return os.open(name, flags, dir_fd=parent_fd), created


def publication_dependencies(run):
    records, error = _read_publication_log(Path(run) / 'publication.jsonl')
    if error or records is None:
        return [], error
    blockers, seen, roots = [], set(), set()
    for root_text, relative in records:
        root = Path(root_text)
        if root_text not in roots:
            try:
                fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            except OSError:
                return [], 'A recorded publication root is not an existing directory.'
            os.close(fd)
            roots.add(root_text)
        try:
            parent_fd = _open_parent(root, relative, create=False)
            if parent_fd is None:
                continue
            try:
                os.stat(relative.name, dir_fd=parent_fd, follow_symlinks=False)
            except FileNotFoundError:
                continue
            finally:
                os.close(parent_fd)
        except OSError:
            return [], 'A recorded publication path cannot be checked safely.'
        identity = root_text, relative.as_posix()
        if identity not in seen:
            blockers.append({'root': root_text, 'path': identity[1]})
            seen.add(identity)
    return blockers, None


def _read_publication_log(path):
    try:
        content = path.read_bytes()
    except FileNotFoundError:
        return None, None
    except OSError:
        return None, 'Publication log is unreadable.'
    if not content or not content.endswith(b'\n'):
        return None, 'Publication log is empty or truncated.'
    records = []
    for line in content.splitlines(keepends=True):
        try:
            event = json.loads(line)
            root, relative = _publication_record(event)
        except (UnicodeDecodeError, ValueError, TypeError, OSError, RuntimeError):
            return None, 'Publication log is malformed.'
        if root is None:
            return None, 'A publication record has no root.'
        if relative is None:
            return None, 'A publication record has an invalid root or path.'
        records.append((root, relative))
    return records, None


def _publication_record(event):
    if not isinstance(event, dict) or event.get('kind') not in {'intent', 'result'}:
        return None, None
    root, path = event.get('root'), event.get('path')
    if not isinstance(root, str) or not root:
        return None, None
    if not isinstance(path, str) or not path or '\\' in path or '\x00' in path:
        return root, None
    root_path, relative = Path(root), Path(path)
    if (
        not root_path.is_absolute()
        or '..' in root_path.parts
        or str(root_path.resolve()) != root
        or relative.is_absolute()
        or any(part in {'', '.', '..'} for part in path.split('/'))
    ):
        return root, None
    return root, relative


def _copy_each(run, preflight, root):
    results = []
    for index, item in enumerate(preflight):
        operation = uuid4().hex
        state = {'linked': False}
        status = 'not_written'
        try:
            _append_event(run, _event('intent', operation, item, root))
            if item['status'] == 'created':
                status = _copy_item(item, root, state)
            else:
                status = _check_unchanged(item, root)
            _append_event(run, _event('result', operation, item, root, status=status))
            results.append(_result(item, status))
        except (OSError, PublishError) as exc:
            status = 'uncertain' if state['linked'] else status
            results.append(_result(item, status))
            results.extend(_result(other, 'not_written') for other in preflight[index + 1 :])
            return _report(run, 1, results, f'Publication stopped: {exc}')
    return _report(run, 0, results, 'Publication complete.')


def _check_unchanged(item, root):
    relative = item['destination_relative']
    status = _destination_status(root, relative, item['sha256'], item['size_bytes'])
    if status != 'unchanged':
        raise PublishError(f'Manifest destination changed after preflight: {relative}')
    return status


def _copy_item(item, root, state):
    directory_fd = _open_parent(root, item['destination_relative'], create=True)
    temporary = None
    try:
        temporary = _stage_file(item, directory_fd)
        _link_staged(item, root, directory_fd, temporary, state)
        os.unlink(temporary, dir_fd=directory_fd)
        return 'created'
    finally:
        if temporary:
            try:
                os.unlink(temporary, dir_fd=directory_fd)
            except FileNotFoundError:
                pass
        os.close(directory_fd)


def _stage_file(item, directory_fd):
    fingerprint = item['sha256'], item['size_bytes']
    data = _read_verified_source(item['source'], fingerprint)
    _verify_manifest_bytes(data, fingerprint, item['url'], item.get('legacy_url'))
    name = f'.{item["destination_relative"].name}.{uuid4().hex}.tmp'
    created = False
    try:
        fd = os.open(
            name,
            os.O_CREAT | os.O_EXCL | os.O_RDWR | os.O_NOFOLLOW,
            0o600,
            dir_fd=directory_fd,
        )
        created = True
        with os.fdopen(fd, 'r+b') as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
            handle.seek(0)
            staged = handle.read()
            _verify_manifest_bytes(staged, fingerprint, item['url'], item.get('legacy_url'))
            os.fchmod(handle.fileno(), 0o644)
            os.fsync(handle.fileno())
    except Exception:
        if created:
            try:
                os.unlink(name, dir_fd=directory_fd)
            except FileNotFoundError:
                pass
        raise
    return name


def _link_staged(item, root, directory_fd, temporary, state):
    relative = item['destination_relative']
    current_fd = _open_parent(root, relative, create=False)
    try:
        if current_fd is None or _directory_identity(current_fd) != _directory_identity(
            directory_fd
        ):
            raise PublishError('Manifest destination directory changed after preflight.')
        try:
            os.stat(relative.name, dir_fd=directory_fd, follow_symlinks=False)
        except FileNotFoundError:
            pass
        else:
            raise PublishError(f'Manifest destination appeared after preflight: {relative}')
    finally:
        if current_fd is not None:
            os.close(current_fd)
    os.link(
        temporary,
        relative.name,
        src_dir_fd=directory_fd,
        dst_dir_fd=directory_fd,
        follow_symlinks=False,
    )
    state['linked'] = True
    os.fsync(directory_fd)


def _directory_identity(fd):
    info = os.fstat(fd)
    return info.st_dev, info.st_ino


def _event(kind, operation, item, root, status=None):
    event = {
        'kind': kind,
        'operation_id': operation,
        'root': str(root),
        'path': item['destination_relative'].as_posix(),
        'sha256': item['sha256'],
        'size_bytes': item['size_bytes'],
        'timestamp': now(),
    }
    if status:
        event['status'] = status
    return event


def _append_event(run, event):
    path = run / 'publication.jsonl'
    flags = os.O_WRONLY | os.O_CREAT | os.O_APPEND | os.O_NOFOLLOW | os.O_NONBLOCK
    try:
        fd = os.open(path, flags | os.O_EXCL, 0o600)
        created = True
    except FileExistsError:
        fd = os.open(path, flags)
        created = False
    except OSError as exc:
        raise OSError('Cannot append publication evidence.') from exc
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise OSError('Publication evidence is not a regular file.')
        os.fchmod(fd, 0o600)
        offset = os.lseek(fd, 0, os.SEEK_END)
        payload = (json.dumps(event, ensure_ascii=False, sort_keys=True) + '\n').encode('utf-8')
        try:
            while payload:
                written = os.write(fd, payload)
                if not written:
                    raise OSError('Publication evidence write made no progress.')
                payload = payload[written:]
            os.fsync(fd)
        except OSError as exc:
            os.ftruncate(fd, offset)
            os.fsync(fd)
            raise OSError('Cannot persist publication evidence.') from exc
    finally:
        os.close(fd)
    if created:
        _fsync_directory(run)


def _fsync_directory(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _result(item, status):
    return {
        'path': item['destination_relative'].as_posix(),
        'status': status,
        'sha256': item['sha256'],
        'size_bytes': item['size_bytes'],
    }


def _report(run, exit_code, results, message):
    return {'run_id': run.name, 'exit_code': exit_code, 'message': message, 'results': results}
