"""Single-host writer exclusion. Production root is provisioned, never inferred."""
import hashlib
import json
import os
from pathlib import Path
import socket
import stat
from uuid import uuid4

from .runs import atomic_write, json_bytes, now

LOCK_ROOT = Path('/var/lib/workbench-lite/locks')


class LockError(RuntimeError):
    pass


class WriterLocks:
    def __init__(self, root=LOCK_ROOT):
        # Injection is a Python test seam, not a CLI/config/environment setting.
        self.root = Path(root)
        self.owned = {}
        self.token = uuid4().hex

    def __enter__(self):
        return self

    def _validate_root(self):
        try:
            info = self.root.lstat()
            if (not self.root.is_absolute() or not stat.S_ISDIR(info.st_mode)
                    or self.root.is_symlink() or info.st_mode & 0o007
                    or info.st_uid not in {0, os.getuid()}
                    or not os.access(self.root, os.R_OK | os.W_OK | os.X_OK)):
                raise LockError('Lock root is unsafe or inaccessible; check host provisioning.')
        except OSError as exc:
            raise LockError('Persistent lock root unavailable; provision /var/lib/workbench-lite/locks. No fallback is allowed.') from exc

    def acquire(self, identity):
        self._validate_root()
        key = hashlib.sha256(identity.encode('utf-8')).hexdigest()
        if key in self.owned:
            return
        path = self.root / key
        try:
            path.mkdir(mode=0o770)
        except FileExistsError as exc:
            raise LockError(f'Writer lock is held: {key}. Inspect ownership; stale locks require explicit recovery.') from exc
        except OSError as exc:
            raise LockError('Cannot acquire writer lock; check shared operator permissions.') from exc
        self.owned[key] = path
        try:
            atomic_write(path/'owner.json', json_bytes({
                'schema_version': 1, 'token': self.token, 'pid': os.getpid(),
                'uid': os.getuid(), 'hostname': socket.gethostname(),
                'created_at': now(), 'identity_sha256': key,
            }))
            (path/'owner.json').chmod(0o660)
            path.chmod(0o770)
        except Exception:
            # No operation is admitted until ownership is durably recorded.
            self.release()
            raise

    def release(self):
        failures = []
        for key, path in reversed(list(self.owned.items())):
            try:
                owner = path/'owner.json'
                if owner.exists() and json.loads(owner.read_text())['token'] != self.token:
                    raise LockError('Lock ownership changed; manual inspection required.')
                owner.unlink(missing_ok=True)
                path.rmdir()
                del self.owned[key]
            except (OSError, ValueError, KeyError, LockError):
                failures.append(key)
        if failures:
            raise LockError('Cannot release owned writer lock; explicit recovery required: ' + ', '.join(failures))

    def __exit__(self, *args):
        self.release()


def lock_output_and_run(locks, output):
    output = Path(output).resolve()
    locks.acquire('output:' + str(output))
    reference = output / '.workbench-run.json'
    if reference.exists():
        try:
            info = json.loads(reference.read_text())
            run_path = Path(info['run_path']).resolve()
        except (OSError, ValueError, KeyError, TypeError) as exc:
            raise LockError('Cannot read attached run identity for writer exclusion.') from exc
        locks.acquire('run:' + str(run_path))


def lock_destination(locks, endpoint, entries):
    # Bucket guards also exclude overlapping prefixes; no cross-host claim.
    for bucket in sorted({entry.bucket for entry in entries}):
        # A bucket-name guard also covers aliases for the same storage endpoint.
        locks.acquire('destination-bucket:' + bucket)
    for bucket in sorted({entry.bucket for entry in entries}):
        prefix = os.path.commonpath([str(Path(entry.key).parent) for entry in entries if entry.bucket == bucket])
        locks.acquire('destination:' + json.dumps([endpoint, bucket, prefix], separators=(',', ':')))
