"""Subprocess entry point used only by the integrated rehearsal tests."""

import os
from pathlib import Path
import sys
import time

from rehearsal_harness import FaultClient, FileS3Client
from workbench_lite import cli
from workbench_lite.s3_client import create_s3_client as make_boto_client


class WorkerClient(FaultClient):
    def __init__(self, client, mode):
        super().__init__(client, mode)
        self.mode = mode
        self.upload_count = 0
        self.delete_count = 0

    def _barrier(self):
        marker = os.environ.get('REHEARSAL_READY_FILE')
        if marker:
            path = Path(marker)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text('ready', encoding='ascii')
        if os.environ.get('REHEARSAL_HOLD_BARRIER') == 'true':
            while True:
                time.sleep(1)
        release = os.environ.get('REHEARSAL_RELEASE_FILE')
        if release:
            release_path = Path(release)
            deadline = time.monotonic() + 120
            while not release_path.exists() and time.monotonic() < deadline:
                time.sleep(0.02)

    def upload_file(self, source_path, bucket, key, checksum):
        self.upload_count += 1
        if self.mode == f'upload_before_{self.upload_count}':
            self._barrier()
        if self.mode == f'upload_after_{self.upload_count}':
            result = self._wrapped.upload_file(source_path, bucket, key, checksum)
            self._barrier()
            return result
        return super().upload_file(source_path, bucket, key, checksum)

    def delete_object(self, bucket, key, *, if_match):
        self.delete_count += 1
        result = self._wrapped.delete_object(bucket, key, if_match=if_match)
        if self.mode == f'delete_after_{self.delete_count}':
            self._barrier()
        return result


def main(arguments):
    mode = os.environ.get('REHEARSAL_FAULT', '') or None
    backend = os.environ.get('REHEARSAL_BACKEND', 'file')
    storage_root = os.environ.get('REHEARSAL_STORAGE_ROOT')
    if backend == 'file' and not storage_root:
        raise RuntimeError('A temporary synthetic storage root is required.')

    def factory(endpoint_url=None, region=None, profile=None, **kwargs):
        if backend == 'localstack':
            if endpoint_url != 'http://localhost:4566' or profile:
                raise RuntimeError(
                    'Rehearsal storage endpoint/profile guard refused this client.'
                )
            client = make_boto_client(endpoint_url, region, None, **kwargs)
        elif backend == 'file':
            client = FileS3Client(
                storage_root, endpoint=endpoint_url or 'http://synthetic.invalid'
            )
        else:
            raise RuntimeError('Unknown rehearsal storage backend.')
        return WorkerClient(client, mode)

    lock_root = os.environ.get('REHEARSAL_LOCK_ROOT')
    if not lock_root:
        raise RuntimeError('A temporary lock root is required.')
    os.environ['WBL_TEST_LOCK_DIR'] = lock_root
    cli.create_s3_client = factory
    from cli_test_runner import main as test_main

    cli.main = test_main
    sys.argv = ['workbench-lite', *arguments]
    return cli.entrypoint()


if __name__ == '__main__':
    raise SystemExit(main(sys.argv[1:]))
