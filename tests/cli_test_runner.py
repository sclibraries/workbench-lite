"""Test harness only: production CLI never reads WBL_TEST_LOCK_DIR."""
import os
from pathlib import Path
import tempfile

from workbench_lite import cli
from workbench_lite.locks import WriterLocks

_original_main = cli.main


def main(argv=None):
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(os.environ.get('WBL_TEST_LOCK_DIR', temporary))
        return _original_main(argv, lock_factory=lambda: WriterLocks(root))


if __name__ == '__main__':
    cli.main = main
    raise SystemExit(cli.entrypoint())
