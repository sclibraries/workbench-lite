"""Test harness only: production CLI never reads WBL_TEST_LOCK_DIR."""
import os
import hashlib
from pathlib import Path
import sys
import tempfile

from workbench_lite import cli
from workbench_lite.locks import WriterLocks

_original_main = cli.main


def main(argv=None):
    arguments = list(sys.argv[1:] if argv is None else argv)
    if arguments and arguments[0] in {'check', 'generate', 'push', 'verify-cantaloupe'} and '--run-dir' not in arguments:
        try:
            config_path = Path(arguments[arguments.index('--config') + 1]).resolve()
        except (ValueError, IndexError):
            config_path = Path.cwd().resolve()
        identity = hashlib.sha256(str(config_path).encode('utf-8')).hexdigest()[:20]
        arguments.extend(['--run-dir', str(Path(tempfile.gettempdir())/'workbench-lite-test-runs'/identity)])
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(os.environ.get('WBL_TEST_LOCK_DIR', temporary))
        return _original_main(arguments, lock_factory=lambda: WriterLocks(root))


if __name__ == '__main__':
    cli.main = main
    raise SystemExit(cli.entrypoint())
