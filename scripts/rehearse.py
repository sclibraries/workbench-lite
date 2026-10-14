#!/usr/bin/env python3
"""Run the documented CLI fault scenarios against isolated LocalStack objects."""

import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
TESTS = ROOT / 'tests'
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(TESTS))

from rehearsal_runner import RehearsalController  # noqa: E402
from rehearsal_storage import ENDPOINT, parse_sample, validate_target  # noqa: E402


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--sample',
        action='append',
        type=parse_sample,
        required=True,
        help='copied input as ALIAS=PATH (single, triple, or large)',
    )
    parser.add_argument('--artifact-root', type=Path, required=True)
    parser.add_argument('--endpoint', default=ENDPOINT)
    parser.add_argument('--private-bucket', default='wbl-local-test-sources')
    parser.add_argument('--public-bucket', default='wbl-local-test-derivatives')
    parser.add_argument('--baseline-check-seconds', type=float, default=2.990)
    parser.add_argument('--baseline-dry-run-seconds', type=float, default=5.656)
    args = parser.parse_args(argv)
    validate_target(args.endpoint, args.private_bucket, args.public_bucket)
    if set(dict(args.sample)) != {'single', 'triple', 'large'}:
        parser.error('provide exactly one sample for each alias: single, triple, and large')
    return args


def main(argv=None):
    args = parse_args(argv)
    return RehearsalController(args).run()


if __name__ == '__main__':
    raise SystemExit(main())
