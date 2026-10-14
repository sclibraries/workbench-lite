"""LocalStack boundaries, storage checks, and copied sample preparation."""

import argparse
import base64
import hashlib
import os
from pathlib import Path
import re
import shutil

from rehearsal_harness import stream_identity

ENDPOINT = 'http://localhost:4566'
SENTINEL_BODIES = {
    'preexisting.txt': b'preexisting rehearsal sentinel',
    'unrelated.txt': b'unrelated rehearsal sentinel',
}


def parse_sample(value):
    if '=' not in value:
        raise argparse.ArgumentTypeError('samples must use ALIAS=PATH syntax')
    alias, path = value.split('=', 1)
    if alias not in {'single', 'triple', 'large'}:
        raise argparse.ArgumentTypeError('sample alias must be single, triple, or large')
    source = Path(path).expanduser().resolve()
    if not source.is_dir():
        raise argparse.ArgumentTypeError(f'sample directory does not exist for alias {alias}')
    return alias, source


def validate_target(endpoint, private_bucket, public_bucket):
    if endpoint != ENDPOINT:
        raise ValueError('Only the specified loopback LocalStack endpoint is permitted.')
    pattern = re.compile(r'^wbl-local-test-[a-z0-9][a-z0-9.-]*[a-z0-9]$')
    if not pattern.fullmatch(private_bucket) or not pattern.fullmatch(public_bucket):
        raise ValueError('Both rehearsal buckets must use the wbl-local-test-* namespace.')
    if private_bucket == public_bucket:
        raise ValueError('Private and public rehearsal buckets must differ.')


def ensure_bucket(client, bucket):
    from botocore.exceptions import ClientError

    try:
        client.head_bucket(Bucket=bucket)
    except ClientError as exc:
        code = exc.response.get('Error', {}).get('Code')
        status = exc.response.get('ResponseMetadata', {}).get('HTTPStatusCode')
        if code not in {'404', 'NoSuchBucket', 'NotFound'} and status != 404:
            raise
        client.create_bucket(Bucket=bucket)


def put_bytes(client, bucket, key, body):
    checksum = base64.b64encode(hashlib.sha256(body).digest()).decode('ascii')
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ChecksumAlgorithm='SHA256',
        ChecksumSHA256=checksum,
    )


def full_identity(storage, bucket, key):
    identity = storage.inspect_object(bucket, key)
    if identity is None:
        return None
    checksum = identity.checksum
    if checksum is None:
        response = storage.client.get_object(Bucket=bucket, Key=key)
        body = response['Body']
        try:
            checksum, size = stream_identity(body)
            if size != identity.size_bytes:
                raise ValueError('Readback size changed during checksum inspection.')
        finally:
            body.close()
    return {
        'sha256': checksum,
        'size_bytes': identity.size_bytes,
        'etag': identity.etag,
        'checksum_type': identity.checksum_type,
    }


def storage_listing(storage, bucket, prefix):
    result = {}
    token = None
    while True:
        options = {'Bucket': bucket, 'Prefix': prefix, 'MaxKeys': 1000}
        if token:
            options['ContinuationToken'] = token
        response = storage.client.list_objects_v2(**options)
        for item in response.get('Contents', []):
            key = item['Key']
            identity = full_identity(storage, bucket, key)
            result[f'{bucket}/{key}'] = identity
        if not response.get('IsTruncated'):
            break
        token = response.get('NextContinuationToken')
        if not token:
            raise RuntimeError('LocalStack listing was truncated without a continuation token.')
    return result


def checksum_sentinels(storage, bucket_keys):
    states = {}
    ok = True
    for bucket, key, expected in bucket_keys:
        observed = full_identity(storage, bucket, key)
        states[f'{bucket}/{key}'] = {'expected_sha256': expected, 'observed': observed}
        ok = ok and observed is not None and observed['sha256'] == expected
    return ok, states


def copy_sample(source, destination):
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(source, destination, copy_function=shutil.copy2)
    return destination


def sample_inputs(source_root):
    configs = sorted([*source_root.rglob('*.yml'), *source_root.rglob('*.yaml')])
    csvs = sorted(source_root.rglob('*.csv'))
    if len(configs) != 1 or len(csvs) != 1:
        raise ValueError('Each supplied sample copy must contain one YAML config and one CSV.')
    return configs[0], csvs[0]


def make_config(source_root, directory, bucket_private, bucket_public, prefix):
    import yaml

    config_source, csv_source = sample_inputs(source_root)
    value = yaml.safe_load(config_source.read_text(encoding='utf-8')) or {}
    configured_input = Path(str(value.get('input_dir') or config_source.parent))
    original_input = (
        configured_input
        if configured_input.is_absolute()
        else config_source.parent / configured_input
    )
    original_input = original_input.resolve()
    try:
        input_relative = original_input.relative_to(source_root.resolve())
        csv_relative = csv_source.resolve().relative_to(original_input)
    except ValueError as exc:
        raise ValueError(
            'Sample inputs must stay inside the supplied copied sample directory.'
        ) from exc
    value['input_dir'] = str((source_root / input_relative).resolve())
    value['input_csv'] = csv_relative.as_posix()
    value['s3_private_bucket'] = bucket_private
    value['s3_public_bucket'] = bucket_public
    value['s3_prefix'] = prefix
    target = directory / 'config.yml'
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(yaml.safe_dump(value, sort_keys=False), encoding='utf-8')
    return target


def hardlink_tree(source, destination):
    destination.mkdir(parents=True, exist_ok=False)
    for path in source.rglob('*'):
        relative = path.relative_to(source)
        target = destination / relative
        if path.is_dir():
            target.mkdir(parents=True, exist_ok=True)
        elif path.is_file():
            target.parent.mkdir(parents=True, exist_ok=True)
            os.link(path, target)


def cleanup_prefix(storage, buckets, prefix):
    before = {bucket: storage_listing(storage, bucket, prefix) for bucket in buckets}
    outcomes = []
    for bucket in buckets:
        keys = [full.split('/', 1)[1] for full in before[bucket]]
        for key in keys:
            expected = before[bucket][f'{bucket}/{key}']
            outcomes.append(_cleanup_object(storage, bucket, key, expected))
    after = {bucket: storage_listing(storage, bucket, prefix) for bucket in buckets}
    return {
        'before': before,
        'deletes': outcomes,
        'after': after,
        'complete': all(not values for values in after.values())
        and all(item['deleted'] for item in outcomes),
    }


def _cleanup_object(storage, bucket, key, expected):
    observed = full_identity(storage, bucket, key)
    if observed is None or observed['sha256'] != expected['sha256']:
        return _cleanup_result(bucket, key, 'checksum changed before cleanup')
    if not observed['etag']:
        return _cleanup_result(bucket, key, 'ETag unavailable for conditional cleanup')
    storage.delete_object(bucket, key, if_match=observed['etag'])
    result = {
        'bucket': bucket,
        'key': key,
        'deleted': True,
        'sha256': observed['sha256'],
        'if_match': observed['etag'],
    }
    if full_identity(storage, bucket, key) is not None:
        result.update(deleted=False, reason='object remains after conditional delete')
    return result


def _cleanup_result(bucket, key, reason):
    return {'bucket': bucket, 'key': key, 'deleted': False, 'reason': reason}
