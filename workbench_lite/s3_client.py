import base64
import binascii
from dataclasses import dataclass
import hashlib
from pathlib import Path
from typing import Optional


@dataclass(frozen=True)
class VerificationResult:
    matches: bool
    checksum: Optional[str]
    size_bytes: Optional[int]
    method: str

    def __bool__(self):
        return self.matches

    def evidence(self):
        return {'observed_checksum': self.checksum, 'observed_size_bytes': self.size_bytes,
                'verification_method': self.method}


class Boto3S3Client:
    def __init__(self, client, *, full_readback=False):
        self.client = client
        self.full_readback = full_readback

    def head_bucket(self, **kwargs):
        return self.client.head_bucket(**kwargs)

    def list_objects_v2(self, **kwargs):
        return self.client.list_objects_v2(**kwargs)

    def object_exists(self, bucket, key):
        from botocore.exceptions import ClientError
        # Fail before any write when an older installed SDK lacks conditional PUT.
        model = self.client.meta.service_model.operation_model('PutObject')
        if 'IfNoneMatch' not in model.input_shape.members:
            raise OSError('Installed SDK lacks conditional PutObject support; upgrade boto3.')
        try:
            self.client.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError as exc:
            if exc.response.get('Error', {}).get('Code') in {'404', 'NoSuchKey', 'NotFound'}:
                return False
            raise

    def upload_file(self, source_path, bucket, key, checksum):
        # A HEAD alone cannot prevent a competing writer replacing a serving key.
        # Conditional single PUT fails closed; never fall back to unconditional PUT.
        with Path(source_path).open('rb') as source:
            self.client.put_object(Bucket=bucket, Key=key, Body=source, IfNoneMatch='*',
                                   ChecksumAlgorithm='SHA256',
                                   ChecksumSHA256=base64.b64encode(bytes.fromhex(checksum)).decode('ascii'))

    def verify_file(self, bucket, key, checksum, size_bytes):
        metadata = self.client.head_object(Bucket=bucket, Key=key, ChecksumMode='ENABLED')
        stored = None
        try:
            decoded = base64.b64decode(metadata.get('ChecksumSHA256') or '', validate=True)
            if len(decoded) == 32:
                stored = decoded.hex()
        except (binascii.Error, ValueError, TypeError):
            pass
        size = metadata.get('ContentLength')
        matches = (stored == checksum and size == size_bytes
                   and metadata.get('ChecksumType', 'FULL_OBJECT') == 'FULL_OBJECT')
        result = VerificationResult(matches, stored, size, 's3-sha256')
        if not matches or not self.full_readback:
            return result
        response = self.client.get_object(Bucket=bucket, Key=key)
        body = response['Body']
        try:
            digest = hashlib.sha256()
            length = 0
            for chunk in iter(lambda: body.read(1024 * 1024), b''):
                length += len(chunk)
                digest.update(chunk)
            return VerificationResult(length == size_bytes and digest.hexdigest() == checksum,
                                      digest.hexdigest(), length, 's3-sha256+readback')
        finally:
            body.close()


def create_s3_client(
    endpoint_url: Optional[str] = None,
    region: Optional[str] = None,
    profile: Optional[str] = None,
    *,
    preflight: bool = False,
    full_readback: bool = False,
) -> Boto3S3Client:
    try:
        import boto3  # type: ignore
    except ImportError as error:
        raise RuntimeError(
            "boto3 is required for push --execute. Install workbench-lite requirements first."
        ) from error

    from botocore.config import Config
    client_kwargs = {}
    if preflight:
        client_kwargs['config'] = Config(
            connect_timeout=5, read_timeout=5, retries={'total_max_attempts': 1}
        )
    if endpoint_url:
        client_kwargs["endpoint_url"] = endpoint_url
    if region:
        client_kwargs["region_name"] = region

    if profile:
        session = boto3.Session(profile_name=profile)
        client = session.client("s3", **client_kwargs)
    else:
        client = boto3.client("s3", **client_kwargs)

    return Boto3S3Client(client, full_readback=full_readback)
