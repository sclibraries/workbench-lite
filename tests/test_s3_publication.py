import base64
import hashlib
import io
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from botocore.exceptions import ClientError
from workbench_lite.s3_client import Boto3S3Client, VerificationResult


class ConditionalStore:
    def __init__(self):
        self.objects = {}
        self.meta = SimpleNamespace(
            service_model=SimpleNamespace(
                operation_model=lambda _: SimpleNamespace(
                    input_shape=SimpleNamespace(members={'IfNoneMatch': None})
                )
            )
        )

    def head_object(self, Bucket, Key, **kwargs):
        if (Bucket, Key) not in self.objects:
            raise ClientError({'Error': {'Code': '404'}}, 'HeadObject')
        value = self.objects[Bucket, Key]
        return {
            'ContentLength': len(value),
            'ChecksumSHA256': base64.b64encode(hashlib.sha256(value).digest()).decode(),
            'ChecksumType': 'FULL_OBJECT',
        }

    def put_object(self, Bucket, Key, Body, IfNoneMatch=None, **kwargs):
        if IfNoneMatch == '*' and (Bucket, Key) in self.objects:
            raise ClientError({'Error': {'Code': 'PreconditionFailed'}}, 'PutObject')
        self.objects[Bucket, Key] = Body.read()

    def get_object(self, Bucket, Key):
        self.body = io.BytesIO(self.objects[Bucket, Key])
        return {'Body': self.body}


class S3PublicationTests(unittest.TestCase):
    def test_race_after_absence_check_cannot_replace_existing_key(self):
        store = ConditionalStore()
        client = Boto3S3Client(store)
        self.assertFalse(client.object_exists('bucket', 'key'))
        store.objects['bucket', 'key'] = b'other writer'
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / 'source'
            path.write_bytes(b'new bytes')
            with self.assertRaises(ClientError):
                client.upload_file(
                    path, 'bucket', 'key', hashlib.sha256(path.read_bytes()).hexdigest()
                )
        self.assertEqual(store.objects['bucket', 'key'], b'other writer')

    def test_readback_checks_actual_bytes_size_and_closes_body(self):
        store = ConditionalStore()
        store.objects['bucket', 'key'] = b'stored bytes'
        client = Boto3S3Client(store, full_readback=True)
        checksum = hashlib.sha256(b'stored bytes').hexdigest()
        self.assertTrue(client.verify_file('bucket', 'key', checksum, 12))
        self.assertTrue(store.body.closed)
        self.assertFalse(client.verify_file('bucket', 'key', checksum, 11))
        self.assertFalse(client.verify_file('bucket', 'key', 'wrong', 12))
        self.assertTrue(store.body.closed)

    def test_permission_denial_is_not_treated_as_absence(self):
        store = ConditionalStore()

        def denied(**kwargs):
            raise ClientError({'Error': {'Code': 'AccessDenied'}}, 'HeadObject')

        store.head_object = denied
        with self.assertRaises(ClientError):
            Boto3S3Client(store).object_exists('bucket', 'key')

    def test_old_sdk_fails_closed_before_destination_checks(self):
        store = ConditionalStore()
        store.meta.service_model.operation_model = lambda _: SimpleNamespace(
            input_shape=SimpleNamespace(members={})
        )
        with self.assertRaises(OSError):
            Boto3S3Client(store).object_exists('bucket', 'key')

    def test_put_supplies_the_expected_checksum_and_conditional_header(self):
        from unittest.mock import Mock
        import base64

        store = Mock()
        value = b'local bytes'
        checksum = hashlib.sha256(value).hexdigest()
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / 'source'
            path.write_bytes(value)
            Boto3S3Client(store).upload_file(path, 'bucket', 'key', checksum)
        request = store.put_object.call_args.kwargs
        self.assertEqual(request['IfNoneMatch'], '*')
        self.assertEqual(request['ChecksumAlgorithm'], 'SHA256')
        self.assertEqual(
            request['ChecksumSHA256'], base64.b64encode(bytes.fromhex(checksum)).decode('ascii')
        )

    def test_default_verification_uses_stored_sha256_and_size_without_get(self):
        from unittest.mock import Mock
        import base64

        checksum = hashlib.sha256(b'stored bytes').hexdigest()
        store = Mock()
        store.head_object.return_value = {
            'ChecksumSHA256': base64.b64encode(bytes.fromhex(checksum)).decode(),
            'ContentLength': 12,
            'ChecksumType': 'FULL_OBJECT',
        }
        client = Boto3S3Client(store)
        self.assertTrue(client.verify_file('bucket', 'key', checksum, 12))
        self.assertIsNone(VerificationResult(False, None, None, 'missing').checksum_type)
        store.head_object.assert_called_once_with(
            Bucket='bucket', Key='key', ChecksumMode='ENABLED'
        )
        store.get_object.assert_not_called()
        cases = [
            ({'ChecksumSHA256': None}, False),
            ({'ChecksumSHA256': 'bad base64'}, False),
            ({'ContentLength': 11}, False),
            ({'ChecksumType': 'COMPOSITE'}, False),
            ({}, True),
        ]
        for changes, missing_type in cases:
            with self.subTest(changes=changes, missing_type=missing_type):
                saved = dict(store.head_object.return_value)
                if missing_type:
                    store.head_object.return_value.pop('ChecksumType')
                else:
                    store.head_object.return_value.update(changes)
                result = client.verify_file('bucket', 'key', checksum, 12)
                self.assertFalse(result)
                if missing_type:
                    self.assertIsNone(result.evidence()['checksum_type'])
                    self.assertIsNone(client.inspect_object('bucket', 'key').checksum_type)
                store.head_object.return_value = saved
        store.get_object.assert_not_called()

    def test_optional_readback_detects_body_mismatch_after_good_head(self):
        from unittest.mock import Mock

        value = b'expected'
        store = Mock()
        store.head_object.return_value = {
            'ChecksumSHA256': base64.b64encode(hashlib.sha256(value).digest()).decode(),
            'ContentLength': len(value),
            'ChecksumType': 'FULL_OBJECT',
        }
        body = io.BytesIO(b'changed!')
        store.get_object.return_value = {'Body': body}
        client = Boto3S3Client(store, full_readback=True)
        checksum = hashlib.sha256(value).hexdigest()
        self.assertFalse(client.verify_file('bucket', 'key', checksum, len(value)))
        self.assertTrue(body.closed)

        store.head_object.return_value.pop('ChecksumType')
        store.get_object.reset_mock()
        store.get_object.return_value = {'Body': io.BytesIO(value)}
        result = client.verify_file('bucket', 'key', checksum, len(value))
        self.assertFalse(result)
        self.assertIsNone(result.evidence()['checksum_type'])
        store.get_object.assert_not_called()

    def test_checksum_head_access_denial_does_not_fall_back_to_download(self):
        from unittest.mock import Mock

        store = Mock()
        store.head_object.side_effect = ClientError(
            {'Error': {'Code': 'AccessDenied'}}, 'HeadObject'
        )
        with self.assertRaises(ClientError):
            Boto3S3Client(store).verify_file('bucket', 'key', 'a' * 64, 1)
        store.get_object.assert_not_called()
