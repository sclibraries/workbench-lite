"""Single source for bounded S3 upload timeouts, attempts, backoff and retry classes.

The operator-facing limits and retryable failures are described in
``docs/publication-holds.md``.
"""

import re

try:
    from botocore.exceptions import (
        ConnectionClosedError,
        ConnectionError,
        ConnectTimeoutError,
        EndpointConnectionError,
        ProxyConnectionError,
        ReadTimeoutError,
    )
except ImportError:
    RETRYABLE_CONNECTION_ERRORS = ()
else:
    RETRYABLE_CONNECTION_ERRORS = (
        ConnectionError,
        ConnectionClosedError,
        ConnectTimeoutError,
        EndpointConnectionError,
        ProxyConnectionError,
        ReadTimeoutError,
    )

UPLOAD_CONNECT_TIMEOUT_SECONDS = 10
UPLOAD_READ_TIMEOUT_SECONDS = 60
UPLOAD_MAX_ATTEMPTS = 3
UPLOAD_RETRY_BACKOFF_SECONDS = (1, 4)
RUN_RETRY_WAIT_BUDGET_SECONDS = 5 * 60
RETRYABLE_HTTP_STATUS_CODES = {429, 500, 502, 503, 504}
RETRYABLE_SERVICE_ERROR_CODES = {'SlowDown', 'RequestTimeout'}
NON_RETRYABLE_SERVICE_ERROR_CODES = {'BadDigest', 'InvalidDigest'}


def upload_error_details(error):
    """Return safe error metadata and whether the upload error is transient."""
    response = getattr(error, 'response', None)
    if not isinstance(response, dict):
        response = {}
    service_error = response.get('Error')
    if not isinstance(service_error, dict):
        service_error = {}
    code = service_error.get('Code')
    if not isinstance(code, str) or not re.fullmatch(r'[A-Za-z0-9_.-]{1,80}', code):
        code = None
    metadata = response.get('ResponseMetadata')
    status = metadata.get('HTTPStatusCode') if isinstance(metadata, dict) else None
    if not isinstance(status, int) or not 100 <= status <= 599:
        status = None
    retryable = code not in NON_RETRYABLE_SERVICE_ERROR_CODES and (
        isinstance(error, RETRYABLE_CONNECTION_ERRORS)
        or status in RETRYABLE_HTTP_STATUS_CODES
        or code in RETRYABLE_SERVICE_ERROR_CODES
    )
    return {
        'error_class': type(error).__name__,
        'error_code': code,
        'http_status': status,
        'retryable': retryable,
    }
