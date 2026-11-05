"""Validation and joining for public URLs embedded in generated manifests."""

from ipaddress import ip_address
import re
from urllib.parse import quote, unquote, urlsplit, urlunsplit
import unicodedata


_LOOPBACK_HTTP_HOSTS = {'localhost', '127.0.0.1', '::1'}
_DNS_LABEL = re.compile(r'[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?\Z')
_BAD_PERCENT_ESCAPE = re.compile(r'%(?![0-9A-Fa-f]{2})')


def normalize_public_base_url(value: str) -> str:
    """Validate and normalize an absolute HTTP(S) base URL."""
    _validate_url_characters(value)
    parsed, hostname, port = _parse_public_url(value)
    scheme = parsed.scheme.lower()
    _validate_url_authority(parsed.netloc, hostname, scheme)
    _validate_url_userinfo(parsed)
    _validate_url_port(parsed.netloc, port)
    host = _normalize_hostname(hostname)
    _validate_http_loopback(scheme, host)
    _validate_url_path(parsed.path)
    return _format_normalized_url(parsed, scheme, host, port)


def _validate_url_characters(value: str) -> None:
    if not isinstance(value, str) or not value:
        raise ValueError('URL must be a non-empty absolute URL.')
    if any(character.isspace() or unicodedata.category(character) == 'Cc' for character in value):
        raise ValueError('URL cannot contain whitespace or control characters.')
    if '\\' in value or '?' in value or '#' in value:
        raise ValueError('URL cannot contain backslashes, queries, or fragments.')


def _parse_public_url(value: str):
    try:
        parsed = urlsplit(value)
        hostname = parsed.hostname
        port = parsed.port
    except ValueError as exc:
        raise ValueError('URL authority or port is invalid.') from exc
    return parsed, hostname, port


def _validate_url_authority(netloc, hostname, scheme):
    if scheme not in {'http', 'https'} or not hostname or not netloc:
        raise ValueError('URL must use an absolute HTTP(S) address.')


def _validate_url_userinfo(parsed):
    if '@' in parsed.netloc or parsed.username is not None or parsed.password is not None:
        raise ValueError('URL cannot contain user information.')


def _validate_url_port(netloc, port):
    if _has_empty_port(netloc):
        raise ValueError('URL port cannot be empty.')
    if port is not None and not 1 <= port <= 65535:
        raise ValueError('URL port must be between 1 and 65535.')


def _validate_http_loopback(scheme, host):
    if scheme == 'http' and host not in _LOOPBACK_HTTP_HOSTS:
        raise ValueError('Plain HTTP is allowed only for the approved loopback hosts.')


def _validate_url_path(path):
    if _BAD_PERCENT_ESCAPE.search(path):
        raise ValueError('URL path contains a malformed percent escape.')


def _format_normalized_url(parsed, scheme, host, port):
    netloc = f'[{host}]' if ':' in host else host
    if port is not None:
        netloc += f':{port}'
    return urlunsplit((scheme, netloc, parsed.path.rstrip('/'), '', ''))


def join_manifest_url(base_url: str, planned_key: str) -> str:
    """Join a base with the complete manifest key, preserving key path separators."""
    base = _normalize_join_base(base_url)
    return f'{base}/{quote(planned_key.lstrip("/"), safe="/")}'


def join_image_service_url(base_url: str, image_key: str) -> str:
    """Join a base with an image key encoded as one path segment."""
    base = _normalize_join_base(base_url)
    return f'{base}/{quote(image_key, safe="")}'


def manifest_url_base(value: str, planned_key: str) -> str:
    """Return the valid base before a manifest URL's encoded planned key."""
    encoded_key = quote(planned_key.lstrip('/'), safe='/')
    suffix = '/' + encoded_key
    if not isinstance(value, str) or not encoded_key or not value.endswith(suffix):
        raise ValueError('Manifest @id does not end in its encoded planned key.')
    base = value[: -len(suffix)]
    if not base or base.endswith('/'):
        raise ValueError('Manifest @id must have one separator before its planned key.')
    normalize_public_base_url(base)
    return base


def image_service_url_base(value: str) -> str:
    """Return the valid base before a canonically encoded image-key segment."""
    if not isinstance(value, str) or '/' not in value:
        raise ValueError('Image service @id must have an encoded path segment.')
    base, encoded_key = value.rsplit('/', 1)
    if not base or base.endswith('/') or '%2F' not in encoded_key.upper():
        raise ValueError('Image service @id must encode its image key in one segment.')
    if quote(unquote(encoded_key), safe='') != encoded_key:
        raise ValueError('Image service @id has a non-canonical encoded path segment.')
    normalize_public_base_url(base)
    return base


def _has_empty_port(netloc: str) -> bool:
    authority = netloc.rsplit('@', 1)[-1]
    return authority.endswith(':')


def _normalize_hostname(hostname: str) -> str:
    host = hostname.lower()
    if not host.isascii() or host.endswith('.'):
        raise ValueError('Hostname must be an ASCII DNS name or IP literal.')
    try:
        ip_address(host)
        return host
    except ValueError:
        if len(host) > 253 or any(not _DNS_LABEL.fullmatch(label) for label in host.split('.')):
            raise ValueError('Hostname must be an ASCII DNS name or IP literal.') from None
        return host


def _normalize_join_base(value: str) -> str:
    parsed = urlsplit(value)
    if parsed.scheme or parsed.netloc:
        return normalize_public_base_url(value)
    return value.rstrip('/')
