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
    host = _normalize_hostname(hostname, bracketed=parsed.netloc.startswith('['))
    _validate_http_loopback(scheme, host)
    _validate_url_path(parsed.path)
    return _format_normalized_url(parsed, scheme, host, port)


def _validate_url_characters(value: str) -> None:
    if not isinstance(value, str) or not value:
        raise ValueError('URL must be a non-empty absolute URL.')
    if any(character.isspace() or unicodedata.category(character) == 'Cc' for character in value):
        raise ValueError('URL cannot contain whitespace or control characters.')
    if '@' in value:
        raise ValueError('URL cannot contain @ characters.')
    if '\\' in value or '?' in value or '#' in value:
        raise ValueError('URL cannot contain backslashes, queries, or fragments.')


def _parse_public_url(value: str):
    try:
        parsed = urlsplit(value)
        if not parsed.netloc.isascii():
            raise ValueError('URL authority must be ASCII.')
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


def join_manifest_url(base_url: str, planned_key: str, prefix: str | None = None) -> str:
    """Join a base with the complete manifest key, preserving key path separators."""
    base = _normalize_join_base(base_url)
    key = strip_key_prefix(planned_key, prefix)
    if not prefix:
        key = key.lstrip('/')
    return f'{base}/{quote(key, safe="/")}'


def join_image_service_url(base_url: str, image_key: str, prefix: str | None = None) -> str:
    """Join a base with an image key encoded as one path segment."""
    base = _normalize_join_base(base_url)
    return f'{base}/{quote(strip_key_prefix(image_key, prefix), safe="")}'


def strip_key_prefix(key: str, prefix: str | None) -> str:
    """Remove one configured prefix at a path boundary."""
    if not prefix:
        return key
    boundary = prefix + '/'
    if not key.startswith(boundary):
        raise ValueError('Key is outside its configured prefix.')
    remainder = key[len(boundary) :]
    if not remainder:
        raise ValueError('Configured prefix leaves an empty key.')
    return remainder


def manifest_url_base(value: str, planned_key: str, prefix: str | None = None) -> str:
    """Return the valid base before a manifest URL's encoded planned key."""
    key = strip_key_prefix(planned_key, prefix)
    encoded_key = quote(key.lstrip('/'), safe='/')
    suffix = '/' + encoded_key
    if not isinstance(value, str) or not encoded_key or not value.endswith(suffix):
        raise ValueError('Manifest @id does not end in its encoded planned key.')
    if prefix and urlsplit(value).path.endswith('/' + quote(planned_key.lstrip('/'), safe='/')):
        raise ValueError('Manifest @id includes its configured key prefix.')
    base = value[: -len(suffix)]
    if not base or base.endswith('/'):
        raise ValueError('Manifest @id must have one separator before its planned key.')
    normalize_public_base_url(base)
    return base


def image_service_url_base(value: str, expected_key: str | None = None) -> str:
    """Return the valid base before a canonically encoded image-key segment."""
    if not isinstance(value, str) or '/' not in value:
        raise ValueError('Image service @id must have an encoded path segment.')
    base, encoded_key = value.rsplit('/', 1)
    if not base or base.endswith('/'):
        raise ValueError('Image service @id must encode its image key in one segment.')
    decoded_key = unquote(encoded_key)
    if '/' not in decoded_key or quote(decoded_key, safe='') != encoded_key:
        raise ValueError('Image service @id has a non-canonical encoded path segment.')
    if expected_key is not None and decoded_key != expected_key:
        raise ValueError('Image service @id does not match its planned key.')
    normalize_public_base_url(base)
    return base


def _has_empty_port(netloc: str) -> bool:
    authority = netloc.rsplit('@', 1)[-1]
    return authority.endswith(':')


def _normalize_hostname(hostname: str, *, bracketed: bool = False) -> str:
    host = hostname.lower()
    if not host.isascii() or host.endswith('.'):
        raise ValueError('Hostname must be an ASCII DNS name or IP literal.')
    try:
        address = ip_address(host)
    except ValueError:
        if bracketed:
            raise ValueError('Bracketed hostname must be a valid IPv6 address.') from None
        if len(host) > 253 or any(not _DNS_LABEL.fullmatch(label) for label in host.split('.')):
            raise ValueError('Hostname must be an ASCII DNS name or IP literal.') from None
        return host
    if bracketed and address.version != 6:
        raise ValueError('Bracketed hostname must be a valid IPv6 address.')
    return host


def _normalize_join_base(value: str) -> str:
    parsed = urlsplit(value)
    if parsed.scheme or parsed.netloc:
        return normalize_public_base_url(value)
    return value.rstrip('/')
