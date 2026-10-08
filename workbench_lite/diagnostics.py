"""Public diagnostics exclude exception payloads and credential-bearing URLs."""
import re


def operation_error(error: Exception) -> str:
    # SDK/server exception strings can contain credentials or signed requests.
    # File identity lives in the result; classify errors without echoing payloads.
    name = type(error).__name__
    code = _service_error_code(error)
    if code:
        name += f" ({code})"
    if isinstance(error, PermissionError):
        advice = "Check file or storage permissions."
    elif isinstance(error, FileNotFoundError):
        advice = "Required file disappeared or is unavailable."
    else:
        advice = "Check file availability, storage access and connectivity."
    return f"{name}: {advice}"


def _service_error_code(error: Exception):
    # Managed transfers may wrap ClientError. Preserve only the structured code,
    # never the SDK message, headers or request URL from the exception chain.
    seen = set()
    while error is not None and id(error) not in seen:
        seen.add(id(error))
        response = getattr(error, "response", None)
        detail = response.get("Error") if isinstance(response, dict) else None
        code = detail.get("Code") if isinstance(detail, dict) else None
        if isinstance(code, str) and re.fullmatch(r"[A-Za-z0-9_.-]{1,80}", code):
            return code
        error = error.__cause__ or error.__context__
    return None


def sanitize(value):
    if isinstance(value, dict):
        return {key: '[redacted]' if str(key).lower() in {
            'password', 'secret', 'token', 'authorization', 'access_key', 'api_key',
            'aws_access_key_id', 'aws_secret_access_key', 'aws_session_token',
        } else sanitize(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [sanitize(item) for item in value]
    if not isinstance(value, str):
        return value
    value = re.sub(r"(?i)(authorization\s*[:=]\s*)(?:bearer|basic)\s+[^\s,;]+", r"\1[redacted]", value)
    value = re.sub(r"(https?://)[^/\s@]+@", r"\1[redacted]@", value, flags=re.I)
    value = re.sub(r"(https?://[^\s?]+)\?[^\s]*", r"\1?[redacted]", value, flags=re.I)
    return value
