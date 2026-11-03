"""Deterministic S3 reservations for package object and page IDs."""

import hashlib
import json
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

from .check import _canonical_id
from .config import ConfigError
from .diagnostics import operation_error
from .storage_plan import DEFAULT_PREFIX, DEFAULT_PRIVATE_BUCKET, _join_key, _normalize_segment

try:
    from botocore.exceptions import BotoCoreError, ClientError
    from boto3.exceptions import S3UploadFailedError
except ImportError:
    STORAGE_ERRORS = (OSError,)
else:
    STORAGE_ERRORS = (OSError, BotoCoreError, ClientError, S3UploadFailedError)


@dataclass(frozen=True)
class IdClaim:
    bucket: str
    key: str
    raw_id: str
    canonical_id: str
    batch_id: str
    kind: str
    parent_id: str | None
    body: bytes
    checksum: str
    size_bytes: int

    def identity(self):
        return {
            'bucket': self.bucket,
            'key': self.key,
            'checksum': self.checksum,
            'size_bytes': self.size_bytes,
        }


@dataclass(frozen=True)
class _ClaimResult:
    status: str
    outcome: str
    observed: object | None = None
    evidence: dict | None = None


@dataclass
class ClaimEvidence:
    expected: set[tuple]
    latest: dict[tuple, dict]
    counts: dict[str, int]
    created: set[tuple]
    expectations_valid: bool

    @property
    def settled(self):
        return self.expectations_valid and all(
            _claim_result_settled(self.latest.get(identity), identity) for identity in self.expected
        )

    @property
    def has_confirmed_creation(self):
        return bool(self.created)

    def retained_claims(self):
        retained = []
        for identity in sorted(self.expected, key=lambda item: item[1]):
            status = _claim_status(self.latest.get(identity))
            if status in {'created', 'matched', 'unknown', 'unsettled'}:
                retained.append(_retained_claim(identity, status))
        return retained


@dataclass
class ClaimCheckResult:
    expected_count: int
    status: str = 'checked'
    created_count: int = 0
    matched_count: int = 0
    conflict_count: int = 0
    errors: list[str] = field(default_factory=list)
    refusal_code: int = 0

    def to_dict(self):
        return {
            'status': self.status,
            'expected_count': self.expected_count,
            'created_count': self.created_count,
            'matched_count': self.matched_count,
            'conflict_count': self.conflict_count,
        }


def build_id_claims(objects, config, config_path):
    prefix = _effective_prefix(config)
    bucket = str(config.get('s3_private_bucket') or DEFAULT_PRIVATE_BUCKET)
    batch_id = _normalize_segment(str(config.get('batch_id') or Path(config_path).stem))
    claims = []
    for workbench_object in objects:
        claims.append(_make_claim(prefix, bucket, batch_id, workbench_object.object_id, 'object'))
        for page in workbench_object.pages:
            claims.append(
                _make_claim(
                    prefix,
                    bucket,
                    batch_id,
                    page.page_id,
                    'page',
                    workbench_object.object_id,
                )
            )
    return sorted(claims, key=lambda claim: claim.canonical_id)


def claim_identities_from_plan(entries, config, batch_id):
    prefix = _effective_prefix(config)
    bucket = str(config.get('s3_private_bucket') or DEFAULT_PRIVATE_BUCKET)
    normalized_batch = _normalize_segment(str(config.get('batch_id') or batch_id))
    objects = _planned_object_ids(entries)
    claims = []
    for object_id, page_ids in objects.items():
        claims.append(_make_claim(prefix, bucket, normalized_batch, object_id, 'object'))
        claims.extend(
            _make_claim(prefix, bucket, normalized_batch, page_id, 'page', object_id)
            for page_id in page_ids
        )
    if not claims:
        raise ValueError('Immutable upload plan contains no claimable IDs.')
    return {_claim_identity(claim.identity()) for claim in claims}


def _planned_object_ids(entries):
    objects = {}
    for entry in entries:
        if not isinstance(entry, dict):
            raise ValueError('Upload plan entries have an invalid structure.')
        object_id = entry.get('object_id', '')
        page_id = entry.get('page_id', '')
        if not isinstance(object_id, str) or not isinstance(page_id, str):
            raise ValueError('Upload plan identifiers have an invalid structure.')
        if page_id and not object_id:
            raise ValueError('Upload plan page has no parent object ID.')
        if object_id:
            objects.setdefault(object_id, set())
            if page_id:
                objects[object_id].add(page_id)
    return objects


def reduce_claim_evidence(events, expected_claims=None, *, require_context=False):
    context_sets, contexts_valid, has_context = _claim_context_sets(events)
    expected = set(expected_claims) if expected_claims is not None else set().union(*context_sets)
    valid = contexts_valid and (not require_context or has_context)
    if expected_claims is not None and any(claims != expected for claims in context_sets):
        valid = False
    latest, created, operations_valid = _claim_operations(events, expected)
    valid = valid and operations_valid
    counts = _claim_counts(expected, latest)
    return ClaimEvidence(expected, latest, counts, created, valid)


def _claim_context_sets(events):
    contexts = [
        event for event in events if event['kind'] == 'context' and event.get('stage') == 'claim'
    ]
    sets = []
    valid = True
    for context in contexts:
        claims = context.get('expected_claims')
        if not isinstance(claims, list) or any(
            not isinstance(claim, dict) or not _valid_claim_identity(claim) for claim in claims
        ):
            return sets, False, bool(contexts)
        identities = [_claim_identity(claim) for claim in claims]
        claim_set = set(identities)
        valid = valid and len(identities) == len(claim_set)
        sets.append(claim_set)
    if sets and any(claim_set != sets[0] for claim_set in sets[1:]):
        valid = False
    return sets, valid, bool(contexts)


def _claim_operations(events, expected):
    intents, latest, created = {}, {}, set()
    valid = True
    for event in events:
        if event.get('stage') != 'claim' or event['kind'] not in {'intent', 'result'}:
            continue
        operation_id = event.get('operation_id')
        if (
            not _valid_claim_identity(event)
            or not isinstance(operation_id, str)
            or not operation_id
        ):
            valid = False
            continue
        identity = _claim_identity(event)
        if identity not in expected:
            valid = False
            continue
        if event['kind'] == 'intent':
            intents[operation_id] = identity
            latest[identity] = event
        elif intents.get(operation_id) == identity:
            latest[identity] = event
            if _claim_result_created(event, identity):
                created.add(identity)
        else:
            valid = False
    return latest, created, valid


def _claim_counts(expected, latest):
    statuses = {status: 0 for status in ('created', 'matched', 'absent', 'conflict', 'unknown')}
    counts = {'expected': len(expected), **statuses, 'unsettled': 0}
    for identity in expected:
        status = _claim_status(latest.get(identity))
        counts[status if status in statuses else 'unsettled'] += 1
    return counts


def _claim_status(event):
    status = event.get('status') if event and event.get('kind') == 'result' else 'unsettled'
    return (
        status if status in {'created', 'matched', 'absent', 'conflict', 'unknown'} else 'unsettled'
    )


def _claim_result_settled(event, identity):
    return bool(
        event
        and event.get('kind') == 'result'
        and event.get('status') in {'created', 'matched'}
        and event.get('remote_outcome') in {'written', 'matched'}
        and event.get('observed_checksum') == identity[2]
        and event.get('observed_size_bytes') == identity[3]
        and event.get('checksum_type') == 'FULL_OBJECT'
    )


def _claim_result_created(event, identity):
    return bool(
        event.get('status') == 'created'
        and event.get('remote_outcome') == 'written'
        and _claim_result_settled(event, identity)
    )


def _retained_claim(identity, status):
    return {
        'bucket': identity[0],
        'key': identity[1],
        'checksum': identity[2],
        'size_bytes': identity[3],
        'status': status,
    }


def _claim_identity(claim):
    return claim.get('bucket'), claim.get('key'), claim.get('checksum'), claim.get('size_bytes')


def _valid_claim_identity(claim):
    checksum = claim.get('checksum')
    size = claim.get('size_bytes')
    return (
        isinstance(claim.get('bucket'), str)
        and bool(claim['bucket'])
        and isinstance(claim.get('key'), str)
        and bool(claim['key'])
        and isinstance(checksum, str)
        and len(checksum) == 64
        and all(character in '0123456789abcdef' for character in checksum)
        and isinstance(size, int)
        and not isinstance(size, bool)
        and size >= 0
    )


def _effective_prefix(config):
    configured = config.get('s3_prefix')
    if configured is not None and not str(configured).strip('/'):
        raise ConfigError('Configuration s3_prefix contains an unsafe key segment.')
    prefix = _join_key(str(configured or DEFAULT_PREFIX))
    if not prefix:
        raise ConfigError('Configuration s3_prefix contains an unsafe key segment.')
    return prefix


def _make_claim(prefix, bucket, batch_id, raw_id, kind, parent_id=None):
    canonical_id = _canonical_id(raw_id)
    body_fields = {
        'batch_id': batch_id,
        'canonical_id': canonical_id,
        'id': raw_id,
        'kind': kind,
        'schema_version': 1,
    }
    if parent_id is not None:
        body_fields['parent_id'] = parent_id
    body = json.dumps(
        body_fields, ensure_ascii=False, sort_keys=True, separators=(',', ':')
    ).encode('utf-8')
    return IdClaim(
        bucket=bucket,
        key=f'{prefix}/_ids/{canonical_id}.json',
        raw_id=raw_id,
        canonical_id=canonical_id,
        batch_id=batch_id,
        kind=kind,
        parent_id=parent_id,
        body=body,
        checksum=hashlib.sha256(body).hexdigest(),
        size_bytes=len(body),
    )


def check_id_claims(claims, read_client, write_client, journal, *, dry_run):
    claims = list(claims)
    result = ClaimCheckResult(len(claims))
    _record_expected_claims(claims, journal)
    if read_client is None:
        result.status = 'not_run'
        return result
    missing = _read_claims(claims, read_client, journal, result)
    if result.refusal_code or not missing or dry_run:
        return result
    if write_client is None:
        return _fail(result, 'ID claim write failed: no upload client is available.')
    _create_claims(missing, read_client, write_client, journal, result)
    return result


def _record_expected_claims(claims, journal):
    if journal is None:
        return
    journal.append(
        'context',
        stage='claim',
        expected_claims=[claim.identity() for claim in claims],
    )


def _read_claims(claims, client, journal, result):
    missing = []
    for claim in claims:
        operation = journal.intent('claim', claim.identity()) if journal else None
        try:
            observed = client.inspect_object(claim.bucket, claim.key)
            if observed is None:
                _record_claim_result(journal, operation, claim, _ClaimResult('absent', 'absent'))
                missing.append(claim)
            elif _identity_matches(observed, claim):
                _record_claim_result(
                    journal, operation, claim, _ClaimResult('matched', 'matched', observed)
                )
                result.matched_count += 1
            else:
                _record_read_conflict(claim, client, journal, operation, observed, result)
        except STORAGE_ERRORS as exc:
            _record_claim_result(journal, operation, claim, _ClaimResult('unknown', 'unknown'))
            _fail(result, f'ID claim read failed for {claim.raw_id!r}: {operation_error(exc)}')
            return missing
    return missing


def _record_read_conflict(claim, client, journal, operation, observed, result):
    owner = _read_owner(client, claim.bucket, claim.key)
    message = _conflict_message(claim, owner)
    _record_claim_result(journal, operation, claim, _ClaimResult('conflict', 'conflict', observed))
    result.status = 'conflict'
    result.refusal_code = 3
    result.conflict_count += 1
    result.errors.append(message)


def _create_claims(claims, read_client, write_client, journal, result):
    try:
        with tempfile.TemporaryDirectory(prefix='workbench-id-claims-') as directory:
            for claim in claims:
                if not _create_claim(claim, directory, read_client, write_client, journal, result):
                    return
    except OSError as exc:
        _fail(result, f'ID claim staging failed: {operation_error(exc)}')


def _create_claim(claim, directory, read_client, write_client, journal, result):
    source = Path(directory) / f'{claim.checksum}.json'
    source.write_bytes(claim.body)
    operation = journal.intent('claim', claim.identity()) if journal else None
    try:
        write_client.upload_file(str(source), claim.bucket, claim.key, claim.checksum)
    except STORAGE_ERRORS as exc:
        if _is_precondition_failed(exc):
            return _resolve_claim_race(claim, read_client, journal, operation, result)
        _record_claim_result(journal, operation, claim, _ClaimResult('unknown', 'unknown'))
        _fail(result, f'ID claim write failed for {claim.raw_id!r}: {operation_error(exc)}')
        return False
    return _verify_created_claim(claim, write_client, journal, operation, result)


def _resolve_claim_race(claim, client, journal, operation, result):
    try:
        observed = client.inspect_object(claim.bucket, claim.key)
        if observed is not None and _identity_matches(observed, claim):
            _record_claim_result(
                journal, operation, claim, _ClaimResult('matched', 'matched', observed)
            )
            result.matched_count += 1
            return True
        owner = _read_owner(client, claim.bucket, claim.key) if observed is not None else None
    except STORAGE_ERRORS as exc:
        _record_claim_result(journal, operation, claim, _ClaimResult('unknown', 'unknown'))
        message = f'ID claim reconciliation failed for {claim.raw_id!r}: {operation_error(exc)}'
        _fail(result, message)
        return False
    _record_claim_result(journal, operation, claim, _ClaimResult('conflict', 'conflict', observed))
    result.status = 'conflict'
    result.refusal_code = 3
    result.conflict_count += 1
    result.errors.append(_conflict_message(claim, owner))
    return False


def _verify_created_claim(claim, client, journal, operation, result):
    try:
        observed = client.inspect_object(claim.bucket, claim.key)
    except STORAGE_ERRORS as exc:
        _record_claim_result(journal, operation, claim, _ClaimResult('unknown', 'unknown'))
        _fail(result, f'ID claim verification failed for {claim.raw_id!r}: {operation_error(exc)}')
        return False
    evidence = _identity_evidence(observed)
    if observed is None or not _identity_matches(observed, claim):
        _record_claim_result(
            journal, operation, claim, _ClaimResult('unknown', 'unknown', evidence=evidence)
        )
        _fail(
            result,
            f'ID claim verification failed for {claim.raw_id!r}: checksum or size mismatch.',
        )
        return False
    _record_claim_result(
        journal, operation, claim, _ClaimResult('created', 'written', evidence=evidence)
    )
    result.created_count += 1
    return True


def _record_claim_result(journal, operation, claim, result):
    if journal is None or operation is None:
        return
    details = result.evidence or _identity_evidence(result.observed)
    journal.result(
        operation,
        'claim',
        {
            **claim.identity(),
            **details,
            'status': result.status,
            'remote_outcome': result.outcome,
        },
    )


def _identity_evidence(observed):
    if observed is None:
        return {}
    return {
        'observed_checksum': getattr(observed, 'checksum', None),
        'observed_size_bytes': getattr(observed, 'size_bytes', None),
        'checksum_type': getattr(observed, 'checksum_type', None),
    }


def _identity_matches(observed, claim):
    return bool(
        getattr(observed, 'checksum', None) == claim.checksum
        and getattr(observed, 'size_bytes', None) == claim.size_bytes
        and getattr(observed, 'checksum_type', None) == 'FULL_OBJECT'
    )


def _read_owner(client, bucket, key):
    getter = getattr(client, 'get_object', None)
    if getter is None:
        getter = client.client.get_object
    response = getter(Bucket=bucket, Key=key)
    body = response['Body']
    try:
        content = body.read() if hasattr(body, 'read') else bytes(body)
    finally:
        close = getattr(body, 'close', None)
        if close:
            close()
    return _parse_owner(content, key)


def _parse_owner(content, key):
    try:
        value = json.loads(content.decode('utf-8'))
    except (UnicodeDecodeError, ValueError, AttributeError):
        return None
    if not _valid_owner(value, key):
        return None
    return value


def _valid_owner(value, key):
    if (
        not isinstance(value, dict)
        or isinstance(value.get('schema_version'), bool)
        or value.get('schema_version') != 1
    ):
        return False
    kind = value.get('kind')
    if not isinstance(kind, str) or kind not in {'object', 'page'}:
        return False
    required = {'batch_id', 'canonical_id', 'id', 'kind', 'schema_version'}
    if kind == 'page':
        required.add('parent_id')
    return (
        set(value) == required
        and isinstance(value['id'], str)
        and bool(value['id'])
        and isinstance(value['batch_id'], str)
        and value['batch_id'] == _normalize_segment(value['batch_id'])
        and isinstance(value['canonical_id'], str)
        and value['canonical_id'] == _canonical_id(value['id'])
        and key.endswith(f'/_ids/{value["canonical_id"]}.json')
        and (kind != 'page' or isinstance(value['parent_id'], str))
    )


def _conflict_message(claim, owner):
    if owner is None:
        return f'ID claim conflict for {claim.raw_id!r}; owner unavailable.'
    parent = f', parent {owner["parent_id"]!r}' if owner['kind'] == 'page' else ''
    return (
        f'ID claim conflict for {claim.raw_id!r}: ID {owner["id"]!r} belongs to '
        f'batch {owner["batch_id"]!r} as {owner["kind"]}{parent}.'
    )


def _is_precondition_failed(error):
    response = getattr(error, 'response', {})
    service_error = response.get('Error', {}) if isinstance(response, dict) else {}
    metadata = response.get('ResponseMetadata', {}) if isinstance(response, dict) else {}
    service_error = service_error if isinstance(service_error, dict) else {}
    metadata = metadata if isinstance(metadata, dict) else {}
    code = service_error.get('Code')
    return (
        code in ('PreconditionFailed', '412')
        or metadata.get('HTTPStatusCode') == 412
        or 'precondition failed' in str(error).lower()
    )


def reconcile_claim(client, identity):
    observed = client.inspect_object(identity['bucket'], identity['key'])
    if observed is None:
        return {**identity, 'status': 'absent', 'remote_outcome': 'absent', 'reconciled': True}
    matches = (
        getattr(observed, 'checksum', None) == identity['checksum']
        and getattr(observed, 'size_bytes', None) == identity['size_bytes']
        and getattr(observed, 'checksum_type', None) == 'FULL_OBJECT'
    )
    status = 'matched' if matches else 'conflict'
    return {
        **identity,
        'status': status,
        'remote_outcome': status,
        'reconciled': True,
        **_identity_evidence(observed),
    }


def _fail(result, message):
    result.status = 'failed'
    result.refusal_code = 1
    result.errors.append(message)
    return result
