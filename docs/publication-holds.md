# Batch publication and failure holds

For workbench-lite ingest, a successful processing step, an uploaded manifest, and
the audit plan are not batch publication approval. The authoritative
handoff check is `inspect-run <run-directory> --format json`: require
`publication.state == "ready_for_review"`, not merely exit 0 or a completed state.
There is no automatic ArchivesSpace update or publication-ready CSV exporter.

## Publication boundary

One batch may contain multiple manifests. All required non-manifest artifacts must
verify before the manifest
phase starts, and every manifest must verify before the entire batch receives a
handoff receipt. A failure after some manifests have uploaded stops the remaining
writes and holds **all** objects, including the earlier successful ones.
Zero-manifest data-only pushes cannot produce a publication receipt.

Uploading a manifest does not itself add an ArchivesSpace File Version. Staff must
wait for the complete-batch receipt before that separate review/update step. Partial
manifests can still be reachable at their direct S3 URLs: this policy is appropriate
only for content approved for public delivery, not as restricted-content access
control. The review CSV exporter remains future work; the CLI does not currently
produce an approved File Version handoff CSV.

Before writing anything, execute checks every planned destination for existing
content. A new key is eligible for a conditional create. An existing content key
is reusable only when `HeadObject(ChecksumMode='ENABLED')` proves the same full
object SHA-256 and size; matching content is recorded as previously published and
skipped. Missing or mismatched checksum evidence, or changed bytes at an existing
content key, holds the batch before any write. ETag is never a checksum.

An existing manifest with matching SHA-256 and size is also skipped. If the newly
generated manifest differs, the CLI records a `manifest replacement required`
hold with the observed and planned SHA-256 values and does not write that key.
The additive run may create and verify its new content and per-run audit keys, and
new parents' manifests use the normal conditional-create path. Any replacement
hold suppresses the complete-batch receipt, so the run remains held for review.
No existing object is overwritten; replacement, deletion and automatic
compensation remain unsupported.

The push report, journal and run summary classify each planned row as `new`,
`previously_published` or `held`. A held manifest includes its planned checksum
and the prior full-object checksum when S3 exposes one; absent or composite
checksum evidence is reported as unavailable and never treated as a match.

Each write uses S3 `PutObject` with `IfNoneMatch='*'`, closing the race between the
absence check and a competing creation. It never falls back to an unconditional
write. The installed boto3 model must support this argument; unsupported SDKs
fail before writes. The upload client sets 10-second connect and 60-second read
timeouts and one total SDK attempt. The application makes at most three attempts
per entry, with full-jitter backoff based on one and four seconds and a five-minute
retry-wait budget per invocation. Each `push --execute` and `--resume` starts with
fresh attempt and wait limits, so a run stopped by retries can be resumed while
journal attempt numbers continue across invocations. It retries only connection errors/timeouts, HTTP
429/500/502/503/504, and S3 SlowDown or RequestTimeout. AccessDenied, other 4xx
responses, validation/local-file/checksum errors and conflicts are not retried.
Preflight retains its separate five-second, one-attempt client.

After every failed or unacknowledged PUT, the CLI issues `HeadObject` while the
destination lock remains held. A matching FULL_OBJECT SHA-256 and size confirms
the write and prevents another PUT. Confirmed absence permits another attempt only
for a retryable error. A different or unreadable identity is a conflict and stops
the batch; a failed HEAD leaves the outcome unknown. HTTP 412 follows this same
reconciliation path and is never retried. Attempt events record the attempt number,
safe error class/code/status, wait and disposition. Use `reconcile RUN_DIR` to
resolve still-unknown attempts with HEAD only; it does not write or delete S3
objects. Resume uses the same run after checking its local evidence, endpoint,
buckets and every previously written object. See [CLI outcomes](cli-outcomes.md).

Only conditional **single-request uploads up to 5 GiB per artifact** are currently
supported. Larger artifacts hold the whole batch before writing; multipart
support needs a separately tested conditional-completion implementation. This
replaces the former managed multipart uploader for safety. AWS documents the
[conditional-write contract](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html).

Each conditional PUT supplies the precomputed local SHA-256 as
`ChecksumAlgorithm='SHA256'` and `ChecksumSHA256` (base64 of the 32 digest bytes,
not base64 of the hex text). S3 checks the body against that checksum. After the
acknowledgment, `HeadObject(ChecksumMode='ENABLED')` must return the same SHA-256
and `ContentLength`. Missing, malformed, mismatched or composite checksum evidence
holds the batch. ETag and user-supplied metadata are not substitutes. The journal
records the observed checksum, size and verification method, which inspection
checks against upload evidence. See [AWS checksum validation](https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity-upload.html).

Default verification downloads no object bodies. Add `--verify-readback` to
`push --execute` for an additional full GET and local hash/length check after the
S3 checksum check. This explicit diagnostic mode adds a full download per artifact;
it does not bypass missing checksum evidence. There is no automatic fallback from
failed checksum verification to GET. Readback bodies are closed on success/failure.
The entire data/audit plan must verify before the first manifest; all manifests
must then verify too. Verification is point-in-time evidence, not protection
against later unrelated writers changing these objects.

**This is a handoff hold, not a privacy or atomic-visibility guarantee.** New
public image keys can be directly accessible while preparation runs. A manifest
may be directly accessible after a lost acknowledgment or a subsequent verification,
evidence or finalization failure. Such a batch remains held: stop ASpace handoff,
retain the exact keys and journal, and reconcile before retry or cleanup. No
existing serving object is intentionally replaced. Restoration/deletion of
uncertain writes remains blocked until a reviewed recovery procedure is available;
never delete by prefix.

## Stage and error matrix

| Stage | Work allowed after a required error | Result / publication |
| --- | --- | --- |
| CSV/YAML/source validation | Collect independent validation errors | Exit 3; no storage writes; held |
| Generate | Continue independent derivatives/diagnostics | Exit 1 for processing failures; held |
| Local push readiness | Inspect the whole local plan for missing files | Exit 1; zero uploads; held |
| Remote access/destination checks | Stop on denial, uncertain checksum evidence, or changed existing content. A differing existing manifest is recorded as a replacement hold; only new content/audit keys and new-parent manifests may proceed, and that manifest is never written. | Exit 1 for a hold/failure; publication held |
| Data/audit upload | Stop subsequent uploads on any required failure | Exit 1; retain acknowledgments/unknown intents; no manifest; held |
| Remote byte verification | Stop subsequent uploads on mismatch/read failure | Exit 1; acknowledged upload remains recorded; held |
| Manifest upload/verification | Stop immediately; no automatic rollback | Exit 1; possibly directly accessible manifest; held |
| Journal intent/result persistence | Abort all remote writes | Exit 1; missing receipt means unknown operation; held |
| Summary persistence | Do not write the completion receipt | Exit 1; finalization incomplete; held |
| Receipt write or marker removal | Keep the pending marker when possible | Exit 1; finalization incomplete; held |
| Lock release | Restore a pending marker if release is uncertain | Exit 1; held |
| SIGINT / SIGTERM | Attempt terminal evidence and release owned locks | Exit 130 / 143, explicit interrupted state; held |
| SIGKILL / process death | No handler/automatic cleanup | Incomplete stage inferred interrupted when PID is gone; held |
| Diagnostic URL verification | Continue independent URLs | Exit 4 if any fail; never publication approval |

Completed-with-errors never permits release. Missing source discovered by source
preflight returns 3; generated files missing at push readiness return 1. Argument
misuse still returns 2. Dry runs never create a publication receipt.

## Durable completion and inspection

Push records each write intent before the request, then its attempt result before
the next attempt. Verified writes have their own intent/result. A missing
acknowledgment remains pending until HEAD reconciliation resolves it; a failed HEAD
leaves it unknown. A failed verification does not erase a confirmed upload from
recovery evidence. The rollback inventory, if requested, must also write
successfully.

Successful execute remains `running` through summary persistence. Finalization
then creates a durable `publication-pending` marker, syncs a terminal journal event
containing the complete-batch receipt, and removes the marker while output, run and
destination locks are still held. The locks are released only after the receipt is
committed. A failed receipt write leaves the marker and keeps the run held; resume
refuses it. A failed append is truncated when possible; if truncation also fails,
the marker still holds publication. If marker removal or lock release is uncertain,
the run remains held. Marker removal is deliberately not followed by another
directory sync: after a crash the marker may conservatively reappear, requiring
reconciliation. It must never be removed automatically or treated as an expired lock.

`inspect-run` validates the entire journal, the frozen plan hash, exact destination
coverage and matching upload/verification fingerprints and any recorded observed checksum/size. A destination verified unchanged by SHA-256 can satisfy plan coverage without an upload; every other key must have a PUT acknowledgment or reconciled write and matching post-upload verification. It requires the final success
receipt, no unknown operations and no pending marker. Changed plan, truncated
journal, incomplete execution, dry run or missing receipt means **held**. Run IDs,
plan hashes and publication owner are present in the receipt. New receipts explicitly
name the `s3-sha256-v1` verification contract and require observed checksum, size and
method for every successful verification. Older completed receipts without this
contract are retained but report held under this inspector; do not relabel or edit
them to bypass reconciliation. Existing generated-only runs can still proceed to
execute if their original input/plan checks pass.

`summary.json` and `summary.txt` are cached before final commit and can still show
`running`/held after success. Push's immediate report also remains held pending
finalization. Use `inspect-run` for current status and handoff; consumers must not
use the cached summary or audit as a release marker. An inspection exit status
alone is insufficient: always require the explicit publication state. This keeps
failure of summary writing or lock release from granting premature approval.

The final receipt is committed after processing and summary persistence but before
lock release. If lock release fails, a hold marker is restored so inspection cannot
report the batch ready. Without `--resume`, `_open_run` rejects execute and dry-run
attachment to a run that has already started execute. Resume validates the recorded
inputs, snapshots, plan, software source hash, endpoint, buckets and rollback/receipt
state, then acquires output, run and destination locks before reconciliation or
remote checks. A subprocess regression pauses finalization and confirms resume and a
second writer are denied while those locks remain held.

A surviving pending marker or uncertain operation requires operator review of
storage and process ownership. Do not manually manufacture a receipt to unblock a
run.

## ArchivesSpace boundary

Workbench-lite does not create or update ArchivesSpace records. Authorized staff
must review File Version changes separately against the complete-batch receipt.
Workbench-lite does not approve content or access policy.

## Execution-host permissions

For the current general-purpose S3 bucket flow, the operator/service role needs:

| Action | Resource / purpose |
| --- | --- |
| `s3:ListBucket` | Each destination bucket ARN, for HeadBucket/ListObjectsV2 preflight and unambiguous missing-key checks |
| `s3:GetObject` | Approved object prefixes, for HeadObject checksum/size and optional full readback |
| `s3:PutObject` | Approved object prefixes, for conditional creation with SHA-256 |
| `s3:DeleteObject` | Approved object prefixes, for an explicitly authorized rollback using conditional `If-Match`; not needed for normal publication |
| `kms:GenerateDataKey`, `kms:Decrypt` when using SSE-KMS | Applicable KMS key, permitted by IAM and key policy, for encrypted upload/checksum retrieval |

There is no `s3:HeadObject` IAM action. `ListAllMyBuckets` is not needed. Conditional
deletes with an ETag require both `s3:DeleteObject` and `s3:GetObject`; keep delete
permission restricted to the authorized rollback role and approved prefixes.
`s3:ListBucket` is bucket-scoped, not an object-ARN action.
The current `HeadBucket` call has no prefix parameter: a policy granting listing
only when `s3:prefix` matches may not authorize this preflight. The execution
environment must either
allow the required bucket-level check or approve a preflight change; do not claim
that prefix-conditioned ListBucket alone satisfies the current implementation.
GetObject and PutObject can stay scoped to approved object prefixes.

Absent ListBucket permission, a HEAD of a missing key can return 403 rather than
404. The CLI correctly treats 403 as an error and never as absence. Existing
HeadBucket preflight can reject the role even earlier. See AWS documentation for
[HeadBucket](https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadBucket.html) and
[HeadObject, missing keys and KMS checksums](https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html).
Document these requirements for the execution environment; local emulator success
does not certify production IAM or KMS policy.

## After a failed execute

The `rollback` command supports deletion only for a complete, reviewable execute
inventory. It previews by default; execution requires the current `--config`,
`--execute` and `--confirm RUN_ID`, and deletes only confirmed new keys whose
current `FULL_OBJECT` SHA-256 and size
still match the source journal. It refuses incomplete stages, unknown outcomes,
damaged evidence and replacements. See [rollback inventory](rollback-inventory.md)
for the preview, staging probe and execution procedure.

An incomplete or uncertain execute is not eligible for automatic rollback. Stop
intake for that batch, retain its run ID/journal and exact destination keys, and
escalate for reconciliation. Do not delete objects by hand, remove the run
reference, or change the batch prefix to evade the hold. Once a supported rollback
starts, the source run stays held permanently, even after rollback completes;
republishing requires a new run. Rollback does not restore ArchivesSpace values.
