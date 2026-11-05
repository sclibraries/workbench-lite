# Workbench lite CLI outcomes and run evidence

The CLI records command outcomes and local run evidence. See [preflight checks and
single-host writer locks](preflight-and-locks.md). Check both the exit status and the
report. Publication and recovery limits are described in the linked guides.

## Exit statuses

| Code | Meaning |
| --- | --- |
| 0 | Command completed without required failures |
| 1 | Processing aborted, required upload artifact missing, upload or claim storage/final verification failed, or evidence/rollback inventory could not be written |
| 2 | Invalid arguments, including execute with dummy files or without a required generated directory |
| 3 | Input/configuration validation errors, invalid manifests or public manifest URLs, or a package ID claim conflict |
| 4 | Generation or URL verification completed independent work with failures |
| 130 / 143 | SIGINT / SIGTERM; an interruption event is recorded when storage permits |

A source missing at required preflight validation produces 3. A source disappearing afterward stops push with 1; generation may finish independent work and return 4. Missing-file warnings never make a missing upload successful. Invalid execute flags are rejected before input access. SIGKILL cannot write a terminal event; inspection identifies the incomplete stage instead.

`check` returns 3 when different row IDs have the same canonical form: Unicode NFC
normalization, then case folding, then the storage-segment normalizer. Parent and page IDs
share one namespace, and comparison does not rewrite their original spelling. Exact duplicate
IDs and upload-plan collisions continue to be reported by their existing checks.

`push --execute` checks validated package IDs against private-bucket claims after existing
local and destination blockers pass and before any claim or content write. A claim conflict,
including a conditional-create race with a different owner, returns 3; claim storage failures
return 1 and hold the batch. Online `push --dry-run` performs the read-only claim check with or
without generated files. `push --dry-run --offline` reports the check as `not_run` and makes
no storage call.

`generate --manifests` requires valid `manifest_base_url` and `cantaloupe_base_url` settings
from YAML or their flags. A YAML value takes precedence, including an empty value; a flag
supplies a missing setting. Both bases are checked before anything in the generated directory
is changed. They must be absolute HTTPS URLs or HTTP URLs for exactly `localhost`, `127.0.0.1`
or `[::1]`, with no userinfo, query, fragment, whitespace, control character or backslash.
`check` and `generate` without `--manifests` do not need either setting.

Before local readiness completes, push validates every planned manifest's `@id` against its
encoded planned S3 key and validates each image service `@id` as one encoded image-key segment
after a valid public base. It does not compare those bases with current configuration. An
invalid base or identifier returns 3 before storage setup or writes. Other local readiness
failures return 1; if URL validation and another readiness check fail together, the report
includes both and returns 3. Online and offline dry runs perform the same local URL check when
generated files are present; offline mode creates no storage client. A dry run without
generated files remains planning only.

## Run locations and workflow

Each check, generation, standalone push or verification creates an exclusively allocated run ID under `./workbench-lite-runs` relative to the invocation directory. `--run-dir /absolute/root` overrides that root. The CLI prints the resolved run directory to stderr; JSON reports include `run_id` and `run_path`. Run directories are 0700 and evidence files are 0600. No automatic retention expiry or deletion is performed.

```sh
python3 -m workbench_lite.cli generate --config package.yml \
  --output-dir generated --manifests --thumbnails --run-dir ./workbench-lite-runs
python3 -m workbench_lite.cli push --config package.yml \
  --generated-dir generated --dry-run --offline
# Execute only against an authorized test destination, with its explicit endpoint/profile.
python3 -m workbench_lite.cli inspect-run /absolute/root/RUN_ID --format json
```

Generation writes an owner-private `.workbench-run.json` reference inside the generated directory. Push follows that reference, reuses the run ID and verifies input hashes and the immutable plan. `--run-dir` controls new runs; it does not relocate an attached run. Keep the run directory and reference together at their recorded paths. A fresh generation creates a new run ID and audit, including when using the same batch ID/output directory. Old audits are retained.

Repeated execute and dry-run attachments are rejected by default; incomplete/failed generation, changed inputs and conflicting plans are also rejected. A failed or interrupted dry run does not invalidate a successful generation; restore the missing file or resolve the read-only check, then rerun push. Reuse checks the latest generation stage specifically. Legacy outputs without a recorded generation retain their existing first-attachment checks. Locks protect generated output and attached runs before reuse checks, including dry-run journal writes. Do not delete the reference to bypass a rejected attempt. Inspect and reconcile first. Existing generated directories without a reference get a new run and automatic audit on their first push; this does not certify their content or establish prior remote ownership.

An execute that stopped after writing or losing an acknowledgment can be inspected and resumed with the same generated directory:

```sh
workbench-lite reconcile /absolute/path/to/RUN_ID
workbench-lite push --config package.yml --generated-dir generated --execute --resume
```

Reconciliation performs only S3 HEAD requests. An uncertain claim is recorded as `matched`,
`absent` or `conflict`; reconciliation never labels one `created`. Resume validates the
recorded input files, private snapshots, immutable plan, software source hash, endpoint and
buckets; it rechecks prior writes by full-object SHA-256 and size, then repeats claim checks
before content. A claim conflict on resume returns 3. A receipt, pending finalization marker,
rollback hold, unresolved outcome or conflict prevents resume. Without `--resume`, the
existing execute guard still rejects reuse. The generated-output and run locks are acquired
before the reference and run are examined; destination locks are held through reconciliation,
upload verification, receipt commit and release.

Read-only inspection derives current state from the valid journal prefix. It neither modifies evidence nor calls S3. It returns 1 for journal damage, an incomplete stage or unknown operations; otherwise it returns the recorded terminal exit status. Claim events are validated by type, and a malformed claim event makes the journal invalid. A claim phase with no recorded claim context was not reached, so inspection preserves the run's existing interruption state. Claim counts and unresolved claim operations support inspection, reconciliation and resume; claim journal values do not authorize publication. Local PID/hostname liveness helps distinguish running from inferred interrupted stages; this is advisory, not a lock or proof of process identity. A torn final line is reported without discarding earlier records. Do not append to a damaged journal. Stored summaries are checkpoints and may be stale after abrupt termination; use inspection for the current view.

Before committing a successful execute receipt, finalization performs one read-only,
checksum-enabled `HeadObject` for each claim derived from the immutable upload plan, with
no object readback. It requires the planned SHA-256 and size with checksum type
`FULL_OBJECT`. The receipt's
`verified_claims` field contains exactly one row per claim, each with `key`, `checksum`,
`size_bytes` and `checksum_type`; `verified_count` remains the number of content-plan
entries. Inspection validates the rows against the plan without calling S3. A missing or
changed claim, mismatched checksum or size, wrong checksum type, or failed final HEAD
returns 1 and leaves publication held without a receipt. Resume repeats all final claim
checks fresh after an interruption. On a successful execute with N expected claims and M
claims absent on the initial read pass, claim storage makes `2N + M` HEAD requests and M
PUT requests; an online dry run makes N HEAD requests.

The unanchored `workbench-lite-runs/` gitignore rule covers default directories at any repository depth. Arbitrary `--run-dir` paths need their own ignore rule or an external location. Logs never determine the fixed machine-wide lock location. Archive or delete resolved evidence only after an explicit retention/reconciliation review; unresolved runs retain rollback provenance.

## Evidence files

- `run.json`: schema version, run/batch ID, software version and source hash, input hashes, snapshot references/hashes, creation time and planned bucket/key destinations.
- `input.csv` and `config.json`: owner-private snapshots. YAML configuration uses an allowlist; credential fields and URL credentials/query parameters are removed. CSV descriptions remain intact while credential columns/URLs are sanitized. Original input hashes identify the submitted files; snapshot hashes identify the sanitized copies.
- `events.jsonl`: append-only UTF-8 JSON Lines, ordered sequence and UTC timestamps, stage, file/object/page identity, attempt number, safe error class/code/HTTP status, delay, checksums/sizes and disposition. Effective SDK endpoint/region and configured profile are recorded before uploads. Every PUT attempt has a durable intent and result; an unknown outcome remains pending only while its latest result is unknown.
- `summary.json` and `summary.txt`: atomically replaced journal-derived summaries with running, completed, completed-with-errors, failed or interrupted states. A local completed generation does not mean remote publication. A failed or interrupted run can contain acknowledged uploads alongside unknown operations.
- `upload-plan.json`: immutable pre-upload snapshot. The generated copy uploads to the private bucket at `{batch_prefix}/audit/{run_id}/upload-plan.json`. It is a plan, not an execution receipt, and excludes its own checksum. Old shared audit objects are untouched.
- Claim context and claim-stage events: expected claim keys, SHA-256 and sizes, plus `intent`, `created`, `matched`, `absent`, `conflict` or `unknown` outcomes. Claims are not content-plan entries or `push` results.

No transaction spans S3 and the local journal. A write can reach S3 before its receipt can be stored. Logging failures stop the command and produce a nonzero result. Journals preserve facts for reconciliation; they do not authorize deletion or retries themselves. See [publication holds](publication-holds.md) and [rollback inventory](rollback-inventory.md).

Canonical `PushResult.to_dict()` and `PushReport.to_dict()` preserve keys and descriptions exactly. Public diagnostic rendering handles redaction. Evidence preserves ordinary canonical identities (including `Secret: A history…`); credential-bearing values instead use explicitly named `*_display` and `*_sha256` fields. Display values must never be treated as exact storage keys. SDK exception messages, headers and payloads are omitted; safe structured codes such as AccessDenied, NoSuchBucket and RequestTimeout remain available.

## Upload and generation outcomes

Execute checks the entire plan for missing required sources and generated files before the first upload. Any missing artifact, including the audit, returns 1 with zero uploads. Otherwise-ready entries are unattempted; every missing entry is reported. Files are checked again during processing because they can disappear after readiness.

All non-manifest artifacts, including the required audit, precede every manifest. The audit is generated automatically; an attached missing audit is not silently regenerated, and a changed snapshot blocks execution. Missing/unwritable audit output fails. Generation's derivative/manifest counts retain their existing meaning; audit generation appears in the journal and upload plan.

Uploads use one SDK attempt per call and at most three application attempts per entry. Only connection/timeouts, HTTP 429/500/502/503/504, and S3 SlowDown/RequestTimeout are retried, with full-jitter delays based on one and four seconds and a five-minute wait budget per invocation. Each `push --execute` and `--resume` starts with fresh attempt and wait limits, so a run stopped by retries can be resumed while journal attempt numbers continue across invocations. Access denial, other client errors, local/validation/checksum errors and precondition conflicts are not retried. After every failed or unacknowledged PUT, the CLI performs a HEAD while holding the destination lock. Matching FULL_OBJECT SHA-256 and size confirms the write without another PUT; confirmed absence permits a retry only when the failure is retryable; a different identity is a conflict; an unreadable identity or failed HEAD remains unknown. A required terminal failure stops subsequent writes. The rollback inventory is evidence, not deletion authorization; the separate `rollback` command requires a complete reviewable execute inventory and explicit confirmation.

`skipped_generated_count` and `deferred_count` remain zero-valued compatibility fields. Required generated artifacts are never silently skipped. A dry run without generated output reports planned generation; an explicit directory is checked for expected files. It performs no uploads but writes local run evidence. Online dry-run now performs bounded read-only storage checks; use --offline explicitly to skip credential/access verification. Generation and attached push require the provisioned fixed lock directory.

## Limits and verification

Manifests-last is not atomic publication: some manifests may succeed before another fails, and existing manifests can reference changed image keys. Follow the preflight, publication-hold and rollback guidance before production use. No dashboard, remote deployment or ArchivesSpace mutation is included.

Run the maintained suite from the repository root:

```sh
python3 -m unittest discover -s tests -v
```

Tests use synthetic local fixtures, recording/failing storage clients, subprocess signals and mocked HTTP. No production writes are required.
