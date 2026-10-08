# Workbench lite CLI outcomes and run evidence

The CLI records command outcomes and local run evidence. See [preflight checks and
single-host writer locks](preflight-and-locks.md). Check both the exit status and the
report. Publication and recovery limits are described in the linked guides.

## Exit statuses

| Code | Meaning |
| --- | --- |
| 0 | Command completed without required failures |
| 1 | Processing aborted, required upload artifact missing, upload failed, or evidence/rollback inventory could not be written |
| 2 | Invalid arguments, including execute with dummy files or without a required generated directory |
| 3 | Input/configuration validation errors, including check-only errors and invalid manifests |
| 4 | Generation or URL verification completed independent work with failures |
| 130 / 143 | SIGINT / SIGTERM; an interruption event is recorded when storage permits |

A source missing at required preflight validation produces 3. A source disappearing afterward stops push with 1; generation may finish independent work and return 4. Missing-file warnings never make a missing upload successful. Invalid execute flags are rejected before input access. SIGKILL cannot write a terminal event; inspection identifies the incomplete stage instead.

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

Repeated execute, incomplete/failed generation, changed inputs and conflicting plans are rejected. A failed or interrupted dry run does not invalidate a successful generation; restore the missing file or resolve the read-only check, then rerun push. Reuse checks the latest generation stage specifically. Legacy outputs without a recorded generation retain their existing first-attachment checks. This is a guard against unverified reuse, **not resume**. Locks protect generated output and attached runs before reuse checks, including dry-run journal writes. Do not delete the reference to bypass a rejected attempt. Inspect and reconcile first. Existing generated directories without a reference get a new run and automatic audit on their first push; this does not certify their content or establish prior remote ownership.

Read-only inspection derives current state from the valid journal prefix. It neither modifies evidence nor calls S3. It returns 1 for journal damage, an incomplete stage or unknown operations; otherwise it returns the recorded terminal exit status. Local PID/hostname liveness helps distinguish running from inferred interrupted stages; this is advisory, not a lock or proof of process identity. A torn final line is reported without discarding earlier records. Do not append to a damaged journal. Stored summaries are checkpoints and may be stale after abrupt termination; use inspection for the current view.

The unanchored `workbench-lite-runs/` gitignore rule covers default directories at any repository depth. Arbitrary `--run-dir` paths need their own ignore rule or an external location. Logs never determine the fixed machine-wide lock location. Archive or delete resolved evidence only after an explicit retention/reconciliation review; unresolved runs retain rollback provenance.

## Evidence files

- `run.json`: schema version, run/batch ID, software version and source hash, input hashes, snapshot references/hashes, creation time and planned bucket/key destinations.
- `input.csv` and `config.json`: owner-private snapshots. YAML configuration uses an allowlist; credential fields and URL credentials/query parameters are removed. CSV descriptions remain intact while credential columns/URLs are sanitized. Original input hashes identify the submitted files; snapshot hashes identify the sanitized copies.
- `events.jsonl`: append-only UTF-8 JSON Lines, ordered sequence and UTC timestamps, stage, file/object/page identity, attempts, checksums/sizes, outcomes and safe error classifications. Effective SDK endpoint/region and configured profile are recorded before uploads. Intent is flushed/fsynced before each operation; acknowledgment is flushed/fsynced afterward. A missing acknowledgment remains unknown.
- `summary.json` and `summary.txt`: atomically replaced journal-derived summaries with running, completed, completed-with-errors, failed or interrupted states. A local completed generation does not mean remote publication. A failed or interrupted run can contain acknowledged uploads alongside unknown operations.
- `upload-plan.json`: immutable pre-upload snapshot. The generated copy uploads to the private bucket at `{batch_prefix}/audit/{run_id}/upload-plan.json`. It is a plan, not an execution receipt, and excludes its own checksum. Old shared audit objects are untouched.

No transaction spans S3 and the local journal. A write can reach S3 before its receipt can be stored. Logging failures stop the command and produce a nonzero result. Journals preserve facts for reconciliation; they do not authorize deletion or retries themselves. See [publication holds](publication-holds.md) and [rollback inventory](rollback-inventory.md).

Canonical `PushResult.to_dict()` and `PushReport.to_dict()` preserve keys and descriptions exactly. Public diagnostic rendering handles redaction. Evidence preserves ordinary canonical identities (including `Secret: A history…`); credential-bearing values instead use explicitly named `*_display` and `*_sha256` fields. Display values must never be treated as exact storage keys. SDK exception messages, headers and payloads are omitted; safe structured codes such as AccessDenied, NoSuchBucket and RequestTimeout remain available.

## Upload and generation outcomes

Execute checks the entire plan for missing required sources and generated files before the first upload. Any missing artifact, including the audit, returns 1 with zero uploads. Otherwise-ready entries are unattempted; every missing entry is reported. Files are checked again during processing because they can disappear after readiness.

All non-manifest artifacts, including the required audit, precede every manifest. The audit is generated automatically; an attached missing audit is not silently regenerated, and a changed snapshot blocks execution. Missing/unwritable audit output fails. Generation's derivative/manifest counts retain their existing meaning; audit generation appears in the journal and upload plan.

Upload success is recorded only after the SDK returns. A required failure stops subsequent writes. An upload exception records a failed attempt with unknown remote outcome, not confirmed absence. Earlier receipts survive in the journal. The optional rollback inventory also carries the run ID and acknowledged uploads, but remains an end-of-command inventory, not an implemented rollback command.

`skipped_generated_count` and `deferred_count` remain zero-valued compatibility fields. Required generated artifacts are never silently skipped. A dry run without generated output reports planned generation; an explicit directory is checked for expected files. It performs no uploads but writes local run evidence. Online dry-run now performs bounded read-only storage checks; use --offline explicitly to skip credential/access verification. Generation and attached push require the provisioned fixed lock directory.

## Limits and verification

Manifests-last is not atomic publication: some manifests may succeed before another fails, and existing manifests can reference changed image keys. Follow the preflight, publication-hold and rollback guidance before production use. No dashboard, remote deployment or ArchivesSpace mutation is included.

Run the maintained suite from the repository root:

```sh
python3 -m unittest discover -s tests -v
```

Tests use synthetic local fixtures, recording/failing storage clients, subprocess signals and mocked HTTP. No production writes are required.
