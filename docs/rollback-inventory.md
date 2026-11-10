# Rollback inventory

Every `push --dry-run` and `push --execute` with a valid plan writes an owner-private
`rollback-inventory.json` in its run directory. `--rollback-file PATH` writes an
additional atomic copy. The journal and immutable upload plan remain the source of
truth; after a crash, rebuild an inventory without AWS calls using:

```sh
workbench-lite rollback-inventory workbench-lite-runs/RUN_ID --output /secure/path/rollback.json
```

The JSON includes the run/batch identity, input hashes and sanitized references,
planned objects, recorded operation outcomes, confirmed new S3 keys, unknown
outcomes, previously published objects reused by checksum, manifest replacement
holds, local derivatives, and manifest URLs when a base was recorded with the run.
Both `manifest_base_url` and `cantaloupe_base_url` are required for
`generate --manifests`; the full URL rule is in [README Configuration](../README.md#configuration).
The inventory validates and normalizes that base with the same public URL rule as
generation, removes the recorded `manifest_key_prefix` at a key boundary when present,
then joins the remaining planned manifest key. For new runs, each inventory URL equals
the manifest `@id`; older snapshots without a prefix keep using the full key. URLs are
listed only for confirmed uploads.
It also reports `claim_counts` and `retained_claims`; claims are durable registry
records, not upload-plan entries or rollback candidates.
Older generated folders may have no recorded generate stage. For those
older outputs, the inventory uses only a base present in the run's config snapshot
or recorded generation options. A missing or invalid recorded base produces no
manifest URL; current configuration is never consulted. URLs come from the recorded
base and can differ from a manifest that an earlier generation left in the folder.
The `aspace` section explicitly records that the CLI made no ArchivesSpace change.
Staff must restore any separately reviewed File Version change from that update
CSV's old-value column; the inventory cannot infer those prior values.

The internal `push_upload_plan(..., rollback_path=...)` compatibility hook still
emits a `legacy-upload-list` without destination provenance. It is explicitly
marked `deletion_authorized: false`; staff should use the CLI inventory, and
Consumers should accept only the versioned `rollback-inventory` contract.

The CLI saves a pre-receipt inventory before finalizing an execute run, then
refreshes it after the completion receipt. If that final local refresh fails,
the batch receipt can still be valid and the cached inventory may say
`incomplete_stage`. The warning is printed; rebuild with `rollback-inventory`
to read the final journal state. The journal and `inspect-run`, not the cached
inventory state, govern publication handoff.

A dry-run inventory has `mode: preview` and no confirmed new keys. During execute,
a key enters `new_keys` only if the durable journal records an initially absent
destination and a write confirmed by the PUT response or later full-object
checksum/size reconciliation. A resumed push retains that initial ownership
evidence after rechecking the destination. An existing content key enters
`unchanged_keys` only when S3 SHA-256 and size match the current local artifact;
it is skipped and never becomes rollback deletion scope. Changed existing content
enters `conflicts`. A differing existing manifest enters `held_keys` with its
observed prior full-object checksum when S3 provides one, plus its planned
checksum; otherwise the prior checksum is reported unavailable and the manifest
stays held. It is not a replacement and is not included in `new_keys`.
`replacements` remains empty; the CLI has no recoverable overwrite path. These
distinctions do not authorize any deletion. An upload intent with no result or a
latest result whose remote outcome is unknown enters `unknown_keys`, and the
inventory state becomes `needs_reconciliation`. Earlier unknown results are
resolved by later attempt or reconciliation events and do not keep an operation
pending. A damaged
journal or plan produces `invalid_evidence`. An interrupted stage reports
`incomplete_stage` unless an unknown operation already requires reconciliation.
The valid journal prefix still shows confirmed writes, but it does not resolve
missing evidence. A confirmed write remains listed even if later verification or
another upload fails. Reconciliation appends a `confirmed`, `absent` or `conflict`
result for each unknown operation without changing prior journal records. It uses
HEAD only and does not write or delete S3 objects. After all outcomes are resolved,
the inventory no longer reports `needs_reconciliation`; conflicts remain excluded
from `new_keys` and are listed separately.

ID claims are recorded separately from planned content. `retained_claims` lists claims
confirmed as `created` or `matched` and unresolved claims as `unknown` or `unsettled`;
`claim_counts` also reports absent and conflicting claims. Claims are never included in
`new_keys` and rollback never deletes them. Claim journal events support recovery and
reporting only; their checksum values and agreement with the upload plan do not determine
which content keys are eligible for rollback. A malformed claim event is a journal error
and makes the inventory `invalid_evidence`, like other malformed journal records.

Pending claim intents and unknown claim results still require reconciliation and block
rollback. A resolved claim conflict does not by itself block rollback; confirmed content
remains eligible under the normal content rules. With no claim context, the claim phase was
not reached, and an interrupted run keeps its normal `incomplete_stage` or
`needs_reconciliation` state.

If claims were created but a later claim conflict stopped content uploads, a reviewable
inventory can have no `new_keys`. Preview reports zero deletions. Execute permits this
empty-scope case only when the journal records at least one acknowledged `created` claim
result from the run; a later `matched` result does not erase that creation history. This
creation record is the only claim fact rollback uses for the exception. Execute then exits
successfully without deleting anything. Claims remain reserved after rollback.

```sh
workbench-lite reconcile workbench-lite-runs/RUN_ID
workbench-lite rollback-inventory workbench-lite-runs/RUN_ID
```

Every inventory has `deletion_authorized: false`; the inventory itself never grants
permission to delete. The `rollback` command adds a read-only preview and explicit
authorization:

```sh
workbench-lite rollback workbench-lite-runs/RUN_ID --config package.yml
workbench-lite rollback workbench-lite-runs/RUN_ID --config package.yml --execute --confirm RUN_ID \
  --profile APPROVED_PROFILE --endpoint-url APPROVED_ENDPOINT --region APPROVED_REGION
```

Review the full preview first. It lists every proposed key with bucket, key, role
and recorded SHA-256, plus exclusions and reasons. Execution requires a complete,
reviewable execute inventory and the matching source run ID. Preview runs,
unknown or pending operations, incomplete/damaged evidence, and inventories with
replacement entries are refused. The command only considers `new_keys`; it never
deletes unchanged, held, conflicting, unknown, or unrelated objects. It scans later
local run journals and leaves keys reused by a later run as conflicts. Journals on
other machines cannot be seen, so that dependency scan is local to the configured
run root.

Immediately before each delete, the command reads the current S3 `FULL_OBJECT`
SHA-256 and size. Both must match the confirmed write evidence. A mismatch is a
conflict and the object stays. If the object is absent, the result is
`already_deleted`. The delete request also sends the current ETag in `If-Match` as
an extra guard. A 412 is recorded as a conflict and no retry or unconditional
delete is attempted. The checksum-and-size comparison is the required identity
guard; never remove it even on endpoints that ignore `If-Match`.

Rollback order is manifests, derivatives, masters, then audit objects. Rollback
uses the same package/destination locks as writers and records each delete intent
and result in its own run journal. An interrupted rollback can be rerun against
the same source run. Once execution starts, `rollback-hold.json` permanently keeps
the source run held; publishing again requires a new run. Workbench-lite does not
restore ArchivesSpace File Versions. Staff must separately review the update CSV
and restore any prior File Version value when needed. Delete recoverability depends
on bucket versioning, which has not been confirmed for this bucket.

### Before the first real rollback

Before the first rollback against a real S3 environment, an authorized operator
must probe the **staging bucket** once to confirm that a stale ETag is rejected
with HTTP 412. Use a new, unique key and synthetic bytes in the approved staging
bucket: upload version A, record its ETag, overwrite that same synthetic key with
version B, then issue `DeleteObject` with version A's stale ETag. The expected
result is HTTP 412 under [S3 conditional-delete behavior](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-deletes.html).
Confirm version B is still present, then remove only this probe object using its
current ETag. If the stale-ETag request succeeds, returns anything
other than 412, or version B is missing, stop; do not run rollback until the
endpoint behavior has been reviewed. Do not use a production object for this
probe. This staging probe is a required future runbook step and was not run during
implementation.

LocalStack 3.8.1 was separately observed to ignore stale `If-Match` and return 204.
The LocalStack rehearsal therefore relies on the required checksum-and-size check;
it does not certify conditional-delete enforcement. Never fall back to an
unconditional delete if an SDK or endpoint rejects `If-Match`.
