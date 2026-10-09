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
holds, local derivatives, and manifest URLs when a base URL was configured in
YAML or passed to `generate --manifest-base-url`. These URLs follow the generated
manifest `@id` and are listed only for confirmed uploads.
Older generated folders may have no recorded generate stage. For those
older test outputs, a command-line-only base URL cannot be reconstructed from
the journal; the inventory lists no manifest URL unless YAML supplies one.
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
a key enters `new_keys` only if the durable journal records both an absent
destination check and an acknowledged upload. An existing content key enters
`unchanged_keys` only when S3 SHA-256 and size match the current local artifact;
it is skipped and never becomes rollback deletion scope. Changed existing content
enters `conflicts`. A differing existing manifest enters `held_keys` with its
observed prior full-object checksum when S3 provides one, plus its planned
checksum; otherwise the prior checksum is reported unavailable and the manifest
stays held. It is not a replacement and is not included in `new_keys`.
`replacements` remains empty until WBL-0307b defines a
recoverable overwrite path. These distinctions do not authorize any deletion.
An upload intent with a missing or uncertain acknowledgment enters
`unknown_keys`, and the inventory state becomes `needs_reconciliation`. A damaged
journal or plan produces `invalid_evidence`. An interrupted stage reports
`incomplete_stage` unless an unknown operation already requires reconciliation.
The valid journal prefix still shows
acknowledged writes, but it does not resolve missing evidence. An acknowledged
write remains listed even if later verification or another upload fails.

Every inventory has `deletion_authorized: false`. A future rollback command needs a
read-only preview, current-object identity checks, and explicit authorization before
any delete/restore operation. For each new key, it must compare the current S3
checksum with the inventory's uploaded checksum before deleting it; mere key
existence is insufficient, including when this run's verification failed. Do
not delete by prefix or act on `new_keys` alone;
inspect unknown outcomes, later object changes, and the full run first. The
publication hold described in [publication-holds.md](publication-holds.md) stays
in force until recovery and review are complete.
