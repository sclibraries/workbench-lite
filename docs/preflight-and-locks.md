# Workbench-lite preflight and writer locks

The CLI validates inputs and excludes concurrent writers on one host. These checks
do not approve publication or reconcile uncertain remote writes.

## What is checked

`check` and the preflight used by `generate`/`push` validate required CSV headers, nonempty IDs and page-image filenames, supported object models, parent relationships, numeric page weights, duplicate weights, file extensions and normalized destination collisions. Pages are attached by parent ID and sorted numerically; physical parent-row precedence remains unnecessary. Duplicate page weights now block because their ordering is ambiguous.

Sources must stay under the configured `input_dir`; traversal, backslash/NUL paths, external HTTP file references and symlink escapes are rejected. The input CSV override can live elsewhere. A CSV hOCR column is validated even when `additional_files` omits it, because it is consumed by the object builder. Dummy-file checks still enforce path boundaries. Execute with dummy files remains invalid.

Existing required TIFFs are fully decoded and must contain a single frame. JPEG outputs are decoded, JSON outputs must parse as objects, hOCR must decode as UTF-8, and PDFs must have a PDF header and EOF marker. These are not full PDF conformance, IIIF schema, OCR semantic, antivirus or preservation validation. A corrupted source discovered here returns 3 before generation or upload. Unsupported multi-frame TIFFs must be split into page files.

Image safety policy explicitly sets `Image.MAX_IMAGE_PIXELS = 89_478_485` for both validation and generation. This preserves the existing Pillow behavior: a warning above 89,478,485 pixels and rejection above 178,956,970 pixels. Rejection says "Image exceeds configured pixel limit" so staff can distinguish a large map/poster scan from unreadable data. Review scan dimensions before changing this shared policy; the CLI does not disable the safeguard.

The declared PyYAML dependency is required; missing parser support fails closed. Consumed YAML scalar/container types, non-mapping roots, duplicate mapping keys, safe S3 prefix/batch segments and basic DNS-style bucket names are checked. Legacy Workbench settings that this CLI does not consume remain ignored; they do not enable Drupal operations. Missing-file warnings under `allow_missing_files` do not authorize missing uploads.

Before generation, output targets are checked for containment, directory/file collisions, writable existing ancestors and overwrite permissions. Disk reporting conservatively estimates source RGB bytes plus 64 KiB per planned generated entry. It reports available space and blocks insufficient capacity; this is not a reservation or a precise compressed-output forecast. Preflight cannot prevent another process changing permissions/files/free space afterward; operation-time failures remain authoritative.

## Online and offline dry runs

Push now performs bounded read-only preflight by default for both dry-run and execute. It uses a dedicated read-only preflight client with the same endpoint, region and profile selection as the upload client, and records:

- Actual endpoint, intended buckets and scoped prefixes.
- HeadBucket followed by ListObjectsV2 with `MaxKeys=1` per bucket.
- Durable intent/result records for those calls and safe error codes on failure.
- Read-access status and **write permission: unverified**.

Only the preflight client uses five-second connect/read timeouts and one total SDK attempt. Execute creates a separate upload client after successful preflight, without overriding SDK timeout/retry configuration (including profile/environment settings). Dry runs create no upload client. No PutObject probe, delete, ACL update or other remote mutation is used to test permissions. Read checks do not prove PutObject permission, validate all existing objects, or authorize replacement. Failed access checks prevent uploads. Existing destination keys are rejected before execute and writes use conditional creation; see [publication holds](publication-holds.md).

For explicitly local checks without credentials or network access:

```sh
python3 -m workbench_lite.cli push --config package.yml --dry-run --offline
```

Offline reports access as `not_checked`; it cannot be combined with execute. Online dry-run requires read credentials/access. A dry-run with no generated directory reports planned outputs. An explicit directory is checked for missing or invalid artifacts; missing outputs produce a nonzero result. A failed/interrupted dry-run still does not invalidate successful generation once the local issue is fixed.

## Provision the fixed lock root

The CLI uses `/var/lib/workbench-lite/locks`. It never provisions this directory automatically, falls back to a working-directory/user/temp location, or accepts a config/CLI/environment lock-root override. Generation and pushes attaching generated output fail closed without a safe accessible root. Standalone checks and verification create unique journals and need no shared writer lock.

On the Linux execution host, an administrator creates a shared operator group, adds only authorized operators/service users, then provisions:

```sh
sudo install -d -o root -g workbench-lite -m 2770 /var/lib/workbench-lite/locks
```

The `workbench-lite` group must exist first. Operators must start a session with that group active. The root must be a real absolute directory, owned by root or the current user, inaccessible to other users, and readable/writable/searchable by the operator. Symlink roots and world-accessible roots are rejected. Lock subdirectories are 0770, and ownership records are 0660; setgid on the provisioned root supplies the shared group. Run evidence remains owner-private.

Test the deployed operator and service accounts: one holds a lock, another is denied,
both can read ownership evidence, and the next operator acquires after release. An
inaccessible root must reject work. Local container checks do not certify the target
execution host.

Systemd services and interactive shells must see the **same** persistent directory and filesystem. Verify `RootDirectory`, `BindPaths`, `ReadWritePaths`, UID/GID mappings and any container mounts. `PrivateTmp` does not isolate this `/var/lib` path, but other sandboxing can. Do not put real locks in `/tmp` or `/var/tmp`. Initial support is one execution host/process namespace; multiple hosts or isolated PID namespaces need a separately approved coordination/recovery design.

For local Docker development, all Workbench invocations must mount the same named volume at that fixed path, for example `--volume workbench-lite-writer-locks:/var/lib/workbench-lite/locks`. Provision that volume with the same owner/group/mode before using it. Use a fixed volume name across project directories, and the same mount for service and interactive runs. An ordinary unprovisioned ephemeral container will fail closed. Do not treat isolated per-container lock directories as writer exclusion.

## Lock scope and lifetime

Acquisition order is generated-output directory, attached run, then sorted destination bucket guards/scopes. Output exclusion is acquired before reading the run reference; attached-run exclusion is acquired before `_open_run` examines prior execution. Generation participates because it replaces the reference and outputs. Dry-run participates because it appends evidence even without remote writes. Locks stay held through processing and summary writes and are released on normal completion, errors and handled signals. The final publication receipt is committed after successful lock release; the existing-execute guard excludes other appenders during that gap. See the [completion and inspection details](publication-holds.md#durable-completion-and-inspection). Any future resume support must preserve that exclusion.

Destination locks include endpoint, bucket and prefix. A conservative bucket-name guard additionally serializes different prefixes, overlapping prefixes and endpoint aliases—even independent test endpoints with the same bucket name. This trades concurrency for predictable exclusion on the supported single host. The run/output locks separately prevent two destinations from appending to one journal.

Acquisition uses atomic directory creation. A competing process is rejected immediately. `owner.json` records hostname, UID, PID, creation time, a random ownership token and hashed lock identity. No input metadata or credential-bearing destination is written there. A partial acquisition releases only locks this invocation owns.

## Stale-lock recovery

Abrupt death can leave lock directories. The CLI never steals them automatically, even if a PID appears absent. It also never removes a lock whose ownership token has changed.

Before explicit recovery, an operator must:

1. Stop intake/scheduled writers and prevent new invocations during inspection.
2. Read the exact lock's ownership record. Confirm hostname, user, PID and creation time against the host's process records; a reused PID or a permission-denied process check is not proof of death. An incomplete ownership record needs investigation, not automatic removal.
3. Confirm the owning operation has stopped and inspect its run journal. Reconcile unknown remote outcomes before deciding any next upload; removing a lock does not authorize retry or deletion.
4. With that review recorded and all writers excluded, remove only the confirmed stale lock's ownership file and empty directory. Never clear the lock root wholesale.
5. Re-enable intake only after verifying the shared root and remaining ownership records. Prior-execute guards still apply; stale-lock removal does not authorize another upload.

There is no automatic TTL cleanup. Archive or remove run evidence only after the
retention and reconciliation review required by your operating procedures. Test code
injects isolated lock providers; only the test harness reads `WBL_TEST_LOCK_DIR`. The
production entrypoint ignores it.
