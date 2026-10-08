# AGENTS.md

Guidance for AI coding agents working in this repository. `CLAUDE.md` imports this file; edit
here only.

## What this is

workbench-lite, a Python 3.12 command-line tool. It validates a Workbench-style YAML and CSV
package, generates derivatives and IIIF manifests, and uploads them to S3 when explicitly told to.
It does not create repository records.

## Test

```sh
python3 -m unittest discover -s tests -v
```

The suite uses synthetic packages and local test doubles. It must keep passing without AWS
credentials, network access or staff inputs; do not add tests that need them.

## Safety rules for storage

These protect production storage. Do not weaken them unless your task explicitly says so.

- Uploads happen only with an explicit execute step; checks and dry runs never write.
- Writes create **new keys only**; replacing an existing object stays blocked.
- Preflight, locks and publication holds behave as documented in `docs/preflight-and-locks.md`
  and `docs/publication-holds.md`. Rollback records follow `docs/rollback-inventory.md`.
- Run against a staging or LocalStack target first. Never run an execute step against
  production buckets as part of a coding task.
- No credentials in code, config examples or tests.

## Interfaces other systems depend on

Report any change to these in your handoff.

| Interface | Defined in | Who depends on it |
|---|---|---|
| S3 key plan: buckets, `s3_prefix`, `DEFAULT_PUBLIC_BUCKET` | `workbench_lite/storage_plan.py`, README "Configuration" | The IIIF image server and anything that reads delivered files |
| IIIF manifest shape, canvas IDs and image service URLs | `workbench_lite/manifest.py` | The ArchivesSpace viewer; OCR search addresses pages by canvas ID, so keep them stable |
| Package CSV columns and YAML keys | README "Package input" | Staff package preparation |
| CLI outcomes and exit codes | `docs/cli-outcomes.md` | Operators and scripts |

## Public repository rules

This repository is public and dedicated to the public domain under CC0 1.0. No tickets, run
journals, staff package inputs, staff names, AWS account IDs or ARNs, internal server paths or
credentials. Test data stays synthetic. Run output belongs in an ignored directory, never in git.

## Working rules

- Work on a branch named for your task. Do not push or merge to `main`; a reviewer does that.
- Keep to the files your task names. If the task is wrong or incomplete, stop and report.
- End with: branch and commit, every command run with its result, anything you changed beyond the
  task and why, and open questions.
