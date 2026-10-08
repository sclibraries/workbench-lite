# Source

Imported from the coordination repository at commit
`57de9b770c32a129c04fa890631b500f6a5466c0` on 2026-10-08.

Copied into this repository:

- `workbench-lite/workbench_lite/` to `workbench_lite/`
- `workbench-lite/tests/` to `tests/`, with the real package example replaced by a
  synthetic fixture and exporter test values made synthetic
- `workbench-lite/requirements.txt` to `requirements.txt`
- `scripts/export_compass_s3_package.py` to `scripts/export_compass_s3_package.py`
- `docker/workbench-lite/Dockerfile` adapted as the root `Dockerfile`
- The CLI outcomes, preflight/locks, rollback inventory, and publication holds guides
  to `docs/`
- The public package contract and command reference rewritten as `README.md`

Coordination tickets, backlogs, completion records, evidence, architecture and pilot
notes, and the real local package CSV/YAML inputs were not copied. No license was supplied;
see the open question in `README.md`.
