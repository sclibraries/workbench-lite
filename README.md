# Workbench-lite

Workbench-lite reads a Workbench-style YAML configuration and CSV package, validates
the parent/page structure and source files, plans delivery assets, and can generate
local derivatives and IIIF manifests. Uploads to S3 are explicit. The package format
is described below; the CLI does not create repository records.

## Package input

The CSV must contain these columns:

| Column | Use |
| --- | --- |
| `title` | Parent or page title. |
| `id` | Stable package identifier for the row. |
| `field_model` | `Paged Content` for a parent or `Page` for a page. |
| `parent_id` | Parent row ID for each page. |
| `field_weight` | Numeric display order for each page. |
| `file` | Parent PDF or page TIFF/TIFF path under `input_dir`. |
| `hocr` | Optional hOCR/HTML sidecar path for a page. |

Other Workbench columns are accepted and preserved in the input snapshot. Page rows
must reference a parent row. Page weights must be numeric and unique within each
parent. Source paths must stay inside `input_dir`. The CLI accepts one-page TIFFs;
multi-frame TIFFs need to be split into page files first.

Row IDs are compared by canonical form: Unicode NFC normalization, case folding, then
the storage-segment normalizer. Parent and page IDs share one namespace. `check` rejects
different original IDs with the same canonical form; it preserves the original spelling.
Exact duplicate IDs and upload-plan collisions remain covered by their existing checks.

## Configuration

Create a YAML mapping in your working directory. Put the CSV and source files under
the configured `input_dir`:

```yaml
input_dir: package-files
input_csv: ingest.csv
additional_files:
  - hocr: 3507
manifest_base_url: https://manifests.example.org/manifests
cantaloupe_base_url: https://images.example.org/iiif/2
s3_private_bucket: your-approved-private-bucket
s3_public_bucket: your-approved-public-bucket
s3_prefix: workbench-lite
batch_id: example-batch
```

| Key | Meaning |
| --- | --- |
| `input_dir` | Directory containing package sources; relative paths resolve from the YAML file. |
| `input_csv` | CSV path, relative to `input_dir`, unless overridden with `--input-csv`. |
| `additional_files` | Optional list of single-field mappings for sidecars, such as `hocr: 3507`. |
| `allow_missing_files` | Optional boolean for warning-only missing-file reports. It does not permit uploads with missing sources. |
| `manifest_base_url` | Required for `generate --manifests`; public base for manifest identifiers. |
| `cantaloupe_base_url` | Required for `generate --manifests`; public base for IIIF image services. |
| `s3_private_bucket` | Bucket for private masters and audit files. |
| `s3_public_bucket` | Bucket for delivery assets and manifests. |
| `s3_prefix` | Prefix shared by package destinations; defaults to `workbench-lite`. |
| `batch_id` | Safe path segment for the package; defaults to the YAML filename without its extension. |

Set both destination buckets explicitly for the environment before an execute run.
The public bucket defaults to `islandora-derivatives`. AWS credentials
come from the selected AWS profile or the standard AWS credential chain; do not put
credentials in the YAML file.

For `generate --manifests`, both URL bases must be absolute HTTPS URLs, or HTTP URLs
whose host is exactly `localhost`, `127.0.0.1` or `[::1]`. Bases cannot contain
userinfo, queries, fragments, whitespace, control characters or backslashes. A path
prefix is kept and trailing slashes are removed. `check` and `generate` without
`--manifests` do not require either setting.

## S3 layout and ID reservations

Content keeps its batch-specific layout under `{s3_prefix}/{batch_id}`. Workbench-lite also
reserves each canonical object and page ID at
`{s3_prefix}/_ids/{canonical_id}.json` in the private bucket. The claim registry is scoped to
one S3 endpoint, private bucket and effective `s3_prefix`; changing the private bucket or
prefix starts a separate registry. Claims remain after rollback, and parent and page IDs
share the same claim namespace. Canonicalization is used only for the registry key; stored
content keys and manifest canvas IDs retain the original spelling.
An existing claim is reusable only when the original ID, normalized batch, kind and (for a
page) parent ID all match. A mismatch stops push before content uploads.

## Install and run

Python 3.12 is used by the Docker image. Install dependencies and view the CLI help:

```sh
python3 -m pip install -r requirements.txt
python3 -m workbench_lite.cli --help
```

Validate a package and print a JSON report:

```sh
python3 -m workbench_lite.cli check --config package.yml --format json
```

Generate derivatives and manifests locally:

```sh
python3 -m workbench_lite.cli generate --config package.yml \
  --output-dir generated --manifests --thumbnails
```

Review an offline dry run without AWS access:

```sh
python3 -m workbench_lite.cli push --config package.yml --dry-run --offline
```

An upload requires an explicit `--execute` and configured destination credentials.
For example, select an AWS profile and destination endpoint for the approved
environment:

```sh
python3 -m workbench_lite.cli push --config package.yml --execute --profile PROFILE \
  --endpoint-url ENDPOINT --region REGION --generated-dir generated
```

Other commands include `verify-cantaloupe`, `inspect-run`, `rollback-inventory`, and
`rollback`.
Run `python3 -m workbench_lite.cli COMMAND --help` for their options. The Compass S3
inventory utility is available at `scripts/export_compass_s3_package.py`; run it with
`--help` for its options.

## Docker

Build from the repository root. The image defaults to `check --help`:

```sh
docker build -t workbench-lite .
docker run --rm workbench-lite
```

Mount a package directory at `/workspace` to run commands against local files.

## Tests

```sh
python3 -m unittest discover -s tests -v
```

The suite uses synthetic package data and local test doubles. It does not require
the coordination repository, staff package inputs, AWS credentials, or live storage.

## License

This project is dedicated to the public domain under [CC0 1.0 Universal](https://creativecommons.org/publicdomain/zero/1.0/).

## Operational notes

- [CLI outcomes and run evidence](docs/cli-outcomes.md)
- [Preflight and writer locks](docs/preflight-and-locks.md)
- [Rollback inventory](docs/rollback-inventory.md)
- [Publication holds](docs/publication-holds.md)
