import argparse
import csv
import json
import os
import signal
import sys
from collections import Counter
from pathlib import Path

from .check import run_check, _resolve_csv_path
from .config import ConfigError, load_config
from .generate import generate_service_jpgs, generate_thumbnails
from .manifest import generate_manifests
from .verify import verify_manifest_service_urls
from .push import STORAGE_ERRORS, push_upload_plan
from .diagnostics import operation_error, sanitize
from .s3_client import create_s3_client
from .runs import RunJournal, JournalError, inspect_run, read_events, digest
from .locks import WriterLocks, LockError, lock_output_and_run, lock_destination
from .preflight import output_readiness, storage_readiness
from .rollback_inventory import build_inventory, write_inventory


def main(argv=None, *, lock_factory=WriterLocks) -> int:
    parser = argparse.ArgumentParser(prog="workbench-lite")
    subparsers = parser.add_subparsers(dest="command", required=True)

    check_parser = subparsers.add_parser("check", help="Validate a Workbench-lite package")
    check_parser.add_argument("--config", required=True, type=Path)
    check_parser.add_argument("--input-csv", type=Path, default=None)
    check_parser.add_argument("--allow-dummy-files", action="store_true")
    check_parser.add_argument("--format", choices=["text", "json"], default="text")

    push_parser = subparsers.add_parser("push", help="Upload or dry-run a Workbench-lite package")
    push_parser.add_argument("--config", required=True, type=Path)
    push_parser.add_argument("--input-csv", type=Path, default=None)
    push_parser.add_argument("--allow-dummy-files", action="store_true")
    push_parser.add_argument("--dry-run", action="store_true")
    push_parser.add_argument("--execute", action="store_true")
    push_parser.add_argument("--verify-readback", action="store_true", help="Also download uploaded objects for full-byte verification after S3 checksum verification")
    push_parser.add_argument("--offline", action="store_true", help="Dry-run only: skip credential/access checks, explicitly unverified")
    push_parser.add_argument("--endpoint-url", default=None)
    push_parser.add_argument("--region", default=None)
    push_parser.add_argument("--profile", default=None)
    push_parser.add_argument("--generated-dir", type=Path, default=None)
    push_parser.add_argument("--rollback-file", type=Path, default=None)
    push_parser.add_argument("--format", choices=["text", "json"], default="text")

    generate_parser = subparsers.add_parser(
        "generate", help="Generate local derivative files from source images"
    )
    generate_parser.add_argument("--config", required=True, type=Path)
    generate_parser.add_argument("--input-csv", type=Path, default=None)
    generate_parser.add_argument("--allow-dummy-files", action="store_true")
    generate_parser.add_argument("--output-dir", type=Path, default=None)
    generate_parser.add_argument("--manifests", action="store_true")
    generate_parser.add_argument("--cantaloupe-base-url", default=None)
    generate_parser.add_argument("--manifest-base-url", default=None)
    generate_parser.add_argument("--thumbnails", action="store_true")
    generate_parser.add_argument("--max-long-side", type=int, default=2000)
    generate_parser.add_argument("--quality", type=int, default=82)
    generate_parser.add_argument("--format", choices=["text", "json"], default="text")

    verify_parser = subparsers.add_parser(
        "verify-cantaloupe", help="Verify manifest image service IDs against Cantaloupe"
    )
    verify_parser.add_argument(
        "--manifest",
        action="append",
        default=[],
        type=Path,
        help="One or more manifest files to verify",
    )
    verify_parser.add_argument(
        "--manifest-dir",
        type=Path,
        help="Directory to scan for *.json manifests",
    )
    verify_parser.add_argument("--timeout", type=int, default=5)
    verify_parser.add_argument(
        "--cantaloupe-base-url",
        help="Base URL to prepend for relative service IDs in manifests",
    )
    verify_parser.add_argument("--format", choices=["text", "json"], default="text")

    inspect_parser = subparsers.add_parser("inspect-run", help="Read evidence without retrying operations")
    inspect_parser.add_argument("path", type=Path)
    inspect_parser.add_argument("--format", choices=["text", "json"], default="text")
    inventory_parser = subparsers.add_parser("rollback-inventory", help="Rebuild read-only rollback evidence from a run")
    inventory_parser.add_argument("path", type=Path)
    inventory_parser.add_argument("--output", type=Path)
    inventory_parser.add_argument("--format", choices=["text", "json"], default="text")
    for command_parser in [check_parser, push_parser, generate_parser, verify_parser]:
        command_parser.add_argument("--run-dir", type=Path, default=Path("workbench-lite-runs"))

    args = parser.parse_args(argv)
    if args.command == "push":
        if args.offline and args.execute:
            parser.error("--offline is only valid with --dry-run")
        if args.execute and args.allow_dummy_files:
            parser.error("push --execute cannot be combined with --allow-dummy-files")
        if args.dry_run and args.execute:
            parser.error("push accepts only one of --dry-run or --execute")
        if not args.dry_run and not args.execute:
            parser.error("push currently requires --dry-run or --execute")
    if args.command == "inspect-run":
        summary = inspect_run(args.path)
        print(json.dumps(sanitize(summary), indent=2) if args.format == "json" else
              f"Run: {args.path.resolve()}\nState: {summary['state']}\n"
              f"Unknown operations: {len(summary['unknown_operations'])}\n"
              f"Publication: {summary['publication']['state']}\n"
              f"Journal error: {summary['journal_error'] or 'none'}")
        return 1 if summary['journal_error'] or summary['incomplete_stage'] or summary['unknown_operations'] else (summary['exit_code'] or 0)
    if args.command == "rollback-inventory":
        try:
            inventory = write_inventory(args.path, args.output) if args.output else build_inventory(args.path)
        except JournalError as exc:
            _print_error(args, str(exc))
            return 1
        print(json.dumps(sanitize(inventory), indent=2) if args.format == 'json' else
              f"Run: {inventory['run_id']}\nInventory: {inventory['state']}\n"
              f"Confirmed new keys: {len(inventory['new_keys'])}\n"
              f"Unknown outcomes: {len(inventory['unknown_keys'])}\n"
              "Deletion authorized: no")
        return 1 if inventory['state'] in {'needs_reconciliation', 'invalid_evidence', 'incomplete_stage'} else 0
    journal = None
    locks = lock_factory()
    args.locks = locks
    code = 1
    terminal_error = None
    try:
        if args.command == 'generate':
            config = load_config(args.config)
            output = args.output_dir or (_resolve_input_dir(args.config, config) / 'generated')
            lock_output_and_run(locks, output)
        elif args.command == 'push' and args.generated_dir is not None:
            lock_output_and_run(locks, args.generated_dir)
        journal = _open_run(args)
        args.journal = journal
        _print_safe(f"Run directory: {journal.path}", file=sys.stderr)
        journal.start("push-dry-run" if args.command == "push" and not args.execute else args.command,
                      options={key: str(value) if isinstance(value, Path) else value
                               for key, value in vars(args).items() if key not in {'journal', 'locks'}})
        code = _run(args, parser)
        return code
    except (ConfigError, UnicodeDecodeError, csv.Error) as exc:
        message = str(exc) if isinstance(exc, ConfigError) else "Invalid input encoding or CSV syntax."
        terminal_error = message
        _print_error(args, message, validation=True)
        code = 3
        return code
    except (JournalError, LockError) as exc:
        terminal_error = str(exc)
        _print_error(args, str(exc))
        return 1
    except STORAGE_ERRORS as exc:
        terminal_error = operation_error(exc)
        _print_error(args, terminal_error)
        return 1
    except KeyboardInterrupt:
        code = 130
        raise
    except SystemExit as exc:
        code = exc.code if isinstance(exc.code, int) else 1
        raise
    except Exception as exc:
        terminal_error = f"Fatal {type(exc).__name__}; command aborted."
        raise
    finally:
        try:
            if journal is not None:
                try:
                    journal.finish(code, error=terminal_error)
                    if args.command == 'push' and (journal.path / 'upload-plan.json').exists():
                        write_inventory(journal.path)
                        if args.rollback_file:
                            write_inventory(journal.path, args.rollback_file)
                except JournalError as exc:
                    _print_error(args, str(exc))
                    raise SystemExit(1) from exc
        finally:
            try:
                locks.release()
            except LockError as exc:
                _print_error(args, str(exc))
                raise SystemExit(1) from exc

        if journal is not None and code == 0:
            try:
                journal.complete_publication()
                if args.command == 'push' and args.execute and (journal.path / 'upload-plan.json').exists():
                    # The pre-receipt inventory is already durable. Refresh its
                    # stage state after receipt; a refresh failure cannot revoke
                    # the independently committed publication receipt.
                    try:
                        write_inventory(journal.path)
                        if args.rollback_file:
                            write_inventory(journal.path, args.rollback_file)
                    except JournalError as exc:
                        _print_error(args, f"Rollback inventory refresh failed: {exc}")
            except JournalError as exc:
                _print_error(args, str(exc))
                raise SystemExit(1) from exc


def _open_run(args):
    if args.command == 'push' and args.generated_dir is not None:
        reference = args.generated_dir / '.workbench-run.json'
        if reference.exists():
            try:
                info = json.loads(reference.read_text())
                run = RunJournal.open(info['run_path'])
                if run.run_id != info['run_id']:
                    raise JournalError('Generated output has a conflicting run reference.')
            except (ValueError, KeyError, TypeError, OSError) as exc:
                raise JournalError('Cannot read generated output run reference.') from exc
            events, _ = read_events(run.path)
            if any(e['kind'] == 'stage_started' and e['stage'] == 'push' for e in events):
                raise JournalError('This run already attempted execute; inspect it before reconciliation.')
            generation = [e for e in events if e['stage'] == 'generate'
                          and e['kind'] in {'stage_started', 'stage_finished'}]
            if generation:
                # Read-only stages cannot invalidate or certify generation.
                completed = (generation[-1]['kind'] == 'stage_finished'
                             and generation[-1]['state'] == 'completed')
            else:
                # Legacy generated directories may first attach during dry-run.
                latest = inspect_run(run.path)
                completed = not latest['incomplete_stage'] and latest['state'] == 'completed'
            if not completed:
                raise JournalError('Generation did not complete cleanly; create a new generation run.')
            config = load_config(args.config)
            csv_path = _resolve_csv_path(args.config, config, args.input_csv)
            if not csv_path.is_file():
                raise ConfigError(f"CSV file not found: {csv_path}")
            run.inputs(args.config, config, csv_path)
            return run
    return RunJournal.create(args.run_dir)


def _checked(args):
    config = load_config(args.config)
    report = run_check(args.config, args.input_csv, args.allow_dummy_files, run_id=args.journal.run_id)
    args.journal.inputs(args.config, config, _resolve_csv_path(args.config, config, args.input_csv))
    if not report.validation_errors:
        args.journal.plan(report.upload_plan)
    else:
        args.journal.append("context", stage=args.command, validation_errors=report.validation_errors)
    return report


def _print_preflight(args):
    if args.format == 'json':
        print(json.dumps(sanitize({'preflight': args.preflight,
                                  'run_id': args.journal.run_id, 'run_path': str(args.journal.path)}), indent=2))
    else:
        _print_safe('Preflight:', json.dumps(sanitize(args.preflight), indent=2))


def _run_payload(report, journal):
    return {**report.to_dict(), **({'run_id': journal.run_id, 'run_path': str(journal.path)} if journal else {})}


def _print_safe(*values, **kwargs) -> None:
    print(*(sanitize(value) for value in values), **kwargs)


def _print_error(args, message, validation=False) -> None:
    payload = {"validation_errors" if validation else "errors": [sanitize(message)]}
    if args.format == "json":
        print(json.dumps(sanitize(payload), indent=2))
    else:
        _print_safe(message)


def _print_check(report, output_format, journal=None) -> None:
    if output_format == "json":
        print(json.dumps(sanitize(_run_payload(report, journal)), indent=2))
    else:
        _print_text_report(report)


def _run(args, parser) -> int:
    if args.command == "check":
        report = _checked(args)
        _print_check(report, args.format, args.journal)
        return 3 if report.validation_errors else 0

    if args.command == "push":
        check_report = _checked(args)
        if check_report.validation_errors:
            _print_check(check_report, args.format, args.journal)
            return 3
        if args.execute and args.generated_dir is None and any(
            entry.generated
            for entry in check_report.upload_plan
        ):
            parser.error("push --execute requires --generated-dir for planned generated artifacts")
        config = load_config(args.config)
        input_dir = _resolve_input_dir(args.config, config)
        dry_run = not args.execute
        new_audit = args.generated_dir is not None and not (args.generated_dir / '.workbench-run.json').exists()
        local = output_readiness(check_report.upload_plan, input_dir, args.generated_dir, new_audit=new_audit)
        args.preflight = {'local': local}
        args.journal.append('context', stage=args.journal.stage, preflight=args.preflight)
        if local['errors']:
            _print_preflight(args)
            return 1
        if args.generated_dir is not None:
            reference = args.generated_dir / '.workbench-run.json'
            if not reference.exists():
                args.generated_dir.mkdir(parents=True, exist_ok=True)
                args.journal.write_audit(check_report.upload_plan, args.generated_dir)
                args.journal.attach(args.generated_dir)
            else:
                audit = next(e for e in check_report.upload_plan if e.role == 'audit')
                audit_path = args.generated_dir / audit.key
                if audit_path.exists() and digest(audit_path) != digest(args.journal.path / 'upload-plan.json'):
                    raise JournalError('Audit snapshot changed since generation; upload blocked.')
        preflight_client = (
            None
            if args.offline
            else create_s3_client(args.endpoint_url, args.region, args.profile, preflight=True)
        )
        sdk_meta = getattr(getattr(preflight_client, 'client', None), 'meta', None)
        if not dry_run:
            endpoint = getattr(sdk_meta, 'endpoint_url', None) or args.endpoint_url or os.environ.get('AWS_ENDPOINT_URL_S3') or os.environ.get('AWS_ENDPOINT_URL') or 'aws-s3'
            lock_destination(args.locks, endpoint.rstrip('/').lower(), check_report.upload_plan)
        args.journal.append('context', stage='push', storage={
            'effective_endpoint_url': getattr(sdk_meta, 'endpoint_url', None),
            'effective_region': getattr(sdk_meta, 'region_name', None),
            'configured_endpoint_url': args.endpoint_url or os.environ.get('AWS_ENDPOINT_URL_S3') or os.environ.get('AWS_ENDPOINT_URL'),
            'configured_region': args.region or os.environ.get('AWS_DEFAULT_REGION') or os.environ.get('AWS_REGION'),
            'configured_profile': args.profile or os.environ.get('AWS_PROFILE') or 'default',
        })
        endpoint = getattr(sdk_meta, 'endpoint_url', None) or args.endpoint_url or os.environ.get('AWS_ENDPOINT_URL_S3') or os.environ.get('AWS_ENDPOINT_URL') or 'aws-s3 (SDK default)'
        remote = storage_readiness(preflight_client, check_report.upload_plan, endpoint, offline=args.offline, journal=args.journal)
        args.preflight['storage'] = remote
        args.journal.append('context', stage=args.journal.stage, preflight=args.preflight)
        if remote['errors']:
            _print_preflight(args)
            return 1
        s3_client = None if dry_run else create_s3_client(args.endpoint_url, args.region, args.profile, full_readback=args.verify_readback)
        push_report = push_upload_plan(
            check_report.upload_plan,
            input_dir=input_dir,
            s3_client=s3_client,
            dry_run=dry_run,
            generated_dir=args.generated_dir,
            endpoint_url=args.endpoint_url,
            region=args.region,
            profile=args.profile,
            rollback_path=None,
            journal=args.journal,
            preflight_client=preflight_client,
        )
        if args.format == "json":
            print(json.dumps(sanitize({**_run_payload(push_report, args.journal), "preflight": args.preflight}), indent=2))
        else:
            _print_preflight(args)
            _print_push_report(push_report)
        return 1 if push_report.has_errors else 0

    if args.command == "generate":
        check_report = _checked(args)
        if check_report.validation_errors:
            _print_check(check_report, args.format, args.journal)
            return 3
        config = load_config(args.config)
        input_dir = _resolve_input_dir(args.config, config)
        output_dir = args.output_dir or (input_dir / "generated")
        local = output_readiness(check_report.upload_plan, input_dir, output_dir, generating=True)
        args.preflight = {'local': local}
        args.journal.append('context', stage='generate', preflight=args.preflight)
        if local['errors']:
            _print_preflight(args)
            return 3
        output_dir.mkdir(parents=True, exist_ok=True)
        args.journal.attach(output_dir)
        args.journal.write_audit(check_report.upload_plan, output_dir)
        generate_report = generate_service_jpgs(
            upload_plan=check_report.upload_plan,
            input_dir=input_dir,
            output_dir=output_dir,
            max_long_side=args.max_long_side,
            quality=args.quality,
            journal=args.journal,
        )

        if args.manifests:
            manifest_base_url = config.get("manifest_base_url")
            if manifest_base_url is None:
                manifest_base_url = args.manifest_base_url

            manifest_report = generate_manifests(
                objects=check_report.objects,
                upload_plan=check_report.upload_plan,
                output_dir=output_dir,
                cantaloupe_base_url=str(
                    config.get("cantaloupe_base_url", args.cantaloupe_base_url or "/iiif/2")
                ),
                manifest_base_url=manifest_base_url,
                journal=args.journal,
            )
            generate_report = _combine_generate_reports(generate_report, manifest_report)

        if args.thumbnails:
            thumbnail_report = generate_thumbnails(
                upload_plan=check_report.upload_plan,
                input_dir=input_dir,
                output_dir=output_dir,
                max_long_side=300,
                quality=max(min(args.quality, 100), 1),
                journal=args.journal,
            )
            generate_report = _combine_generate_reports(generate_report, thumbnail_report)

        if args.format == "json":
            print(json.dumps(sanitize({**_run_payload(generate_report, args.journal), "preflight": args.preflight}), indent=2))
        else:
            _print_generate_report(generate_report)
        return 4 if generate_report.failed_count or generate_report.missing_source_count else 0

    if args.command == "verify-cantaloupe":
        manifest_paths: list[Path] = []
        manifest_paths.extend(args.manifest)

        if args.manifest_dir is not None:
            if not args.manifest_dir.exists():
                raise ConfigError(f"Manifest directory not found: {args.manifest_dir}")
            manifest_paths.extend(sorted(path for path in args.manifest_dir.rglob("*.json")
                                         if path.name not in {'.workbench-run.json', 'upload-plan.json'}))

        if not manifest_paths:
            if args.manifest_dir is not None:
                raise ConfigError("Manifest directory contains no JSON manifests.")
            parser.error("verify-cantaloupe requires --manifest or --manifest-dir")
        if args.timeout <= 0:
            parser.error("--timeout must be greater than zero")

        report = verify_manifest_service_urls(
            manifest_paths=[str(path) for path in manifest_paths],
            base_url_override=args.cantaloupe_base_url,
            timeout_seconds=args.timeout,
            journal=args.journal,
        )

        payload = _run_payload(report, args.journal)
        if args.format == "json":
            print(json.dumps(sanitize(payload), indent=2))
        else:
            _print_safe("Workbench-lite Cantaloupe verification report")
            _print_safe(f"Checked: {report.tested_count}")
            _print_safe(f"Passed: {report.passed_count}")
            _print_safe(f"Failed: {report.failed_count}")
            if report.failed_count:
                _print_safe("Failures:")
                for result in report.results:
                    if result.ok:
                        continue
                    _print_safe(
                        f"  - {result.canvas_id} ({result.manifest_path}) -> {result.info_url}"
                    )
                    if result.status_code is not None:
                        _print_safe(f"    status: {result.status_code}")
                    if result.content_type:
                        _print_safe(f"    content-type: {result.content_type}")
                    if result.message:
                        _print_safe(f"    {result.message}")
            else:
                _print_safe("All tested service URLs returned 200")
                for result in report.results:
                    dims = (
                        f"{result.width}x{result.height}"
                        if result.width is not None and result.height is not None
                        else "unknown dimensions"
                    )
                    _print_safe(f"  - {result.canvas_id}: {result.info_url} ({dims})")

        return 4 if report.failed_count else 0

    return 1


def _resolve_input_dir(config_path: Path, config) -> Path:
    input_dir = Path(str(config.get("input_dir", config_path.parent)))
    if input_dir.is_absolute():
        return input_dir
    return config_path.parent / input_dir


def _print_text_report(report) -> None:
    _print_safe("Workbench-lite check report")
    _print_safe(f"Rows: {report.row_count}")
    _print_safe(f"Columns: {report.column_count}")
    _print_safe(f"Paged Content parents: {report.parent_count}")
    _print_safe(f"Pages: {report.page_count}")
    _print_safe(f"Unique child parent IDs: {report.unique_child_parent_count}")
    _print_safe(f"Bad child weights: {report.bad_child_weight_count}")
    _print_safe("File counts:")
    for role, count in sorted(report.file_counts.items()):
        _print_safe(f"  {role}: {count}")
    _print_safe("Upload plan:")
    for role, count in sorted(Counter(entry.role for entry in report.upload_plan).items()):
        _print_safe(f"  {role}: {count}")
    if report.validation_warnings:
        _print_safe("Warnings:")
        for warning in report.validation_warnings:
            _print_safe(f"  - {warning}")
    if report.validation_errors:
        _print_safe("Errors:")
        for error in report.validation_errors:
            _print_safe(f"  - {error}")
    else:
        _print_safe("Validation: OK")


def _print_push_report(report) -> None:
    mode = "dry-run" if report.dry_run else "upload"
    _print_safe(f"Workbench-lite push report ({mode})")
    _print_safe("Publication held until finalization; use inspect-run for the authoritative handoff status.")
    if report.endpoint_url:
        _print_safe(f"Endpoint: {report.endpoint_url}")
    _print_safe(f"Region: {report.region}")
    _print_safe(f"Planned entries: {report.planned_count}")
    _print_safe(f"Source-backed entries: {report.source_backed_count}")
    _print_safe(f"Generated entries: {report.generated_count}")
    _print_safe(f"Missing sources: {report.missing_source_count}")
    _print_safe(f"Missing generated: {report.missing_generated_count}")
    _print_safe(f"Uploaded entries: {report.uploaded_count}")
    _print_safe(f"Failed operations: {report.failed_count}")
    for error in report.errors:
        _print_safe(error)
    _print_safe("Results:")
    for result in report.results:
        _print_safe(f"  {result.status}: {result.bucket}/{result.key}: {result.message}")
    _print_safe("Role counts:")
    for role, count in sorted(report.role_counts.items()):
        _print_safe(f"  {role}: {count}")


def _print_generate_report(report) -> None:
    _print_safe("Workbench-lite generate report")
    _print_safe(f"Targets: {report.target_count}")
    _print_safe(f"Generated: {report.generated_count}")
    _print_safe(f"Missing source: {report.missing_source_count}")
    _print_safe(f"Failed: {report.failed_count}")
    for result in report.results:
        _print_safe(f"  {result.status}: {result.source_path} -> {result.output_path}: {result.message}")


def _combine_generate_reports(*reports):
    results = []
    generated_count = 0
    missing_source_count = 0
    failed_count = 0
    target_count = 0
    for report in reports:
        generated_count += report.generated_count
        missing_source_count += report.missing_source_count
        failed_count += report.failed_count
        target_count += report.target_count
        results.extend(report.results)

    return type(reports[0])(
        generated_count=generated_count,
        missing_source_count=missing_source_count,
        failed_count=failed_count,
        target_count=target_count,
        results=results,
    )


def _terminate(signum, frame) -> None:
    raise SystemExit(128 + signum)


def entrypoint() -> int:
    signal.signal(signal.SIGTERM, _terminate)
    try:
        return main()
    except KeyboardInterrupt:
        return 130
    except Exception as exc:
        _print_safe(f"Fatal {type(exc).__name__}; command aborted.", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(entrypoint())
