"""Write command, scenario, and summary evidence for one rehearsal."""

import json
import platform
import subprocess
import sys

from PIL import __version__ as pillow_version

from rehearsal_storage import ENDPOINT
from workbench_lite.runs import inspect_run, read_events


class EvidenceWriter:
    def __init__(self, root):
        self.root = root
        self.commands = []
        self.results = []
        self.findings = []

    def record_command(self, directory, display, argv, result):
        item = {
            'command': display,
            'argv': [str(value) for value in argv],
            'exit': result['exit'],
            'stdout': result['stdout'],
            'stderr': result['stderr'],
        }
        self.commands.append(item)
        directory.mkdir(parents=True, exist_ok=True)
        index = len(list(directory.glob('command-*.json'))) + 1
        path = directory / f'command-{index:02d}.json'
        path.write_text(json.dumps(item, indent=2), encoding='utf-8')

    def add_finding(self, finding):
        self.findings.append(finding)

    def record_scenario(self, directory, record):
        path = directory / 'result.json'
        path.write_text(json.dumps(record, indent=2), encoding='utf-8')
        self.results.append(record)

    def write_summary(self, path, args, cleanup, prefix, copied, error, resources):
        lines = _summary_header(path, args, prefix, copied, resources, git_commit(self.root))
        lines.extend(_scenario_table(self.results))
        lines.extend(_findings_section(self.findings))
        lines.extend(_timing_section(resources.get('timing')))
        lines.extend(_cleanup_section(path, cleanup))
        lines.extend(_command_notes(error, self.results))
        path.write_text('\n'.join(lines) + '\n', encoding='utf-8')


def _summary_header(path, args, prefix, copied, resources, commit):
    copied_samples = ', '.join(f'`{alias}` ({size:,} bytes)' for alias, size in copied.items())
    elapsed = resources.get('elapsed_seconds')
    resource_line = '- Resource use: unavailable.'
    if elapsed is not None:
        resource_line = (
            f'- Resource use: summed scenario and timed CLI elapsed `{elapsed:.1f}s`; '
            f'copied source bytes `{resources["copied_bytes"]:,}`; '
            f'artifact bytes `{resources["artifact_bytes"]:,}`; free disk '
            f'`{resources["free_bytes_before"]:,}` → '
            f'`{resources["free_bytes_after"]:,}`.'
        )
    return [
        '# Integrated CLI rehearsal summary',
        '',
        f'- Artifact directory: `{path.parent}`',
        f'- Rehearsal prefix: `{prefix}`',
        f'- Endpoint: `{ENDPOINT}`',
        f'- Buckets: `{args.private_bucket}`, `{args.public_bucket}`',
        f'- Sample copies: {copied_samples}',
        f'- Machine: `{platform.platform()}`; Python `{sys.version.split()[0]}`; '
        f'Pillow `{pillow_version}`',
        f'- Workbench-lite commit: `{commit}`',
        resource_line,
        '- Credentials: dummy environment keys; AWS profile/config files disabled.',
    ]


def _scenario_table(results):
    rows = [
        '',
        '## Expected versus observed',
        '',
        '| Sample alias | Scenario | Name | Expected step exits | '
        'Observed step exits | '
        'Expected run/publication | Observed run/publication | Result |',
        '|---|---:|---|---|---|---|---|---|',
    ]
    for result in results:
        status = 'finding' if result['finding'] or not result['sentinels_unchanged'] else 'recorded'
        expected = result['expected']
        observed = result['observed']
        expected_state = _state_label(expected)
        observed_state = _state_label(observed)
        rows.append(
            f'| {result["sample_alias"]} | {result["scenario"]} | '
            f'{result["scenario_name"]} | '
            f'{json.dumps(expected["steps"], sort_keys=True)} | '
            f'{json.dumps(observed.get("steps", {}), sort_keys=True)} | '
            f'{expected_state} | {observed_state} | {status} |'
        )
    return rows


def _state_label(outcome):
    run_state = outcome.get('run_state', '—')
    publication = outcome.get('publication', '—')
    return f'{run_state} / {publication}'


def _findings_section(findings):
    lines = ['', '## Findings', '']
    if not findings:
        return lines + ['None observed.']
    for finding in findings:
        observed = finding.get('reproduction', {}).get('observed_error', finding.get('observed'))
        lines.append(
            f'- Sample `{finding.get("sample_alias")}`, '
            f'scenario {finding.get("scenario")}: {observed}'
        )
        reproduction = json.dumps(finding.get('reproduction', {}), sort_keys=True)
        lines.append(f'  Minimal reproduction: {reproduction}')
    return lines


def _timing_section(timing):
    lines = [
        '',
        '## Local timing',
        '',
        '| Operation | Previous local baseline | Current local rehearsal | Difference |',
        '|---|---:|---:|---:|',
    ]
    if not timing:
        return lines + ['Timing was not run.']
    lines.append(
        f'| Check | {timing["baseline_check_seconds"]:.3f}s | '
        f'{timing["check_seconds"]:.3f}s | '
        f'{timing["check_seconds"] - timing["baseline_check_seconds"]:+.3f}s |'
    )
    lines.append(
        f'| Push dry-run offline | {timing["baseline_dry_run_seconds"]:.3f}s | '
        f'{timing["dry_run_seconds"]:.3f}s | '
        f'{timing["dry_run_seconds"] - timing["baseline_dry_run_seconds"]:+.3f}s |'
    )
    return lines


def _cleanup_section(path, cleanup):
    return [
        '',
        '## Sentinel and cleanup evidence',
        '',
        f'- Sentinels were checked after each completed scenario. Cleanup complete: '
        f'`{cleanup.get("complete")}`.',
        f'- Cleanup evidence: `{path.parent / "cleanup.json"}`',
    ]


def _command_notes(error, results):
    lines = [
        '',
        '## Commands and limitations',
        '',
        '- Commands for each scenario are stored in that scenario directory as `command-*.json`;',
        '  raw per-file journal events and before/after storage listings are in `result.json`.',
        '- The source samples were copied before any fault was applied; only those '
        'copies were modified.',
        '- LocalStack rehearsal does not establish production IAM, versioning, or '
        'hosted-service behavior.',
    ]
    if error:
        lines.extend(['', f'- Rehearsal stopped with: `{error}`'])
    if not results:
        lines.extend(['', '- No scenarios completed.'])
    return lines


def git_commit(root):
    result = subprocess.run(['git', 'rev-parse', 'HEAD'], cwd=root, capture_output=True, text=True)
    return result.stdout.strip() if result.returncode == 0 else 'unavailable'


def event_records(directory):
    records = []
    for journal in sorted(directory.rglob('events.jsonl')):
        events, error = read_events(journal.parent)
        try:
            relative = str(journal.parent.relative_to(directory))
        except ValueError:
            relative = str(journal.parent)
        summary = inspect_run(journal.parent)
        records.append(
            {
                'run': relative,
                'journal_error': error,
                'state': summary['state'],
                'exit_code': summary['exit_code'],
                'publication': summary['publication'],
                'counts': summary['counts'],
                'unknown_operations': summary['unknown_operations'],
                'events': events,
            }
        )
    return records
