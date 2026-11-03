"""Behavioral tests for canonical IDs in package checks."""

import csv
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from cli_test_runner import main as cli_main


class CanonicalIdCheckTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.config = self.root / 'package.yml'
        self.csv_path = self.root / 'input.csv'
        self.config.write_text(
            'input_dir: .\ninput_csv: input.csv\nbatch_id: package\n',
            encoding='utf-8',
        )

    def check_ids(self, identifiers):
        rows = [
            (f'Row {index}', identifier, 'Paged Content', '', '', 'parent.pdf')
            for index, identifier in enumerate(identifiers, 1)
        ]
        return self.check_rows(rows, 'runs')

    def check_rows(self, rows, run_name):
        with self.csv_path.open('w', encoding='utf-8', newline='') as handle:
            writer = csv.writer(handle)
            writer.writerow(['title', 'id', 'field_model', 'parent_id', 'field_weight', 'file'])
            writer.writerows(rows)
        output, error = tempfile.TemporaryFile(mode='w+'), tempfile.TemporaryFile(mode='w+')
        try:
            with patch('sys.stdout', output), patch('sys.stderr', error):
                code = cli_main(
                    [
                        'check',
                        '--config',
                        str(self.config),
                        '--allow-dummy-files',
                        '--format',
                        'json',
                        '--run-dir',
                        str(self.root / run_name),
                    ]
                )
            output.seek(0)
            return code, json.loads(output.read())
        finally:
            output.close()
            error.close()

    def test_check_rejects_casefold_and_unicode_equivalent_distinct_ids(self):
        pairs = (
            ('ABC', 'abc'),
            ('é', 'e\u0301'),
            ('ß', 'SS'),
            ('SS', 'ss'),
            ('Σ', 'σ'),
            ('σ', 'ς'),
            ('I', 'i'),
            ('i', 'İ'),
        )
        for first, second in pairs:
            with self.subTest(ids=(first, second)):
                code, payload = self.check_ids((first, second))
                self.assertEqual(code, 3, payload)
                errors = '\n'.join(payload['validation_errors'])
                self.assertIn(first, errors)
                self.assertIn(second, errors)

    def test_check_keeps_dotless_i_distinct(self):
        code, payload = self.check_ids(('ı', 'i'))
        self.assertEqual(code, 0, payload)

    def test_check_still_rejects_exact_duplicate_ids(self):
        code, payload = self.check_ids(('same-id', 'same-id'))

        self.assertEqual(code, 3, payload)
        self.assertIn('Duplicate id values: same-id', payload['validation_errors'])

    def test_check_still_refuses_ids_that_normalize_to_one_storage_segment(self):
        code, payload = self.check_ids(('a b', 'a_b'))
        self.assertEqual(code, 3, payload)
        errors = '\n'.join(payload['validation_errors'])
        self.assertIn('a b', errors)
        self.assertIn('a_b', errors)

    def test_storage_segment_pair_is_reported_once_as_plan_collision(self):
        rows = (
            ('First parent', 'a b', 'Paged Content', '', '', 'first.pdf'),
            ('Second parent', 'a_b', 'Paged Content', '', '', 'second.pdf'),
        )
        code, payload = self.check_rows(rows, 'plan-collision-runs')

        self.assertEqual(code, 3, payload)
        self.assertEqual(len(payload['validation_errors']), 1, payload)
        error = payload['validation_errors'][0]
        self.assertIn('Upload plan collision', error)
        self.assertIn('a b', error)
        self.assertIn('a_b', error)

    def test_check_refuses_equivalent_page_ids_under_different_parents(self):
        rows = (
            ('Parent A', 'parent-a', 'Paged Content', '', '', 'parent-a.pdf'),
            ('Parent B', 'parent-b', 'Paged Content', '', '', 'parent-b.pdf'),
            ('Composed page', 'é', 'Page', 'parent-a', 1, 'page-a.tif'),
            ('Decomposed page', 'e\u0301', 'Page', 'parent-b', 1, 'page-b.tif'),
        )
        code, payload = self.check_rows(rows, 'page-collision-runs')

        self.assertEqual(code, 3, payload)
        errors = '\n'.join(payload['validation_errors'])
        for value in ('é', 'e\u0301'):
            self.assertIn(value, errors)


if __name__ == '__main__':
    unittest.main()
