"""Synthetic claim evidence for rollback journal fixtures."""

import json

from workbench_lite.id_claims import claim_identities_from_plan
from workbench_lite.runs import RunJournal as _RunJournal


class ClaimAwareRunJournal(_RunJournal):
    def start(self, stage, options=None):
        super().start(stage, options)
        if stage in {'push', 'push-dry-run'}:
            self._record_claim_evidence()

    def _record_claim_evidence(self):
        metadata = json.loads((self.path / 'run.json').read_text())
        plan = json.loads((self.path / 'upload-plan.json').read_text())
        config = json.loads((self.path / 'config.json').read_text())
        identities = sorted(
            claim_identities_from_plan(plan['entries'], config, metadata['batch_id']),
            key=lambda identity: identity[1],
        )
        expected = [
            {'bucket': bucket, 'key': key, 'checksum': checksum, 'size_bytes': size}
            for bucket, key, checksum, size in identities
        ]
        self.append('context', stage='claim', expected_claims=expected)
        for claim in expected:
            operation = self.intent('claim', claim)
            self.result(
                operation,
                'claim',
                {
                    **claim,
                    'status': 'matched',
                    'remote_outcome': 'matched',
                    'observed_checksum': claim['checksum'],
                    'observed_size_bytes': claim['size_bytes'],
                    'checksum_type': 'FULL_OBJECT',
                },
            )
