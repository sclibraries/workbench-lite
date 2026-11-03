"""Synthetic claim evidence for rollback journal fixtures."""

import json

from workbench_lite.id_claims import claim_identities_from_plan
from workbench_lite.runs import RunJournal as _RunJournal, read_events


class ClaimAwareRunJournal(_RunJournal):
    def intent(self, stage, identity, *, operation_id=None, attempt=1):
        if stage == 'push':
            self._record_claim_evidence()
        return super().intent(stage, identity, operation_id=operation_id, attempt=attempt)

    def finish(self, code, error=None):
        if self.stage in {'push', 'push-dry-run'} and self._has_destination_check_result():
            self._record_claim_evidence()
        return super().finish(code, error)

    def _has_destination_check_result(self):
        events, error = read_events(self.path)
        return not error and any(
            event.get('stage') == 'destination-check' and event['kind'] == 'result'
            for event in events
        )

    def _record_claim_evidence(self):
        events, error = read_events(self.path)
        if error or any(
            event.get('stage') == 'claim' and event['kind'] == 'context' for event in events
        ):
            return
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
