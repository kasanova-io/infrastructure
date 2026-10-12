import unittest
from backup import property_tail_offsets, property_metrics, property_batch_drained, property_tail_preserved, ingestion_drained


class PropertyDefinitionDrainTest(unittest.TestCase):
    def report(self, current=99, end=100):
        return ('GROUP property-defs-rs\nTOPIC PARTITION CURRENT-OFFSET LOG-START-OFFSET LOG-END-OFFSET LAG MEMBER-ID CLIENT-ID HOST\n'
                f'clickhouse_events_json 0 {current} 0 {end} {end-current} member client host\n')

    def lags(self):
        return {'group1': 0, 'clickhouse-ingestion': 0, 'clickhouse-ingestion-historical': 0, 'property-defs-rs': 1}

    def sample(self, cycles=10):
        return {'offsets': property_tail_offsets(self.report()), 'metrics': {
            'prop_defs_events_received': 10, 'prop_defs_seen_updates': 100,
            'prop_defs_batch_acquire_time_ms_count': cycles,
            'prop_defs_channel_messages_in_flight': 0,
            'prop_defs_channel_capacity': 1000, 'prop_defs_channel_capacity_total': 1000}}

    def test_inclusive_offset_requires_exact_partition_evidence(self):
        self.assertFalse(ingestion_drained(self.lags()))
        self.assertTrue(ingestion_drained(self.lags(), property_tail_offsets(self.report())))
        lags = self.lags(); lags['property-defs-rs'] = 2
        self.assertFalse(ingestion_drained(lags, property_tail_offsets(self.report(98))))
        lags['clickhouse-ingestion'] = 1
        self.assertFalse(ingestion_drained(lags, property_tail_offsets(self.report())))

    def test_unknown_missing_and_duplicate_partition_fail_closed(self):
        for text in ('', self.report().replace('99', '-'), self.report().replace('clickhouse_events_json 0', 'other 0'),
                     self.report() + self.report().splitlines()[-1] + '\n'):
            with self.subTest(text=text), self.assertRaises(RuntimeError):
                property_tail_offsets(text)

    def test_tail_offset_alone_does_not_prove_buffered_database_writes(self):
        self.assertFalse(property_batch_drained(self.sample(), self.sample(11)))
        self.assertTrue(property_batch_drained(self.sample(), self.sample(12)))
        for key in ('prop_defs_events_received', 'prop_defs_seen_updates', 'prop_defs_channel_messages_in_flight'):
            latest = self.sample(12); latest['metrics'][key] += 1
            self.assertFalse(property_batch_drained(self.sample(), latest))
        latest = self.sample(12); latest['metrics']['prop_defs_channel_capacity'] -= 1
        self.assertFalse(property_batch_drained(self.sample(), latest))

    def test_offset_changes_and_database_failures_reject_proof(self):
        latest = self.sample(12); latest['offsets'] = property_tail_offsets(self.report(100, 101))
        self.assertFalse(property_batch_drained(self.sample(), latest))
        for key in ('prop_defs_issue_failed', 'prop_defs_event_parse_error', 'prop_defs_offset_store_failures'):
            latest = self.sample(12); latest['metrics'][key] = 1
            with self.assertRaises(RuntimeError):
                property_batch_drained(self.sample(), latest)

    def test_missing_and_nonfinite_metrics_cannot_certify_drain(self):
        for text in ('prop_defs_events_received 1', 'metric NaN', 'metric -1'):
            with self.assertRaises(RuntimeError):
                property_metrics(text)

    def test_final_shutdown_may_commit_forward_but_never_reset_or_hide_new_events(self):
        before = property_tail_offsets(self.report())
        self.assertTrue(property_tail_preserved(before, property_tail_offsets(self.report(100))))
        self.assertFalse(property_tail_preserved(before, property_tail_offsets(self.report(98))))
        self.assertFalse(property_tail_preserved(before, property_tail_offsets(self.report(100, 101))))
