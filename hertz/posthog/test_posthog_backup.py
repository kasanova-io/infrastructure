import unittest

from backup import ingestion_drained, parse_group_lags


class ConsumerDrainTest(unittest.TestCase):
    def test_recognizes_all_group_offsets_including_nonzero_backlog(self):
        output = 'GROUP group1\nSTATE Stable\nTOTAL-LAG 0\n\nGROUP clickhouse-ingestion\nTOTAL-LAG 7\n'
        self.assertEqual(parse_group_lags(output, ['group1', 'clickhouse-ingestion']),
                         {'group1': 0, 'clickhouse-ingestion': 7})

    def test_missing_group_cannot_be_reported_as_drained(self):
        with self.assertRaises(RuntimeError):
            parse_group_lags('GROUP group1\nTOTAL-LAG 0\n', ['group1', 'clickhouse-ingestion'])

    def test_unknown_or_duplicate_offsets_cannot_be_reported_as_drained(self):
        for output in ['GROUP group1\nTOTAL-LAG -\n',
                       'GROUP group1\nTOTAL-LAG 0\nTOTAL-LAG 0\n',
                       'TOTAL-LAG 0\n', 'GROUP group1\nSTATE Empty\n']:
            with self.subTest(output=output), self.assertRaises(RuntimeError):
                parse_group_lags(output, ['group1'])

    def test_preview_offsets_do_not_hide_storage_backlog(self):
        lags = {'group1': 0, 'clickhouse-ingestion': 0,
                'clickhouse-ingestion-historical': 0, 'livestream': 3}
        self.assertTrue(ingestion_drained(lags))
        lags['clickhouse-ingestion-historical'] = 1
        self.assertFalse(ingestion_drained(lags))
        lags['clickhouse-ingestion-historical'] = 0
        lags['unrecognized-new-consumer'] = 1
        self.assertFalse(ingestion_drained(lags))

    def test_missing_ingestion_group_cannot_be_reported_as_drained(self):
        with self.assertRaises(RuntimeError):
            ingestion_drained({'livestream': 3})


if __name__ == '__main__':
    unittest.main()
