import unittest

from backup import parse_group_lags


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


if __name__ == '__main__':
    unittest.main()
