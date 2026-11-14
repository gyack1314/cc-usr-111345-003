"""与 data/ 资料契约的集成测试。

把 facts 作为插入批、changes 作为更正批喂给引擎，再用 queries 里的
有理数分位参数核对结果；同时验证资料完整性检查仍然有效。
"""
import shutil
import tempfile
import unittest

from quantile_data import AggregationEngine, Budget
from quantile_data.domain import inspect_tables
from quantile_data.io import load


class MaterialIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.dir, ignore_errors=True)

    def _drive(self, manifest):
        tables = load(manifest)
        inspect_tables(tables)
        engine = AggregationEngine(self.dir, Budget(2, 64, 2), seed=5)
        inserts = [{'op': 'insert', 'row_id': row['id'], 'old': None,
                    'new': {'tenant': row['tenant'], 'day': row['day'],
                            'latency': row['latency'],
                            'source': row['source']}}
                   for row in tables['facts']]
        engine.apply_batch('facts', inserts)
        # changes 的 old/new 是完整像；按 row 指向的 row_id 做更正/撤回。
        change_ops = []
        for change in tables['changes']:
            if change['new'] is None:
                change_ops.append({'op': 'retract',
                                   'row_id': change['row'],
                                   'old': change['old'], 'new': None})
            else:
                change_ops.append({'op': 'correct',
                                   'row_id': change['row'],
                                   'old': change['old'],
                                   'new': change['new']})
        if change_ops:
            engine.apply_batch('changes', change_ops)
        return engine, tables

    def test_normal_manifest_queries(self):
        engine, tables = self._drive('data/normal.json')
        for query in tables['queries']:
            result = engine.quantile(query['tenant'], query['day'],
                                     query['p_numerator'],
                                     query['p_denominator'])
            self.assertTrue(result.value.denominator >= 1)
            self.assertEqual(result.n,
                             engine.counts(query['tenant'],
                                           query['day'])['count_nonnull'])
        engine.close()

    def test_boundary_manifest_handles_null_group(self):
        engine, tables = self._drive('data/boundary.json')
        # boundary：r4 为全 null 行；r3 更正后进入 B 组（latency=20），
        # 因此 B 组同时含非空与 null，两种计数分别维护。
        b_counts = engine.counts('B', 1)
        self.assertEqual(b_counts['count_nonnull'], 1)
        self.assertEqual(b_counts['count_null'], 1)
        self.assertEqual(b_counts['count_all'], 2)
        for query in tables['queries']:
            result = engine.quantile(query['tenant'], query['day'],
                                     query['p_numerator'],
                                     query['p_denominator'])
            self.assertIsNotNone(result.low)
        engine.close()


if __name__ == '__main__':
    unittest.main()
