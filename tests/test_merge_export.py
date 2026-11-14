"""分片合并与只读导出测试。"""
import os
import shutil
import tempfile
import unittest

from quantile_data import AggregationEngine, Budget
from quantile_data.errors import MergeRejected, NotAStateError, \
    ReadOnlyExportError


def img(tenant, day, latency, source):
    return {'tenant': tenant, 'day': day, 'latency': latency,
            'source': source}


class MergeExportTests(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def _source_state(self, name, prefix, source, n):
        path = os.path.join(self.root, name)
        engine = AggregationEngine(path, Budget(2, 32, 2),
                                   seed=hash(name) & 0xffff)
        engine.apply_batch('load', [
            {'op': 'insert', 'row_id': f'{prefix}{i}', 'old': None,
             'new': img('A', 1, i, source)} for i in range(n)])
        engine.close()
        return path, os.path.join(path, 'shards', 'g1.db')

    def test_disjoint_sources_merge_and_keep_identity(self):
        _p1, s1 = self._source_state('src1', 'a', 's1', 30)
        _p2, s2 = self._source_state('src2', 'b', 's2', 20)
        dst_dir = os.path.join(self.root, 'dst')
        dst = AggregationEngine(dst_dir, Budget(1, 16, 1), seed=9)
        receipt = dst.merge_shard([s1, s2], 'merge1')
        self.assertEqual(receipt.state, 'COMMITTED')
        self.assertEqual(sorted(receipt.sources), ['s1', 's2'])
        self.assertEqual(dst.counts('A', 1)['count_all'], 50)
        # 合并后仍可按原 row_id 撤回。
        dst.apply_batch('after', [
            {'op': 'retract', 'row_id': 'a0',
             'old': img('A', 1, 0, 's1'), 'new': None}])
        self.assertEqual(dst.counts('A', 1)['count_all'], 49)
        dst.close()

    def test_overlapping_sources_rejected(self):
        _p1, s1 = self._source_state('o1', 'a', 'shared', 5)
        _p2, s2 = self._source_state('o2', 'b', 'shared', 5)
        dst_dir = os.path.join(self.root, 'dst2')
        dst = AggregationEngine(dst_dir, Budget(2, 32, 2), seed=10)
        before = dst.catalog.list_groups()
        with self.assertRaises(MergeRejected):
            dst.merge_shard([s1, s2], 'bad')
        self.assertEqual(dst.catalog.list_groups(), before)
        dst.close()

    def test_row_id_overlap_rejected_even_if_sources_differ(self):
        path1 = os.path.join(self.root, 'r1')
        path2 = os.path.join(self.root, 'r2')
        e1 = AggregationEngine(path1, Budget(2, 32, 2), seed=11)
        e1.apply_batch('x', [{'op': 'insert', 'row_id': 'dup',
                              'old': None, 'new': img('A', 1, 1, 's1')}])
        e2 = AggregationEngine(path2, Budget(2, 32, 2), seed=12)
        e2.apply_batch('y', [{'op': 'insert', 'row_id': 'dup',
                              'old': None, 'new': img('A', 1, 2, 's2')}])
        e1.close(); e2.close()
        dst = AggregationEngine(os.path.join(self.root, 'r3'),
                                Budget(2, 32, 2), seed=13)
        with self.assertRaises(MergeRejected):
            dst.merge_shard([os.path.join(path1, 'shards', 'g1.db'),
                             os.path.join(path2, 'shards', 'g1.db')], 'm')
        dst.close()

    def test_readonly_export_is_queryable_but_not_writable(self):
        path = os.path.join(self.root, 'w')
        engine = AggregationEngine(path, Budget(2, 32, 2), seed=14)
        engine.apply_batch('load', [
            {'op': 'insert', 'row_id': f'q{i}', 'old': None,
             'new': img('A', 1, i % 4, 's1')} for i in range(40)])
        export_dir = os.path.join(path, 'export')
        ro = engine.export_readonly(export_dir)
        self.assertEqual(ro.counts('A', 1), engine.counts('A', 1))
        self.assertEqual(ro.quantile('A', 1, 1, 2).value,
                         engine.quantile('A', 1, 1, 2).value)
        self.assertEqual(ro.rank_range('A', 1, 0, 39),
                         engine.rank_range('A', 1, 0, 39))
        with self.assertRaises(ReadOnlyExportError):
            ro.apply_batch('x', [])
        # 导出目录不能被可写引擎打开。
        with self.assertRaises(NotAStateError):
            AggregationEngine(export_dir, Budget(2, 32, 2))
        ro.release()
        engine.close()


if __name__ == '__main__':
    unittest.main()
