"""引擎与独立全排序参考的逐版本核对。

场景覆盖：插入/撤回/同组更正/跨组更正、null、空组与全 null 组、
超大重复值组、重复值跨状态块、小资源预算下的连接/缓存淘汰、
精确有理分位、严格小于/小于等于、秩区间与重数。
"""
import random
import shutil
import tempfile
import unittest
from fractions import Fraction

from quantile_data import (AggregationEngine, Budget, ReferenceState,
                           QuantileUndefined, EmptyGroup,
                           RankOutOfRange)
from quantile_data.ops import group_key

PS = [(0, 1), (1, 4), (1, 2), (3, 4), (1, 1)]


def make_image(tenant, day, latency, source):
    return {'tenant': tenant, 'day': day, 'latency': latency,
            'source': source}


class EngineReferenceTests(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.engine = AggregationEngine(
            self.dir, Budget(open_shards=3, cached_nodes=24,
                             open_snapshots=2), seed=2024)
        self.ref = ReferenceState()
        self.rng = random.Random(77)
        self.seq = 0

    def tearDown(self):
        self.engine.close()
        shutil.rmtree(self.dir, ignore_errors=True)

    def _next_id(self):
        self.seq += 1
        return f'row{self.seq}'

    def _check_all_groups(self):
        for gkey in self.ref.all_groups() | {
                group_key(t, d) for t, d in [('T1', 1), ('T2', 1),
                                              ('T1', 2)]}:
            self._check_group(gkey)

    def _check_group(self, gkey):
        # gkey 形如 ["g", tenant, day]
        _, tenant, day = __import__('json').loads(gkey)
        rc = self.ref.counts(tenant, day)
        try:
            ec = self.engine.counts(tenant, day)
        except Exception:
            # 从未建立的分组：参考实现也应为空。
            self.assertEqual(rc['count_all'], 0)
            return
        self.assertEqual(ec, rc, gkey)
        if rc['count_all'] == 0:
            with self.assertRaises(EmptyGroup):
                self.engine.quantile(tenant, day, 1, 2)
            return
        if rc['count_nonnull'] == 0:
            with self.assertRaises(QuantileUndefined):
                self.engine.quantile(tenant, day, 1, 2)
            return
        for pn, pd in PS:
            q = self.engine.quantile(tenant, day, pn, pd)
            rv, rlo, rhi, rlor, rhir, rn = self.ref.quantile(
                tenant, day, pn, pd)
            self.assertEqual(q.value, rv, (gkey, pn, pd))
            self.assertEqual(q.n, rn)
            self.assertEqual((q.low, q.high), (rlo, rhi))
            self.assertIsInstance(q.value, Fraction)
            # 精确性：插值结果分母整除 p_den*(n-1) 的路径，绝不返回 float
            self.assertNotIsInstance(q.value, float)
        values = self.ref._sorted_values(tenant, day)
        for probe in set([v - 1 for v in values] + values +
                         [v + 1 for v in values]):
            self.assertEqual(self.engine.count_less(tenant, day, probe),
                             self.ref.count_less(tenant, day, probe))
            self.assertEqual(self.engine.count_leq(tenant, day, probe),
                             self.ref.count_leq(tenant, day, probe))
            self.assertEqual(
                self.engine.value_multiplicity(tenant, day, probe),
                self.ref.multiplicity(tenant, day, probe))
        n = rc['count_nonnull']
        self.assertEqual(
            self.engine.rank_range(tenant, day, 0, n - 1),
            self.ref.rank_range(tenant, day, 0, n - 1))
        if n > 3:
            self.assertEqual(
                self.engine.rank_range(tenant, day, 1, n - 2),
                self.ref.rank_range(tenant, day, 1, n - 2))

    def _apply_ok(self, ops):
        bid = self._next_id()
        receipt = self.engine.apply_batch(bid, ops)
        self.assertEqual(receipt.state, 'COMMITTED')
        self.ref.apply(ops)
        return bid

    def test_mixed_lifecycle_matches_reference_version_by_version(self):
        # 初始插入：含重复值与 null
        for _ in range(12):
            rid = self._next_id()
            tenant = self.rng.choice(['T1', 'T2'])
            latency = self.rng.choice([None, 1, 1, 2, 3, 5, 8, 13])
            self._apply_ok([{'op': 'insert', 'row_id': rid, 'old': None,
                             'new': make_image(tenant, 1, latency, 's1')}])
        self._check_all_groups()
        # 混合批次：撤回、同组更正、跨组更正
        for _ in range(40):
            if not self.ref.rows:
                break
            rid = self.rng.choice(list(self.ref.rows))
            old = self.ref.rows[rid]
            roll = self.rng.random()
            if roll < 0.35:
                ops = [{'op': 'retract', 'row_id': rid,
                        'old': dict(old), 'new': None}]
            elif roll < 0.7:
                new = dict(old, latency=self.rng.choice(
                    [None, 1, 2, 3, 5, 8, 13, 21]))
                ops = [{'op': 'correct', 'row_id': rid,
                        'old': dict(old), 'new': new}]
            else:
                new = dict(old)
                new['tenant'] = 'T2' if old['tenant'] == 'T1' else 'T1'
                new['day'] = self.rng.choice([1, 2])
                ops = [{'op': 'correct', 'row_id': rid,
                        'old': dict(old), 'new': new}]
            self._apply_ok(ops)
            if self.seq % 5 == 0:
                self._check_all_groups()
        self._check_all_groups()

    def test_huge_duplicate_group_with_tiny_budget(self):
        # 一个超大重复值组：8000 行同值，预算极小。
        huge = 8000
        chunk = 500
        for start in range(0, huge, chunk):
            ops = [{'op': 'insert', 'row_id': f'h{i}', 'old': None,
                    'new': make_image('H', 1, 7, 's1')}
                   for i in range(start, min(start + chunk, huge))]
            self._apply_ok(ops)
        ec = self.engine.counts('H', 1)
        self.assertEqual(ec['count_nonnull'], huge)
        # 重复值只有一个 treap 节点：物理节点数远小于行数。
        shard = self.engine._open_shard(
            self.engine.catalog.get_group(group_key('H', 1))['gid'])
        node_rows = shard.conn.execute(
            'SELECT COUNT(*) FROM nodes').fetchone()[0]
        self.assertLess(node_rows, 50)
        # 秩查询只走秩路径；缓存预算被严格遵守。
        stats = self.engine.stats_snapshot()
        self.assertLessEqual(stats['cache_size_peak'],
                             self.engine.budget.cached_nodes)
        self.assertLessEqual(stats['open_shards_peak'],
                             self.engine.budget.open_shards)
        for pn, pd in PS:
            q = self.engine.quantile('H', 1, pn, pd)
            self.assertEqual(q.value, Fraction(7))
        self.assertEqual(
            self.engine.rank_range('H', 1, 123, huge - 456),
            [(7, huge - 456 - 123 + 1)])
        # 撤回一半后精确减半，重数不会变负。
        retract_ops = [
            {'op': 'retract', 'row_id': f'h{i}',
             'old': make_image('H', 1, 7, 's1'), 'new': None}
            for i in range(0, huge, 2)]
        # 分批撤回
        for i in range(0, len(retract_ops), chunk):
            self._apply_ok(retract_ops[i:i + chunk])
        self.assertEqual(
            self.engine.counts('H', 1)['count_nonnull'], huge // 2)
        self.assertEqual(
            self.engine.value_multiplicity('H', 1, 7), huge // 2)

    def test_duplicates_across_state_blocks_not_lost(self):
        # 同值在多个批次/多个 treap 版本块中累计，秩与计数不漏。
        for block in range(10):
            ops = [{'op': 'insert', 'row_id': f'd{block}_{i}', 'old': None,
                    'new': make_image('D', 1, 5, 's1')}
                   for i in range(20)]
            ops += [{'op': 'insert', 'row_id': f'e{block}_{i}', 'old': None,
                     'new': make_image('D', 1, 9, 's2')}
                    for i in range(5)]
            self._apply_ok(ops)
        self.assertEqual(self.engine.count_less('D', 1, 9), 200)
        self.assertEqual(self.engine.count_leq('D', 1, 9), 250)
        self.assertEqual(self.engine.value_multiplicity('D', 1, 5), 200)
        self.assertEqual(self.engine.value_multiplicity('D', 1, 9), 50)
        full = self.engine.rank_range('D', 1, 0, 249)
        self.assertEqual(full, [(5, 200), (9, 50)])

    def test_null_and_empty_groups_distinguished(self):
        self._apply_ok([
            {'op': 'insert', 'row_id': 'n1', 'old': None,
             'new': make_image('N', 1, None, 's1')},
            {'op': 'insert', 'row_id': 'n2', 'old': None,
             'new': make_image('N', 1, None, 's1')}])
        counts = self.engine.counts('N', 1)
        self.assertEqual(counts, {'count_all': 2, 'count_nonnull': 0,
                                  'count_null': 2})
        with self.assertRaises(QuantileUndefined):
            self.engine.quantile('N', 1, 1, 2)
        self._apply_ok([
            {'op': 'retract', 'row_id': 'n1',
             'old': make_image('N', 1, None, 's1'), 'new': None},
            {'op': 'retract', 'row_id': 'n2',
             'old': make_image('N', 1, None, 's1'), 'new': None}])
        with self.assertRaises(EmptyGroup):
            self.engine.quantile('N', 1, 1, 2)
        self.assertEqual(self.engine.counts('N', 1)['count_all'], 0)

    def test_rank_out_of_range_rejected(self):
        self._apply_ok([{'op': 'insert', 'row_id': 'k1', 'old': None,
                         'new': make_image('K', 1, 3, 's1')}])
        with self.assertRaises(RankOutOfRange):
            self.engine.rank_range('K', 1, 0, 1)
        with self.assertRaises(RankOutOfRange):
            self.engine.rank_range('K', 1, -1, 0)

    def test_budget_evictions_observed(self):
        # 分组数超过打开分片预算，触发连接淘汰；峰值不破预算。
        for t in range(8):
            self._apply_ok([
                {'op': 'insert', 'row_id': f't{t}', 'old': None,
                 'new': make_image(f'T{t}', 1, t, 's1')}])
        stats = self.engine.stats_snapshot()
        self.assertGreater(stats['shard_evictions'], 0)
        self.assertLessEqual(stats['open_shards_peak'],
                             self.engine.budget.open_shards)


if __name__ == '__main__':
    unittest.main()
