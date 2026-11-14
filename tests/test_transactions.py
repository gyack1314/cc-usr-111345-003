"""批事务、崩溃注入与恢复的测试。

在 INTENT 后、各分片提交后、COMMITTED 后注入中断，验证：
* 重开引擎自动恢复，结果与参考一致；
* 分片 applied 回执使重放幂等，重试不重复累计；
* 校验类拒绝不产生任何状态变化；
* 旧读句柄在保留期内稳定。
"""
import shutil
import tempfile
import unittest

from quantile_data import AggregationEngine, Budget, ReferenceState
from quantile_data.errors import BatchRejected, HandleReleased


def img(tenant, day, latency, source='s1'):
    return {'tenant': tenant, 'day': day, 'latency': latency,
            'source': source}


class TransactionTests(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.dir, ignore_errors=True)

    def _seed(self, engine, ref):
        ops = []
        for i in range(60):
            ops.append({'op': 'insert', 'row_id': f'r{i}', 'old': None,
                        'new': img('T1', 1, i % 5,
                                   's1' if i % 2 else 's2')})
        for i in range(10):
            ops.append({'op': 'insert', 'row_id': f'n{i}', 'old': None,
                        'new': img('T2', 1, None)})
        engine.apply_batch('seed', ops)
        ref.apply(ops)

    def test_crash_at_every_point_recovers_consistently(self):
        for point in ('after_intent', 'after_shard', 'after_commit'):
            directory = tempfile.mkdtemp(dir=self.dir)
            ref = ReferenceState()
            engine = AggregationEngine(directory, Budget(2, 32, 2), seed=1)
            self._seed(engine, ref)

            target_ops = [
                {'op': 'correct', 'row_id': 'r1',
                 'old': img('T1', 1, 1, 's1'),
                 'new': img('T3', 7, 99, 's3')},
                {'op': 'retract', 'row_id': 'r2',
                 'old': img('T1', 1, 2, 's2'), 'new': None},
                {'op': 'correct', 'row_id': 'r3',
                 'old': img('T1', 1, 3, 's1'),
                 'new': img('T1', 1, 4, 's1')}]

            def hook(p, ctx, _point=point):
                if p == _point and ctx.get('batch_id') == 'crash':
                    raise RuntimeError(f'crash@{_point}')

            engine.crash_hook = hook
            try:
                engine.apply_batch('crash', target_ops)
                crashed = False
            except RuntimeError:
                crashed = True
            self.assertTrue(crashed)
            engine.close()

            # 重开：恢复 INTENT；after_commit 场景下批次可能已完成。
            engine2 = AggregationEngine(directory, Budget(2, 32, 2), seed=1)
            ref.apply(target_ops)
            for tenant, day in (('T1', 1), ('T2', 1), ('T3', 7)):
                self.assertEqual(engine2.counts(tenant, day),
                                 ref.counts(tenant, day))
            # 原 row_id 在新组仍可撤回。
            engine2.apply_batch('late', [
                {'op': 'retract', 'row_id': 'r1',
                 'old': img('T3', 7, 99, 's3'), 'new': None}])
            # 同批重试：已提交则幂等返回，计数不翻倍。
            again = engine2.apply_batch('crash', target_ops) \
                if point != 'after_commit' else None
            before = {g: engine2.counts(*g) for g in
                      (('T1', 1), ('T2', 1), ('T3', 7))}
            engine2.apply_batch('crash', target_ops)
            after = {g: engine2.counts(*g) for g in
                     (('T1', 1), ('T2', 1), ('T3', 7))}
            self.assertEqual(before, after)
            engine2.close()
            shutil.rmtree(directory, ignore_errors=True)

    def test_rejected_batch_leaves_state_untouched(self):
        engine = AggregationEngine(self.dir, Budget(2, 32, 2), seed=2)
        ref = ReferenceState()
        self._seed(engine, ref)
        good = {g: engine.counts(*g) for g in (('T1', 1), ('T2', 1))}
        bad_batch = [
            {'op': 'retract', 'row_id': 'r1',
             'old': img('T1', 1, 999), 'new': None},       # 旧像不一致
            {'op': 'retract', 'row_id': 'ghost',
             'old': img('T1', 1, 1), 'new': None},          # 不存在
            {'op': 'insert', 'row_id': 'r1', 'old': None,
             'new': img('T1', 1, 1)}]                        # 同 ID 已存在
        with self.assertRaises(BatchRejected):
            engine.apply_batch('bad', bad_batch)
        self.assertEqual({g: engine.counts(*g) for g in
                          (('T1', 1), ('T2', 1))}, good)
        # 没有留下 INTENT。
        self.assertEqual(list(engine.catalog.intents()), [])

    def test_duplicate_id_in_batch_rejected(self):
        engine = AggregationEngine(self.dir, Budget(2, 32, 2), seed=3)
        with self.assertRaises(BatchRejected):
            engine.apply_batch('dup', [
                {'op': 'insert', 'row_id': 'x', 'old': None,
                 'new': img('A', 1, 1)},
                {'op': 'insert', 'row_id': 'x', 'old': None,
                 'new': img('A', 1, 2)}])
        self.assertEqual(engine.catalog.list_groups(), [])

    def test_insert_then_retract_same_id_in_batch_rejected(self):
        engine = AggregationEngine(self.dir, Budget(2, 32, 2), seed=31)
        with self.assertRaises(BatchRejected):
            engine.apply_batch('mixed', [
                {'op': 'insert', 'row_id': 'x', 'old': None,
                 'new': img('A', 1, 1)},
                {'op': 'retract', 'row_id': 'x',
                 'old': img('A', 1, 1), 'new': None}])
        # 整批拒绝，没有任何分组或 INTENT 残留。
        self.assertEqual(engine.catalog.list_groups(), [])
        self.assertEqual(list(engine.catalog.intents()), [])
        engine.close()

    def test_snapshot_stable_through_updates_and_gc(self):
        engine = AggregationEngine(self.dir, Budget(2, 32, 2), seed=4)
        self._seed(engine, ReferenceState())
        snap = engine.snapshot()
        frozen = snap.counts('T1', 1)
        engine.apply_batch('churn', [
            {'op': 'retract', 'row_id': f'r{i}',
             'old': img('T1', 1, i % 5, 's1' if i % 2 else 's2'),
             'new': None} for i in range(0, 20, 2)])
        self.assertEqual(snap.counts('T1', 1), frozen)
        self.assertNotEqual(engine.counts('T1', 1), frozen)
        engine.gc_versions()
        # GC 后保留期内的旧句柄仍稳定可读。
        self.assertEqual(snap.quantile('T1', 1, 1, 2).n,
                         frozen['count_nonnull'])
        snap.release()
        with self.assertRaises(HandleReleased):
            snap.counts('T1', 1)
        engine.close()


if __name__ == '__main__':
    unittest.main()
