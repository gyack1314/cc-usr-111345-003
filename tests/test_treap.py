"""函数式 treap 的结构与秩路径测试。

直接对 SQLite 落盘的 treap 做随机化性质核对：任意版本的中序序列、
子树计数、kth、count_lt/le、rank_range 都与 Python 排序参考一致；
旧根在新版本写入后仍可遍历（持久性）。
"""
import os
import sqlite3
import tempfile
import unittest
from collections import Counter

from quantile_data.budget import Stats
from quantile_data.treap import PersistentTreap


def fresh_treap(cache=8):
    tmp = tempfile.mkdtemp()
    conn = sqlite3.connect(os.path.join(tmp, 't.db'))
    conn.executescript(
        'CREATE TABLE nodes (id INTEGER PRIMARY KEY, value INTEGER, '
        'multiplicity INTEGER, priority INTEGER, left_id INTEGER, '
        'right_id INTEGER, subcnt INTEGER, version INTEGER);')
    return tmp, conn, PersistentTreap(conn, Stats(), node_cache_capacity=cache,
                                      seed=12345)


class TreapTests(unittest.TestCase):
    def setUp(self):
        self.tmp, self.conn, self.treap = fresh_treap()

    def tearDown(self):
        self.conn.close()

    def _flatten(self, root):
        return list(self.treap.in_order(root))

    def test_random_deltas_match_sorted_reference(self):
        import random
        rng = random.Random(99)
        root = None
        mult = Counter()
        for version in range(1, 401):
            value = rng.randrange(0, 25)
            if mult[value] == 0 or rng.random() < 0.6:
                root = self.treap.apply_delta(root, value, 1, version)
                mult[value] += 1
            else:
                root = self.treap.apply_delta(root, value, -1, version)
                mult[value] -= 1
                if mult[value] == 0:
                    del mult[value]
            if version % 25 == 0:
                self._check(root, mult)
        self._check(root, mult)

    def _check(self, root, mult):
        expected = sorted(mult.elements())
        pairs = self._flatten(root)
        rebuilt = [v for v, m in pairs for _ in range(m)]
        self.assertEqual(rebuilt, expected)
        # subcnt 一致性
        self.assertEqual(self.treap.subcnt(self.treap.get_node(root)),
                         len(expected))
        # kth 全覆盖
        for rank, value in enumerate(expected):
            self.assertEqual(self.treap.kth(root, rank), value)
        # count_lt / count_le 对边界与重复值
        for probe in range(-2, 28):
            self.assertEqual(self.treap.count_lt(root, probe),
                             sum(1 for v in expected if v < probe))
            self.assertEqual(self.treap.count_le(root, probe),
                             sum(1 for v in expected if v <= probe))
        # rank_range 多区间，含横跨重复值块
        if expected:
            for lo in (0, len(expected) // 3, len(expected) - 1):
                for hi in (lo, min(len(expected) - 1, lo + 7)):
                    got = self.treap.rank_range(root, lo, hi)
                    ref = Counter(expected[lo:hi + 1])
                    self.assertEqual(Counter(dict(got)), ref)
                    self.assertEqual(sum(m for _, m in got), hi - lo + 1)

    def test_persistent_roots_remain_traversable(self):
        root = None
        roots = []
        for v in range(1, 60):
            root = self.treap.apply_delta(root, v % 10, 1, v)
            roots.append(root)
        # 旧根快照保持当时的计数
        for index, old_root in enumerate(roots):
            self.assertEqual(self.treap.subcnt(
                self.treap.get_node(old_root)), index + 1)

    def test_huge_duplicate_value_is_one_node(self):
        root = None
        huge = 5000
        for v in range(huge):
            root = self.treap.apply_delta(root, 42, 1, v + 1)
        self.treap.flush()
        self.assertEqual(self.treap.subcnt(self.treap.get_node(root)), huge)
        # 同一值的重复累计为单节点重数：当前根可达的去重节点只有 1 个。
        reachable = self._flatten(root)
        self.assertEqual(reachable, [(42, huge)])
        self.assertEqual(self.treap.kth(root, huge - 1), 42)
        self.assertEqual(self.treap.rank_range(root, 100, 4999), [(42, 4900)])
        # GC 后物理节点表也只剩这一个节点。
        self.treap.gc([root])
        cur = self.conn.execute('SELECT COUNT(*) FROM nodes').fetchone()[0]
        self.assertEqual(cur, 1)

    def test_build_sorted_matches_incremental(self):
        pairs = [(v, 2) for v in range(0, 100, 3)]
        root = self.treap.build_sorted(iter(pairs), version=1)
        flat = self._flatten(root)
        self.assertEqual(flat, pairs)
        self.assertEqual(self.treap.subcnt(self.treap.get_node(root)),
                         2 * len(pairs))

    def test_build_sorted_beyond_flush_chunk_keeps_subcnt(self):
        # 回归：超过 chunk_flush 后祖先 subcnt 仍必须正确。
        sizes = [1, 2, 4095, 4096, 4097, 9000]
        for n in sizes:
            tmp = tempfile.mkdtemp()
            conn = sqlite3.connect(os.path.join(tmp, 'b.db'))
            conn.executescript(
                'CREATE TABLE nodes (id INTEGER PRIMARY KEY, value INTEGER, '
                'multiplicity INTEGER, priority INTEGER, left_id INTEGER, '
                'right_id INTEGER, subcnt INTEGER, version INTEGER);')
            treap = PersistentTreap(conn, Stats(), node_cache_capacity=8,
                                    seed=n)
            root = treap.build_sorted(((v, 1) for v in range(n)), version=1,
                                      chunk_flush=4096)
            self.assertEqual(treap.subcnt(treap.get_node(root)), n)
            self.assertEqual(treap.kth(root, n - 1), n - 1)
            self.assertEqual(treap.count_lt(root, n // 2), n // 2)
            self.assertEqual(
                treap.rank_range(root, 0, n - 1),
                [(v, 1) for v in range(n)])
            conn.close()

    def test_gc_keeps_pinned_roots(self):
        root = None
        for v in range(30):
            root = self.treap.apply_delta(root, v, 1, v + 1)
        pinned = root
        before = self.conn.execute('SELECT COUNT(*) FROM nodes').fetchone()[0]
        self.treap.gc([pinned])
        after = self.conn.execute('SELECT COUNT(*) FROM nodes').fetchone()[0]
        self.assertLessEqual(after, before)
        # 钉住根仍完整可遍历
        self.assertEqual(len(self._flatten(pinned)), 30)


if __name__ == '__main__':
    unittest.main()
