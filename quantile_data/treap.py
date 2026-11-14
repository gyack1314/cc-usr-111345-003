"""落盘的函数式有序 treap。

每个分组分片持有一棵按整数值排序的 treap：

* 节点不可变。任何插入/撤回/更正都沿秩路径克隆节点，旧根仍可被保留期内的
  读句柄遍历，因此“旧读句柄稳定”由数据结构本身保证，而不是靠加锁或复制整组。
* 每个节点存 ``subcnt = 左子树 subcnt + 重数 + 右子树 subcnt``，
  使点分位、严格小于计数、秩查询都只走 O(log n) 条秩路径。
* 节点行落在 SQLite，Python 侧只保留有界 LRU 缓存；流式中序遍历的栈深
  为 O(log n)，任何查询都不会把整组拉进内存。
"""
import random
import sys

from .budget import BoundedLRU

# treap 高概率为 O(log n)；放宽递归上限以容纳极端优先级序列。
sys.setrecursionlimit(100000)


class PersistentTreap:
    """绑定一个 SQLite 连接的持久 treap 访问器。

    ``readonly=True`` 时禁止任何写路径，供快照只读连接使用。
    新节点先进入待刷缓冲，:meth:`flush` 一次性 executemany 落盘；
    缓冲内节点对同连接读可见，因此小批更正只产生一次批量写。
    """

    def __init__(self, conn, stats, start_node_id=1, node_cache_capacity=256,
                 seed=0, readonly=False):
        self.conn = conn
        self.stats = stats
        self.readonly = readonly
        self._next_id = start_node_id
        self._pending = {}          # id -> (value, mult, prio, left, right, subcnt, ver)
        self._rng = random.Random(seed)
        self.cache = BoundedLRU(node_cache_capacity, stats=stats)
        self._cache_cap = node_cache_capacity

    # ------------------------------------------------------------------ 基础
    def _execute(self, sql, params=()):
        self.stats.inc('sqlite_statements')
        return self.conn.execute(sql, params)

    def get_node(self, node_id):
        """返回 (value, mult, prio, left, right, subcnt, version) 或 None。"""
        if node_id is None:
            return None
        pending = self._pending.get(node_id)
        if pending is not None:
            self.stats.inc('node_hits')
            return pending
        cached = self.cache.get(node_id)
        if cached is not None:
            self.stats.inc('node_hits')
            return cached
        cur = self._execute(
            'SELECT value, multiplicity, priority, left_id, right_id, '
            'subcnt, version FROM nodes WHERE id=?', (node_id,))
        row = cur.fetchone()
        self.stats.inc('node_loads')
        self.stats.inc('node_misses')
        if row is None:
            return None
        node = tuple(row)
        self.cache.put(node_id, node)
        self.stats.observe_peak('cache_size_peak', len(self.cache))
        return node

    @staticmethod
    def subcnt(node):
        return 0 if node is None else node[5]

    def _alloc(self, value, mult, priority, left, right, version):
        if self.readonly:
            raise RuntimeError('只读 treap 不允许写入')
        node_id = self._next_id
        self._next_id += 1
        sub = mult + self.subcnt(self.get_node(left)) + \
            self.subcnt(self.get_node(right))
        row = (value, mult, priority, left, right, sub, version)
        self._pending[node_id] = row
        return node_id

    def _clone(self, node, left, right, version):
        """克隆节点并替换左右孩子；未变化的孩子直接复用。"""
        value, mult, prio, old_left, old_right, _sub, old_ver = node
        return self._alloc(value, mult, prio, left, right, version)

    def flush(self):
        if not self._pending:
            return 0
        rows = [(nid, *row) for nid, row in self._pending.items()]
        self.conn.executemany(
            'INSERT INTO nodes (id, value, multiplicity, priority, left_id, '
            'right_id, subcnt, version) VALUES (?,?,?,?,?,?,?,?)', rows)
        count = len(rows)
        self.stats.inc('tree_nodes_written', count)
        self._pending.clear()
        return count

    # ------------------------------------------------- 函数式 split / merge
    def split(self, root, key):
        """按值切分：左树值 < key，右树值 >= key。"""
        if root is None:
            return None, None
        node = self.get_node(root)
        value, mult, prio, left, right = node[0], node[1], node[2], node[3], node[4]
        if value < key:
            a, b = self.split(right, key)
            new_root = self._clone(node, left, a, self._version_of(node))
            return new_root, b
        a, b = self.split(left, key)
        new_root = self._clone(node, b, right, self._version_of(node))
        return a, new_root

    def merge(self, a, b, version):
        """合并两棵树（a 全部值 < b 全部值），返回新根。"""
        if a is None:
            return b
        if b is None:
            return a
        na, nb = self.get_node(a), self.get_node(b)
        if na[2] > nb[2]:                       # 最小堆优先级
            new_right = self.merge(na[4], b, version)
            return self._clone(na, na[3], new_right, version)
        new_left = self.merge(a, nb[3], version)
        return self._clone(nb, new_left, nb[4], version)

    @staticmethod
    def _version_of(node):
        return node[6]

    def apply_delta(self, root, value, delta, version):
        """对某值的重数施加增量（可为负），返回新根。

        重数归零即删除该值；调用方负责保证不会减成负数。
        """
        lt, ge = self.split(root, value)
        eq, gt = self.split(ge, value + 1)
        if eq is None:
            if delta == 0:
                return self.merge(self.merge(lt, gt, version), None, version)
            node_id = self._alloc(value, delta, self._rng.getrandbits(63),
                                  None, None, version)
            return self.merge(self.merge(lt, node_id, version), gt, version)
        node = self.get_node(eq)
        new_mult = node[1] + delta
        if new_mult == 0:
            return self.merge(lt, gt, version)
        if new_mult < 0:
            raise AssertionError('值重数减成负数，校验层应当拦截')
        replaced = self._alloc(value, new_mult, node[2], None, None, version)
        return self.merge(self.merge(lt, replaced, version), gt, version)

    # ------------------------------------------------------------- 秩查询
    def kth(self, root, rank):
        """0 基秩（按重数计）对应的值。"""
        cur = root
        while cur is not None:
            node = self.get_node(cur)
            left_cnt = self.subcnt(self.get_node(node[3]))
            if rank < left_cnt:
                cur = node[3]
            elif rank < left_cnt + node[1]:
                return node[0]
            else:
                rank -= left_cnt + node[1]
                cur = node[4]
        raise IndexError('秩超出树范围')

    def count_lt(self, root, value):
        """严格小于 value 的值个数（含重数）。"""
        total = 0
        cur = root
        while cur is not None:
            node = self.get_node(cur)
            if value <= node[0]:
                cur = node[3]
            else:
                total += self.subcnt(self.get_node(node[3])) + node[1]
                cur = node[4]
        return total

    def count_le(self, root, value):
        return self.count_lt(root, value + 1)

    def rank_range(self, root, rank_lo, rank_hi):
        """返回升序秩区间 [rank_lo, rank_hi] 内的 (值, 重数) 序列。

        先沿一条秩路径定位起点节点，再走中序后继；访问量为
        O(log n + 输出节点数)，重复值以重数裁剪，不展开成单行。
        """
        if rank_lo < 0 or rank_hi < rank_lo:
            raise ValueError('秩区间非法')
        total = self.subcnt(self.get_node(root))
        if rank_hi >= total:
            raise IndexError('秩区间超出非空计数')
        # 祖先栈：定位起点节点时经过的路径；起点节点位于栈顶。
        stack = []
        cur = root
        target = rank_lo
        start_rank = 0
        while cur is not None:                       # 定位含 rank_lo 的节点
            node = self.get_node(cur)
            left_cnt = self.subcnt(self.get_node(node[3]))
            if target < left_cnt:
                stack.append(cur)
                cur = node[3]
            elif target < left_cnt + node[1]:
                start_rank = rank_lo - (target - left_cnt)
                stack.append(cur)
                break
            else:
                target -= left_cnt + node[1]
                cur = node[4]
        out = []
        # running 为当前节点起始值在全序列中的秩，沿中序递增，无需回查。
        running = start_rank
        slots = rank_hi - rank_lo + 1
        while stack and slots > 0:
            nid = stack.pop()
            node = self.get_node(nid)
            value, mult = node[0], node[1]
            start = running
            end = start + mult - 1
            clip_lo = max(start, rank_lo)
            clip_hi = min(end, rank_hi)
            if clip_lo <= clip_hi:
                take = clip_hi - clip_lo + 1
                out.append((value, take))
                slots -= take
            running = end + 1
            succ = node[4]                            # 中序后继：右子树最左
            while succ is not None:
                s = self.get_node(succ)
                stack.append(succ)
                succ = s[3]
        return out

    def in_order(self, root):
        """流式生成 (value, multiplicity)，栈深 O(log n)。"""
        stack = []
        cur = root
        while cur is not None or stack:
            while cur is not None:
                node = self.get_node(cur)
                stack.append(cur)
                cur = node[3]
            cur = stack.pop()
            node = self.get_node(cur)
            yield node[0], node[1]
            self.stats.inc('ledger_rows_streamed')
            cur = node[4]

    # ----------------------------------------------------------- 批量构建
    def build_sorted(self, sorted_pairs, version, chunk_flush=4096):
        """从已排序 (value, multiplicity) 流线性构建新 treap。

        两阶段以保证任意规模下 ``subcnt`` 都正确：

        1. 笛卡尔树建链：按最小堆优先级确定父子关系，节点分块刷盘，
           Python 侧只保留期望 O(log n) 深度的 id/priority 栈；
        2. :meth:`_recompute_subcnt` 自底向上重算全部子树计数并批量回修，
           因此跨越刷盘块边界的祖先节点也能拿到正确的 ``subcnt``。

        仅用于全新分片（合并/导出），不与任何旧根共享节点。
        """
        if self.readonly:
            raise RuntimeError('只读 treap 不允许构建')
        stack = []          # (node_id, priority)
        since_flush = 0
        root = None
        for value, mult in sorted_pairs:
            if mult <= 0:
                raise ValueError('构建流中的重数必须为正')
            priority = self._rng.getrandbits(63)
            last = None
            while stack and stack[-1][1] > priority:
                last, _ = stack.pop()
            # subcnt 先给占位值，阶段 2 统一重算。
            node_id = self._alloc(value, mult, priority, last, None, version)
            if stack:
                parent_id = stack[-1][0]
                parent = self._pending.get(parent_id)
                if parent is None:
                    parent = self.get_node(parent_id)
                    self._execute(
                        'UPDATE nodes SET right_id=? WHERE id=?',
                        (node_id, parent_id))
                    self.cache.put(
                        parent_id, (parent[0], parent[1], parent[2],
                                    parent[3], node_id, parent[5],
                                    parent[6]))
                else:
                    self._pending[parent_id] = (
                        parent[0], parent[1], parent[2], parent[3],
                        node_id, parent[5], parent[6])
            stack.append((node_id, priority))
            root = stack[0][0]
            since_flush += 1
            if since_flush >= chunk_flush:
                self.flush()
                since_flush = 0
        self.flush()
        self._recompute_subcnt(root)
        return root

    def _recompute_subcnt(self, root):
        """对新建树自底向上重算 subcnt 并批量回修（含已刷盘节点）。

        迭代后序遍历，栈深 O(log n)；待回修行攒批 executemany，
        不在 Python 内持有全部节点对象。
        """
        # 标准迭代后序：先按“根->右->左”入 order，再反转为“左->右->根”，
        # 保证处理某节点时其两个孩子的 subcnt 已算好。
        order = []
        work = [root]
        while work:
            nid = work.pop()
            order.append(nid)
            node = self.get_node(nid)
            if node[3] is not None:
                work.append(node[3])
            if node[4] is not None:
                work.append(node[4])
        # 本地保存重算结果，避免 LRU 缓存里的占位 subcnt 干扰孩子读取。
        computed = {}
        updates = []
        for nid in reversed(order):
            node = self.get_node(nid)
            left_cnt = computed.get(node[3], 0)
            right_cnt = computed.get(node[4], 0)
            sub = node[1] + left_cnt + right_cnt
            computed[nid] = sub
            updates.append((sub, nid))
            if nid in self._pending:
                row = self._pending[nid]
                self._pending[nid] = (row[0], row[1], row[2], row[3],
                                      row[4], sub, row[6])
        self.flush()
        if updates:
            self.conn.executemany(
                'UPDATE nodes SET subcnt=? WHERE id=?', updates)
            self.stats.inc('sqlite_statements')
            # 节点缓存可能保留占位行，整批重算后清空，下次读取重新装载。
            self.cache = BoundedLRU(self._cache_cap, stats=self.stats)

    def max_node_id(self):
        cur = self._execute('SELECT COALESCE(MAX(id), 0) FROM nodes')
        return cur.fetchone()[0]

    # ------------------------------------------------------------------ GC
    def gc(self, pinned_roots):
        """删除从所有钉住根不可达的节点；可达性在 SQLite 递归 CTE 内计算。"""
        if self.readonly:
            raise RuntimeError('只读 treap 不允许 GC')
        pinned = [r for r in pinned_roots if r is not None]
        if not pinned:
            raise ValueError('GC 至少需要一个钉住根')
        roots_sql = ' UNION ALL '.join('SELECT ? AS root_id'
                                       for _ in pinned)
        self._execute(
            'WITH RECURSIVE reach(id) AS ('
            f'SELECT root_id FROM ({roots_sql}) '
            'UNION ALL SELECT n.left_id FROM nodes n '
            'JOIN reach r ON n.id=r.id WHERE n.left_id IS NOT NULL '
            'UNION ALL SELECT n.right_id FROM nodes n '
            'JOIN reach r ON n.id=r.id WHERE n.right_id IS NOT NULL) '
            'DELETE FROM nodes WHERE id NOT IN (SELECT id FROM reach)',
            tuple(pinned))
        # 统计删除量（同一事务内可见）。
        cur = self._execute('SELECT changes()')
        removed = cur.fetchone()[0]
        self.stats.inc('tree_nodes_gced', removed)
        # 节点缓存可能保留已删行；直接清空，容量峰值不受影响。
        self.cache = BoundedLRU(self._cache_cap, stats=self.stats)
        return removed
