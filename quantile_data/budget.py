"""显式资源预算与访问计量。

引擎不假定“整组能放进内存”。调用方给出三个硬预算：

* ``open_shards``：写路径可同时打开的分片（SQLite 文件）数量；
* ``cached_nodes``：每个打开分片缓存的 treap 节点数；
* ``open_snapshots``：保留期内可同时打开的旧读句柄数量。

所有预算都以峰值表 + 累计计数的形式计量，测试可据此证明实现没有
偷偷把整组拉进 Python 内存、也没有无界地打开文件。
"""
import threading
from collections import OrderedDict


class Stats:
    """一组计数器与峰值表。

    计数类别：
        node_hits/node_misses: 节点缓存命中/未命中（物理节点读取）
        node_loads:           实际从 SQLite 读出的节点行数
        sqlite_statements:    执行的 SQL 语句条数
        rows_scanned:         SQL 扫描/返回行数（不含节点读取的单列统计）
        shard_opens/shard_evictions: 分片连接打开/预算淘汰
        snapshot_opens/snapshot_evictions: 读句柄连接打开/淘汰
        cache_size_peak:      节点缓存占用峰值
        open_shards_peak:     同时打开分片峰值
        open_snapshots_peak:  同时打开只读连接峰值
        tree_nodes_written:   新写入的不可变节点数
        tree_nodes_gced:      GC 删除的不可达节点数
        ledger_lookups:       身份账点查次数
        ledger_rows_streamed: 身份账/值账流式扫描行数（游标逐行计）
    """

    _CUMULATIVE = (
        'node_hits', 'node_misses', 'node_loads', 'sqlite_statements',
        'rows_scanned', 'shard_opens', 'shard_evictions', 'snapshot_opens',
        'snapshot_evictions', 'tree_nodes_written', 'tree_nodes_gced',
        'ledger_lookups', 'ledger_rows_streamed',
    )
    _PEAKS = ('cache_size_peak', 'open_shards_peak', 'open_snapshots_peak')

    def __init__(self):
        for name in self._CUMULATIVE:
            setattr(self, name, 0)
        for name in self._PEAKS:
            setattr(self, name, 0)
        self._lock = threading.Lock()

    def inc(self, name, amount=1):
        with self._lock:
            setattr(self, name, getattr(self, name) + amount)

    def observe_peak(self, name, value):
        with self._lock:
            if value > getattr(self, name):
                setattr(self, name, value)

    def snapshot(self):
        with self._lock:
            return {name: getattr(self, name)
                    for name in self._CUMULATIVE + self._PEAKS}

    def __repr__(self):
        return 'Stats(' + ', '.join(f'{k}={v}'
                                    for k, v in self.snapshot().items()) + ')'


class Budget:
    """资源预算配置。预算必须为正整数；0/负数会被拒绝。"""

    __slots__ = ('open_shards', 'cached_nodes', 'open_snapshots')

    def __init__(self, open_shards=4, cached_nodes=256, open_snapshots=4):
        for name, value in (('open_shards', open_shards),
                            ('cached_nodes', cached_nodes),
                            ('open_snapshots', open_snapshots)):
            if not isinstance(value, int) or value <= 0:
                raise ValueError(f'{name} 必须为正整数')
        self.open_shards = open_shards
        self.cached_nodes = cached_nodes
        self.open_snapshots = open_snapshots


class BoundedLRU:
    """带硬容量的 LRU，超容量时回调淘汰项。

    线程安全；淘汰动作通过 ``on_evict`` 回调完成（例如关闭连接）。
    """

    def __init__(self, capacity, on_evict=None, stats=None,
                 evict_counter=None):
        self.capacity = capacity
        self._data = OrderedDict()
        self._on_evict = on_evict
        self._stats = stats
        self._evict_counter = evict_counter
        self._lock = threading.Lock()

    def get(self, key):
        with self._lock:
            if key not in self._data:
                return None
            self._data.move_to_end(key)
            return self._data[key]

    def put(self, key, value):
        """插入/更新；超容量时淘汰最久未用项并返回被淘汰的 (key, value)。"""
        evicted = []
        with self._lock:
            if key in self._data:
                self._data.move_to_end(key)
                self._data[key] = value
                return evicted
            self._data[key] = value
            while len(self._data) > self.capacity:
                old_key, old_value = self._data.popitem(last=False)
                evicted.append((old_key, old_value))
                if self._stats is not None and self._evict_counter:
                    self._stats.inc(self._evict_counter)
        for old_key, old_value in evicted:
            if self._on_evict is not None:
                self._on_evict(old_key, old_value)
        return evicted

    def pop(self, key):
        with self._lock:
            if key in self._data:
                return self._data.pop(key)
            return None

    def __contains__(self, key):
        with self._lock:
            return key in self._data

    def __len__(self):
        with self._lock:
            return len(self._data)

    def keys(self):
        with self._lock:
            return list(self._data.keys())

    def items(self):
        with self._lock:
            return list(self._data.items())

    def clear(self):
        with self._lock:
            items = list(self._data.items())
            self._data.clear()
        for key, value in items:
            if self._on_evict is not None:
                self._on_evict(key, value)
