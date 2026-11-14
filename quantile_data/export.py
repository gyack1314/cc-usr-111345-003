"""只读轻量导出。

导出目录只保留服务查询所需的 treap 结构、计数元数据与来源清单，
**不携带 row_id 身份账与批回执**，因此无法再撤回/更正/合并。目录带显式
只读标记，:class:`AggregationEngine` 打开它时会拒绝写入；查询则由
:class:`ReadOnlyState` 提供，接口与引擎读路径一致。
"""
import json
import os

from .budget import Stats
from .errors import NotAStateError, ReadOnlyExportError
from .shard import ShardStore, open_shard_conn
from .treap import PersistentTreap
from .engine import EXPORT_MARKER


def build_readonly_export(engine, target_dir):
    """流式构建只读导出；身份账与批回执不复制。"""
    target_dir = os.path.abspath(target_dir)
    os.makedirs(os.path.join(target_dir, 'shards'), exist_ok=True)
    with open(os.path.join(target_dir, 'STATE_MARKER'), 'w',
              encoding='utf-8') as fh:
        fh.write(EXPORT_MARKER)
    index = []
    for info in engine.catalog.list_groups():
        src = engine._open_shard(info['gid'])
        meta = src.get_meta()
        out_path = os.path.join(target_dir, 'shards',
                                f"g{info['gid']}.db")
        out = ShardStore(out_path, info['gid'], engine.stats,
                         node_cache_capacity=engine.budget.cached_nodes,
                         seed=(engine.seed + info['gid']) & 0xFFFFFFFF)
        out.begin()
        try:
            # 值重数 treap：中序流式读出 -> 线性重建，全程不在 Python
            # 内持有整组（构建器分块刷盘，读侧栈深 O(log n)）。
            new_root = out.treap.build_sorted(
                src.treap.in_order(meta['root']), version=1)
            out._write_meta(
                root=new_root if new_root is not None else '',
                version=1, count_all=info['count_all'],
                count_nonnull=info['count_nonnull'],
                count_null=info['count_null'])
            out.add_sources(src.sources_stream())
            out.commit()
        except Exception:
            out.rollback()
            raise
        out.close()
        index.append({'gid': info['gid'], 'gkey': info['gkey'],
                      'shard': f"shards/g{info['gid']}.db",
                      'count_all': info['count_all'],
                      'count_nonnull': info['count_nonnull'],
                      'count_null': info['count_null']})
    with open(os.path.join(target_dir, 'export_index.json'), 'w',
              encoding='utf-8') as fh:
        json.dump({'format': EXPORT_MARKER, 'groups': index}, fh,
                  ensure_ascii=False, indent=2)
    return ReadOnlyState(target_dir)


class ReadOnlyState:
    """只读导出状态：支持全部读查询，任何写操作都显式报错。"""

    def __init__(self, directory):
        self.directory = os.path.abspath(directory)
        marker = os.path.join(self.directory, 'STATE_MARKER')
        if not os.path.exists(marker):
            raise NotAStateError('导出目录缺少状态标记')
        with open(marker, encoding='utf-8') as fh:
            if fh.read().strip() != EXPORT_MARKER:
                raise NotAStateError('不是只读导出目录')
        with open(os.path.join(self.directory, 'export_index.json'),
                  encoding='utf-8') as fh:
            self._index = json.load(fh)
        self._by_key = {g['gkey']: g for g in self._index['groups']}
        self._conns = {}
        self.stats = Stats()

    def _view(self, gkey):
        if gkey not in self._by_key:
            from .errors import UnknownGroup
            raise UnknownGroup(gkey)
        entry = self._by_key[gkey]
        if entry['gid'] not in self._conns:
            path = os.path.join(self.directory, entry['shard'])
            conn = open_shard_conn(path, readonly=True)
            treap = PersistentTreap(conn, self.stats, readonly=True,
                                    node_cache_capacity=4096)
            cur = conn.execute('SELECT value FROM meta WHERE key="root"')
            root_raw = cur.fetchone()[0]
            entry['root'] = int(root_raw) if root_raw != '' else None
            self._conns[entry['gid']] = (conn, treap)
        return self._conns[entry['gid']][1], entry

    def counts(self, tenant, day):
        from .ops import group_key
        entry = self._by_key.get(group_key(tenant, day))
        if entry is None:
            from .errors import UnknownGroup
            raise UnknownGroup(group_key(tenant, day))
        return {'count_all': entry['count_all'],
                'count_nonnull': entry['count_nonnull'],
                'count_null': entry['count_null']}

    def quantile(self, tenant, day, p_num, p_den):
        from .ops import group_key
        from .engine import AggregationEngine
        gkey = group_key(tenant, day)
        treap, entry = self._view(gkey)
        root = self._root_of(entry)
        return AggregationEngine._quantile_on(
            treap, root, entry['count_all'], entry['count_nonnull'],
            gkey, p_num, p_den)

    def count_less(self, tenant, day, value):
        from .ops import group_key
        treap, entry = self._view(group_key(tenant, day))
        return treap.count_lt(self._root_of(entry), value)

    def count_leq(self, tenant, day, value):
        from .ops import group_key
        treap, entry = self._view(group_key(tenant, day))
        return treap.count_le(self._root_of(entry), value)

    def rank_range(self, tenant, day, rank_lo, rank_hi):
        from .ops import group_key
        gkey = group_key(tenant, day)
        treap, entry = self._view(gkey)
        if rank_lo < 0 or rank_hi < rank_lo \
                or rank_hi >= entry['count_nonnull']:
            from .errors import RankOutOfRange
            raise RankOutOfRange(f'秩区间非法: [{rank_lo}, {rank_hi}]')
        return treap.rank_range(self._root_of(entry), rank_lo, rank_hi)

    @staticmethod
    def _root_of(entry):
        return entry['root']

    # 只读边界：写入口一律拒绝。
    def apply_batch(self, *args, **kwargs):
        raise ReadOnlyExportError('轻量导出为只读状态，不能接收更新；'
                                  '撤回/更正需要带身份账的可写状态')

    def merge_shard(self, *args, **kwargs):
        raise ReadOnlyExportError('只读导出不能参与合并')

    def snapshot(self):
        # 内容不可变，自身即可作为稳定读句柄。
        return self

    def release(self):
        for conn, _treap in self._conns.values():
            conn.close()
        self._conns.clear()

    def stats_snapshot(self):
        return self.stats.snapshot()
