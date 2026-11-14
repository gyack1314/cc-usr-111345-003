"""分组有序聚合状态引擎。

组合 :mod:`catalog`（目录库/全局身份账/批日志）与 :mod:`shard`
（每分组一个函数式 treap 分片）：

* 写入严格“先全量校验、后落盘”。撤回不存在、旧像不一致、同 ID 不同内容、
  重数减负、批内 ID 重复都会整批拒绝，磁盘状态不变；
* 批事务为 INTENT -> 各分片 applied 回执（含全局身份账调整）-> COMMITTED。
  提交前后任意时刻注入中断，重开引擎都能按回执重放收尾；分片
  ``applied`` 使重放幂等，重试不会重复累计；
* 读路径只打开受影响分组；分片连接数、节点缓存、快照连接数都受
  :class:`Budget` 硬限制，treap 查询只走秩路径，不把整组拉入 Python。
"""
import datetime
import json
import os
from fractions import Fraction

from .budget import Budget, BoundedLRU, Stats
from .catalog import Catalog, canonical_digest
from .errors import (BatchRejected, EmptyGroup, HandleReleased,
                     NotAStateError, QuantileUndefined, RankOutOfRange,
                     UnknownGroup, WriterLocked)
from .ops import build_plan, group_key, image_equal, normalize_ops
from .rational import QuantileResult, quantile_position
from .shard import ShardStore, open_shard_conn
from .treap import PersistentTreap

ENGINE_MARKER = 'quantile-state/v1 writable'
EXPORT_MARKER = 'quantile-state/v1 readonly-export'


def _now_iso():
    return datetime.datetime.utcnow().isoformat() + 'Z'


class BatchReceipt:
    """批回执：批 ID、摘要、受影响分组、最终状态与是否重放。"""

    __slots__ = ('batch_id', 'digest', 'affected_gids', 'state',
                 'replayed', 'sources')

    def __init__(self, batch_id, digest, affected_gids, state,
                 replayed=False, sources=None):
        self.batch_id = batch_id
        self.digest = digest
        self.affected_gids = affected_gids
        self.state = state
        self.replayed = replayed
        self.sources = sources

    def to_dict(self):
        return {'batch_id': self.batch_id, 'digest': self.digest,
                'affected_gids': list(self.affected_gids),
                'state': self.state, 'replayed': self.replayed,
                'sources': self.sources}

    def __repr__(self):
        return (f'BatchReceipt(batch_id={self.batch_id!r}, '
                f'state={self.state}, replayed={self.replayed}, '
                f'gids={self.affected_gids})')


class AggregationEngine:
    def __init__(self, directory, budget=None, seed=0, crash_hook=None):
        self.directory = os.path.abspath(directory)
        os.makedirs(os.path.join(self.directory, 'shards'), exist_ok=True)
        self.budget = budget or Budget()
        self.stats = Stats()
        self.seed = seed
        self.crash_hook = crash_hook
        self.marker_path = os.path.join(self.directory, 'STATE_MARKER')
        self._check_or_write_marker()
        self.lock_path = os.path.join(self.directory, 'writer.lock')
        self.catalog = Catalog(
            os.path.join(self.directory, 'catalog.db'), self.lock_path)
        try:
            self.catalog.acquire_lock()
        except WriterLocked:
            self.catalog.close()
            raise
        self._shards = BoundedLRU(
            self.budget.open_shards, on_evict=self._evict_shard,
            stats=self.stats, evict_counter='shard_evictions')
        self._snapshot_conns = BoundedLRU(
            self.budget.open_snapshots, on_evict=self._evict_snapshot,
            stats=self.stats, evict_counter='snapshot_evictions')
        self._snapshots = []
        self._closed = False
        self.recovery_report = []
        self._recover()

    # ------------------------------------------------------------ 生命周期
    def _check_or_write_marker(self):
        if not os.path.exists(self.marker_path):
            with open(self.marker_path, 'w', encoding='utf-8') as fh:
                fh.write(ENGINE_MARKER)
            return
        with open(self.marker_path, encoding='utf-8') as fh:
            marker = fh.read().strip()
        if marker == EXPORT_MARKER:
            raise NotAStateError('该目录是只读轻量导出，不能接收更新')
        if marker != ENGINE_MARKER:
            raise NotAStateError('状态标记无法识别')

    def _evict_shard(self, gid, store):
        store.close()

    def _evict_snapshot(self, key, view):
        view[0].close()

    def _crash(self, point, context):
        if self.crash_hook is not None:
            self.crash_hook(point, context)

    def close(self):
        if self._closed:
            return
        self._shards.clear()
        self._snapshot_conns.clear()
        self.catalog.close()
        self._closed = True

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # ------------------------------------------------------------ 分片管理
    def _shard_path(self, gid):
        return os.path.join(self.directory, 'shards', f'g{gid}.db')

    def _open_shard(self, gid):
        store = self._shards.get(gid)
        if store is not None:
            return store
        store = ShardStore(
            self._shard_path(gid), gid, self.stats,
            node_cache_capacity=self.budget.cached_nodes,
            seed=(self.seed + gid) & 0xFFFFFFFF)
        self._shards.put(gid, store)
        self.stats.inc('shard_opens')
        self.stats.observe_peak('open_shards_peak', len(self._shards))
        return store

    def _gid_lookup(self, gkey):
        info = self.catalog.get_group(gkey)
        return None if info is None else info['gid']

    def _get_or_create_group(self, gkey):
        gid = self._gid_lookup(gkey)
        if gid is not None:
            return gid, False
        gid = self.catalog.next_value('gid')
        path_rel = os.path.join('shards', f'g{gid}.db')
        self.catalog.conn.execute(
            'INSERT INTO groups(gid, gkey, shard_path) VALUES(?, ?, ?)',
            (gid, gkey, path_rel))
        store = self._open_shard(gid)
        store.initialize_meta()
        return gid, True

    # ---------------------------------------------------------------- 校验
    def _validate(self, ops):
        """纯读全量校验，不创建分组、不写任何状态。"""
        reasons = []
        row_ids = [op['row_id'] for op in ops]
        seen = set()
        for index, rid in enumerate(row_ids):
            if rid in seen:
                reasons.append(f'第{index}条 row_id={rid} 在批内重复')
            seen.add(rid)
        existing = self.catalog.identity_exists_bulk(row_ids)
        # 打开所有涉及的已有分组。
        gids_needed = set()
        for rid in existing:
            gids_needed.add(self.catalog.identity_lookup(rid))
        stores = {gid: self._open_shard(gid) for gid in gids_needed}
        # 形态约束与旧像校验。
        for index, op in enumerate(ops):
            rid, kind = op['row_id'], op['op']
            if kind == 'insert':
                if op['old'] is not None:
                    reasons.append(f'第{index}条插入的 old 必须为 null')
                if op['new'] is None:
                    reasons.append(f'第{index}条插入缺少 new 像')
                    continue
                if rid in existing:
                    reasons.append(
                        f'第{index}条插入的 row_id={rid} 已存在；'
                        '同 ID 不同内容拒绝')
            else:
                if rid not in existing:
                    reasons.append(
                        f'第{index}条{"撤回" if kind == "retract" else "更正"}'
                        f'的 row_id={rid} 不存在')
                    continue
                gid = self.catalog.identity_lookup(rid)
                actual = stores[gid].lookup_row(rid)
                if actual is None:
                    reasons.append(
                        f'第{index}条 row_id={rid} 全局身份账与分片不一致')
                    continue
                if not image_equal(actual, op['old']):
                    reasons.append(
                        f'第{index}条 row_id={rid} 旧像不一致：实际 '
                        f'{json.dumps(actual, sort_keys=True, ensure_ascii=False)}')
                if kind == 'retract' and op['new'] is not None:
                    reasons.append(f'第{index}条撤回的 new 必须为 null')
                if kind == 'correct' and op['new'] is None:
                    reasons.append(f'第{index}条更正缺少 new 像')
        if reasons:
            raise BatchRejected(reasons)
        # 批内生命周期冲突：新插入的 row_id 不能在同批被撤回/更正，
        # 同一 row_id 的插入也不允许出现两次（批内重复已在前面拦截）。
        inserted_in_batch = {op['row_id'] for op in ops
                             if op['op'] == 'insert'}
        for index, op in enumerate(ops):
            if op['op'] != 'insert' and op['row_id'] in inserted_in_batch:
                reasons.append(
                    f"第{index}条引用了同批刚插入的 row_id="
                    f"{op['row_id']}，请合并为一条插入或拆成多个批次")
        if reasons:
            raise BatchRejected(reasons)
        # 计划与重数减负检查（null 不进 treap，无需检查）。
        _, deltas, _ = build_plan(
            ops, self.catalog.identity_lookup, self._gid_lookup)
        for coord, value_deltas in deltas.items():
            if coord[0] == 'new':
                continue                        # 新分组当前重数为 0，只可能加
            gid = coord[1]
            store = stores.get(gid) or self._open_shard(gid)
            root = store.get_meta()['root']
            for value, delta in value_deltas.items():
                if delta < 0:
                    have = (store.treap.count_le(root, value)
                            - store.treap.count_lt(root, value))
                    if have + delta < 0:
                        reasons.append(
                            f'分组 g{coord[1]} 值 {value} 撤回后重数为负 '
                            f'({have}{delta:+d})')
        if reasons:
            raise BatchRejected(reasons)

    # ---------------------------------------------------------------- 写入
    def apply_batch(self, batch_id, ops):
        if self._closed:
            raise RuntimeError('引擎已关闭')
        if not isinstance(batch_id, str) or not batch_id:
            raise BatchRejected('batch_id 非法')
        normalized = normalize_ops(ops)
        digest = canonical_digest(normalized)
        existing = self.catalog.batch_state(batch_id)
        if existing is not None:
            if existing['digest'] != digest:
                raise BatchRejected(
                    f'batch_id={batch_id} 已存在但摘要不一致，'
                    '同 ID 不同内容拒绝')
            if existing['state'] == 'COMMITTED':
                return BatchReceipt(
                    batch_id, digest, existing['affected_gids'],
                    'COMMITTED')
            return self._finish_intent(batch_id, replayed=True)  # 崩溃残留
        self._validate(normalized)
        # 先把所有目标坐标解析成真实 gid（必要时创建新分组），得到与
        # 崩溃时刻无关的固定计划；计划随 INTENT 持久化，重放不再依赖
        # 可能已经变化的当前身份账。
        plan = self._build_resolved_plan(normalized)
        self.catalog.begin()
        self.catalog.insert_intent(
            batch_id, digest, normalized, plan, plan['affected'])
        self.catalog.commit()
        self._crash('after_intent', {'batch_id': batch_id})
        return self._finish_intent(batch_id, replayed=False)

    def _build_resolved_plan(self, ops):
        """把操作解析成按 gid 固定的动作计划与身份账调整。"""
        actions, _deltas, moves = build_plan(
            ops, self.catalog.identity_lookup, self._gid_lookup)
        gid_actions = {}
        for coord, seq in actions.items():
            gid = self._resolve_coord(coord)
            gid_actions.setdefault(str(gid), []).extend(
                [list(a) for a in seq])
        gid_moves = []
        for rid, coord in moves:
            target_gid = None if coord is None else self._resolve_coord(coord)
            gid_moves.append([rid, target_gid])
        affected = sorted(int(g) for g in gid_actions)
        return {'actions': gid_actions, 'moves': gid_moves,
                'affected': affected}

    def _resolve_coord(self, coord):
        if isinstance(coord, tuple):
            kind, token = coord
            if kind == 'existing':
                return token
            gkey = token
        else:
            # 恢复时 affected 里存的可能是 gid（int）或 gkey（str）。
            if isinstance(coord, int):
                return coord
            gkey = coord
        gid, _ = self._get_or_create_group(gkey)
        return gid

    def _finish_intent(self, batch_id, replayed=False):
        record = self.catalog.batch_state(batch_id)
        digest = record['digest']
        plan = record['plan']
        gid_actions = {int(gid): [tuple(a) for a in seq]
                       for gid, seq in plan['actions'].items()}
        moves = [(rid, target) for rid, target in plan['moves']]
        # 防御：恢复时确保计划中的分组都已存在。
        for gid in plan['affected']:
            self._open_shard(gid)
        affected = sorted(gid_actions)
        for gid in affected:
            store = self._open_shard(gid)
            if store.is_applied(batch_id) is not None:
                # 该分片已提交：其身份账调整也已生效，重放跳过整组。
                continue
            store.begin()
            try:
                meta = store.get_meta()
                root, version = meta['root'], meta['version']
                count_all, count_null = meta['count_all'], meta['count_null']
                for action in gid_actions.get(gid, []):
                    root, version, count_all, count_null = self._apply_one(
                        store, action, root, version, count_all, count_null)
                nonnull = (store.treap.subcnt(store.treap.get_node(root))
                           if root is not None else 0)
                # 防御：计数减负必须在事务内再次断言。
                store._write_meta(
                    root=root if root is not None else '', version=version,
                    count_all=count_all, count_nonnull=nonnull,
                    count_null=count_null)
                self._apply_identity_moves(moves, gid)
                store.mark_applied(batch_id, digest, _now_iso())
                store.commit()
            except Exception:
                store.rollback()
                raise
            self.catalog.update_counts(gid, count_all, nonnull, count_null)
            self._gc_shard_if_possible(gid)
            self._crash('after_shard', {'batch_id': batch_id, 'gid': gid})
        self.catalog.begin()
        self.catalog.mark_committed(batch_id)
        self.catalog.commit()
        self._crash('after_commit', {'batch_id': batch_id})
        return BatchReceipt(batch_id, digest, list(affected), 'COMMITTED',
                            replayed=replayed)

    def _pinned_roots(self, gid, current):
        pinned = {current}
        for snap in self._snapshots:
            entry = snap.roots.get(gid)
            if entry:
                pinned.add(entry['root'])
        pinned.discard(None)
        return list(pinned)

    def _gc_shard_if_possible(self, gid):
        """提交后回收旧版本不可达节点；活跃快照根始终钉住。"""
        store = self._open_shard(gid)
        current = store.get_meta()['root']
        pinned = self._pinned_roots(gid, current)
        if not pinned:
            return
        store.begin()
        try:
            store.gc_unreachable(pinned)
            store.commit()
        except Exception:
            store.rollback()
            raise

    def _apply_identity_moves(self, moves, gid):
        """在分片事务内同步调整全局身份账（崩溃由 INTENT 计划兜底）。"""
        for rid, target_gid in moves:
            if target_gid is None:
                if self.catalog.identity_lookup(rid) == gid:
                    self.catalog.identity_delete(rid)
            elif target_gid == gid:
                self.catalog.identity_upsert(rid, gid)

    @staticmethod
    def _apply_one(store, action, root, version, count_all, count_null):
        kind = action[0]
        version += 1
        if kind == 'insert':
            _, rid, image = action
            if image['latency'] is not None:
                root = store.treap.apply_delta(
                    root, image['latency'], 1, version)
            count_all += 1
            if image['latency'] is None:
                count_null += 1
            store.upsert_row(rid, image)
            store.add_sources([image['source']])
        elif kind == 'retract':
            _, rid, old = action
            if old['latency'] is not None:
                root = store.treap.apply_delta(
                    root, old['latency'], -1, version)
            count_all -= 1
            if old['latency'] is None:
                count_null -= 1
            store.delete_row(rid)
        else:
            _, rid, old, new = action
            if old['latency'] is not None:
                root = store.treap.apply_delta(
                    root, old['latency'], -1, version)
            if new['latency'] is not None:
                root = store.treap.apply_delta(
                    root, new['latency'], 1, version)
            count_null += (1 if new['latency'] is None else 0) \
                - (1 if old['latency'] is None else 0)
            store.upsert_row(rid, new)
            store.add_sources([new['source']])
        if count_all < 0 or count_null < 0:
            raise AssertionError('计数减负，校验层应当拦截')
        return root, version, count_all, count_null

    # ---------------------------------------------------------------- 恢复
    def _recover(self):
        for intent in list(self.catalog.intents()):
            receipt = self._finish_intent(intent['batch_id'], replayed=True)
            self.recovery_report.append(receipt.to_dict())

    # ---------------------------------------------------------------- 查询
    def counts(self, tenant, day):
        info = self.catalog.get_group(group_key(tenant, day))
        if info is None:
            raise UnknownGroup(group_key(tenant, day))
        return {'count_all': info['count_all'],
                'count_nonnull': info['count_nonnull'],
                'count_null': info['count_null']}

    def _read_root(self, tenant, day):
        gkey = group_key(tenant, day)
        info = self.catalog.get_group(gkey)
        if info is None:
            raise UnknownGroup(gkey)
        store = self._open_shard(info['gid'])
        return store, store.get_meta(), info

    def quantile(self, tenant, day, p_numerator, p_denominator):
        _store, meta, info = self._read_root(tenant, day)
        return self._quantile_on(
            _store.treap, meta['root'], info['count_all'],
            info['count_nonnull'], group_key(tenant, day),
            p_numerator, p_denominator)

    @staticmethod
    def _quantile_on(treap, root, count_all, count_nonnull, gkey,
                     p_num, p_den):
        if count_all == 0:
            raise EmptyGroup(gkey)
        if count_nonnull == 0:
            raise QuantileUndefined(gkey)
        h = quantile_position(p_num, p_den, count_nonnull)
        lo = h.numerator // h.denominator
        rem = h.numerator - lo * h.denominator
        hi = lo + (1 if rem else 0)
        v_lo = treap.kth(root, lo)
        v_hi = treap.kth(root, hi)
        value = Fraction(v_lo) if rem == 0 else Fraction(v_lo) + Fraction(
            (v_hi - v_lo) * rem, h.denominator)
        return QuantileResult(value, count_nonnull, v_lo, v_hi, lo, hi,
                              Fraction(p_num, p_den))

    def count_less(self, tenant, day, value):
        self._require_int(value)
        store, meta, _ = self._read_root(tenant, day)
        return store.treap.count_lt(meta['root'], value)

    def count_leq(self, tenant, day, value):
        self._require_int(value)
        store, meta, _ = self._read_root(tenant, day)
        return store.treap.count_le(meta['root'], value)

    def value_multiplicity(self, tenant, day, value):
        self._require_int(value)
        store, meta, _ = self._read_root(tenant, day)
        root = meta['root']
        return store.treap.count_le(root, value) \
            - store.treap.count_lt(root, value)

    def rank_range(self, tenant, day, rank_lo, rank_hi):
        if isinstance(rank_lo, bool) or isinstance(rank_hi, bool) or \
                not isinstance(rank_lo, int) or not isinstance(rank_hi, int):
            raise RankOutOfRange('秩必须为整数')
        store, meta, info = self._read_root(tenant, day)
        if rank_lo < 0 or rank_hi < rank_lo:
            raise RankOutOfRange(f'秩区间非法: [{rank_lo}, {rank_hi}]')
        if rank_hi >= info['count_nonnull']:
            raise RankOutOfRange(
                f'秩 {rank_hi} 超出非空计数 {info["count_nonnull"]}')
        return store.treap.rank_range(meta['root'], rank_lo, rank_hi)

    @staticmethod
    def _require_int(value):
        if not isinstance(value, int) or isinstance(value, bool):
            raise ValueError('查询值必须为定标整数')

    # ---------------------------------------------------------------- 快照
    def snapshot(self):
        roots = {}
        for info in self.catalog.list_groups():
            store = self._open_shard(info['gid'])
            meta = store.get_meta()
            roots[info['gid']] = {
                'root': meta['root'], 'version': meta['version'],
                'count_all': info['count_all'],
                'count_nonnull': info['count_nonnull'],
                'count_null': info['count_null'], 'gkey': info['gkey']}
        handle = SnapshotHandle(self, roots)
        self._snapshots.append(handle)
        return handle

    def _release_snapshot(self, handle):
        if handle in self._snapshots:
            self._snapshots.remove(handle)
        stale = [key for key in self._snapshot_conns.keys()
                 if key[0] in handle.roots
                 and not self._root_pinned_by_others(key[0], key[1], handle)]
        for key in stale:
            view = self._snapshot_conns.pop(key)
            if view is not None:
                view[0].close()
        self.gc_versions()

    def _root_pinned_by_others(self, gid, root, exclude):
        for snap in self._snapshots:
            if snap is exclude:
                continue
            entry = snap.roots.get(gid)
            if entry and entry['root'] == root:
                return True
        return False

    def gc_versions(self):
        """删除当前根与所有活跃快照根不可达的旧节点。"""
        for gid in list(self._shards.keys()):
            store = self._shards.get(gid)
            if store is None:
                continue
            current = store.get_meta()['root']
            if current is None:
                continue
            self._gc_shard_if_possible(gid)

    # ---------------------------------------------------------------- 合并
    def merge_shard(self, source_dirs, batch_id, destination=None):
        from .merge import merge_into_engine
        return merge_into_engine(self, source_dirs, batch_id, destination)

    def export_readonly(self, target_dir):
        from .export import build_readonly_export
        return build_readonly_export(self, target_dir)

    def stats_snapshot(self):
        return self.stats.snapshot()


class SnapshotHandle:
    """钉住旧根的只读句柄；release 前对后续一切更新稳定。"""

    def __init__(self, engine, roots):
        self._engine = engine
        self.roots = roots
        self._released = False

    def _check(self):
        if self._released:
            raise HandleReleased('快照已过保留期')

    def _entry(self, gkey):
        self._check()
        for entry in self.roots.values():
            if entry['gkey'] == gkey:
                return entry
        raise UnknownGroup(gkey)

    def _read_view(self, gkey):
        entry = self._entry(gkey)
        gid = next(g for g, e in self.roots.items() if e is entry)
        key = (gid, entry['root'])
        view = self._engine._snapshot_conns.get(key)
        if view is None:
            conn = open_shard_conn(self._engine._shard_path(gid),
                                   readonly=True)
            treap = PersistentTreap(
                conn, self._engine.stats, readonly=True,
                node_cache_capacity=self._engine.budget.cached_nodes)
            view = (conn, treap)
            self._engine._snapshot_conns.put(key, view)
            self._engine.stats.inc('snapshot_opens')
            self._engine.stats.observe_peak(
                'open_snapshots_peak',
                len(self._engine._snapshot_conns))
        return view[1], entry

    def counts(self, tenant, day):
        entry = self._entry(group_key(tenant, day))
        return {'count_all': entry['count_all'],
                'count_nonnull': entry['count_nonnull'],
                'count_null': entry['count_null']}

    def quantile(self, tenant, day, p_numerator, p_denominator):
        gkey = group_key(tenant, day)
        treap, entry = self._read_view(gkey)
        return AggregationEngine._quantile_on(
            treap, entry['root'], entry['count_all'],
            entry['count_nonnull'], gkey, p_numerator, p_denominator)

    def count_less(self, tenant, day, value):
        treap, entry = self._read_view(group_key(tenant, day))
        return treap.count_lt(entry['root'], value)

    def count_leq(self, tenant, day, value):
        treap, entry = self._read_view(group_key(tenant, day))
        return treap.count_le(entry['root'], value)

    def rank_range(self, tenant, day, rank_lo, rank_hi):
        gkey = group_key(tenant, day)
        treap, entry = self._read_view(gkey)
        if rank_lo < 0 or rank_hi < rank_lo \
                or rank_hi >= entry['count_nonnull']:
            raise RankOutOfRange(f'秩区间非法: [{rank_lo}, {rank_hi}]')
        return treap.rank_range(entry['root'], rank_lo, rank_hi)

    def release(self):
        if self._released:
            return
        self._released = True
        self._engine._release_snapshot(self)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.release()
