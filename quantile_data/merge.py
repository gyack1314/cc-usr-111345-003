"""互斥来源分片的状态合并。

合并不是计数相加：每个来源分片带着自己的来源身份清单（``sources`` 表），
清单交叠即拒绝；除此之外还做 row_id 级交叠检测作为深度防御。合并通过普通
批事务通道（INTENT + applied 回执）落盘，因此中断可恢复、重放幂等，
合并后的状态保留完整身份账，仍可按原 row_id 撤回。
"""
import os

from .errors import MergeRejected
from .export import EXPORT_MARKER
from .shard import ShardStore


def _read_source_shard(path, stats, cache_cap):
    """只读打开来源分片并返回 (store, 来源集合, 元数据)。"""
    marker = os.path.join(os.path.dirname(os.path.abspath(path)),
                          'STATE_MARKER')
    if os.path.exists(marker):
        with open(marker, encoding='utf-8') as fh:
            if fh.read().strip() == EXPORT_MARKER:
                raise MergeRejected('只读轻量导出不能作为合并来源')
    store = ShardStore(os.path.abspath(path), gid=-1, stats=stats,
                       node_cache_capacity=cache_cap, readonly=True)
    return store, store.sources(), store.get_meta()


def merge_into_engine(engine, source_dirs, batch_id, destination=None):
    if not source_dirs:
        raise MergeRejected('合并来源为空')
    # ----------------------------------------------------------- 打开来源
    readers = []
    all_sources = set()
    source_row_ids = set()
    try:
        for src in source_dirs:
            shard_path = src if os.path.isfile(src) else os.path.join(
                src, 'shards')
            if os.path.isdir(shard_path):
                # 状态目录：合并其所有分片。
                raise MergeRejected(
                    '当前合并接口接收分片文件；多分片目录请逐分片合并')
            store, sources, meta = _read_source_shard(
                src, engine.stats, engine.budget.cached_nodes)
            readers.append((store, meta))
            overlap = all_sources & sources
            if overlap:
                raise MergeRejected(
                    f'来源身份清单交叠，拒绝合并: {sorted(overlap)}')
            all_sources |= sources
        # --------------------------------------------- row_id 交叠深度防御
        for store, _meta in readers:
            for row_id, _image in store.ledger_stream():
                if row_id in source_row_ids:
                    raise MergeRejected(
                        f'来源分片间 row_id 交叠: {row_id}')
                source_row_ids.add(row_id)
        dest_existing = engine.catalog.identity_exists_bulk(source_row_ids)
        if dest_existing:
            sample = sorted(dest_existing)[:5]
            raise MergeRejected(
                f'row_id 与目标状态交叠，拒绝合并: {sample}')
        # ----------------------------------------------- 构造插入型批操作
        ops = []
        for store, _meta in readers:
            for row_id, image in store.ledger_stream():
                ops.append({'op': 'insert', 'row_id': row_id,
                            'old': None, 'new': image})
        # 新分组可能与目标已有分组同键；insert 校验要求 row_id 全新，
        # 上面的交叠检测已保证，因此同键合流是安全的。插入动作在分片
        # 事务内会顺带把每行的 source 登记进目标分组的来源清单。
        receipt = engine.apply_batch(batch_id, ops)
        receipt.sources = sorted(all_sources)
        return receipt
    finally:
        for store, _meta in readers:
            store.close()
