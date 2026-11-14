"""可撤回、可更正的分组精确分位聚合状态。

核心入口：
    :class:`AggregationEngine` —— 落盘的可更新聚合状态；
    :class:`ReadOnlyState` —— 明确只读的轻量导出；
    :class:`ReferenceState` —— 独立全排序参考实现（测试预言机）；
    :class:`Budget` / :class:`Stats` —— 显式资源预算与访问计量。
"""
from .budget import Budget, Stats
from .engine import AggregationEngine, BatchReceipt, SnapshotHandle
from .errors import (BatchRejected, CrashInjected, EmptyGroup,
                     HandleReleased, MergeRejected, NotAStateError,
                     QuantileError, QuantileUndefined, RankOutOfRange,
                     ReadOnlyExportError, UnknownGroup, WriterLocked)
from .export import ReadOnlyState
from .rational import QuantileResult, quantile_position
from .reference import ReferenceState

__all__ = [
    'AggregationEngine', 'BatchReceipt', 'SnapshotHandle', 'ReadOnlyState',
    'ReferenceState', 'Budget', 'Stats', 'QuantileResult',
    'quantile_position', 'QuantileError', 'BatchRejected', 'CrashInjected',
    'EmptyGroup', 'HandleReleased', 'MergeRejected', 'NotAStateError',
    'QuantileUndefined', 'RankOutOfRange', 'ReadOnlyExportError',
    'UnknownGroup', 'WriterLocked',
]
