"""异常类型。

所有引擎层错误都继承 :class:`QuantileError`，便于调用方统一捕获。
校验类错误表示整批被拒绝，状态不会发生任何变化。
"""


class QuantileError(Exception):
    """引擎公共错误基类。"""


class BatchRejected(QuantileError):
    """变更批未通过校验，整批拒绝（不写入任何状态）。"""

    def __init__(self, reasons):
        if isinstance(reasons, str):
            reasons = [reasons]
        self.reasons = list(reasons)
        super().__init__('；'.join(self.reasons))


class UnknownGroup(QuantileError):
    """分组从未建立。"""


class EmptyGroup(QuantileError):
    """分组存在但 COUNT(*)=0（行已全部撤回），分位无定义。"""


class QuantileUndefined(QuantileError):
    """分组非空但全部为 null，分位只在非空值上定义。"""


class RankOutOfRange(QuantileError):
    """秩参数超出 [0, 非空计数) 范围。"""


class MergeRejected(QuantileError):
    """分片合并被拒绝（来源交叠或身份冲突）。"""


class ReadOnlyExportError(QuantileError):
    """轻量导出为只读状态，不能接收插入/撤回/更正。"""


class NotAStateError(QuantileError):
    """目标目录不是可更新状态（例如只读导出目录）。"""


class HandleReleased(QuantileError):
    """快照句柄已释放，保留期结束，不能再读取。"""


class WriterLocked(QuantileError):
    """已有写者持有状态目录的排他锁。"""


class CrashInjected(QuantileError):
    """测试钩子注入的模拟中断。"""
