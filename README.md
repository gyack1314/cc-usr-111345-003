# 可撤回请求明细的精确分位聚合状态

本项目在资料完整性检查之外，实现了一套**落盘的、支持插入/撤回/更正的分组
有序聚合状态**，用于在可变更明细上回答当前版本的精确分位与排名查询。压缩分位
摘要不可逆，无法减掉被撤回的重复请求；本实现不使用近似摘要，所有分位以有理数
精确给出。

## 能力

- **可逆更新**：插入、撤回、同组/跨组更正按批提交；撤回不存在、旧像不一致、
  同 ID 不同内容、批内重复、重数减负都会整批拒绝，状态不变。
- **精确分位与排名**：连续分位按 `p*(n-1)` 线性插值，返回 `Fraction`；
  支持严格小于/小于等于计数、指定秩区间的值与重数。`COUNT(*)` 与非空计数
  分别维护，空组与全 null 组明确区分。
- **落盘有界**：每分组一个 SQLite 分片，内含函数式 treap（子树计数、值重数）、
  行身份账、来源清单与批回执。重复值累计为单节点重数；历史节点由递归 CTE
  垃圾回收，节点表按去重值有界，不把整组拉进 Python 内存。
- **事务与恢复**：INTENT → 分片 applied 回执 → COMMITTED；提交前后可注入
  中断，重开自动恢复，重试幂等不重复累计。
- **快照稳定**：旧读句柄钉住不可变旧根，保留期内对后续更新稳定。
- **分片合并**：多个互斥来源分片可合并，来源身份清单或 row_id 交叠即拒绝；
  合并后仍可按原 row_id 撤回。
- **只读导出**：轻量导出明确标记只读，不能以同一种状态类型接收更新。
- **预算与计量**：打开分片、节点缓存、只读连接均有硬预算，输出峰值与实际
  访问统计。

## 运行环境

Python 3.11+，仅标准库。建议用隔离模式运行（`-I`），由 `run_tests.py`
显式注入项目根，避免拾取工作区外的同名包。

```sh
# 全部测试（27 个：treap、引擎逐版本核对、事务恢复、合并导出、资料集成）
python3 -I run_tests.py

# 兼容标准入口
python -m unittest discover -s tests
python -m compileall -q quantile_data tests

# 原有资料完整性检查
python3 -I -c "import sys; sys.path.insert(0,'.'); \
  from quantile_data.cli import main; main(['check','data/normal.json'])"

# 小预算演示：超大重复值组、跨组更正、整批拒绝、资源峰值与访问统计
python3 -I -c "import sys; sys.path.insert(0,'.'); \
  from quantile_data.cli import main; main(['demo','--dir','out/demo-state'])"
```

## 快速上手

```python
from quantile_data import AggregationEngine, Budget

engine = AggregationEngine('state-dir',
                           Budget(open_shards=4, cached_nodes=256,
                                  open_snapshots=4))
engine.apply_batch('b1', [
    {'op': 'insert', 'row_id': 'r1', 'old': None,
     'new': {'tenant': 'A', 'day': 1, 'latency': 10, 'source': 's1'}},
    {'op': 'insert', 'row_id': 'r2', 'old': None,
     'new': {'tenant': 'A', 'day': 1, 'latency': 10, 'source': 's1'}},
])
engine.quantile('A', 1, 1, 2)          # 精确中位数 Fraction
engine.count_less('A', 1, 10)          # 严格小于
engine.rank_range('A', 1, 0, 1)        # 秩区间 (值, 重数)
engine.apply_batch('b2', [{
    'op': 'retract', 'row_id': 'r1',
    'old': {'tenant': 'A', 'day': 1, 'latency': 10, 'source': 's1'},
    'new': None}])                     # 撤回后精确减量
```

## 资料约定

`data/` 下 facts/changes/queries 的字段、摘要绑定与引用关系见
[docs/data-contract.md](docs/data-contract.md)；聚合状态的存储、事务、
预算与合并设计见 [docs/design.md](docs/design.md)。
`io.py` 负责资料读取，`domain.py` 负责表集合与引用检查，`cli.py` 提供
检查与演示入口。

容器内：

```sh
docker build -t quantile-data .
docker run --rm quantile-data
```
