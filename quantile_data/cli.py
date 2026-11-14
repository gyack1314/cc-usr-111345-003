"""命令行入口：资料检查与聚合状态演示。

子命令：
    check <manifest>          原有资料完整性检查；
    demo [--dir DIR]          用小预算跑插入/超大重复值组/跨组更正/
                              撤回/合并，输出各版本分位、计数与资源统计。
"""
import argparse
import json
import shutil

from .budget import Budget
from .domain import inspect_tables
from .engine import AggregationEngine
from .errors import BatchRejected, QuantileUndefined, EmptyGroup
from .io import load, DataError


def _img(tenant, day, latency, source):
    return {'tenant': tenant, 'day': day, 'latency': latency,
            'source': source}


def run_demo(directory):
    shutil.rmtree(directory, ignore_errors=True)
    budget = Budget(open_shards=2, cached_nodes=16, open_snapshots=2)
    engine = AggregationEngine(directory, budget=budget, seed=42)
    print(f'预算: open_shards={budget.open_shards} '
          f'cached_nodes={budget.cached_nodes} '
          f'open_snapshots={budget.open_snapshots}')

    # 版本 1：超大重复值组（8000 行同值）+ 少量其他值，分批写入。
    huge = 8000
    for start in range(0, huge, 1000):
        ops = [{'op': 'insert', 'row_id': f'h{i}', 'old': None,
                'new': _img('svc', 1, 7, 's1')}
               for i in range(start, min(start + 1000, huge))]
        engine.apply_batch(f'load-{start}', ops)
    engine.apply_batch('extra', [
        {'op': 'insert', 'row_id': 'e1', 'old': None,
         'new': _img('svc', 1, 3, 's2')},
        {'op': 'insert', 'row_id': 'e2', 'old': None,
         'new': _img('svc', 1, 100, 's2')},
        {'op': 'insert', 'row_id': 'null1', 'old': None,
         'new': _img('svc', 1, None, 's2')}])
    print('\n[版本1] 超大重复值组:')
    print('  counts =', engine.counts('svc', 1))
    q = engine.quantile('svc', 1, 1, 2)
    print('  精确中位数 =', q.to_dict())
    print('  count_lt(7) =', engine.count_less('svc', 1, 7),
          ' count_le(7) =', engine.count_leq('svc', 1, 7))
    print('  rank[100..105] =', engine.rank_range('svc', 1, 100, 105))

    snapshot = engine.snapshot()

    # 版本 2：跨组更正 + 撤回一批重复请求。
    correct = [
        {'op': 'correct', 'row_id': 'e1',
         'old': _img('svc', 1, 3, 's2'),
         'new': _img('svc2', 2, 50, 's2')}]
    engine.apply_batch('cross-group', correct)
    retract = [{'op': 'retract', 'row_id': f'h{i}',
                'old': _img('svc', 1, 7, 's1'), 'new': None}
               for i in range(0, huge, 2)]
    for i in range(0, len(retract), 1000):
        engine.apply_batch(f'retract-{i}', retract[i:i + 1000])
    print('\n[版本2] 跨组更正与重复请求撤回后:')
    print('  svc/1 counts =', engine.counts('svc', 1))
    print('  svc2/2 counts =', engine.counts('svc2', 2))
    print('  当前中位数 =', engine.quantile('svc', 1, 1, 2).value)
    print('  旧句柄中位数（保留期内稳定）=',
          snapshot.quantile('svc', 1, 1, 2).value)
    snapshot.release()

    # 整批拒绝演示：旧像不一致 + 撤回不存在，状态不变。
    before = engine.counts('svc', 1)
    try:
        engine.apply_batch('rejected', [
            {'op': 'retract', 'row_id': 'ghost',
             'old': _img('svc', 1, 1, 's1'), 'new': None},
            {'op': 'retract', 'row_id': 'e2',
             'old': _img('svc', 1, 999, 's2'), 'new': None}])
    except BatchRejected as exc:
        print('\n[整批拒绝]', exc)
    assert engine.counts('svc', 1) == before

    # 空组 vs 全 null 组。
    try:
        engine.quantile('svc', 1, 1, 2)
    except (EmptyGroup, QuantileUndefined) as exc:
        print(type(exc).__name__, exc)

    print('\n[资源计量]', json.dumps(engine.stats_snapshot(),
                                     ensure_ascii=False, indent=2))
    engine.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description='精确分位聚合状态')
    sub = parser.add_subparsers(dest='command')
    check = sub.add_parser('check', help='检查资料完整性')
    check.add_argument('manifest')
    demo = sub.add_parser('demo', help='运行聚合状态演示')
    demo.add_argument('--dir', default='out/demo-state')
    args = parser.parse_args(argv)
    if args.command == 'check':
        try:
            print(json.dumps(inspect_tables(load(args.manifest)),
                             ensure_ascii=False, indent=2))
        except DataError as exc:
            parser.exit(2, str(exc) + '\n')
    elif args.command == 'demo':
        run_demo(args.dir)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
