"""独立全排序参考实现。

与生产引擎完全不同的数据结构：每张表就是一个 dict，每次查询重新排序。
测试把它当作预言机，对引擎每个版本的计数、分位、严格小于/小于等于、
秩区间逐项核对，任何不一致都说明引擎可逆状态出错。
"""
from collections import Counter
from fractions import Fraction

from .ops import group_key
from .rational import quantile_position


class ReferenceState:
    def __init__(self):
        # row_id -> image
        self.rows = {}

    # ------------------------------------------------------------- 变更
    def apply(self, ops):
        """模拟整批：先校验全部，再一次性生效。失败时状态不变。"""
        problems = []
        for i, op in enumerate(ops):
            rid, kind = op['row_id'], op['op']
            if kind == 'insert':
                if rid in self.rows:
                    problems.append(f'{rid} 已存在')
            else:
                if rid not in self.rows:
                    problems.append(f'{rid} 不存在')
                elif self.rows[rid] != op['old']:
                    problems.append(f'{rid} 旧像不一致')
        if problems:
            raise ValueError('参考实现拒绝: ' + '；'.join(problems))
        for op in ops:
            rid, kind = op['row_id'], op['op']
            if kind == 'insert':
                self.rows[rid] = dict(op['new'])
            elif kind == 'retract':
                del self.rows[rid]
            else:
                self.rows[rid] = dict(op['new'])

    # ------------------------------------------------------------- 视图
    def _group(self, tenant, day):
        gkey = group_key(tenant, day)
        images = [img for img in self.rows.values()
                  if group_key(img['tenant'], img['day']) == gkey]
        return images

    def counts(self, tenant, day):
        images = self._group(tenant, day)
        nonnull = [img['latency'] for img in images
                   if img['latency'] is not None]
        return {'count_all': len(images),
                'count_nonnull': len(nonnull),
                'count_null': len(images) - len(nonnull)}

    def _sorted_values(self, tenant, day):
        images = self._group(tenant, day)
        values = sorted(img['latency'] for img in images
                        if img['latency'] is not None)
        return values

    def quantile(self, tenant, day, p_num, p_den):
        values = self._sorted_values(tenant, day)
        counts = self.counts(tenant, day)
        if counts['count_all'] == 0:
            raise KeyError('empty')
        if not values:
            raise KeyError('all-null')
        n = len(values)
        h = quantile_position(p_num, p_den, n)
        lo = h.numerator // h.denominator
        rem = h.numerator - lo * h.denominator
        hi = lo + (1 if rem else 0)
        v_lo, v_hi = values[lo], values[hi]
        value = Fraction(v_lo) if rem == 0 else Fraction(v_lo) + Fraction(
            (v_hi - v_lo) * rem, h.denominator)
        return value, v_lo, v_hi, lo, hi, n

    def count_less(self, tenant, day, value):
        return sum(1 for v in self._sorted_values(tenant, day) if v < value)

    def count_leq(self, tenant, day, value):
        return sum(1 for v in self._sorted_values(tenant, day) if v <= value)

    def multiplicity(self, tenant, day, value):
        return Counter(self._sorted_values(tenant, day))[value]

    def rank_range(self, tenant, day, rank_lo, rank_hi):
        values = self._sorted_values(tenant, day)
        selected = values[rank_lo:rank_hi + 1]
        out = []
        for value in sorted(set(selected)):
            start = values.index(value)
            end = start + values.count(value) - 1
            lo = max(start, rank_lo)
            hi = min(end, rank_hi)
            out.append((value, hi - lo + 1))
        return out

    def all_groups(self):
        keys = {group_key(img['tenant'], img['day'])
                for img in self.rows.values()}
        return keys

    def sources(self):
        return {img['source'] for img in self.rows.values()}

    def row_ids(self):
        return set(self.rows)
