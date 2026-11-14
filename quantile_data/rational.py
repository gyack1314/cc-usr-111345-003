"""精确有理数与连续分位。

分位结果绝不使用浮点近似：插值系数 p*(n-1) 用分子/分母两个整数表示，
最终值以 :class:`fractions.Fraction` 返回，并附带定位到的两个相邻值。
"""
from fractions import Fraction


def quantile_position(p_num, p_den, n):
    """返回排序后位置 h = p*(n-1) 的精确有理数位置。

    ``p`` 由两个整数给出，必须满足 0 <= p_num <= p_den 且 p_den > 0。
    """
    if not isinstance(p_num, int) or not isinstance(p_den, int):
        raise ValueError('分位分子分母必须为整数')
    if p_den <= 0 or p_num < 0 or p_num > p_den:
        raise ValueError('分位必须满足 0 <= p_num/p_den <= 1')
    if n <= 0:
        raise ValueError('非空计数必须为正')
    return Fraction(p_num * (n - 1), p_den)


def interpolate(values_by_rank, p_num, p_den):
    """在可按下标取值的有序序列上计算精确连续分位。

    规则：位置 h = p*(n-1)，取下标 floor(h) 与 ceil(h) 的值做线性插值。
    返回 ``(Fraction, low_value, high_value, low_rank, high_rank)``。
    """
    n = len(values_by_rank)
    h = quantile_position(p_num, p_den, n)
    lo = h.numerator // h.denominator
    rem = h.numerator - lo * h.denominator
    hi = lo + (1 if rem else 0)
    v_lo = values_by_rank[lo]
    v_hi = values_by_rank[hi]
    if rem == 0:
        value = Fraction(v_lo)
    else:
        value = Fraction(v_lo) + Fraction((v_hi - v_lo) * rem, h.denominator)
    return value, v_lo, v_hi, lo, hi


class QuantileResult:
    """精确分位结果。

    属性：
        value:       精确分位值（``Fraction``）
        n:           参与计算的非空值个数（含重数）
        low/high:    插值两端的整数值（位置为整数时相同）
        low_rank/high_rank: 两端在升序序列中的秩（0 基，按重数计）
        p:           分位（``Fraction``）
    """

    __slots__ = ('value', 'n', 'low', 'high', 'low_rank', 'high_rank', 'p')

    def __init__(self, value, n, low, high, low_rank, high_rank, p):
        self.value = value
        self.n = n
        self.low = low
        self.high = high
        self.low_rank = low_rank
        self.high_rank = high_rank
        self.p = p

    def as_fraction(self):
        return self.value

    def as_decimal_string(self):
        """精确十进制串；无限小数给出完整循环展开不现实，返回分数串。"""
        return str(self.value)

    def __eq__(self, other):
        return isinstance(other, QuantileResult) and (
            self.value, self.n) == (other.value, other.n)

    def __repr__(self):
        return (f'QuantileResult(value={self.value!r}, n={self.n}, '
                f'interp=({self.low}@{self.low_rank}, '
                f'{self.high}@{self.high_rank}))')

    def to_dict(self):
        return {
            'value_num': self.value.numerator,
            'value_den': self.value.denominator,
            'n': self.n,
            'low': self.low,
            'high': self.high,
            'low_rank': self.low_rank,
            'high_rank': self.high_rank,
            'p_num': self.p.numerator,
            'p_den': self.p.denominator,
        }
