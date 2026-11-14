"""批操作的分组计划与镜像工具。

校验期与写入期共用同一套“操作 -> 分组动作”的纯函数，避免两处逻辑漂移。
计划里的分组用 ``('existing', gid)`` 或 ``('new', gkey)`` 两种坐标表示，
校验期不创建任何分组，写入期才把新坐标解析成真实 gid。
"""
from .errors import BatchRejected

IMAGE_FIELDS = ('tenant', 'day', 'latency', 'source')


def group_key(tenant, day):
    import json
    return json.dumps(['g', tenant, day], separators=(',', ':'),
                      sort_keys=True)


def image_equal(actual, expected):
    if not isinstance(actual, dict) or not isinstance(expected, dict):
        return False
    if set(actual) != set(expected):
        return False
    return all(actual[k] == expected[k] for k in actual)


def valid_image(image):
    if not isinstance(image, dict) or set(image) != set(IMAGE_FIELDS):
        return False
    if not isinstance(image['tenant'], str) or not image['tenant']:
        return False
    if not isinstance(image['day'], int) or isinstance(image['day'], bool):
        return False
    if not isinstance(image['source'], str) or not image['source']:
        return False
    latency = image['latency']
    if latency is not None and (
            not isinstance(latency, int) or isinstance(latency, bool)):
        return False
    return True


def normalize_ops(ops):
    """结构校验并规范化操作列表；结构错误直接整批拒绝。"""
    normalized = []
    for index, op in enumerate(ops):
        if not isinstance(op, dict):
            raise BatchRejected(f'第{index}条操作不是对象')
        kind = op.get('op')
        row_id = op.get('row_id')
        if not isinstance(row_id, str) or not row_id:
            raise BatchRejected(f'第{index}条操作 row_id 非法')
        if kind not in ('insert', 'retract', 'correct'):
            raise BatchRejected(f'第{index}条操作类型 {kind!r} 非法')
        old, new = op.get('old'), op.get('new')
        if old is not None and not valid_image(old):
            raise BatchRejected(f'第{index}条操作 old 像字段非法')
        if new is not None and not valid_image(new):
            raise BatchRejected(f'第{index}条操作 new 像字段非法')
        normalized.append({'op': kind, 'row_id': row_id,
                           'old': old, 'new': new})
    return normalized


def target_coord(op, gid_lookup):
    """返回操作写入侧的分组坐标。

    ``gid_lookup(gkey) -> gid|None``；新分组返回 ('new', gkey)。
    """
    if op['op'] == 'insert':
        gkey = group_key(op['new']['tenant'], op['new']['day'])
        gid = gid_lookup(gkey)
        return ('existing', gid) if gid is not None else ('new', gkey)
    if op['op'] == 'correct':
        gkey = group_key(op['new']['tenant'], op['new']['day'])
        gid = gid_lookup(gkey)
        return ('existing', gid) if gid is not None else ('new', gkey)
    return None


def build_plan(ops, gid_of_row, gid_lookup):
    """构造每分组的局部动作序列与值重数增量。

    返回 ``(actions_by_coord, deltas_by_coord, identity_moves)``：
      * actions: 坐标 -> [('insert', rid, img) | ('retract', rid, old) |
                           ('correct_local', rid, old, new)]
      * deltas:  坐标 -> {value: 重数增量}（null 不进 treap）
      * identity_moves: [(rid, new_coord_or_None)]，全局身份账的最终调整
    """
    actions, deltas, moves = {}, {}, []

    def bump(coord, value, delta):
        if value is None:
            return
        deltas.setdefault(coord, {}).setdefault(value, 0)
        deltas[coord][value] += delta

    for op in ops:
        rid = op['row_id']
        kind = op['op']
        if kind == 'insert':
            coord = target_coord(op, gid_lookup)
            actions.setdefault(coord, []).append(
                ('insert', rid, op['new']))
            bump(coord, op['new']['latency'], 1)
            moves.append((rid, coord))
        elif kind == 'retract':
            old_gid = gid_of_row(rid)
            coord = ('existing', old_gid)
            actions.setdefault(coord, []).append(
                ('retract', rid, op['old']))
            bump(coord, op['old']['latency'], -1)
            moves.append((rid, None))
        else:
            old_gid = gid_of_row(rid)
            coord = target_coord(op, gid_lookup)
            if coord == ('existing', old_gid):
                actions.setdefault(coord, []).append(
                    ('correct_local', rid, op['old'], op['new']))
            else:
                actions.setdefault(('existing', old_gid), []).append(
                    ('retract', rid, op['old']))
                actions.setdefault(coord, []).append(
                    ('insert', rid, op['new']))
            bump(('existing', old_gid), op['old']['latency'], -1)
            bump(coord, op['new']['latency'], 1)
            moves.append((rid, coord))
    return actions, deltas, moves
