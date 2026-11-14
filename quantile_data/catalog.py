"""状态目录：全局身份账、分组计数与批事务日志。

目录库 ``catalog.db`` 与分组分片分离，承担三类职责：

1. 全局行身份账 ``identity``：row_id -> 所属分片，使跨组更正、
   “同 ID 不同内容”以及合并身份冲突无需打开分片即可检测；
2. 分组表 ``groups``：分组键、分片路径与 COUNT(*)/非空/null 计数元数据；
3. 批事务日志 ``batches``：INTENT（含完整载荷与摘要）-> COMMITTED，
   配合分片 ``applied`` 回执构成可恢复事务，提交前后注入中断都能正确收尾。

另用 ``O_CREAT|O_EXCL`` 非阻塞锁文件保证同一状态目录同时只有一个写者。
"""
import hashlib
import json
import os
import sqlite3
import time

from .errors import WriterLocked

CATALOG_SCHEMA = """
CREATE TABLE IF NOT EXISTS groups (
    gid INTEGER PRIMARY KEY,
    gkey TEXT NOT NULL UNIQUE,
    shard_path TEXT NOT NULL,
    count_all INTEGER NOT NULL DEFAULT 0,
    count_nonnull INTEGER NOT NULL DEFAULT 0,
    count_null INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS identity (
    row_id TEXT PRIMARY KEY,
    gid INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_identity_gid ON identity(gid);
CREATE TABLE IF NOT EXISTS batches (
    batch_id TEXT PRIMARY KEY,
    state TEXT NOT NULL CHECK (state IN ('INTENT', 'COMMITTED')),
    digest TEXT NOT NULL,
    payload TEXT NOT NULL,
    plan TEXT NOT NULL DEFAULT '{}',
    affected_gids TEXT NOT NULL,
    created_at REAL NOT NULL,
    committed_at REAL
);
CREATE TABLE IF NOT EXISTS seq (
    name TEXT PRIMARY KEY,
    value INTEGER NOT NULL
);
"""


def canonical_digest(ops):
    """批操作规范化摘要：同 ID 不同内容、重试载荷不一致都靠它识别。"""
    blob = json.dumps(ops, sort_keys=True, separators=(',', ':'))
    return hashlib.sha256(blob.encode('utf-8')).hexdigest()


class Catalog:
    def __init__(self, path, lock_path):
        self.path = path
        self.lock_path = lock_path
        self._lock_fd = None
        self.conn = sqlite3.connect(str(path), isolation_level=None)
        self.conn.execute('PRAGMA journal_mode=WAL')
        self.conn.execute('PRAGMA synchronous=NORMAL')
        self.conn.executescript(CATALOG_SCHEMA)

    # --------------------------------------------------------------- 锁
    def acquire_lock(self):
        try:
            fd = os.open(self.lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.write(fd, str(os.getpid()).encode())
        except FileExistsError as exc:
            raise WriterLocked('状态目录已被其他写者持有') from exc
        self._lock_fd = fd

    def release_lock(self):
        if self._lock_fd is not None:
            os.close(self._lock_fd)
            try:
                os.unlink(self.lock_path)
            except FileNotFoundError:
                pass
            self._lock_fd = None

    def close(self):
        self.conn.close()
        self.release_lock()

    # ------------------------------------------------------------- 序列
    def next_value(self, name):
        self.conn.execute('BEGIN IMMEDIATE')
        cur = self.conn.execute(
            'SELECT value FROM seq WHERE name=?', (name,))
        row = cur.fetchone()
        if row is None:
            value = 1
            self.conn.execute(
                'INSERT INTO seq(name, value) VALUES(?, ?)', (name, value))
        else:
            value = row[0] + 1
            self.conn.execute(
                'UPDATE seq SET value=? WHERE name=?', (value, name))
        self.conn.execute('COMMIT')
        return value

    # ------------------------------------------------------------- 分组
    def get_group(self, gkey):
        cur = self.conn.execute(
            'SELECT gid, shard_path, count_all, count_nonnull, count_null '
            'FROM groups WHERE gkey=?', (gkey,))
        row = cur.fetchone()
        if row is None:
            return None
        return {'gid': row[0], 'shard_path': row[1], 'count_all': row[2],
                'count_nonnull': row[3], 'count_null': row[4]}

    def create_group(self, gkey, shard_path):
        gid = self.next_value('gid')
        self.conn.execute(
            'INSERT INTO groups(gid, gkey, shard_path) VALUES(?, ?, ?)',
            (gid, gkey, shard_path))
        return gid

    def list_groups(self):
        cur = self.conn.execute(
            'SELECT gid, gkey, shard_path, count_all, count_nonnull, '
            'count_null FROM groups ORDER BY gid')
        return [{'gid': r[0], 'gkey': r[1], 'shard_path': r[2],
                 'count_all': r[3], 'count_nonnull': r[4],
                 'count_null': r[5]} for r in cur.fetchall()]

    def update_counts(self, gid, count_all, count_nonnull, count_null):
        self.conn.execute(
            'UPDATE groups SET count_all=?, count_nonnull=?, count_null=? '
            'WHERE gid=?', (count_all, count_nonnull, count_null, gid))

    # ------------------------------------------------------------- 身份
    def identity_lookup(self, row_id):
        cur = self.conn.execute(
            'SELECT gid FROM identity WHERE row_id=?', (row_id,))
        row = cur.fetchone()
        return None if row is None else row[0]

    def identity_upsert(self, row_id, gid):
        self.conn.execute(
            'INSERT INTO identity(row_id, gid) VALUES(?, ?) '
            'ON CONFLICT(row_id) DO UPDATE SET gid=excluded.gid',
            (row_id, gid))

    def identity_delete(self, row_id):
        self.conn.execute('DELETE FROM identity WHERE row_id=?', (row_id,))

    def identity_exists_bulk(self, row_ids):
        """分批点查一组 row_id 是否存在于全局身份账，返回存在的集合。"""
        found = set()
        ids = list(row_ids)
        for i in range(0, len(ids), 500):
            chunk = ids[i:i + 500]
            marks = ','.join('?' for _ in chunk)
            cur = self.conn.execute(
                f'SELECT row_id FROM identity WHERE row_id IN ({marks})',
                chunk)
            found.update(r[0] for r in cur.fetchall())
        return found

    def group_row_ids_stream(self, gid, batch_size=1024):
        last = ''
        while True:
            cur = self.conn.execute(
                'SELECT row_id FROM identity WHERE gid=? AND row_id > ? '
                'ORDER BY row_id LIMIT ?', (gid, last, batch_size))
            rows = cur.fetchall()
            if not rows:
                return
            for (row_id,) in rows:
                yield row_id
                last = row_id

    # ------------------------------------------------------------- 批日志
    def batch_state(self, batch_id):
        cur = self.conn.execute(
            'SELECT state, digest, payload, plan, affected_gids FROM batches '
            'WHERE batch_id=?', (batch_id,))
        row = cur.fetchone()
        if row is None:
            return None
        return {'state': row[0], 'digest': row[1],
                'payload': json.loads(row[2]),
                'plan': json.loads(row[3]),
                'affected_gids': json.loads(row[4])}

    def insert_intent(self, batch_id, digest, payload, plan, affected_gids):
        self.conn.execute(
            'INSERT INTO batches(batch_id, state, digest, payload, plan, '
            'affected_gids, created_at) VALUES(?, ?, ?, ?, ?, ?, ?)',
            (batch_id, 'INTENT', digest,
             json.dumps(payload, separators=(',', ':'), sort_keys=True),
             json.dumps(plan, separators=(',', ':'), sort_keys=True),
             json.dumps(sorted(affected_gids)), time.time()))

    def mark_committed(self, batch_id):
        self.conn.execute(
            "UPDATE batches SET state='COMMITTED', committed_at=? "
            'WHERE batch_id=?', (time.time(), batch_id))

    def intents(self):
        cur = self.conn.execute(
            'SELECT batch_id, digest, payload, plan, affected_gids FROM '
            "batches WHERE state='INTENT' ORDER BY created_at")
        for batch_id, digest, payload, plan, gids in cur.fetchall():
            yield {'batch_id': batch_id, 'digest': digest,
                   'payload': json.loads(payload),
                   'plan': json.loads(plan),
                   'affected_gids': json.loads(gids)}

    def begin(self):
        self.conn.execute('BEGIN IMMEDIATE')

    def commit(self):
        self.conn.execute('COMMIT')
