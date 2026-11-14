"""分组分片的 SQLite 持久结构。

一个分组（租户 + 日期）对应 ``shards/g<gid>.db``，内含：

* ``nodes``：函数式 treap 节点（值、重数、左右孩子、子树计数、版本）；
* ``ledger``：row_id -> 当前整行，是撤回/更正旧像校验与合并身份冲突的权威依据；
* ``applied``：已提交批在本分片的回执，使重放幂等、重试不重复累计；
* ``sources``：本分片包含的来源身份清单，合并时据此检测交叠；
* ``meta``：当前根、版本、COUNT(*)、非空计数等计数元数据。

身份账点查走索引；流式扫描使用键集分页，不把整表读入内存。
"""
import json
import sqlite3

from .treap import PersistentTreap

SHARD_SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS ledger (
    row_id TEXT PRIMARY KEY,
    image TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS applied (
    batch_id TEXT PRIMARY KEY,
    digest TEXT NOT NULL,
    committed_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS sources (
    source TEXT PRIMARY KEY
);
CREATE TABLE IF NOT EXISTS nodes (
    id INTEGER PRIMARY KEY,
    value INTEGER NOT NULL,
    multiplicity INTEGER NOT NULL CHECK (multiplicity > 0),
    priority INTEGER NOT NULL,
    left_id INTEGER,
    right_id INTEGER,
    subcnt INTEGER NOT NULL,
    version INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_nodes_left ON nodes(left_id);
CREATE INDEX IF NOT EXISTS idx_nodes_right ON nodes(right_id);
"""


def open_shard_conn(path, readonly=False):
    if readonly:
        uri = f'file:{path}?mode=ro'
        conn = sqlite3.connect(uri, uri=True, isolation_level=None)
        conn.execute('PRAGMA query_only=ON')
    else:
        conn = sqlite3.connect(str(path), isolation_level=None)
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('PRAGMA synchronous=NORMAL')
        conn.execute('PRAGMA foreign_keys=ON')
    conn.execute('PRAGMA recursive_triggers=ON')
    return conn


class ShardStore:
    """单个分组分片的读写句柄。"""

    def __init__(self, path, gid, stats, node_cache_capacity, seed=0,
                 readonly=False):
        self.path = path
        self.gid = gid
        self.stats = stats
        self.readonly = readonly
        self.conn = open_shard_conn(path, readonly=readonly)
        if not readonly:
            self.conn.executescript(SHARD_SCHEMA)
        self._meta_cache = None
        start_id = 1 if readonly else self._max_node_id() + 1
        self.treap = PersistentTreap(
            self.conn, stats, start_node_id=start_id,
            node_cache_capacity=node_cache_capacity, seed=seed,
            readonly=readonly)

    def close(self):
        self.conn.close()

    # ------------------------------------------------------------- 元数据
    def _max_node_id(self):
        cur = self.conn.execute('SELECT COALESCE(MAX(id), 0) FROM nodes')
        self.stats.inc('sqlite_statements')
        return cur.fetchone()[0]

    def _read_meta(self):
        cur = self.conn.execute('SELECT key, value FROM meta')
        self.stats.inc('sqlite_statements')
        return dict(cur.fetchall())

    def get_meta(self):
        """返回当前根/版本/计数；缓存只在单连接写者模型下安全。"""
        if self._meta_cache is None:
            raw = self._read_meta()
            self._meta_cache = {
                'root': int(raw['root']) if raw.get('root') not in (None, '') else None,
                'version': int(raw.get('version', 0)),
                'count_all': int(raw.get('count_all', 0)),
                'count_nonnull': int(raw.get('count_nonnull', 0)),
                'count_null': int(raw.get('count_null', 0)),
            }
        return dict(self._meta_cache)

    def _write_meta(self, **changes):
        for key, value in changes.items():
            self.conn.execute(
                'INSERT INTO meta(key, value) VALUES(?, ?) '
                'ON CONFLICT(key) DO UPDATE SET value=excluded.value',
                (key, str(value)))
            self.stats.inc('sqlite_statements')
        self._meta_cache = None

    def initialize_meta(self):
        self._write_meta(root='', version=0, count_all=0,
                         count_nonnull=0, count_null=0)

    # ------------------------------------------------------------- 身份账
    def lookup_row(self, row_id):
        cur = self.conn.execute(
            'SELECT image FROM ledger WHERE row_id=?', (row_id,))
        self.stats.inc('sqlite_statements')
        self.stats.inc('ledger_lookups')
        row = cur.fetchone()
        if row is None:
            return None
        return json.loads(row[0])

    def upsert_row(self, row_id, image):
        self.conn.execute(
            'INSERT INTO ledger(row_id, image) VALUES(?, ?) '
            'ON CONFLICT(row_id) DO UPDATE SET image=excluded.image',
            (row_id, json.dumps(image, separators=(',', ':'),
                                sort_keys=True)))
        self.stats.inc('sqlite_statements')

    def delete_row(self, row_id):
        self.conn.execute('DELETE FROM ledger WHERE row_id=?', (row_id,))
        self.stats.inc('sqlite_statements')

    def ledger_stream(self, batch_size=1024):
        """键集分页流式产出 (row_id, image)，不整表载入内存。"""
        last = ''
        while True:
            cur = self.conn.execute(
                'SELECT row_id, image FROM ledger WHERE row_id > ? '
                'ORDER BY row_id LIMIT ?', (last, batch_size))
            self.stats.inc('sqlite_statements')
            rows = cur.fetchall()
            if not rows:
                return
            for row_id, image in rows:
                self.stats.inc('ledger_rows_streamed')
                yield row_id, json.loads(image)
                last = row_id

    # ------------------------------------------------------------- 回执
    def is_applied(self, batch_id):
        cur = self.conn.execute(
            'SELECT digest FROM applied WHERE batch_id=?', (batch_id,))
        self.stats.inc('sqlite_statements')
        row = cur.fetchone()
        return None if row is None else row[0]

    def mark_applied(self, batch_id, digest, committed_at):
        self.conn.execute(
            'INSERT OR IGNORE INTO applied(batch_id, digest, committed_at) '
            'VALUES(?, ?, ?)', (batch_id, digest, committed_at))
        self.stats.inc('sqlite_statements')

    def applied_stream(self):
        cur = self.conn.execute(
            'SELECT batch_id, digest, committed_at FROM applied ORDER BY batch_id')
        self.stats.inc('sqlite_statements')
        for row in cur:
            self.stats.inc('ledger_rows_streamed')
            yield row

    # ------------------------------------------------------------- 来源
    def sources(self):
        cur = self.conn.execute('SELECT source FROM sources ORDER BY source')
        self.stats.inc('sqlite_statements')
        return {r[0] for r in cur.fetchall()}

    def add_sources(self, source_ids):
        self.conn.executemany(
            'INSERT OR IGNORE INTO sources(source) VALUES(?)',
            [(s,) for s in source_ids])
        self.stats.inc('sqlite_statements')

    def sources_stream(self):
        cur = self.conn.execute('SELECT source FROM sources ORDER BY source')
        self.stats.inc('sqlite_statements')
        for row in cur:
            yield row[0]

    # ------------------------------------------------------------- 事务
    def begin(self):
        self.conn.execute('BEGIN IMMEDIATE')
        self.stats.inc('sqlite_statements')

    def commit(self):
        self.treap.flush()
        self.conn.execute('COMMIT')
        self.stats.inc('sqlite_statements')
        self._meta_cache = None

    def rollback(self):
        self.conn.execute('ROLLBACK')
        self.stats.inc('sqlite_statements')
        # 待刷节点随事务废弃，id 计数器回到已落盘最大值之后。
        self.treap._pending.clear()
        self.treap._next_id = self._max_node_id() + 1
        self._meta_cache = None

    # ------------------------------------------------------------- GC
    def gc_unreachable(self, pinned_roots):
        removed = self.treap.gc(pinned_roots)
        return removed

    def checkpoint(self):
        if not self.readonly:
            self.conn.execute('PRAGMA wal_checkpoint(PASSIVE)')
            self.stats.inc('sqlite_statements')
