"""Apply Debezium change events to an analytics warehouse, idempotently.

For every source table the sink maintains two warehouse tables:

* ``<table>``          current state, one row per primary key (soft-deleted rows keep ``_deleted = true``)
* ``<table>_history``  SCD type 2: every version with ``_valid_from`` / ``_valid_to`` / ``_is_current``

Guarantees:

* **Idempotent and order-safe.** Each key remembers the log position (LSN) of the last event applied to it. A
  replayed or older event (LSN <= last applied) is skipped, so Kafka's at-least-once delivery becomes
  effectively-once in the warehouse.
* **Atomic per batch.** A batch of events and the consumer offset it ends at are committed in one transaction.
  On restart the consumer resumes from the stored offset.
* **Schema evolution.** A new column in the source shows up in ``after``; the sink adds it to both tables.
* **Poison messages** (unparseable, missing key) go to a dead-letter table instead of stopping the stream.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import duckdb

META_DDL = """
CREATE SCHEMA IF NOT EXISTS cdc;
CREATE TABLE IF NOT EXISTS cdc._offsets (topic TEXT, partition INTEGER, next_offset BIGINT, PRIMARY KEY (topic, partition));
CREATE TABLE IF NOT EXISTS cdc._dead_letters (topic TEXT, partition INTEGER, kafka_offset BIGINT, error TEXT,
                                              payload TEXT, received_at TIMESTAMP DEFAULT current_timestamp);
"""
PG_TYPES = {bool: "BOOLEAN", int: "BIGINT", float: "DOUBLE", str: "TEXT"}


@dataclass
class Message:
    """What a Kafka consumer hands us: coordinates plus raw key/value bytes (or already-decoded dicts)."""
    topic: str
    partition: int
    offset: int
    key: Any
    value: Any


@dataclass
class Change:
    table: str
    op: str                    # c (create), u (update), d (delete), r (snapshot read)
    pk: Dict[str, Any]
    before: Optional[Dict[str, Any]]
    after: Optional[Dict[str, Any]]
    lsn: int
    ts: datetime


@dataclass
class BatchResult:
    applied: int = 0
    skipped_stale: int = 0
    tombstones: int = 0
    dead_letters: int = 0


def _decode(raw: Any) -> Any:
    if raw is None or isinstance(raw, dict):
        return raw
    return json.loads(raw.decode() if isinstance(raw, (bytes, bytearray)) else raw)


def parse(msg: Message, primary_keys: Dict[str, List[str]]) -> Optional[Change]:
    """Debezium envelope -> Change. Returns None for tombstones (null value after a delete)."""
    value = _decode(msg.value)
    if value is None:
        return None
    payload = value.get("payload", value)          # with or without the JSON schema wrapper
    source = payload["source"]
    table = source["table"]
    op = payload["op"]
    row = payload["after"] if op != "d" else payload["before"]
    if table not in primary_keys:
        raise ValueError(f"no primary key configured for table {table!r}")
    pk = {k: row[k] for k in primary_keys[table]}
    lsn = int(source.get("lsn") or source.get("pos") or msg.offset)
    ts = datetime.fromtimestamp(source.get("ts_ms", payload.get("ts_ms", 0)) / 1000, tz=timezone.utc).replace(tzinfo=None)
    return Change(table, op, pk, payload.get("before"), payload.get("after"), lsn, ts)


class WarehouseSink:
    def __init__(self, path: str, primary_keys: Dict[str, List[str]]) -> None:
        self.con = duckdb.connect(path)
        self.con.execute(META_DDL)
        self.pks = primary_keys

    # ------------------------------------------------------------- schema
    def _columns(self, table: str) -> Dict[str, str]:
        rows = self.con.execute("SELECT column_name, data_type FROM information_schema.columns "
                                "WHERE table_schema = 'cdc' AND table_name = ?", [table]).fetchall()
        return dict(rows)

    def _ensure_tables(self, table: str, row: Dict[str, Any]) -> None:
        cols = {c: PG_TYPES.get(type(v), "TEXT") for c, v in row.items()}
        if not self._columns(table):
            body = ", ".join(f'"{c}" {t}' for c, t in cols.items())
            pk = ", ".join(f'"{c}"' for c in self.pks[table])
            self.con.execute(f'CREATE TABLE cdc."{table}" ({body}, _lsn BIGINT, _op TEXT, _deleted BOOLEAN, '
                             f'_updated_at TIMESTAMP, PRIMARY KEY ({pk}))')
            self.con.execute(f'CREATE TABLE cdc."{table}_history" ({body}, _lsn BIGINT, _op TEXT, '
                             f'_valid_from TIMESTAMP, _valid_to TIMESTAMP, _is_current BOOLEAN)')
            return
        existing = self._columns(table)
        for c, t in cols.items():          # schema evolution: add new source columns
            if c not in existing:
                self.con.execute(f'ALTER TABLE cdc."{table}" ADD COLUMN "{c}" {t}')
                self.con.execute(f'ALTER TABLE cdc."{table}_history" ADD COLUMN "{c}" {t}')

    # -------------------------------------------------------------- apply
    def _last_lsn(self, table: str, pk: Dict[str, Any]) -> Optional[int]:
        if not self._columns(table):
            return None
        where = " AND ".join(f'"{k}" = ?' for k in pk)
        row = self.con.execute(f'SELECT _lsn FROM cdc."{table}" WHERE {where}', list(pk.values())).fetchone()
        return row[0] if row else None

    def _apply(self, ch: Change) -> bool:
        row = ch.after if ch.op != "d" else ch.before
        self._ensure_tables(ch.table, row)
        last = self._last_lsn(ch.table, ch.pk)
        if last is not None and ch.lsn <= last:
            return False                                       # replay or out-of-order: already reflected

        where = " AND ".join(f'"{k}" = ?' for k in ch.pk)
        keys = list(ch.pk.values())
        deleted = ch.op == "d"
        # current state: upsert
        cols = list(row) + ["_lsn", "_op", "_deleted", "_updated_at"]
        vals = list(row.values()) + [ch.lsn, ch.op, deleted, ch.ts]
        col_sql = ", ".join(f'"{c}"' for c in cols)
        self.con.execute(f'DELETE FROM cdc."{ch.table}" WHERE {where}', keys)
        self.con.execute(f'INSERT INTO cdc."{ch.table}" ({col_sql}) VALUES ({", ".join("?" * len(vals))})', vals)

        # history: close the open version, open a new one unless the row was deleted
        self.con.execute(f'UPDATE cdc."{ch.table}_history" SET _valid_to = ?, _is_current = false '
                         f'WHERE {where} AND _is_current', [ch.ts] + keys)
        if not deleted:
            hcols = list(row) + ["_lsn", "_op", "_valid_from", "_valid_to", "_is_current"]
            hvals = list(row.values()) + [ch.lsn, ch.op, ch.ts, None, True]
            self.con.execute(f'INSERT INTO cdc."{ch.table}_history" ({", ".join(chr(34) + c + chr(34) for c in hcols)}) '
                             f'VALUES ({", ".join("?" * len(hvals))})', hvals)
        return True

    def process_batch(self, messages: Iterable[Message]) -> BatchResult:
        """Apply a batch and store the next offsets in the same transaction."""
        result = BatchResult()
        offsets: Dict[Tuple[str, int], int] = {}
        self.con.execute("BEGIN")
        try:
            for msg in messages:
                offsets[(msg.topic, msg.partition)] = msg.offset + 1
                try:
                    ch = parse(msg, self.pks)
                except (ValueError, KeyError, TypeError) as exc:
                    self.con.execute("INSERT INTO cdc._dead_letters (topic, partition, kafka_offset, error, payload) "
                                     "VALUES (?, ?, ?, ?, ?)", [msg.topic, msg.partition, msg.offset,
                                                                f"{type(exc).__name__}: {exc}", str(msg.value)[:4000]])
                    result.dead_letters += 1
                    continue
                if ch is None:
                    result.tombstones += 1
                elif self._apply(ch):
                    result.applied += 1
                else:
                    result.skipped_stale += 1
            for (topic, part), nxt in offsets.items():
                self.con.execute("INSERT INTO cdc._offsets VALUES (?, ?, ?) ON CONFLICT DO UPDATE SET next_offset = "
                                 "greatest(cdc._offsets.next_offset, excluded.next_offset)", [topic, part, nxt])
            self.con.execute("COMMIT")
        except Exception:
            self.con.execute("ROLLBACK")
            raise
        return result

    def committed_offset(self, topic: str, partition: int = 0) -> int:
        row = self.con.execute("SELECT next_offset FROM cdc._offsets WHERE topic = ? AND partition = ?",
                               [topic, partition]).fetchone()
        return row[0] if row else 0

    def current(self, table: str, include_deleted: bool = False) -> List[Dict[str, Any]]:
        sql = f'SELECT * FROM cdc."{table}"' + ("" if include_deleted else " WHERE NOT _deleted")
        cur = self.con.execute(sql + " ORDER BY 1")
        names = [d[0] for d in cur.description]
        return [dict(zip(names, r)) for r in cur.fetchall()]
