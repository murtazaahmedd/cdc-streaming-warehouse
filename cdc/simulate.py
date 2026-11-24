"""Offline simulator: an OLTP workload emitting Debezium-shaped events into an in-memory log.

It produces everything that makes CDC hard: updates and deletes, tombstones, a schema change mid-stream
(a new ``phone`` column), a poison message, and consumer crashes that cause redelivery. ``run_demo`` streams it
through the real sink and reconciles the warehouse with the source's true final state.
"""
from __future__ import annotations

import json
import random
from dataclasses import dataclass, field
from typing import Dict, List

from .sink import Message, WarehouseSink

PRIMARY_KEYS = {"customers": ["id"], "orders": ["id"]}
TOPICS = {"customers": "shop.public.customers", "orders": "shop.public.orders"}


@dataclass
class SourceDB:
    """The 'truth': what the source tables contain after the workload."""
    tables: Dict[str, Dict[int, dict]] = field(default_factory=lambda: {"customers": {}, "orders": {}})
    log: Dict[str, List[Message]] = field(default_factory=lambda: {t: [] for t in TOPICS.values()})
    lsn: int = 1_000
    ts_ms: int = 1_763_942_400_000   # 2025-11-24
    schema_evolved: bool = False

    def _emit(self, table: str, op: str, before, after) -> None:
        self.lsn += 8
        self.ts_ms += 1_000
        topic = TOPICS[table]
        key = {"id": (after or before)["id"]}
        value = {"payload": {"op": op, "before": before, "after": after, "ts_ms": self.ts_ms,
                             "source": {"connector": "postgresql", "db": "shop", "schema": "public",
                                        "table": table, "lsn": self.lsn, "ts_ms": self.ts_ms}}}
        log = self.log[topic]
        log.append(Message(topic, 0, len(log), json.dumps(key).encode(), json.dumps(value).encode()))
        if op == "d":   # Debezium follows a delete with a tombstone so compacted topics can drop the key
            log.append(Message(topic, 0, len(log), json.dumps(key).encode(), None))

    def insert(self, table: str, row: dict) -> None:
        self.tables[table][row["id"]] = dict(row)
        self._emit(table, "c", None, dict(row))

    def update(self, table: str, pk: int, **changes) -> None:
        before = dict(self.tables[table][pk])
        after = {**before, **changes}
        self.tables[table][pk] = after
        self._emit(table, "u", before, after)

    def delete(self, table: str, pk: int) -> None:
        before = self.tables[table].pop(pk)
        self._emit(table, "d", before, None)


def workload(seed: int = 42, n_customers: int = 50, n_orders: int = 300) -> SourceDB:
    rng = random.Random(seed)
    db = SourceDB()
    for i in range(1, n_customers + 1):
        db.insert("customers", {"id": i, "name": f"Customer {i}", "email": f"c{i}@example.com", "tier": "free"})
    for i in range(1, n_orders + 1):
        cust = rng.choice(list(db.tables["customers"]))
        db.insert("orders", {"id": i, "customer_id": cust, "amount": round(rng.uniform(5, 500), 2), "status": "placed"})
        roll = rng.random()
        if roll < 0.5:
            db.update("orders", i, status=rng.choice(["paid", "shipped"]))
        elif roll < 0.6:
            db.delete("orders", i)
        if rng.random() < 0.1:
            cid = rng.choice(list(db.tables["customers"]))
            db.update("customers", cid, tier=rng.choice(["free", "pro", "enterprise"]))
        if i == n_orders // 2 and not db.schema_evolved:
            # ALTER TABLE customers ADD COLUMN phone: subsequent events carry the new field
            db.schema_evolved = True
            for cid in list(db.tables["customers"])[:10]:
                db.update("customers", cid, phone=f"+92-300-{cid:07d}")
    # a malformed message, e.g. from a misconfigured producer
    log = db.log[TOPICS["orders"]]
    log.append(Message(TOPICS["orders"], 0, len(log), b"{}", b'{"payload": {"op": "u"}}'))
    return db


def stream(db: SourceDB, sink: WarehouseSink, batch_size: int = 50, crash_every: int = 4, rewind: int = 30) -> dict:
    """Consume every topic in batches. Every few batches the consumer 'crashes' *after* applying a batch but
    before Kafka learns about it, so on restart it re-reads ``rewind`` messages that were already applied."""
    totals = {"applied": 0, "skipped_stale": 0, "tombstones": 0, "dead_letters": 0, "redelivered": 0}
    for topic, log in db.log.items():
        pos, batches = 0, 0
        while pos < len(log):
            batch = log[pos:pos + batch_size]
            r = sink.process_batch(batch)
            for k in ("applied", "skipped_stale", "tombstones", "dead_letters"):
                totals[k] += getattr(r, k)
            batches += 1
            pos += len(batch)
            if batches % crash_every == 0 and pos < len(log):
                back = min(rewind, pos)
                pos -= back                # crash: redeliver the tail of what was already processed
                totals["redelivered"] += back
    return totals


def reconcile(db: SourceDB, sink: WarehouseSink) -> Dict[str, dict]:
    """Compare the warehouse's live rows with the source's true final state, column by column."""
    report = {}
    for table, rows in db.tables.items():
        live = {r["id"]: r for r in sink.current(table)}
        mismatched = [pk for pk, src in rows.items()
                      if pk not in live or any(live[pk].get(c) != v for c, v in src.items())]
        extra = [pk for pk in live if pk not in rows]
        report[table] = {"source_rows": len(rows), "warehouse_rows": len(live), "mismatched": len(mismatched),
                         "extra": len(extra)}
    return report


def run_demo(path: str = ":memory:") -> None:
    db = workload()
    sink = WarehouseSink(path, PRIMARY_KEYS)
    totals = stream(db, sink)
    print("stream:", totals)
    for table, r in reconcile(db, sink).items():
        status = "OK" if r["mismatched"] == r["extra"] == 0 and r["source_rows"] == r["warehouse_rows"] else "MISMATCH"
        print(f"{table:<10} source={r['source_rows']:<4} warehouse={r['warehouse_rows']:<4} "
              f"mismatched={r['mismatched']} extra={r['extra']}  {status}")
    hist = sink.con.execute("SELECT count(*), count(*) FILTER (WHERE _is_current) FROM cdc.orders_history").fetchone()
    print(f"orders_history: {hist[0]} versions, {hist[1]} current")
    print("dead letters:", sink.con.execute("SELECT count(*) FROM cdc._dead_letters").fetchone()[0])


if __name__ == "__main__":
    run_demo()
