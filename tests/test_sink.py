import json

from cdc.simulate import PRIMARY_KEYS, reconcile, stream, workload
from cdc.sink import Message, WarehouseSink

T = "shop.public.customers"


def event(op, before, after, lsn, ts_ms=1_700_000_000_000, offset=0, wrapped=True):
    body = {"op": op, "before": before, "after": after,
            "source": {"table": "customers", "lsn": lsn, "ts_ms": ts_ms}}
    value = {"payload": body} if wrapped else body
    return Message(T, 0, offset, b'{"id": 1}', json.dumps(value).encode())


def sink():
    return WarehouseSink(":memory:", PRIMARY_KEYS)


ALICE = {"id": 1, "name": "Alice", "email": "a@x.com", "tier": "free"}


def test_insert_update_delete_flow_and_history():
    s = sink()
    s.process_batch([event("c", None, ALICE, 10, offset=0),
                     event("u", ALICE, {**ALICE, "tier": "pro"}, 20, ts_ms=1_700_000_100_000, offset=1)])
    assert s.current("customers")[0]["tier"] == "pro"
    s.process_batch([event("d", {**ALICE, "tier": "pro"}, None, 30, ts_ms=1_700_000_200_000, offset=2)])
    assert s.current("customers") == []
    assert s.current("customers", include_deleted=True)[0]["_deleted"] is True
    hist = s.con.execute("SELECT tier, _valid_to IS NULL, _is_current FROM cdc.customers_history ORDER BY _lsn").fetchall()
    assert hist == [("free", False, False), ("pro", False, False)]    # both versions closed by the delete


def test_replayed_events_are_skipped():
    s = sink()
    batch = [event("c", None, ALICE, 10, offset=0), event("u", ALICE, {**ALICE, "tier": "pro"}, 20, offset=1)]
    s.process_batch(batch)
    r = s.process_batch(batch)                     # redelivery after a crash
    assert (r.applied, r.skipped_stale) == (0, 2)
    assert s.con.execute("SELECT count(*) FROM cdc.customers_history").fetchone()[0] == 2


def test_older_event_never_overwrites_newer_state():
    s = sink()
    s.process_batch([event("u", ALICE, {**ALICE, "tier": "enterprise"}, 50, offset=5)])
    s.process_batch([event("u", ALICE, {**ALICE, "tier": "pro"}, 40, offset=4)])
    assert s.current("customers")[0]["tier"] == "enterprise"


def test_schema_evolution_adds_new_columns():
    s = sink()
    s.process_batch([event("c", None, ALICE, 10)])
    s.process_batch([event("u", ALICE, {**ALICE, "phone": "+92-300-1"}, 20, offset=1)])
    assert s.current("customers")[0]["phone"] == "+92-300-1"


def test_tombstones_and_poison_messages_do_not_stop_the_stream():
    s = sink()
    r = s.process_batch([Message(T, 0, 0, b'{"id": 1}', None),
                         Message(T, 0, 1, b"{}", b'{"payload": {"op": "u"}}'),
                         event("c", None, ALICE, 10, offset=2)])
    assert (r.tombstones, r.dead_letters, r.applied) == (1, 1, 1)
    assert s.con.execute("SELECT count(*) FROM cdc._dead_letters").fetchone()[0] == 1


def test_unwrapped_envelope_is_accepted():
    s = sink()
    s.process_batch([event("c", None, ALICE, 10, wrapped=False)])
    assert s.current("customers")[0]["name"] == "Alice"


def test_offsets_commit_with_the_batch():
    s = sink()
    s.process_batch([event("c", None, ALICE, 10, offset=0), event("u", ALICE, ALICE, 11, offset=1)])
    assert s.committed_offset(T) == 2


def test_failed_batch_rolls_back_data_and_offsets(monkeypatch):
    s = sink()
    s.process_batch([event("c", None, ALICE, 10, offset=0)])
    monkeypatch.setattr(s, "_apply", lambda ch: (_ for _ in ()).throw(RuntimeError("disk full")))
    try:
        s.process_batch([event("u", ALICE, {**ALICE, "tier": "pro"}, 20, offset=1)])
    except RuntimeError:
        pass
    assert s.committed_offset(T) == 1 and s.current("customers")[0]["tier"] == "free"


def test_end_to_end_with_crashes_matches_source_exactly():
    db = workload(seed=7)
    s = sink()
    totals = stream(db, s, batch_size=40, crash_every=3, rewind=25)
    assert totals["redelivered"] > 0 and totals["skipped_stale"] > 0
    for table, r in reconcile(db, s).items():
        assert r["mismatched"] == r["extra"] == 0, (table, r)
        assert r["source_rows"] == r["warehouse_rows"]
