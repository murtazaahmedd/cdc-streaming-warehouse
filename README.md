# CDC Streaming Warehouse

**Change data capture** from Postgres into an analytics warehouse: **Debezium** reads the Postgres write-ahead
log, publishes every insert, update and delete to **Kafka (Redpanda)**, and a Python sink applies them to
**DuckDB** as a current-state table plus a full **SCD type 2 history**, effectively once, even when the consumer
crashes and Kafka redelivers.

```text
 Postgres ──WAL──► Debezium (Kafka Connect) ──► Redpanda topics ──► sink ──────────────► DuckDB
 (wal_level=logical,  pgoutput, snapshot +       shop.public.*        process_batch()     cdc.customers            current state
  replica identity    streaming, tombstones                           (one transaction:   cdc.customers_history    SCD2 versions
  full)                                                                data + offsets)    cdc._offsets             consumer position
                                                                                          cdc._dead_letters        poison messages
```

## The hard parts, and how they are handled

| Problem | Handling |
|---|---|
| **Redelivery.** Kafka is at-least-once; after a crash the consumer re-reads messages it already applied. | Every row stores the log position (LSN) of the last change applied to it. An event with an LSN at or below that is skipped, so replays change nothing. |
| **Crash between writing data and committing the offset.** | The batch's rows and the next offset are written in **one DuckDB transaction**. On start-up the consumer seeks to the offset the warehouse recorded, not Kafka's. |
| **Out-of-order or stale events.** | Same LSN check: an older change can never overwrite a newer one. |
| **Deletes.** | Soft delete in the current table (`_deleted = true`); the open history version is closed. The tombstone Debezium sends after a delete is recognised and ignored. |
| **History.** | Each change closes the current version (`_valid_to`) and opens a new one, giving point-in-time queries for free. |
| **Schema changes** (`ALTER TABLE ... ADD COLUMN`). | New fields in `after` are added to both warehouse tables automatically. |
| **Poison messages.** | Anything unparseable goes to `cdc._dead_letters` with the error; the stream keeps flowing. |

## Proof: crash-and-redeliver test

`make demo` runs a simulated shop workload (50 customers, 300 orders, updates, deletes, a mid-stream schema
change, a malformed message) through the real sink, crashing the consumer every few batches so 30 already-applied
messages are delivered again. It then compares the warehouse with the source table by table, column by column:

```text
stream: {'applied': 581, 'skipped_stale': 58, 'tombstones': 30, 'dead_letters': 1, 'redelivered': 60}
customers  source=50   warehouse=50   mismatched=0 extra=0  OK
orders     source=272  warehouse=272  mismatched=0 extra=0  OK
orders_history: 460 versions, 272 current
dead letters: 1
```

Every redelivered message was skipped, and the warehouse matches the source exactly.

## Run it

**Offline** (no Docker; what CI runs):

```bash
pip install -r requirements.txt
make demo       # simulated stream + reconciliation
make test       # 9 tests: CRUD + history, replays, stale events, schema evolution, tombstones,
                # dead letters, atomic offsets, rollback on failure, end-to-end with crashes
```

**Full stack** (Docker):

```bash
make up          # Postgres, Redpanda, Debezium Connect, Redpanda Console on :8080
make connector   # register the Debezium source (connect/postgres-source.json)
make sink        # start the consumer (writes warehouse.duckdb)
make workload    # in another terminal: inserts, updates, deletes, ALTER TABLE
```

> The Docker stack and the Kafka consumer (`cdc/consumer.py`) are not run in CI; the same sink code they call is
> fully tested through the simulator, which produces Debezium-format events.

## Querying the result

```sql
-- current orders, deletes excluded
SELECT * FROM cdc.orders WHERE NOT _deleted;

-- a customer's tier as it was on a given day
SELECT tier FROM cdc.customers_history
WHERE id = 7 AND TIMESTAMP '2025-11-24 01:00' >= _valid_from
  AND (_valid_to IS NULL OR TIMESTAMP '2025-11-24 01:00' < _valid_to);
```

## Project layout

```text
cdc/
  sink.py       WarehouseSink: parse Debezium envelopes, idempotent apply, SCD2, schema evolution, DLQ, offsets
  consumer.py   Kafka consumer that seeks to warehouse-stored offsets
  simulate.py   offline workload + crash/redelivery simulator + reconciliation
docker/         docker-compose (Postgres, Redpanda, Debezium Connect, Console), seed schema, workload
connect/        Debezium Postgres connector config
tests/          unit and end-to-end tests
```

## Ideas to extend

- Sink to BigQuery with the Storage Write API, using the same LSN guard in a `MERGE`.
- Add a Schema Registry and Avro, and handle column type changes, not just additions.
- Compute lag (source commit time vs warehouse apply time) and export it to Prometheus.
