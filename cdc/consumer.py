"""Kafka consumer that feeds Debezium topics into the warehouse sink.

Offsets live in the warehouse, written in the same transaction as the data (see ``WarehouseSink.process_batch``).
On start-up the consumer seeks every partition to the offset the warehouse recorded, so a crash at any point
resumes exactly where the last committed batch ended. Kafka's own committed offsets are only advisory.

    KAFKA_BOOTSTRAP=localhost:19092 python -m cdc.consumer
"""
from __future__ import annotations

import logging
import os
import signal

from .simulate import PRIMARY_KEYS
from .sink import Message, WarehouseSink

log = logging.getLogger("cdc.consumer")
TOPICS = os.getenv("CDC_TOPICS", "shop.public.customers,shop.public.orders").split(",")


def main() -> None:  # pragma: no cover - needs a running Kafka; exercised by `make up`, not by unit tests
    from confluent_kafka import Consumer, TopicPartition

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    sink = WarehouseSink(os.getenv("WAREHOUSE_PATH", "warehouse.duckdb"), PRIMARY_KEYS)
    consumer = Consumer({"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP", "localhost:19092"),
                         "group.id": "cdc-warehouse-sink", "enable.auto.commit": False,
                         "auto.offset.reset": "earliest"})

    def on_assign(c, partitions):
        for p in partitions:
            p.offset = sink.committed_offset(p.topic, p.partition) or p.offset
        c.assign(partitions)
        log.info("assigned %s", [(p.topic, p.partition, p.offset) for p in partitions])

    consumer.subscribe(TOPICS, on_assign=on_assign)
    running = True
    signal.signal(signal.SIGTERM, lambda *_: globals().__setitem__("running", False))
    try:
        while running:
            batch = consumer.consume(num_messages=500, timeout=1.0)
            msgs = [Message(m.topic(), m.partition(), m.offset(), m.key(), m.value()) for m in batch if not m.error()]
            if not msgs:
                continue
            result = sink.process_batch(msgs)
            consumer.commit(offsets=[TopicPartition(m.topic, m.partition, m.offset + 1) for m in msgs[-1:]],
                            asynchronous=True)
            log.info("batch of %d: %s", len(msgs), result)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
