"""A real-broker recovery experiment with a durable, idempotent SQLite sink."""
import argparse
import json
import sqlite3
import time
import uuid
from pathlib import Path

from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic


def apply_event(db, key, body):
    event = json.loads(body)
    if set(event) != {"id", "amount_cents"} or type(event["amount_cents"]) is not int:
        raise ValueError("Invalid event")
    if key != event["id"].encode():
        raise ValueError("Message key does not match event id")
    canonical = json.dumps(event, sort_keys=True)
    previous = db.execute("SELECT payload FROM events WHERE id=?", (event["id"],)).fetchone()
    if previous:
        if previous[0] != canonical:
            raise ValueError("Conflicting event payload")
        return False
    with db:
        db.execute("INSERT INTO events VALUES (?, ?)", (event["id"], canonical))
    return True


def run(bootstrap, state_dir):
    # Keep this experiment local by default; a different cluster needs an explicit adaptation.
    if bootstrap not in {"127.0.0.1:19092", "localhost:19092"}:
        raise ValueError("This sandbox experiment accepts only the dedicated local broker")
    run_id = uuid.uuid4().hex[:12]
    topic, group = "jenny-recovery-" + run_id, "jenny-consumer-" + run_id
    admin = AdminClient({"bootstrap.servers": bootstrap})
    for future in admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)]).values():
        future.result(timeout=20)
    producer = Producer({"bootstrap.servers": bootstrap, "enable.idempotence": True, "acks": "all"})
    delivered, failures = [], []
    def acknowledged(err, msg):
        if err:
            failures.append(str(err))
        else:
            delivered.append(msg.offset())
    events = [{"id": "o1", "amount_cents": 10000}, {"id": "o2", "amount_cents": 5000},
              {"id": "o3", "amount_cents": -500}, {"id": "o2", "amount_cents": 5000}]
    for event in events:
        producer.produce(topic, key=event["id"].encode(), value=json.dumps(event).encode(),
                         partition=0, on_delivery=acknowledged)
    if producer.flush(20) or failures or sorted(delivered) != [0, 1, 2, 3]:
        raise RuntimeError(f"Delivery was not confirmed: {failures}, {delivered}")
    state_dir.mkdir(parents=True, exist_ok=True)
    db_path = state_dir / f"{run_id}.sqlite"
    db = sqlite3.connect(db_path)
    db.execute("CREATE TABLE events (id TEXT PRIMARY KEY, payload TEXT NOT NULL)")
    config = {"bootstrap.servers": bootstrap, "group.id": group,
              "enable.auto.commit": False, "enable.auto.offset.store": False,
              "auto.offset.reset": "earliest"}
    received, duplicates = [], 0
    def next_message(consumer):
        deadline = time.monotonic() + 25
        while time.monotonic() < deadline:
            msg = consumer.poll(1)
            if msg is None:
                continue
            if msg.error():
                raise RuntimeError(str(msg.error()))
            return msg
        raise TimeoutError("No message within 25 seconds")
    first = Consumer(config)
    try:
        first.subscribe([topic])
        msg = next_message(first)
        assert msg.offset() == 0
        apply_event(db, msg.key(), msg.value())
        received.append(msg.offset())
        first.commit(message=msg, asynchronous=False)
        msg = next_message(first)
        assert msg.offset() == 1
        apply_event(db, msg.key(), msg.value())
        received.append(msg.offset())
        # Simulate a process loss after the durable sink write but BEFORE offset commit.
    finally:
        first.close()
        db.close()
    db = sqlite3.connect(db_path)
    restarted = Consumer(config)
    try:
        checkpoint = restarted.committed([TopicPartition(topic, 0)], timeout=15)[0].offset
        assert checkpoint == 1, checkpoint
        restarted.subscribe([topic])
        for expected in [1, 2, 3]:
            msg = next_message(restarted)
            assert msg.offset() == expected, (msg.offset(), expected)
            duplicates += not apply_event(db, msg.key(), msg.value())
            received.append(msg.offset())
            restarted.commit(message=msg, asynchronous=False)
        final_offset = restarted.committed([TopicPartition(topic, 0)], timeout=15)[0].offset
        unique = db.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        assert final_offset == 4 and unique == 3 and duplicates == 2
        return {"scope": "REAL_LOCAL_KAFKA_BROKER", "topic": topic, "group": group,
                "acknowledged_offsets": sorted(delivered), "processed_offsets": received,
                "committed_before_restart": checkpoint, "committed_after_restart": final_offset,
                "unique_sink_events": unique, "duplicate_deliveries_suppressed": duplicates,
                "sink": str(db_path), "status": "PASS"}
    finally:
        restarted.close()
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="127.0.0.1:19092")
    parser.add_argument("--state-dir", type=Path, default=Path("work/kafka-state"))
    args = parser.parse_args()
    print(json.dumps(run(args.bootstrap, args.state_dir), indent=2))
