# scripts/consumers/consume_topic_bounded.py
import os
import json
import time
from quixstreams import Application

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:19092")
TOPIC = os.getenv("TOPIC")
TIMEOUT_SECS = int(os.getenv("TIMEOUT_SECS", "10"))
MAX_MSGS = int(os.getenv("MAX_MSGS", "5000"))
GROUP = os.getenv("GROUP", "verify-bounded")

if not TOPIC:
    raise SystemExit("TOPIC env var is required")

app = Application(
    broker_address=KAFKA_BOOTSTRAP,
    consumer_group=GROUP,
    auto_offset_reset="earliest",
)

topic = app.topic(TOPIC)
TOPIC_STR = TOPIC if isinstance(TOPIC, str) else str(TOPIC)

count = 0
first_ts = None
last_ts = None
samples = []

with app.get_consumer() as consumer:
    consumer.subscribe([TOPIC_STR])
    t0 = time.time()
    while time.time() - t0 < TIMEOUT_SECS and count < MAX_MSGS:
        msg = consumer.poll(0.2)
        if not msg:
            continue
        count += 1
        try:
            payload = json.loads(msg.value().decode("utf-8"))
        except Exception:
            payload = None
        ts = msg.timestamp()
        if first_ts is None:
            first_ts = ts
        last_ts = ts
        if len(samples) < 5 and payload is not None:
            samples.append(payload)

print(json.dumps({
    "topic": TOPIC,
    "count": count,
    "first_ts": first_ts,
    "last_ts": last_ts,
    "samples": samples,
}, default=str))
