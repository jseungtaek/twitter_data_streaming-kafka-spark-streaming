from ensurepip import bootstrap
from kafka import KafkaConsumer
import json

topic = "twitterstream"

consumer = KafkaConsumer(
    topic,
    bootstrap_servers='localhost:9092',
    auto_offset_reset='latest',
    enable_auto_commit=True,
    auto_commit_interval_ms=5000,
    fetch_max_bytes=128,
    max_poll_records=100,
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))

for msg in consumer:
    tweets = json.loads(json.dumps(msg.value))
    print(tweets)
