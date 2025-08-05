from kafka import KafkaConsumer, KafkaProducer
import json
from utils import KAFKA_BROKER, IOT_TOPIC, ALERT_TOPIC


consumer = KafkaConsumer(
    IOT_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    group_id="iot_alert_group",  # consumer group ID
    auto_offset_reset="earliest",  # mulai dari awal
    enable_auto_commit=True
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("Consumer started. Listening for IoT data...")

for message in consumer:
    data = message.value
    print(f"[Received] {data}")
    if data["temperature"] > 95:
        print("ALERT: Temperature > 95!")
        producer.send(ALERT_TOPIC, value=data)
        producer.flush()