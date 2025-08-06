import json
import time
from datetime import datetime
from kafka import KafkaProducer
from dotenv import load_dotenv
import os

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

IP_ADDRESS = "37.125.171.238"
URL = "https://redis.io/docs/latest/develop/clients/redis-py/connect/"

def produce_event():
    payload = {
        "timestamp": datetime.utcnow().isoformat(),
        "url": URL,
        "ip_address": IP_ADDRESS
    }
    producer.send(KAFKA_TOPIC, value=payload)
    print(f"[Sent] {payload}")

if __name__ == "__main__":
    print("Kafka Producer started. Sending web access logs...")
    while True:
        produce_event()
        time.sleep(3)  # setiap 3 detik dia ngeproduce event brati dalam 1menit dia bisa 20x 
