import json
import os
import psycopg2
from kafka import KafkaConsumer
from redis import Redis
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

# Kafka config
KAFKA_BROKER = os.getenv("KAFKA_BROKER")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")

# Redis config
REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = int(os.getenv("REDIS_PORT"))

# Postgres config
PG_CONN = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST"),
    port=os.getenv("POSTGRES_PORT"),
    database=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD")
)

redis_client = Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
cursor = PG_CONN.cursor()

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='access_checker'
)

def block_ip(ip):
    now = datetime.utcnow()
    cursor.execute(
        "INSERT INTO blocked_ip_address (ip_address, blocked_at) VALUES (%s, %s)",
        (ip, now)
    )
    PG_CONN.commit()
    print(f"IP {ip} | BLOCKED at {now}")

print("Listening for web access logs...")

for message in consumer:
    data = message.value
    ip = data["ip_address"]

    key = f"access:{ip}"
    count = redis_client.incr(key)

    if count == 1:
        redis_client.expire(key, 60)  # TTL 60 detik 

    print(f"Access from {ip} ➝ count: {count}")

    if count > 10:
        if not redis_client.get(f"blocked:{ip}"):
            block_ip(ip)
            redis_client.setex(f"blocked:{ip}", 3600, 1)  # cegah duplicate block selama 1 jam
