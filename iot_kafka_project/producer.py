from kafka import KafkaProducer
from apscheduler.schedulers.blocking import BlockingScheduler
import json, random, time
from datetime import datetime
from utils import KAFKA_BROKER, IOT_TOPIC

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

device_ids = [f'dev-{i}' for i in range(1,11)]

def generating_data():
    data = []
    for dev_id in device_ids:
        temp = round(random.uniform(75,100),2)
        payload = {
            "device_id": dev_id,
            "temperature": temp,
            "humidity": round(random.uniform(30, 60), 2),
            "pressure": round(random.uniform(1100, 1020), 2),
            "status": "OK" if temp <= 95 else "OVERHEAT",
            "timestamp": datetime.utcnow().isoformat(),
            "location": random.choice(["Jogja", "KulonProgo", "GunungKidul","Bantul"])
        }
        data.append(payload)
        producer.send(IOT_TOPIC,value=payload)
        print(f'SUCCESSFUL SENT | {payload}')
    producer.flush()

scheduler = BlockingScheduler()
scheduler.add_job(generating_data, 'cron', minute='*')  # sched kirim data tiap 1 menit
print("Producer started. Sending IoT data every 1 minute...")
scheduler.start()