import os
from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER")
IOT_TOPIC = os.getenv("IOT_TOPIC")
ALERT_TOPIC = os.getenv("ALERT_TOPIC")
