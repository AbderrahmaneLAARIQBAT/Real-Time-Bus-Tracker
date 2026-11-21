from confluent_kafka import Producer
import json
import random
import time
from datetime import datetime

# إعدادات Kafka
producer = Producer({'bootstrap.servers': 'localhost:9092'})

# معرّفات الطوبيسات وخطوط السير
bus_ids = [f"BUS_{i}" for i in range(1, 6)]
line_ids = ["L1", "L2", "L3", "L4", "L5"]

def generate_data():
    return {
        "bus_id": random.choice(bus_ids),
        "line_id": random.choice(line_ids),
        "latitude": round(random.uniform(34.0, 34.1), 6),
        "longitude": round(random.uniform(-6.85, -6.8), 6),
        "timestamp": datetime.utcnow().isoformat(),
        "speed": round(random.uniform(20, 80), 2)
    }

# إرسال البيانات
while True:
    data = generate_data()
    producer.produce(
        topic="bus_gps",
        value=json.dumps(data),
        callback=lambda err, msg: print(f"✅ Sent: {data}" if not err else f"❌ Error: {err}")
    )
    producer.flush()
    time.sleep(1)
