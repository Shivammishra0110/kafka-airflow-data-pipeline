import time
import json
import random
from kafka import KafkaProducer

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers='kafka:29092',  # IMPORTANT for Docker
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

print("✅ Connected to Kafka")

# Limit number of messages (VERY IMPORTANT for Airflow)
max_messages = 10
count = 0

while count < max_messages:
    data = {
        "order_id": random.randint(1000, 9999),
        "product": random.choice(["Phone", "Laptop", "Tablet", "Headphones"]),
        "price": random.randint(10000, 50000),
        "quantity": random.randint(1, 5)
    }

    producer.send('orders', value=data)
    print(f"Sent: {data}")

    count += 1
    time.sleep(1)

# Flush and close
producer.flush()
producer.close()

print("✅ Producer finished successfully")