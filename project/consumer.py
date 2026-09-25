from kafka import KafkaConsumer
import json
import sqlite3

# Connect to SQLite DB
conn = sqlite3.connect('orders.db')
cursor = conn.cursor()

# Create table if not exists
cursor.execute("""
CREATE TABLE IF NOT EXISTS orders (
    order_id INTEGER,
    product TEXT,
    price INTEGER,
    quantity INTEGER
)
""")

consumer = KafkaConsumer(
    'orders',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True
)

print("Consumer started...")

for message in consumer:
    data = message.value

    cursor.execute("""
    INSERT INTO orders (order_id, product, price, quantity)
    VALUES (?, ?, ?, ?)
    """, (
        data['order_id'],
        data['product'],
        data['price'],
        data['quantity']
    ))

    conn.commit()
    print(f"Inserted: {data}")