from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

from kafka import KafkaConsumer
import json
import sqlite3
import time


# =========================
# 🔽 CONSUMER FUNCTION
# =========================
def consume_data():
    print("Starting Kafka Consumer...")

    # 🔁 Retry logic (wait for Kafka to be ready)
    for i in range(10):
        try:
            consumer = KafkaConsumer(
                'orders',
                bootstrap_servers='kafka:29092',
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='airflow-group'
            )
            print("✅ Connected to Kafka!")
            break
        except Exception as e:
            print(f"⏳ Kafka not ready, retrying... ({i+1}/10)", e)
            time.sleep(5)
    else:
        raise Exception("❌ Could not connect to Kafka after retries")

    # 🗄️ Database setup
    db_path = "/opt/airflow/project/orders.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            order_id INT,
            product TEXT,
            price INT,
            quantity INT
        )
    """)

    print("📥 Starting to consume messages...")

    # ⚠️ IMPORTANT: limit messages (so Airflow task doesn't run forever)
    count = 0
    max_messages = 10

    for message in consumer:
        data = message.value
        print("Received:", data)

        cursor.execute(
            "INSERT INTO orders VALUES (?, ?, ?, ?)",
            (data['order_id'], data['product'], data['price'], data['quantity'])
        )
        conn.commit()

        count += 1
        if count >= max_messages:
            print("✅ Processed 10 messages, stopping task")
            break

    conn.close()


# =========================
# 🔽 DAG CONFIG
# =========================
default_args = {
    'start_date': datetime(2024, 1, 1)
}

dag = DAG(
    'kafka_data_pipeline',
    default_args=default_args,
    schedule_interval='*/5 * * * *',
    catchup=False
)


# =========================
# 🔽 TASK 1: PRODUCER
# =========================
run_producer = BashOperator(
    task_id='run_producer',
    bash_command='python3 /opt/airflow/project/producer.py',
    dag=dag
)


# =========================
# 🔽 TASK 2: CONSUMER
# =========================
run_consumer = PythonOperator(
    task_id='consume_kafka_data',
    python_callable=consume_data,
    dag=dag
)


# =========================
# 🔗 TASK FLOW
# =========================
run_producer >> run_consumer