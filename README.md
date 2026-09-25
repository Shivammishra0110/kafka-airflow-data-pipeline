# Kafka-Airflow Data Pipeline

## 📌 Project Overview

This project demonstrates a data pipeline using **Apache Kafka, Apache Airflow, Python, and Docker**.

The pipeline uses Kafka to produce and consume streaming data, while Apache Airflow is used to **orchestrate and schedule the pipeline tasks**.

The entire environment runs using Docker containers, making the project easy to set up and reproduce.

## 🏗️ Architecture

```text
                ┌──────────────────┐
                │   Kafka Producer │
                │     (Python)     │
                └────────┬─────────┘
                         │
                         ▼
                ┌──────────────────┐
                │      Kafka       │
                │      Broker      │
                └────────┬─────────┘
                         │
                         ▼
                ┌──────────────────┐
                │   Kafka Consumer │
                │     (Python)     │
                └──────────────────┘
                         ▲
                         │
                ┌────────┴─────────┐
                │  Apache Airflow  │
                │  DAG Orchestration│
                └──────────────────┘

              All services run using Docker
```

## 🛠️ Technologies Used

* **Python** – Producer and consumer applications
* **Apache Kafka** – Real-time messaging and streaming
* **Apache Airflow** – Workflow orchestration
* **Docker** – Containerization
* **Docker Compose** – Managing multiple containers
* **Git & GitHub** – Version control

## 📂 Project Structure

```text
airflow_docker/
│
├── dags/
│   └── kafka_pipeline_dag.py
│
├── project/
│   ├── producer.py
│   └── consumer.py
│
├── docker-compose.yaml
├── requirements.txt
├── .gitignore
└── README.md
```

## 🔄 How the Pipeline Works

### 1. Kafka Producer

The Python producer connects to the Kafka broker and publishes messages to a Kafka topic.

```text
Producer → Kafka Topic
```

### 2. Kafka Broker

Kafka acts as the messaging system between the producer and consumer.

It receives messages from the producer and makes them available to consumers.

```text
Producer → Kafka → Consumer
```

### 3. Kafka Consumer

The consumer connects to Kafka and reads messages from the configured Kafka topic.

```text
Kafka Topic → Consumer
```

### 4. Apache Airflow

Airflow is used to orchestrate the pipeline.

The DAG defines the tasks and their execution order.

For example:

```text
Start
  │
  ▼
Run Producer
  │
  ▼
Run Consumer
  │
  ▼
End
```

The Airflow DAG controls when the pipeline tasks should execute.

## 🐳 Running the Project with Docker

### Prerequisites

Make sure you have installed:

* Docker Desktop
* Git

### Start the containers

From the project directory, run:

```bash
docker compose up -d
```

### Check running containers

```bash
docker ps
```

### Stop the containers

```bash
docker compose down
```

## 🌐 Airflow

Once the Airflow containers are running, open the Airflow web interface in your browser using the port configured in `docker-compose.yaml`.

From the Airflow UI, you can:

* View the DAG
* Enable/disable the DAG
* Trigger the DAG manually
* Monitor task execution
* Check task logs

## 📊 Project Workflow

The overall workflow is:

```text
Docker
   │
   ├── Apache Airflow
   │       │
   │       ▼
   │   Kafka Pipeline DAG
   │       │
   │       ├── Producer Task
   │       │       │
   │       │       ▼
   │       │     Kafka
   │       │       │
   │       │       ▼
   │       │   Consumer Task
   │
   └── Kafka Services
```

## 🎯 Key Concepts Demonstrated

This project demonstrates practical understanding of:

* Kafka producers and consumers
* Kafka topics
* Message streaming
* Airflow DAGs
* Airflow task orchestration
* PythonOperator
* BashOperator
* Docker containers
* Docker Compose
* Service-to-service communication
* Git and GitHub



## 👨‍💻 Author

**Shivam Mishra**

GitHub: [Shivammishra0110](https://github.com/Shivammishra0110)
