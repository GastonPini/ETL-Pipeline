## 🧬 Aladia — Real-Time ETL CDC Demo (PySpark)

**Author:** Gaston Pini  
**Repo:** [https://github.com/GastonPini/ETL-Pipeline](https://github.com/GastonPini/ETL-Pipeline)  

---

### 🎯 Goal

A small, end-to-end **CDC → Kafka → PySpark → Parquet** demo that implements a real-time ETL pipeline:

- **Source:** MongoDB with Change Streams (CDC)
- **Message Bus:** Kafka
- **Processor:** PySpark Structured Streaming consuming Kafka
- **Sink:** Parquet files (queried with DuckDB for demo)

This project demonstrates architectural design, trade-offs, maintainability, and scalability reasoning.

---

### ⚡ Quick Status

All components run locally via **Docker Compose**.  
These can be queried with:
```bash
python demo/demo_query.py
```
---
##### **Repository Structure:**

```bash
etl-pipeline/
│
├── docker-compose.yml              # Infrastructure (Kafka, Zookeeper, MongoDB)
├── .env.example                    # Environment variables
├── requirements.txt                # Python dependencies
├── README.md                       # Documentation
├── run.sh                          # Automated demo script
│
├── cdc/                            # ① Data Capture (CDC - Change Data Capture)
│   ├── cdc_to_kafka.py             # Listens to MongoDB changes and sends to Kafka
│   └── insert_users.py             # Generates test data (inserts/updates)
│
├── spark/                          # ② Data Processing (PySpark)
│   └── process_users_stream.py     # Consumes Kafka messages and transforms events
│
├── demo/                           # ③ Query / Analytics Layer
│   └── demo_query.py               # Reads transformed data (Parquet → DuckDB)
│
└── infra/                          # ④ Auxiliary Infrastructure
    └── kafka-init-topics.sh        # Optional script to create Kafka topics manually
```

#### 🧰 Prerequisites
🐳 Docker & Docker Compose

☕ Java 11 (required by Spark)

⚙️ Apache Spark (local installation or PATH-configured spark-submit)

🐍 Python 3.9+ and pip

💻 (Windows users) Ensure python command works and PYSPARK_PYTHON=python


⚙️ Local Setup

### 1️⃣ Create & activate virtual environment
###### Git Bash / MINGW:
```bash
python -m venv .venv
source .venv/Scripts/activate
```

### 2️⃣ Install dependencies
```bash
pip install -r requirements.txt
```

### 3️⃣ Start infrastructure
```bash
docker compose up -d
```

Verify containers are running:
```bash
docker ps
```

You should see:

etl-pipeline-mongo-1

etl-pipeline-kafka-1

etl-pipeline-zookeeper-1

### 4️⃣ Initialize MongoDB Replica Set (first time only)
```bash
docker exec -it etl-pipeline-mongo-1 mongosh --eval 'rs.initiate({_id:"rs0", members:[{_id:0, host:"mongo:27017"}]})'
```

### 🧪 Run the Demo (3 terminals recommended)
#### 🧩 Terminal A — Start CDC Listener (MongoDB → Kafka)
```bash
source .venv/Scripts/activate
python cdc/cdc_to_kafka.py
```

#### 🧩 Terminal B — Start PySpark Streamer (Kafka → Parquet)
```bash
# Ensure environment variables:
# PYSPARK_PYTHON=python
# PYSPARK_DRIVER_PYTHON=python

spark-submit spark/process_users_stream.py
```

Leave this running — it’s a continuous streaming job.

#### 💾 Terminal C — Generate Test Data (Inserts + Updates)
```bash
python cdc/insert_users.py
```

You should see [INSERT] and [UPDATE] logs as the CDC events are captured and streamed.

📊 Query Output Data

Once Spark has written to Parquet, query it:
```bash
python demo/demo_query.py
```

🧠 Alternative: One-Click Run

You can run the full demo automatically:
```bash
./run.sh
```