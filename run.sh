#!/bin/bash
set -e

echo "🚀 Starting Aladia CDC ETL Pipeline Demo..."

# Step 1: Start infrastructure
echo "🧱 Starting MongoDB + Kafka + Zookeeper..."
docker compose up -d

echo "⏳ Waiting 20 seconds for Kafka & Mongo to be ready..."
sleep 20

# Step 2: Create virtualenv if not exists
if [ ! -d ".venv" ]; then
  echo "🐍 Creating Python virtual environment..."
  python -m venv .venv
fi

source .venv/Scripts/activate
pip install -r requirements.txt --quiet

# Step 3: Start CDC script in background
echo "🔄 Starting CDC listener (MongoDB → Kafka)..."
python cdc/cdc_to_kafka.py &
CDC_PID=$!
sleep 5

# Step 4: Generate some data in Mongo
echo "🧩 Generating user activity data in MongoDB..."
python cdc/insert_users.py

# Step 5: Start Spark streaming job
echo "⚙️ Starting PySpark streaming job (Kafka → Parquet)..."
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 spark/process_users_stream.py &
SPARK_PID=$!
sleep 15

# Step 6: Run a quick query with DuckDB
echo "📊 Querying the transformed parquet data..."
python demo/demo_query.py || true

# Step 7: Cleanup
echo "🧹 Stopping background jobs..."
kill $CDC_PID || true
kill $SPARK_PID || true

echo "✅ Demo completed successfully!"
echo "📁 You can explore 'output/parquet/users/' for the written data."