#!/usr/bin/env python3
# spark/process_users_stream.py
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, MapType

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "cdc.users")
OUTPUT_PATH = os.getenv("OUTPUT_PATH", "output/parquet/users")
CHECKPOINT = os.getenv("CHECKPOINT_PATH", "output/checkpoints/users")

schema = StructType([
    StructField("operation", StringType()),
    StructField("ns", MapType(StringType(), StringType())),
    StructField("documentKey", MapType(StringType(), StringType())),
    StructField("fullDocument", MapType(StringType(), StringType())),
    StructField("clusterTime", StringType())
])

def main():
    spark = (
        SparkSession.builder
        .appName("users-cdc-processor")
        .config("spark.sql.shuffle.partitions", "1")  # small for local
        .getOrCreate()
    )

    df_raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    df = df_raw.selectExpr("CAST(value AS STRING) as json_str")

    df_parsed = df.select(from_json(col("json_str"), schema).alias("data")).select("data.*")

    # extract fields from fullDocument map
    users = df_parsed \
        .withColumn("email", col("fullDocument").getItem("email")) \
        .withColumn("name", col("fullDocument").getItem("name")) \
        .withColumn("operation", col("operation"))

    # For demo: write append into parquet partitioned by operation
    query = (
        users.writeStream
        .outputMode("append")
        .format("parquet")
        .option("path", OUTPUT_PATH)
        .option("checkpointLocation", CHECKPOINT)
        .start()
    )

    print(f"[SPARK] Stream started. Writing to {OUTPUT_PATH}")
    query.awaitTermination()

if __name__ == "__main__":
    main()