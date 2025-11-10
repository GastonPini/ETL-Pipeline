# process_users_stream.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, to_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, MapType

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "cdc.users"
OUTPUT_PATH = "output/parquet/users"   # carpeta de parquet (actúa como "warehouse")
CHECKPOINT = "output/checkpoints/users"

schema = StructType([
    StructField("operation", StringType()),
    StructField("ns", MapType(StringType(), StringType())),
    StructField("documentKey", MapType(StringType(), StringType())),
    StructField("fullDocument", MapType(StringType(), StringType())),
    StructField("clusterTime", StringType())
])

spark = (
    SparkSession.builder
    .appName("users-cdc-processor")
    .getOrCreate()
)

# Read from Kafka
df_raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

# value is binary -> string
df = df_raw.selectExpr("CAST(value AS STRING) as json_str")

# parse JSON
df_parsed = df.select(from_json(col("json_str"), schema).alias("data")).select("data.*")

# Example transform: pull fields out of fullDocument and add metadata
# Assumes fullDocument contains keys "email","name","createdAt"
users = df_parsed.withColumn("email", col("fullDocument").getItem("email")) \
                 .withColumn("name", col("fullDocument").getItem("name")) \
                 .withColumn("op", col("operation")) \
                 .withColumn("ts", to_timestamp(col("clusterTime")))

# Upsert logic: use checkpoint + partition mode, and write parquet by email as id
# In Structured Streaming we can write append mode and later compact, or use foreachBatch for upserts.

def foreach_batch_function(batch_df, batch_id):
    # In each micro-batch, we can implement idempotent upsert into parquet/duckdb
    from pyspark.sql import functions as F
    # simple approach: write partitioned by email (replace existing by overwrite mode for that partition)
    emails = [r['email'] for r in batch_df.select('email').distinct().collect()]
    if not emails:
        return
    # Write out the batch to a temp location, then move/merge — for demo we overwrite partition.
    batch_df.write.mode("overwrite").parquet(OUTPUT_PATH + "/stage/partition_"+str(batch_id))

query = (
    users.writeStream
    .foreachBatch(foreach_batch_function)
    .option("checkpointLocation", CHECKPOINT)
    .start()
)

query.awaitTermination()