#!/usr/bin/env python3
# demo/demo_query.py
import duckdb
import os

OUTPUT_PATH = os.getenv("OUTPUT_PATH", "output/parquet/users")

print("[DEMO] Querying parquet files in:", OUTPUT_PATH)
con = duckdb.connect(database=':memory:')
# read all parquet files produced by Spark
# If none exist, DuckDB will error — we handle gracefully.
try:
    df = con.execute(f"SELECT email, name, operation FROM read_parquet('{OUTPUT_PATH}/*.parquet') LIMIT 100").fetchdf()
    print(df.head(50).to_string())
except Exception as e:
    print("[DEMO] No parquet files found or error:", e)