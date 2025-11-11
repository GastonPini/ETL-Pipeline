#!/usr/bin/env python3
# cdc/cdc_to_kafka.py
import os
import json
import time
from dotenv import load_dotenv
from pymongo import MongoClient
from kafka import KafkaProducer
from bson import ObjectId, Timestamp

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "cdc.users")

# ✅ Fix: ObjectId & Timestamp serialization
class EnhancedJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, ObjectId):
            return str(o)
        if isinstance(o, Timestamp):
            return str(o)
        return json.JSONEncoder.default(self, o)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v, cls=EnhancedJSONEncoder).encode("utf-8"),
    linger_ms=10
)

def format_change(change):
    op = change.get("operationType")
    envelope = {
        "operation": op,
        "ns": change.get("ns"),
        "documentKey": change.get("documentKey"),
        "fullDocument": change.get("fullDocument"),
        "clusterTime": str(change.get("_clusterTime")) if change.get("_clusterTime") else None
    }
    return envelope

def main():
    client = MongoClient("mongodb://localhost:27017/?replicaSet=rs0")
    db = client.get_database("aladia_db")
    coll = db.get_collection("users")

    print(f"[CDC] Watching collection {db.name}.users -> Kafka {TOPIC} @ {KAFKA_BOOTSTRAP}")

    with coll.watch(full_document='updateLookup') as stream:
        for change in stream:
            ev = format_change(change)
            print("[CDC] event:", ev["operation"], ev.get("documentKey"))
            try:
                producer.send(TOPIC, ev)
                producer.flush()
            except Exception as e:
                print("[CDC] Error sending to Kafka:", e)
                time.sleep(1)

if __name__ == "__main__":
    main()