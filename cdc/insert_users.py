#!/usr/bin/env python3
# cdc/insert_users.py
import os
import time
import random
from datetime import datetime
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()
#MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
#client = MongoClient(MONGO_URI)
client = MongoClient("mongodb://localhost:27017/?replicaSet=rs0")
db = client.get_database("aladia_db")
coll = db.get_collection("users")

def create_user(i):
    return {
        "email": f"user{i}@example.com",
        "name": f"User {i}",
        "signup_ts": datetime.utcnow().isoformat(),
        "last_seen": datetime.utcnow().isoformat()
    }

def main():
    print("[INSERT] Starting to insert/update users for testing...")
    for i in range(1, 11):
        user = create_user(i)
        coll.update_one({"email": user["email"]}, {"$set": user}, upsert=True)
        print("[INSERT] upserted", user["email"])
        time.sleep(1)

    # Then do some updates
    for i in range(1, 6):
        coll.update_one({"email": f"user{i}@example.com"}, {"$set": {"last_seen": datetime.utcnow().isoformat()}})
        print("[UPDATE] updated", f"user{i}@example.com")
        time.sleep(1)

if __name__ == "__main__":
    main()