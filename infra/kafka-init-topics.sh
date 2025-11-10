#!/bin/bash
# Ensure Kafka is up, then create topic if kafka-topics.sh is available locally
# This script assumes kafka tools are installed. Otherwise, create topic via kafka container or use kafka-python producer (auto-creates topic).
KAFKA_HOST=localhost:9092
TOPIC=cdc.users
echo "Creating topic $TOPIC (if not exists) via kafka-python (producer will auto-create)."