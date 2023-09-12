#!/bin/bash

# Set Kafka home directory
KAFKA_HOME="/usr/local/kafka/kafka_2.13-3.2.1"

# Navigate to Kafka home directory
cd $KAFKA_HOME

# Start ZooKeeper
echo "Starting ZooKeeper..."
bin/zookeeper-server-start.sh config/zookeeper.properties &

# Wait for ZooKeeper to initialize
sleep 10

# Start Kafka server
echo "Starting Kafka..."
bin/kafka-server-start.sh config/server.properties &

# Wait for Kafka to initialize
sleep 10

# Create topic 'sparkab'
echo "Creating topic 'test'..."
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic sparkab

# List topics to confirm 'test' topic is created
echo "Listing topics..."
bin/kafka-topics.sh --list --bootstrap-server localhost:9092

# Done
echo "Kafka setup complete."

