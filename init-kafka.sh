#!/bin/bash
echo "Waiting for Kafka to be ready..."
sleep 8

echo "Creating SCRAM user: admin"

kafka-configs \
  --bootstrap-server localhost:9092 \
  --alter \
  --add-config 'SCRAM-SHA-512=[password=admin]' \
  --entity-type users \
  --entity-name admin

echo "Done."

exec /etc/confluent/docker/run
