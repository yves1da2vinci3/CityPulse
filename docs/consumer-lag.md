# Consumer lag

Measure lag with Kafka CLI (no Grafana).

```bash
CTR=citypulse-kafka
docker ps --format '{{.Names}}' | grep -q citypulse-kafka1 && CTR=citypulse-kafka1

docker exec "$CTR" /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --all-groups
```

Create lag: stop `analytics`, publish 5 events, re-describe group `analytics` → `LAG` > 0. Restart analytics → lag returns toward 0.

Correlation id in consumer logs = `event_id`.
