# CityPulse demo runbook

PLAINTEXT path (no SASL).

## Boot infra

```bash
export KAFKA_BROKERS=localhost:9092

cd deployments/kafka
docker compose up -d
# HA optional: docker compose -f docker-compose.ha.yml up -d
# then KAFKA_BROKERS=localhost:9092,localhost:9093,localhost:9094

sleep 10
docker exec citypulse-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list
```

Topics (depending on chapters done): `telemetry.events`, `telemetry.dlq`, `alerts.events`, `statistics.events`.

Postgres table (storage chapter):

```bash
docker exec -i citypulse-pg psql -U citypulse -d citypulse <<'SQL'
CREATE TABLE IF NOT EXISTS telemetry (
  event_id TEXT PRIMARY KEY,
  vehicle_id TEXT,
  latitude DOUBLE PRECISION,
  longitude DOUBLE PRECISION,
  speed DOUBLE PRECISION,
  timestamp TIMESTAMPTZ,
  heading_deg DOUBLE PRECISION
);
SQL
```

## Boot Go processes

Separate terminals:

```bash
go run ./cmd/telemetry
go run ./cmd/analytics
go run ./cmd/storage
go run ./cmd/alert
go run ./cmd/stream          # optional
go run ./cmd/eos-alert       # optional
```

## Demo scenario

```bash
curl -s -X POST http://localhost:8080/telemetry -H 'Content-Type: application/json' \
  -d '{"vehicle_id":"BUS-001","latitude":5.35,"longitude":-4.01,"speed":42}'

curl -s -X POST http://localhost:8080/telemetry -H 'Content-Type: application/json' \
  -d '{"vehicle_id":"BUS-001","latitude":5.36,"longitude":-4.01,"speed":125}'
```

Expect:

- analytics / storage log both events (storage inserts Postgres)
- alert / stream / eos-alert signal speeding on 125
- `kafka-consumer-groups --describe --all-groups` → lag ~0 on active groups

## Checklist

| Item | OK |
|------|----|
| `go test ./internal/events ./internal/dedup` | |
| Broker healthy | |
| `POST /telemetry` → 202 | |
| Consumer groups see the event | |
| Tag `cp-producer` (or later) present | |
