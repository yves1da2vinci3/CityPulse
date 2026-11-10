# CityPulse

![Cover — CityPulse : télémétrie transport urbain](assets/coverImage.jpg)

> Télémétrie transport urbain en temps réel — Go + franz-go + Kafka KRaft : producer HTTP, consumer groups, partitions, DLQ, EOS, stream, Postgres

## Avertissement — projet fictif

CityPulse est un **projet pédagogique fictif**. Il simule une plateforme de télémétrie pour bus urbains (type flotte SOTRA / smart city) dans le but d’apprendre Apache Kafka de bout en bout. Il ne s’agit pas d’un produit réel, d’un opérateur de transport ni d’un déploiement en production en Côte d’Ivoire. Noms, bus Abidjan (`BUS-001`, coordonnées `5.35, -4.01`) et métriques illustrent un scénario fictif uniquement.

## À propos

**CityPulse** est une **simulation** d’ingestion et de traitement d’événements GPS — architecture événementielle résiliente avec Kafka KRaft : producer HTTP (JSON in → Protobuf on wire), plusieurs consumer groups, clé de partition, retry / DLQ, idempotence `event_id`, transactions EOS, fenêtres de stream et persistance Postgres.

Le système cible un déploiement régional centré sur **Abidjan** et la **Côte d’Ivoire**.

Module : `github.com/yves1da2vinci3/CityPulse`.

## Architecture

### Composants

| Composant | Rôle |
|-----------|------|
| **`cmd/telemetry`** | `POST /telemetry` — JSON HTTP → Protobuf Kafka (`acks=all`) |
| **`cmd/kafkaping`** | Smoke : joint le broker via `KAFKA_BROKERS` |
| **Kafka KRaft** | Topics, partitions, replication, consumer groups |
| **`cmd/analytics`** | Group `analytics` — log vitesse |
| **`cmd/storage`** | Group `storage` — INSERT Postgres (`pgx`) |
| **`cmd/alert`** | Group `alert` — speeding + retry/DLQ + dedup mémoire |
| **`cmd/stream`** | Filtre `speed > 100` + moyenne tumbling 5 min → `statistics.events` |
| **`cmd/eos-alert`** | Produce transactionnel → `alerts.events` |
| **`cmd/json-bridge`** | Miroir proto → topic JSON `telemetry.json` (voie Connect) |
| **`cmd/saslping`** | Smoke client SASL/PLAIN (overlay lab) |

### Topics

| Topic | Usage |
|-------|--------|
| `telemetry.events` | Flux principal (clé = `vehicle_id`, payload Protobuf) |
| `telemetry.dlq` | Poison / échecs de decode |
| `alerts.events` | Alertes vitesse (EOS / stream filter) |
| `statistics.events` | Moyennes de vitesse fenêtre 5 min |
| `telemetry.json` | Miroir JSON pour Connect (via `json-bridge`) |

### Flux global

```mermaid
flowchart TB
    subgraph producer [Producer]
        HTTP["POST /telemetry JSON"]
    end
    subgraph kafka [Kafka KRaft]
        Telemetry["telemetry.events\nProtobuf key vehicle_id"]
        DLQ["telemetry.dlq"]
        Alerts["alerts.events"]
        Stats["statistics.events"]
        JSON["telemetry.json"]
    end
    subgraph consumers [Consumer groups]
        Analytics["analytics"]
        Storage["storage"]
        Alert["alert"]
        Stream["stream"]
        Eos["eos-alert"]
        Bridge["json-bridge"]
    end
    subgraph sink [Sink]
        PG["Postgres"]
    end
    HTTP --> Telemetry
    Telemetry --> Analytics
    Telemetry --> Storage --> PG
    Telemetry --> Alert
    Telemetry --> Stream
    Telemetry --> Eos
    Telemetry --> Bridge --> JSON
    Telemetry -->|"poison / retry"| DLQ
    Stream -->|"speed > 100 + fenêtre 5m"| Stats
    Stream -->|"SPEED_FILTER"| Alerts
    Eos -->|"tx EOS speed > 100"| Alerts
```

## Features

### 1. Ingestion HTTP → Protobuf

**Scénario :** les bus poussent position / vitesse / horodatage. HTTP parle JSON ; Kafka transporte Protobuf (`api/telemetry.proto`, champ optionnel `heading_deg`).

**Impl :** `cmd/telemetry` + `events.MarshalProto` ; `event_id` généré si absent ; clé = `vehicle_id` ; `RequiredAcks(AllISRAcks)`.

**Attendu :** HTTP `202` + body JSON echo ; consumers lisent via `UnmarshalProto`.

### 2. Consumer groups indépendants

**Scénario :** analytics, storage et alert traitent le **même** event sans se bloquer.

**Impl :** groups `analytics`, `storage`, `alert` sur `telemetry.events`.

**Attendu :** un `curl` → logs dans les 3 process.

### 3. Clé `vehicle_id`

**Scénario :** ordre relatif par bus.

**Impl :** clé Kafka = `vehicle_id`. Voir [docs/partitions-vehicle-id.md](docs/partitions-vehicle-id.md).

### 4. Résilience — retry, DLQ, dedup, acks

**Scénario :** poison message et doublons HTTP.

**Impl :** `internal/kafka/retry.go` → `telemetry.dlq` ; `internal/dedup` sur `event_id` dans `alert` ; producer `acks=all` (idempotence franz-go par défaut).

### 5. Cluster HA + EOS

**Scénario :** alerte speeding atomique avec commit d’offset.

**Impl :** [`deployments/kafka/docker-compose.ha.yml`](deployments/kafka/docker-compose.ha.yml) (3 nœuds RF=3) ; `cmd/eos-alert` + `TransactionalID`.

### 6. Stream, Postgres, lag, SASL

**Scénario :** filtre vitesse, agrégats, SQL, lag, auth broker.

**Impl :**

- `cmd/stream` → `statistics.events` / `alerts.events`
- `cmd/storage` → table `telemetry` (Postgres 16 dans Compose)
- Connect image dans Compose + `json-bridge` (voie JDBC optionnelle)
- Lag : [docs/consumer-lag.md](docs/consumer-lag.md)
- Overlay SASL : `docker-compose.sasl.yml` + `cmd/saslping` (PLAINTEXT inchangé)

## Stack technique

- **Go** 1.24.2 (`go.mod`)
- **franz-go** v1.18.1 + **kadm**
- **`net/http`** — ingest
- **Protobuf** — `google.golang.org/protobuf` + `api/telemetry.proto`
- **pgx/v5** — sink SQL
- **Docker Compose** — `apache/kafka:3.9.1` KRaft ; Postgres 16 ; Connect optionnel
- Config : `KAFKA_BROKERS`, `HTTP_ADDR`, `DATABASE_URL`

## Quick Start

Prérequis : Docker Compose + Go ≥ 1.24.

```bash
export KAFKA_BROKERS=localhost:9092
export DATABASE_URL='postgres://citypulse:citypulse@localhost:5432/citypulse?sslmode=disable'

cd deployments/kafka
docker compose up -d
sleep 10

docker exec citypulse-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic telemetry.events --partitions 3 --replication-factor 1

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

cd ../..
go run ./cmd/kafkaping
# terminaux séparés :
go run ./cmd/telemetry
go run ./cmd/analytics
go run ./cmd/storage
go run ./cmd/alert
```

```bash
curl -s -X POST http://localhost:8080/telemetry -H 'Content-Type: application/json' \
  -d '{"vehicle_id":"BUS-001","latitude":5.35,"longitude":-4.01,"speed":42}'

curl -s -X POST http://localhost:8080/telemetry -H 'Content-Type: application/json' \
  -d '{"vehicle_id":"BUS-001","latitude":5.36,"longitude":-4.01,"speed":125}'
```

Attendu : analytics log ; storage `pg insert` ; alert `ALERT` sur 125.

Runbook complet : [docs/runbook.md](docs/runbook.md).

## Tests

```bash
go test ./internal/events ./internal/dedup
# optionnel broker live :
KAFKA_BROKERS=localhost:9092 go test ./internal/kafka -count=1
```

## Checklist Scalabilité et Robustesse

> Projet fictif — implémenté vs extensions futures.

### Scalabilité

- [x] **Infra KRaft locale (Compose)** — mono-nœud + `docker-compose.ha.yml` 3 nœuds
- [x] **Producer HTTP + Protobuf** — JSON in, proto on wire, `heading_deg` optionnel
- [x] **3 consumer groups** — analytics / storage / alert
- [x] **Clé `vehicle_id`** — sticky partition
- [x] **Stream fenêtre 5 min** — `cmd/stream`
- [x] **Postgres via `cmd/storage`** — Connect Compose prêt ; sink vérifiable = Go+pgx
- [ ] **Grafana / Prometheus**
- [ ] **Multi-région / MirrorMaker**
- [ ] **Schema Registry**

### Robustesse

- [x] **Retry + DLQ + dedup `event_id`**
- [x] **Acks=all + producer idempotent (défaut franz-go)**
- [x] **Transactions EOS (`eos-alert`)**
- [x] **Lag CLI + SASL overlay + `go test`**
- [ ] **Auth HTTP (JWT) + rate limiting**
- [ ] **UI carte temps réel**

## Documentation

- [docs/runbook.md](docs/runbook.md) — boot démo bout-en-bout
- [docs/consumer-lag.md](docs/consumer-lag.md) — mesurer le lag
- [docs/partitions-vehicle-id.md](docs/partitions-vehicle-id.md) — coloc clé / partition
