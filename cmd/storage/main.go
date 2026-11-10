package main

import (
	"context"
	"database/sql"
	"log"
	"os/signal"
	"syscall"

	"github.com/yves1da2vinci3/CityPulse/internal/config"
	"github.com/yves1da2vinci3/CityPulse/internal/events"
	cpkafka "github.com/yves1da2vinci3/CityPulse/internal/kafka"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	dsn := config.EnvOr("DATABASE_URL", "postgres://citypulse:citypulse@localhost:5432/citypulse?sslmode=disable")
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		log.Fatal(err)
	}

	cl, err := cpkafka.NewClient(
		config.Brokers(),
		kgo.ConsumerGroup("storage"),
		kgo.ConsumeTopics(events.TopicTelemetry),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer cl.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	err = cpkafka.RunConsumer(ctx, cl, func(ctx context.Context, rec *kgo.Record) error {
		ev, err := events.UnmarshalProto(rec.Value)
		if err != nil {
			return cl.CommitRecords(ctx, rec)
		}
		_, err = db.ExecContext(ctx, `
INSERT INTO telemetry (event_id, vehicle_id, latitude, longitude, speed, timestamp, heading_deg)
VALUES ($1,$2,$3,$4,$5,$6,$7)
ON CONFLICT (event_id) DO NOTHING`,
			ev.EventID, ev.VehicleID, ev.Latitude, ev.Longitude, ev.Speed, ev.Timestamp, ev.HeadingDeg)
		if err != nil {
			return err
		}
		log.Printf("pg insert event=%s", ev.EventID)
		return cl.CommitRecords(ctx, rec)
	})
	if err != nil && err != context.Canceled {
		log.Fatal(err)
	}
}
