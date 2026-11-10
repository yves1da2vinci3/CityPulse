package main

import (
	"context"
	"encoding/json"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/yves1da2vinci3/CityPulse/internal/config"
	"github.com/yves1da2vinci3/CityPulse/internal/events"
	cpkafka "github.com/yves1da2vinci3/CityPulse/internal/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	group := "eos-alert"
	cl, err := cpkafka.NewClient(
		config.Brokers(),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(events.TopicTelemetry),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.RequireStableFetchOffsets(),
		kgo.TransactionalID("citypulse-eos-alert"),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer cl.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Println("eos-alert starting")
	for {
		fetches := cl.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if e.Err == ctx.Err() {
					return
				}
				log.Printf("fetch: %v", e)
			}
		}
		iter := fetches.RecordIter()
		for !iter.Done() {
			rec := iter.Next()
			if err := handleOne(ctx, cl, group, rec); err != nil {
				log.Printf("handle: %v", err)
			}
		}
	}
}

func handleOne(ctx context.Context, cl *kgo.Client, group string, rec *kgo.Record) error {
	ev, err := events.UnmarshalProto(rec.Value)
	if err != nil {
		// poison: commit hors txn pour avancer (DLQ simplifié)
		return cl.CommitRecords(ctx, rec)
	}
	if ev.Speed <= 100 {
		return cl.CommitRecords(ctx, rec)
	}

	if err := cl.BeginTransaction(); err != nil {
		return err
	}
	alert, _ := json.Marshal(map[string]any{
		"type":       "SPEEDING",
		"event_id":   ev.EventID,
		"vehicle_id": ev.VehicleID,
		"speed":      ev.Speed,
		"ts":         time.Now().UTC(),
	})
	cl.Produce(ctx, &kgo.Record{
		Topic: events.TopicAlerts,
		Key:   []byte(ev.VehicleID),
		Value: alert,
	}, nil)

	if err := cl.EndTransaction(ctx, kgo.TryCommit); err != nil {
		_ = cl.AbortBufferedRecords(ctx)
		_ = cl.EndTransaction(ctx, kgo.TryAbort)
		return err
	}
	// Offsets committed within transaction via EndTransaction when using
	// transactional producer + consumer group — also commit explicitly for clarity:
	if err := cl.CommitRecords(ctx, rec); err != nil {
		return err
	}
	log.Printf("eos alert vehicle=%s event=%s", ev.VehicleID, ev.EventID)
	return nil
}
