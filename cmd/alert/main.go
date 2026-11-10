package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/yves1da2vinci3/CityPulse/internal/config"
	"github.com/yves1da2vinci3/CityPulse/internal/dedup"
	"github.com/yves1da2vinci3/CityPulse/internal/events"
	cpkafka "github.com/yves1da2vinci3/CityPulse/internal/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	cl, err := cpkafka.NewClient(
		config.Brokers(),
		kgo.ConsumerGroup("alert"),
		kgo.ConsumeTopics(events.TopicTelemetry),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer cl.Close()

	store := dedup.NewMemory()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	err = cpkafka.RunConsumer(ctx, cl, func(ctx context.Context, rec *kgo.Record) error {
		var last error
		process := func() error {
			_, err := events.DecodeTelemetry(rec.Value)
			last = err
			return err
		}
		if err := cpkafka.Retry(ctx, 3, 50*time.Millisecond, process); err != nil {
			dlqBody, _ := json.Marshal(map[string]any{
				"error": err.Error(),
				"raw":   string(rec.Value),
			})
			if perr := cpkafka.Produce(ctx, cl, events.TopicTelemetryDLQ, rec.Key, dlqBody); perr != nil {
				return fmt.Errorf("dlq: %w", perr)
			}
			log.Printf("DLQ: %v", err)
			return cl.CommitRecords(ctx, rec)
		}
		ev, _ := events.DecodeTelemetry(rec.Value)
		if store.SeenOrAdd(ev.EventID) {
			log.Printf("dedup skip event=%s", ev.EventID)
		} else if ev.Speed > 100 {
			log.Printf("ALERT speeding vehicle=%s speed=%.1f event=%s", ev.VehicleID, ev.Speed, ev.EventID)
		}
		_ = last
		return cl.CommitRecords(ctx, rec)
	})
	if err != nil && err != context.Canceled {
		log.Fatal(err)
	}
}
