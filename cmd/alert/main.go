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

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Println("alert consuming with retry+dlq")
	err = cpkafka.RunConsumer(ctx, cl, func(ctx context.Context, rec *kgo.Record) error {
		process := func() error {
			_, err := events.DecodeTelemetry(rec.Value)
			return err
		}
		if err := cpkafka.Retry(ctx, 3, 100*time.Millisecond, process); err != nil {
			dlqBody, _ := json.Marshal(map[string]any{
				"error":     err.Error(),
				"raw":       string(rec.Value),
				"partition": rec.Partition,
				"offset":    rec.Offset,
				"failed_at": time.Now().UTC(),
			})
			if perr := cpkafka.Produce(ctx, cl, events.TopicTelemetryDLQ, rec.Key, dlqBody); perr != nil {
				return fmt.Errorf("dlq produce: %w", perr)
			}
			log.Printf("sent to DLQ offset=%d: %v", rec.Offset, err)
		} else {
			ev, _ := events.DecodeTelemetry(rec.Value)
			if ev.Speed > 100 {
				log.Printf("ALERT speeding vehicle=%s speed=%.1f", ev.VehicleID, ev.Speed)
			}
		}
		return cl.CommitRecords(ctx, rec)
	})
	if err != nil && err != context.Canceled {
		log.Fatal(err)
	}
}
