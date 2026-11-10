package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"github.com/yves1da2vinci3/CityPulse/internal/config"
	"github.com/yves1da2vinci3/CityPulse/internal/events"
	cpkafka "github.com/yves1da2vinci3/CityPulse/internal/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	cl, err := cpkafka.NewClient(
		config.Brokers(),
		kgo.ConsumerGroup("analytics"),
		kgo.ConsumeTopics(events.TopicTelemetry),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer cl.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Println("analytics consuming", events.TopicTelemetry)
	err = cpkafka.RunConsumer(ctx, cl, func(ctx context.Context, rec *kgo.Record) error {
		ev, err := events.DecodeTelemetry(rec.Value)
		if err != nil {
			log.Printf("skip bad record partition=%d offset=%d: %v", rec.Partition, rec.Offset, err)
			if err := cl.CommitRecords(ctx, rec); err != nil {
				return err
			}
			return nil
		}
		log.Printf("analytics vehicle=%s speed=%.1f event=%s", ev.VehicleID, ev.Speed, ev.EventID)
		return cl.CommitRecords(ctx, rec)
	})
	if err != nil && err != context.Canceled {
		log.Fatal(err)
	}
}
