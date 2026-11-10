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

	log.Println("alert consuming")
	err = cpkafka.RunConsumer(ctx, cl, func(ctx context.Context, rec *kgo.Record) error {
		ev, err := events.DecodeTelemetry(rec.Value)
		if err != nil {
			log.Printf("alert bad: %v", err)
			return cl.CommitRecords(ctx, rec)
		}
		if ev.Speed > 100 {
			log.Printf("ALERT speeding vehicle=%s speed=%.1f", ev.VehicleID, ev.Speed)
		} else {
			log.Printf("alert ok vehicle=%s speed=%.1f", ev.VehicleID, ev.Speed)
		}
		return cl.CommitRecords(ctx, rec)
	})
	if err != nil && err != context.Canceled {
		log.Fatal(err)
	}
}
