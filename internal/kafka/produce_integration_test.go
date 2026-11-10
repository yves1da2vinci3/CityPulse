package kafka_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/yves1da2vinci3/CityPulse/internal/events"
	cpkafka "github.com/yves1da2vinci3/CityPulse/internal/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestProduceSmoke(t *testing.T) {
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		t.Skip("KAFKA_BROKERS not set")
	}
	cl, err := cpkafka.NewClient(
		[]string{brokers},
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	payload, _ := events.MarshalProto(events.TelemetryEvent{
		EventID:   "test-1",
		VehicleID: "BUS-TEST",
		Speed:     1,
		Timestamp: time.Now().UTC(),
	})

	if err := cpkafka.Produce(ctx, cl, events.TopicTelemetry, []byte("BUS-TEST"), payload); err != nil {
		t.Fatal(err)
	}
}
