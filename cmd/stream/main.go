package main

import (
	"context"
	"encoding/json"
	"log"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/yves1da2vinci3/CityPulse/internal/config"
	"github.com/yves1da2vinci3/CityPulse/internal/events"
	cpkafka "github.com/yves1da2vinci3/CityPulse/internal/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
)

type bucket struct {
	sum   float64
	count int
	start time.Time
}

type windowStore struct {
	mu   sync.Mutex
	win  time.Duration
	data map[string]*bucket
}

func newStore(win time.Duration) *windowStore {
	return &windowStore{win: win, data: map[string]*bucket{}}
}

func (s *windowStore) add(vehicle string, speed float64, ts time.Time) (avg float64, closed bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.data[vehicle]
	if !ok || ts.Sub(b.start) >= s.win {
		if ok && b.count > 0 {
			avg = b.sum / float64(b.count)
			closed = true
		}
		s.data[vehicle] = &bucket{sum: speed, count: 1, start: ts.Truncate(s.win)}
		return avg, closed
	}
	b.sum += speed
	b.count++
	return 0, false
}

func main() {
	cl, err := cpkafka.NewClient(
		config.Brokers(),
		kgo.ConsumerGroup("stream"),
		kgo.ConsumeTopics(events.TopicTelemetry),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer cl.Close()

	store := newStore(5 * time.Minute)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Println("stream processor up")
	err = cpkafka.RunConsumer(ctx, cl, func(ctx context.Context, rec *kgo.Record) error {
		ev, err := events.UnmarshalProto(rec.Value)
		if err != nil {
			return cl.CommitRecords(ctx, rec)
		}
		if ev.Speed > 100 {
			body, _ := json.Marshal(map[string]any{
				"type": "SPEED_FILTER", "vehicle_id": ev.VehicleID, "speed": ev.Speed, "event_id": ev.EventID,
			})
			_ = cpkafka.Produce(ctx, cl, events.TopicAlerts, []byte(ev.VehicleID), body)
		}
		if avg, closed := store.add(ev.VehicleID, ev.Speed, ev.Timestamp); closed {
			body, _ := json.Marshal(map[string]any{
				"vehicle_id": ev.VehicleID, "avg_speed": avg, "window": "5m",
			})
			_ = cpkafka.Produce(ctx, cl, "statistics.events", []byte(ev.VehicleID), body)
			log.Printf("window closed vehicle=%s avg=%.2f", ev.VehicleID, avg)
		}
		return cl.CommitRecords(ctx, rec)
	})
	if err != nil && err != context.Canceled {
		log.Fatal(err)
	}
}
