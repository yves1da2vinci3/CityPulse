package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/yves1da2vinci3/CityPulse/internal/config"
	"github.com/yves1da2vinci3/CityPulse/internal/events"
	cpkafka "github.com/yves1da2vinci3/CityPulse/internal/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
)

type postBody struct {
	EventID   string    `json:"event_id"`
	VehicleID string    `json:"vehicle_id"`
	Latitude  float64   `json:"latitude"`
	Longitude float64   `json:"longitude"`
	Speed     float64   `json:"speed"`
	Timestamp time.Time `json:"timestamp"`
}

func main() {
	// Idempotent produce ON by default in franz-go (omit DisableIdempotentWrite).
	cl, err := cpkafka.NewClient(
		config.Brokers(),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer cl.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("POST /telemetry", func(w http.ResponseWriter, r *http.Request) {
		var in postBody
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			http.Error(w, "bad json", http.StatusBadRequest)
			return
		}
		if in.VehicleID == "" {
			http.Error(w, "vehicle_id required", http.StatusBadRequest)
			return
		}
		ev := events.TelemetryEvent{
			EventID:   in.EventID,
			VehicleID: in.VehicleID,
			Latitude:  in.Latitude,
			Longitude: in.Longitude,
			Speed:     in.Speed,
			Timestamp: in.Timestamp,
		}
		if ev.EventID == "" {
			ev.EventID = events.NewEventID()
		}
		if ev.Timestamp.IsZero() {
			ev.Timestamp = time.Now().UTC()
		}
		payload, err := events.MarshalProto(ev)
		if err != nil {
			http.Error(w, "encode", http.StatusInternalServerError)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		if err := cpkafka.Produce(ctx, cl, events.TopicTelemetry, []byte(ev.VehicleID), payload); err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(ev)
	})

	addr := config.EnvOr("HTTP_ADDR", ":8080")
	log.Println("telemetry listening", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}
