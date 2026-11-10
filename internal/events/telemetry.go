package events

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"
)

const TopicTelemetry = "telemetry.events"
const TopicTelemetryDLQ = "telemetry.dlq"
const TopicAlerts = "alerts.events"

type TelemetryEvent struct {
	EventID   string    `json:"event_id"`
	VehicleID string    `json:"vehicle_id"`
	Latitude  float64   `json:"latitude"`
	Longitude float64   `json:"longitude"`
	Speed     float64   `json:"speed"`
	Timestamp time.Time `json:"timestamp"`
}

func NewEventID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func (e TelemetryEvent) Marshal() ([]byte, error) {
	return json.Marshal(e)
}

func DecodeTelemetry(b []byte) (TelemetryEvent, error) {
	var e TelemetryEvent
	if err := json.Unmarshal(b, &e); err != nil {
		return e, err
	}
	if e.VehicleID == "" {
		return e, fmt.Errorf("vehicle_id required")
	}
	if e.EventID == "" {
		return e, fmt.Errorf("event_id required")
	}
	return e, nil
}
