package events

import (
	"fmt"
	"time"

	eventsv1 "github.com/yves1da2vinci3/CityPulse/internal/events/v1"
	"google.golang.org/protobuf/proto"
)

func ToProto(e TelemetryEvent) *eventsv1.TelemetryEvent {
	return &eventsv1.TelemetryEvent{
		EventId:         e.EventID,
		VehicleId:       e.VehicleID,
		Latitude:        e.Latitude,
		Longitude:       e.Longitude,
		Speed:           e.Speed,
		TimestampUnixMs: e.Timestamp.UTC().UnixMilli(),
		HeadingDeg:      e.HeadingDeg,
	}
}

func FromProto(p *eventsv1.TelemetryEvent) TelemetryEvent {
	return TelemetryEvent{
		EventID:    p.GetEventId(),
		VehicleID:  p.GetVehicleId(),
		Latitude:   p.GetLatitude(),
		Longitude:  p.GetLongitude(),
		Speed:      p.GetSpeed(),
		Timestamp:  time.UnixMilli(p.GetTimestampUnixMs()).UTC(),
		HeadingDeg: p.GetHeadingDeg(),
	}
}

func MarshalProto(e TelemetryEvent) ([]byte, error) {
	return proto.Marshal(ToProto(e))
}

func UnmarshalProto(b []byte) (TelemetryEvent, error) {
	var p eventsv1.TelemetryEvent
	if err := proto.Unmarshal(b, &p); err != nil {
		return TelemetryEvent{}, err
	}
	e := FromProto(&p)
	if e.VehicleID == "" || e.EventID == "" {
		return e, fmt.Errorf("invalid proto telemetry")
	}
	return e, nil
}
