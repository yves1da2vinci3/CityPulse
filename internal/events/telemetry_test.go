package events

import (
	"testing"
	"time"
)

func TestDecodeTelemetryRequiresFields(t *testing.T) {
	_, err := DecodeTelemetry([]byte(`{"speed":1}`))
	if err == nil {
		t.Fatal("want error")
	}
}

func TestProtoRoundTrip(t *testing.T) {
	in := TelemetryEvent{
		EventID:    "e1",
		VehicleID:  "BUS-1",
		Latitude:   1.5,
		Longitude:  2.5,
		Speed:      40,
		HeadingDeg: 90,
		Timestamp:  time.UnixMilli(1_700_000_000_000).UTC(),
	}
	b, err := MarshalProto(in)
	if err != nil {
		t.Fatal(err)
	}
	out, err := UnmarshalProto(b)
	if err != nil {
		t.Fatal(err)
	}
	if out.EventID != in.EventID || out.VehicleID != in.VehicleID || out.Speed != in.Speed {
		t.Fatalf("got %+v", out)
	}
	if out.HeadingDeg != 90 {
		t.Fatalf("heading %v", out.HeadingDeg)
	}
}
