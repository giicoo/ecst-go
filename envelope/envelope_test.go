package envelope

import (
	"testing"
)

type order struct {
	Total int `json:"total"`
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	in := New("order", "order-1", 7, OpUpdate, &order{Total: 100}).
		WithSource("orders-service", "v1").
		WithTraceID("trace-1")

	raw, err := in.Encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	out, err := Decode[order](raw)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if out.EntityType != in.EntityType || out.EntityID != in.EntityID {
		t.Fatalf("entity = %s/%s, want %s/%s", out.EntityType, out.EntityID, in.EntityType, in.EntityID)
	}
	if out.Version != in.Version || out.Op != in.Op {
		t.Fatalf("version/op = %d/%s, want %d/%s", out.Version, out.Op, in.Version, in.Op)
	}
	if out.Source != in.Source {
		t.Fatalf("source = %+v, want %+v", out.Source, in.Source)
	}
	if out.TraceID != in.TraceID {
		t.Fatalf("trace id = %q, want %q", out.TraceID, in.TraceID)
	}
	if !out.Timestamp.Equal(in.Timestamp) {
		t.Fatalf("timestamp = %s, want %s", out.Timestamp, in.Timestamp)
	}
	if out.Payload == nil || out.Payload.Total != in.Payload.Total {
		t.Fatalf("payload = %+v, want %+v", out.Payload, in.Payload)
	}
}

// Delete приезжает без payload: nil должен пережить round trip
func TestDeleteHasNoPayload(t *testing.T) {
	raw, err := New[order]("order", "order-1", 8, OpDelete, nil).Encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	out, err := Decode[order](raw)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if out.Payload != nil {
		t.Fatalf("payload = %+v, want nil", out.Payload)
	}
}

func TestDecodeBroken(t *testing.T) {
	if _, err := Decode[order]([]byte("not json")); err == nil {
		t.Fatal("want error")
	}
}
