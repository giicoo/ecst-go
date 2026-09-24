package envelope

import (
	"encoding/json"
	"fmt"
	"time"
)

// Record headers
const (
	HeaderEnvelopeType string = "envelope_type"
	HeaderTraceID      string = "trace_id"
)

// Type of operation
type Op string

const (
	OpCreate Op = "c" // create
	OpUpdate Op = "u" // update
	OpDelete Op = "d" // delete
	OpRead   Op = "r" // read
)

// Envelope for event
type Envelope[T any] struct {
	EntityType string    `json:"entity_type"`
	EntityID   string    `json:"entity_id"`
	Version    int64     `json:"version"`
	Op         Op        `json:"op"`
	Payload    *T        `json:"payload,omitempty"`
	Timestamp  time.Time `json:"timestamp"`
	TraceID    string    `json:"trace_id,omitempty"`
	Source     Source    `json:"source"`
}

// Who produced the event and by which schema version
type Source struct {
	Service   string `json:"service"`
	SchemaVer string `json:"schema_ver"`
}

func New[T any](entityType, entityID string, version int64, op Op, payload *T) Envelope[T] {
	return Envelope[T]{
		EntityType: entityType,
		EntityID:   entityID,
		Version:    version,
		Op:         op,
		Payload:    payload,
		Timestamp:  time.Now().UTC(),
	}
}

// WithSource fills [Source]. Without it a consumer cannot tell
// who sent the event and how to read its payload
func (e Envelope[T]) WithSource(service, schemaVer string) Envelope[T] {
	e.Source = Source{Service: service, SchemaVer: schemaVer}

	return e
}

// WithTraceID links the event to the request it was born in
func (e Envelope[T]) WithTraceID(traceID string) Envelope[T] {
	e.TraceID = traceID

	return e
}

// Json Encode
func (e Envelope[T]) Encode() ([]byte, error) {
	const op = "Envelope.Encode"

	raw, err := json.Marshal(e)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	return raw, nil
}

// Json Decode
func Decode[T any](raw []byte) (Envelope[T], error) {
	const op = "envelope.Decode"

	var e Envelope[T]

	if err := json.Unmarshal(raw, &e); err != nil {
		return e, fmt.Errorf("%s: %w", op, err)
	}

	return e, nil
}
