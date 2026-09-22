package envelope

import (
	"encoding/json"
	"fmt"
	"time"
)

const (
	ENVELOPE_TYPE string = "evelope_type"
	TRACE_ID string = "trace_id"
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
func (e Envelope[T]) Decode(v []byte) (Envelope[T], error) {
	const op = "Envelope.Decode"

	var envelope Envelope[T]
	 
	if err := json.Unmarshal(v, &envelope); err != nil {
		return envelope, fmt.Errorf("%s: %w", op, err)
	}

	return envelope, nil
}
