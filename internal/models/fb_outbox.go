package models

import "time"

// FBOutboxRecord represents a row in the Firebird FB_SYNC_OUTBOX table
type FBOutboxRecord struct {
	ID        int64     `db:"ID"`
	TableName string    `db:"TABLE_NAME"`
	OpType    string    `db:"OP_TYPE"` // 'I', 'U', 'D'
	PKValue   string    `db:"PK_VALUE"`
	CreatedAt time.Time `db:"CREATED_AT"`
}

// FBEventPayload represents the JSON message sent to RabbitMQ
// It contains metadata + the full record data (Snapshot)
type FBEventPayload struct {
	EventID   string                 `json:"event_id"` // Unique ID for tracing (UUID)
	UnitID    int                    `json:"unit_id"`  // Origin Unit
	TableName string                 `json:"table_name"`
	Operation string                 `json:"operation"` // I, U, D
	PKValue   string                 `json:"pk_value"`
	Timestamp time.Time              `json:"timestamp"`
	Data      map[string]interface{} `json:"data"` // The full record (Snapshot)
}
