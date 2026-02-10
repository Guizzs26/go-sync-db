package models

import "encoding/json"

type OutboxEntry struct {
	ID            int64           `db:"id"`
	CorrelationID string          `db:"correlation_id"`
	UnitID        int             `db:"unit_id"`
	TableName     string          `db:"table_name"`
	Operation     string          `db:"operation"`
	Payload       json.RawMessage `db:"payload"`
	Attempts      int             `db:"attempts"`
}
