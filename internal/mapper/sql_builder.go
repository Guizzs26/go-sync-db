package mapper

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// SQLBuilder orchestrates the translation from JSON payloads to Firebird-compatible SQL
type SQLBuilder struct {
	// We can add configuration here later, such as table name mappings
}

// NewSQLBuilder initializes a new mapper instance
func NewSQLBuilder() *SQLBuilder {
	return &SQLBuilder{}
}

// BuildInsert generates a standard INSERT statement for Firebird 2.5
func (b *SQLBuilder) BuildInsert(tableName string, data map[string]any) (string, []any, error) {
	if len(data) == 0 {
		return "", nil, fmt.Errorf("no data provided for insert on table %s", tableName)
	}

	var columns []string
	var placeholders []string
	var args []any

	// Sort keys for deterministic SQL generation.
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		// Standardizing to Uppercase to prevent case-sensitivity issues in Firebird
		columns = append(columns, strings.ToUpper(k))
		placeholders = append(placeholders, "?")
		args = append(args, b.formatValue(data[k]))
	}

	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		strings.ToUpper(tableName),
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
	)

	return query, args, nil
}

// BuildUpdate generates an UPDATE statement based on a primary key
func (b *SQLBuilder) BuildUpdate(tableName string, pkColumn string, pkValue any, data map[string]any) (string, []any, error) {
	if len(data) == 0 {
		return "", nil, fmt.Errorf("no data provided for update on table %s", tableName)
	}

	var setClauses []string
	var args []any

	keys := make([]string, 0, len(data))
	for k := range data {
		// Standardize for comparison and skip PK in the SET clause
		if strings.EqualFold(k, pkColumn) {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		setClauses = append(setClauses, fmt.Sprintf("%s = ?", strings.ToUpper(k)))
		args = append(args, b.formatValue(data[k]))
	}

	query := fmt.Sprintf(
		"UPDATE %s SET %s WHERE %s = ?",
		strings.ToUpper(tableName),
		strings.Join(setClauses, ", "),
		strings.ToUpper(pkColumn),
	)
	args = append(args, b.formatValue(pkValue))

	return query, args, nil
}

// formatValue handles type conversion for Firebird 2.5 specificities
func (b *SQLBuilder) formatValue(v any) any {
	switch val := v.(type) {
	case bool:
		if val {
			return 1
		}
		return 0
	case string:
		// 1. Try Full ISO8601/RFC3339 (Timestamp)
		if t, err := time.Parse(time.RFC3339, val); err == nil {
			return t.Format("2006-01-02 15:04:05")
		}
		// 2. Try Simple Date (YYYY-MM-DD)
		if t, err := time.Parse("2006-01-02", val); err == nil {
			return t.Format("2006-01-02")
		}
		return val
	default:
		return val
	}
}
