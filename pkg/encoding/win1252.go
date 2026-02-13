package encoding

import (
	"strings"

	"golang.org/x/text/encoding/charmap"
)

// Decoder handles the conversion from Windows-1252 (common in Firebird legacy DBs) to UTF-8
type Decoder struct{}

// ToUTF8 converts a slice of bytes (WIN1252) to a UTF-8 string
// If the data is already valid UTF-8, it returns it as is
func ToUTF8(b []byte) string {
	if len(b) == 0 {
		return ""
	}

	// Attempt to decode using Windows-1252 charmap
	decoded, err := charmap.Windows1252.NewDecoder().Bytes(b)
	if err != nil {
		// Fallback: return raw string if decoding fails (better than crashing)
		return string(b)
	}

	return strings.TrimSpace(string(decoded))
}
