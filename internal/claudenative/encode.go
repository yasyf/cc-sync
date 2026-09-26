package claudenative

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf16"
)

const projectDirMaxUnits = 200

// ErrInvalidSessionID reports a string that is not a UUID.
var ErrInvalidSessionID = errors.New("invalid session id")

// SessionID is a Claude Code session id in canonical lowercase UUID form.
type SessionID string

// ParseSessionID lowercases s and accepts it only as an 8-4-4-4-12 hex UUID.
func ParseSessionID(s string) (SessionID, error) {
	id := strings.ToLower(s)
	if len(id) != 36 {
		return "", fmt.Errorf("%w: %q", ErrInvalidSessionID, s)
	}
	for i, c := range []byte(id) {
		switch i {
		case 8, 13, 18, 23:
			if c != '-' {
				return "", fmt.Errorf("%w: %q", ErrInvalidSessionID, s)
			}
		default:
			if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
				return "", fmt.Errorf("%w: %q", ErrInvalidSessionID, s)
			}
		}
	}
	return SessionID(id), nil
}

// SanitizePath is Claude's project-name sanitizer: every UTF-16 code unit
// outside [A-Za-z0-9] becomes '-', so a non-BMP rune becomes two dashes.
func SanitizePath(p string) string {
	return sanitizeUnits(utf16.Encode([]rune(p)))
}

// ProjectDirName is the projects/ subdirectory Claude uses for cwd: the
// sanitized path, or past 200 UTF-16 units its first 200 plus "-" and the
// base-36 absolute value of the 32-bit Java string hash of cwd.
func ProjectDirName(cwd string) string {
	units := utf16.Encode([]rune(cwd))
	name := sanitizeUnits(units)
	if len(units) <= projectDirMaxUnits {
		return name
	}
	hash := int64(javaHash(units))
	if hash < 0 {
		hash = -hash
	}
	return name[:projectDirMaxUnits] + "-" + strconv.FormatInt(hash, 36)
}

func sanitizeUnits(units []uint16) string {
	out := make([]byte, len(units))
	for i, u := range units {
		switch {
		case u >= 'a' && u <= 'z', u >= 'A' && u <= 'Z', u >= '0' && u <= '9':
			out[i] = byte(u)
		default:
			out[i] = '-'
		}
	}
	return string(out)
}

func javaHash(units []uint16) int32 {
	var h int32
	for _, u := range units {
		h = h*31 + int32(u)
	}
	return h
}
