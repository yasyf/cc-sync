package sessionrestore

import (
	"bytes"
	"encoding/json"
)

type span struct{ start, end int }

type member struct {
	name     string
	key, val span
}

type edit struct {
	at   span
	repl []byte
}

func isSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r'
}

func skipSpace(b []byte, i int) int {
	for i < len(b) && isSpace(b[i]) {
		i++
	}
	return i
}

func stringEnd(b []byte, i int) int {
	for i++; i < len(b); i++ {
		switch b[i] {
		case '\\':
			i++
		case '"':
			return i + 1
		}
	}
	return len(b)
}

func valueEnd(b []byte, i int) int {
	switch b[i] {
	case '"':
		return stringEnd(b, i)
	case '{', '[':
		depth := 0
		for ; i < len(b); i++ {
			switch b[i] {
			case '"':
				i = stringEnd(b, i) - 1
			case '{', '[':
				depth++
			case '}', ']':
				depth--
				if depth == 0 {
					return i + 1
				}
			}
		}
		return len(b)
	default:
		for i < len(b) && b[i] != ',' && b[i] != '}' && b[i] != ']' && !isSpace(b[i]) {
			i++
		}
		return i
	}
}

func objectMembers(b []byte) ([]member, bool) {
	i := skipSpace(b, 0)
	if i >= len(b) || b[i] != '{' {
		return nil, false
	}
	var out []member
	for i = skipSpace(b, i+1); i < len(b) && b[i] != '}'; {
		keyEnd := stringEnd(b, i)
		var name string
		if err := json.Unmarshal(b[i:keyEnd], &name); err != nil {
			return nil, false
		}
		valStart := skipSpace(b, skipSpace(b, keyEnd)+1)
		valEnd := valueEnd(b, valStart)
		out = append(out, member{name: name, key: span{i, keyEnd}, val: span{valStart, valEnd}})
		i = skipSpace(b, valEnd)
		if i < len(b) && b[i] == ',' {
			i = skipSpace(b, i+1)
		}
	}
	return out, true
}

func arrayElements(b []byte) ([]span, bool) {
	i := skipSpace(b, 0)
	if i >= len(b) || b[i] != '[' {
		return nil, false
	}
	var out []span
	for i = skipSpace(b, i+1); i < len(b) && b[i] != ']'; {
		end := valueEnd(b, i)
		out = append(out, span{i, end})
		i = skipSpace(b, end)
		if i < len(b) && b[i] == ',' {
			i = skipSpace(b, i+1)
		}
	}
	return out, true
}

func memberValue(b []byte, name string) []byte {
	members, _ := objectMembers(b)
	for _, m := range members {
		if m.name == name {
			return b[m.val.start:m.val.end]
		}
	}
	return nil
}

func decodeString(raw []byte) (string, bool) {
	if len(raw) == 0 || raw[0] != '"' {
		return "", false
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return "", false
	}
	return s, true
}

func quote(s string) []byte {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(s); err != nil {
		panic(err)
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n"))
}

func splice(b []byte, edits []edit) []byte {
	out := make([]byte, 0, len(b))
	at := 0
	for _, e := range edits {
		out = append(out, b[at:e.at.start]...)
		out = append(out, e.repl...)
		at = e.at.end
	}
	return append(out, b[at:]...)
}
