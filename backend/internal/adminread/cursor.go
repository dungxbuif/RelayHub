package adminread

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"time"
)

const (
	CursorVersion   = 1
	MaxCursorLength = 512
	DefaultPageSize = 25
	MaxPageSize     = 100
)

var (
	ErrInvalidCursor   = errors.New("invalid cursor")
	ErrInvalidArgument = errors.New("invalid argument")
)

type Cursor struct {
	Version           int
	Timestamp         time.Time
	ID                string
	FilterFingerprint string
}

type cursorEnvelope struct {
	Version int    `json:"v"`
	AtUS    int64  `json:"at_us"`
	ID      string `json:"id"`
	Filter  string `json:"filter"`
}

func FilterFingerprint(parts ...string) string {
	hash := sha256.New()
	for _, part := range parts {
		_, _ = io.WriteString(hash, part)
		_, _ = hash.Write([]byte{0})
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func EncodeCursor(cursor Cursor) (string, error) {
	if cursor.Timestamp.IsZero() || cursor.ID == "" || len(cursor.ID) > 256 ||
		len(cursor.FilterFingerprint) != sha256.Size*2 || strings.TrimSpace(cursor.ID) != cursor.ID {
		return "", ErrInvalidCursor
	}
	envelope := cursorEnvelope{Version: CursorVersion, AtUS: cursor.Timestamp.UTC().UnixMicro(), ID: cursor.ID, Filter: cursor.FilterFingerprint}
	raw, err := json.Marshal(envelope)
	if err != nil {
		return "", ErrInvalidCursor
	}
	encoded := base64.RawURLEncoding.EncodeToString(raw)
	if len(encoded) > MaxCursorLength {
		return "", ErrInvalidCursor
	}
	return encoded, nil
}

func DecodeCursor(raw, expectedFingerprint string) (Cursor, error) {
	if raw == "" || len(raw) > MaxCursorLength || len(expectedFingerprint) != sha256.Size*2 {
		return Cursor{}, ErrInvalidCursor
	}
	decoded, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil || len(decoded) > MaxCursorLength {
		return Cursor{}, ErrInvalidCursor
	}
	decoder := json.NewDecoder(bytes.NewReader(decoded))
	decoder.DisallowUnknownFields()
	var envelope cursorEnvelope
	if err := decoder.Decode(&envelope); err != nil {
		return Cursor{}, ErrInvalidCursor
	}
	if err := ensureJSONEOF(decoder); err != nil || envelope.Version != CursorVersion || envelope.AtUS <= 0 ||
		envelope.ID == "" || len(envelope.ID) > 256 || strings.TrimSpace(envelope.ID) != envelope.ID || envelope.Filter != expectedFingerprint {
		return Cursor{}, ErrInvalidCursor
	}
	return Cursor{Version: envelope.Version, Timestamp: time.UnixMicro(envelope.AtUS).UTC(), ID: envelope.ID, FilterFingerprint: envelope.Filter}, nil
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var trailing any
	err := decoder.Decode(&trailing)
	if errors.Is(err, io.EOF) {
		return nil
	}
	return ErrInvalidCursor
}
