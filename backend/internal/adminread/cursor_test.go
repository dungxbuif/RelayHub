package adminread

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestCursorRoundTripAndFilterBinding(t *testing.T) {
	t.Parallel()
	stamp := time.Date(2026, 9, 20, 3, 4, 5, 600_000, time.UTC)
	fingerprint := FilterFingerprint("type=invoice.created", "source=app_1")
	raw, err := EncodeCursor(Cursor{Timestamp: stamp, ID: "evt_01", FilterFingerprint: fingerprint})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeCursor(raw, fingerprint)
	if err != nil {
		t.Fatal(err)
	}
	if !decoded.Timestamp.Equal(stamp) || decoded.ID != "evt_01" || decoded.Version != CursorVersion {
		t.Fatalf("decoded cursor = %#v", decoded)
	}
	if _, err := DecodeCursor(raw, FilterFingerprint("type=other")); !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("filter mismatch error = %v", err)
	}
}

func TestCursorRejectsMalformedOversizedAndWrongVersion(t *testing.T) {
	t.Parallel()
	for name, raw := range map[string]string{
		"malformed":     "%%%",
		"oversized":     strings.Repeat("a", MaxCursorLength+1),
		"wrong version": "eyJ2Ijo5LCJ0IjoiMjAyNi0wOS0yMFQwMzowNDowNS4wMDAwMDBaIiwiaWQiOiJldnRfMSIsImYiOiJ4In0",
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeCursor(raw, "x"); !errors.Is(err, ErrInvalidCursor) {
				t.Fatalf("DecodeCursor error = %v", err)
			}
		})
	}
}

func TestListOptionsValidation(t *testing.T) {
	t.Parallel()
	for _, limit := range []int{-1, 101} {
		if err := (ListOptions{Limit: limit}).Validate(); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("limit %d error = %v", limit, err)
		}
	}
	if got := (ListOptions{}).NormalizedLimit(); got != DefaultPageSize {
		t.Fatalf("default limit = %d", got)
	}
	if got := (ListOptions{Limit: 100}).NormalizedLimit(); got != 100 {
		t.Fatalf("explicit limit = %d", got)
	}
}

func TestCursorRejectsInvalidEncodeInput(t *testing.T) {
	t.Parallel()
	for _, cursor := range []Cursor{
		{},
		{Timestamp: time.Now(), ID: strings.Repeat("x", 257), FilterFingerprint: "f"},
		{Timestamp: time.Now(), ID: "evt_1", FilterFingerprint: ""},
	} {
		if _, err := EncodeCursor(cursor); !errors.Is(err, ErrInvalidCursor) {
			t.Fatalf("EncodeCursor(%#v) error = %v", cursor, err)
		}
	}
}
