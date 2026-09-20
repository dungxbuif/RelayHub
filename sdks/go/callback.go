package relayhub

import (
	"crypto/hmac"
	"strconv"
	"time"
)

func VerifyCallbackSignature(secret, timestamp, requestTarget string, body []byte, signature string, now time.Time, maxSkew time.Duration) bool {
	seconds, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil || maxSkew <= 0 {
		return false
	}
	at := time.Unix(seconds, 0)
	delta := now.Sub(at)
	if delta < 0 {
		delta = -delta
	}
	if delta > maxSkew {
		return false
	}
	expected := sign(secret, timestamp, "POST", requestTarget, body)
	return hmac.Equal([]byte(expected), []byte(signature))
}
