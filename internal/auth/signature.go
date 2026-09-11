package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strconv"
	"time"
)

type AuthenticationErrorKind string

const (
	AuthenticationMalformed        AuthenticationErrorKind = "malformed_authentication"
	AuthenticationTimestampExpired AuthenticationErrorKind = "timestamp_expired"
	AuthenticationSignatureInvalid AuthenticationErrorKind = "invalid_signature"
)

type AuthenticationError struct {
	Kind AuthenticationErrorKind
}

func (err *AuthenticationError) Error() string {
	return string(err.Kind)
}

func (err *AuthenticationError) Is(target error) bool {
	other, ok := target.(*AuthenticationError)
	return ok && err.Kind == other.Kind
}

var (
	ErrMalformedAuthentication = &AuthenticationError{Kind: AuthenticationMalformed}
	ErrTimestampExpired        = &AuthenticationError{Kind: AuthenticationTimestampExpired}
	ErrInvalidSignature        = &AuthenticationError{Kind: AuthenticationSignatureInvalid}
)

func Sign(secret []byte, timestamp, method, requestTarget string, body []byte) string {
	bodyHash := sha256.Sum256(body)
	canonical := timestamp + "\n" + method + "\n" + requestTarget + "\n" + hex.EncodeToString(bodyHash[:])
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte(canonical))
	return hex.EncodeToString(mac.Sum(nil))
}

func Verify(secret []byte, timestamp, method, requestTarget, signature string, body []byte, now time.Time, maxSkew time.Duration) error {
	unixSeconds, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil || timestamp == "" || maxSkew < 0 {
		return ErrMalformedAuthentication
	}
	if len(signature) != sha256.Size*2 || signature != stringLower(signature) {
		return ErrMalformedAuthentication
	}
	provided, err := hex.DecodeString(signature)
	if err != nil || len(provided) != sha256.Size {
		return ErrMalformedAuthentication
	}

	deltaSeconds := now.Unix() - unixSeconds
	maxSkewSeconds := int64(maxSkew / time.Second)
	if deltaSeconds > maxSkewSeconds || deltaSeconds < -maxSkewSeconds {
		return ErrTimestampExpired
	}

	expected, err := hex.DecodeString(Sign(secret, timestamp, method, requestTarget, body))
	if err != nil {
		return errors.New("compute signature")
	}
	if !hmac.Equal(expected, provided) {
		return ErrInvalidSignature
	}
	return nil
}

func stringLower(value string) string {
	result := make([]byte, len(value))
	for index := range value {
		character := value[index]
		if character >= 'A' && character <= 'Z' {
			character += 'a' - 'A'
		}
		result[index] = character
	}
	return string(result)
}
