package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

const MaxTokenTTL = 15 * time.Minute

var (
	ErrInvalidToken = errors.New("invalid token")
	ErrTokenExpired = errors.New("token expired")
	ErrMissingScope = errors.New("missing required scope")
	ErrInvalidScope = errors.New("invalid scope")
	ErrInvalidTTL   = errors.New("invalid token ttl")
)

type Claims struct {
	Version   int
	AppID     string
	Scopes    []string
	IssuedAt  time.Time
	ExpiresAt time.Time
}

type TokenIssuer struct {
	secret []byte
	now    func() time.Time
}

type tokenClaims struct {
	Version   int      `json:"v"`
	AppID     string   `json:"app_id"`
	Scopes    []string `json:"scopes"`
	IssuedAt  int64    `json:"iat"`
	ExpiresAt int64    `json:"exp"`
}

func NewTokenIssuer(secret []byte, now func() time.Time) *TokenIssuer {
	secretCopy := append([]byte(nil), secret...)
	if now == nil {
		now = time.Now
	}
	return &TokenIssuer{secret: secretCopy, now: now}
}

func (issuer *TokenIssuer) Issue(appID string, scopes []string, ttl time.Duration) (string, error) {
	if appID == "" {
		return "", ErrInvalidToken
	}
	if ttl <= 0 || ttl > MaxTokenTTL || ttl%time.Second != 0 {
		return "", ErrInvalidTTL
	}
	if err := validateScopes(scopes); err != nil {
		return "", err
	}

	now := issuer.now().UTC().Truncate(time.Second)
	payload, err := json.Marshal(tokenClaims{
		Version:   1,
		AppID:     appID,
		Scopes:    append([]string(nil), scopes...),
		IssuedAt:  now.Unix(),
		ExpiresAt: now.Add(ttl).Unix(),
	})
	if err != nil {
		return "", ErrInvalidToken
	}
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"RHT"}`))
	encodedPayload := base64.RawURLEncoding.EncodeToString(payload)
	signingInput := header + "." + encodedPayload
	signature := issuer.tokenSignature(signingInput)
	return signingInput + "." + base64.RawURLEncoding.EncodeToString(signature), nil
}

func (issuer *TokenIssuer) Verify(token, requiredScope string) (Claims, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return Claims{}, ErrInvalidToken
	}
	providedSignature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil || len(providedSignature) != sha256.Size {
		return Claims{}, ErrInvalidToken
	}
	expectedSignature := issuer.tokenSignature(parts[0] + "." + parts[1])
	if !hmac.Equal(expectedSignature, providedSignature) {
		return Claims{}, ErrInvalidToken
	}

	header, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil || string(header) != `{"alg":"HS256","typ":"RHT"}` {
		return Claims{}, ErrInvalidToken
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return Claims{}, ErrInvalidToken
	}
	var encoded tokenClaims
	if err := json.Unmarshal(payload, &encoded); err != nil {
		return Claims{}, ErrInvalidToken
	}
	if encoded.Version != 1 || encoded.AppID == "" || encoded.ExpiresAt <= encoded.IssuedAt || encoded.ExpiresAt-encoded.IssuedAt > int64(MaxTokenTTL/time.Second) {
		return Claims{}, ErrInvalidToken
	}
	if err := validateScopes(encoded.Scopes); err != nil {
		return Claims{}, ErrInvalidToken
	}
	if issuer.now().Unix() >= encoded.ExpiresAt {
		return Claims{}, ErrTokenExpired
	}
	if requiredScope != "" && !containsScope(encoded.Scopes, requiredScope) {
		return Claims{}, ErrMissingScope
	}
	return Claims{
		Version:   encoded.Version,
		AppID:     encoded.AppID,
		Scopes:    append([]string(nil), encoded.Scopes...),
		IssuedAt:  time.Unix(encoded.IssuedAt, 0).UTC(),
		ExpiresAt: time.Unix(encoded.ExpiresAt, 0).UTC(),
	}, nil
}

func (issuer *TokenIssuer) tokenSignature(input string) []byte {
	mac := hmac.New(sha256.New, issuer.secret)
	_, _ = mac.Write([]byte(input))
	return mac.Sum(nil)
}

func validateScopes(scopes []string) error {
	if len(scopes) == 0 {
		return ErrInvalidScope
	}
	seen := make(map[string]struct{}, len(scopes))
	for _, scope := range scopes {
		if !validScope(scope) {
			return ErrInvalidScope
		}
		if _, exists := seen[scope]; exists {
			return ErrInvalidScope
		}
		seen[scope] = struct{}{}
	}
	return nil
}

func validScope(scope string) bool {
	parts := strings.Split(scope, ":")
	if len(parts) != 2 {
		return false
	}
	for _, part := range parts {
		if part == "" {
			return false
		}
		for index, character := range part {
			if character >= 'a' && character <= 'z' {
				continue
			}
			if index > 0 && character >= '0' && character <= '9' {
				continue
			}
			return false
		}
	}
	return true
}

func containsScope(scopes []string, required string) bool {
	for _, scope := range scopes {
		if scope == required {
			return true
		}
	}
	return false
}
