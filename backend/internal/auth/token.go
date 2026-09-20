package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/dungxbuif/RelayHub/internal/domain"
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
	ClientID  string
	Channels  map[string][]string
}

type TokenIssuer struct {
	secret []byte
	now    func() time.Time
}

type tokenClaims struct {
	Version   int                 `json:"v"`
	AppID     string              `json:"app_id"`
	Scopes    []string            `json:"scopes"`
	IssuedAt  int64               `json:"iat"`
	ExpiresAt int64               `json:"exp"`
	ClientID  string              `json:"client_id,omitempty"`
	Channels  map[string][]string `json:"channels,omitempty"`
}

func NewTokenIssuer(secret []byte, now func() time.Time) *TokenIssuer {
	secretCopy := append([]byte(nil), secret...)
	if now == nil {
		now = time.Now
	}
	return &TokenIssuer{secret: secretCopy, now: now}
}

func (issuer *TokenIssuer) Issue(appID string, scopes []string, ttl time.Duration) (string, error) {
	return issuer.issue(appID, scopes, "", nil, ttl)
}

func (issuer *TokenIssuer) IssueRealtime(appID, clientID string, channels map[string][]string, ttl time.Duration) (string, error) {
	if !validClientID(clientID) || validateChannelCapabilities(channels) != nil {
		return "", ErrInvalidToken
	}
	return issuer.issue(appID, []string{"ws:connect"}, clientID, channels, ttl)
}

func (issuer *TokenIssuer) issue(appID string, scopes []string, clientID string, channels map[string][]string, ttl time.Duration) (string, error) {
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
		ClientID:  clientID,
		Channels:  copyCapabilities(channels),
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
	if (encoded.ClientID == "") != (len(encoded.Channels) == 0) || encoded.ClientID != "" && (!validClientID(encoded.ClientID) || validateChannelCapabilities(encoded.Channels) != nil) {
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
		ClientID:  encoded.ClientID,
		Channels:  copyCapabilities(encoded.Channels),
	}, nil
}

func validClientID(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for _, character := range value {
		if (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') || (character >= '0' && character <= '9') || strings.ContainsRune("_.:-", character) {
			continue
		}
		return false
	}
	return true
}

func validateChannelCapabilities(channels map[string][]string) error {
	if len(channels) == 0 || len(channels) > 100 {
		return ErrInvalidScope
	}
	allowed := map[string]bool{"subscribe": true, "publish": true, "presence": true, "history": true, "annotate": true, "file.publish": true, "push.manage": true}
	for channel, actions := range channels {
		if !domain.ValidRealtimeChannelGrant(channel) || len(actions) == 0 || len(actions) > len(allowed) {
			return ErrInvalidScope
		}
		seen := map[string]bool{}
		for _, action := range actions {
			if !allowed[action] || seen[action] {
				return ErrInvalidScope
			}
			seen[action] = true
		}
	}
	return nil
}

func copyCapabilities(channels map[string][]string) map[string][]string {
	if len(channels) == 0 {
		return nil
	}
	result := make(map[string][]string, len(channels))
	for channel, actions := range channels {
		result[channel] = append([]string(nil), actions...)
	}
	return result
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
