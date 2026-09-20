// Package relayhub provides context-aware HTTP and WebSocket clients for RelayHub v1.
package relayhub

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/gorilla/websocket"
)

var ErrInvalidInput = errors.New("relayhub: invalid input")
var ErrClosed = errors.New("relayhub: client is closed")
var ErrProtocol = errors.New("relayhub: invalid server protocol")

const maxHTTPBody = 1 << 20
const maxResponse = 2 << 20
const maxFrame = 64 << 10

// Config belongs to a trusted backend. TokenProvider is called afresh on each
// connection attempt; when nil the client signs a socket-token request.
type Config struct {
	BaseURL, APIKey, HMACSecret    string
	AdminToken                     string
	HTTPClient                     *http.Client
	Dialer                         *websocket.Dialer
	TokenProvider                  func(context.Context, string) (string, error)
	RealtimeTokenProvider          func(context.Context, RealtimeTokenRequest) (string, error)
	ReconnectMin, ReconnectMax     time.Duration
	HandshakeTimeout, WriteTimeout time.Duration
}

type Client struct {
	base   *url.URL
	config Config
	http   *http.Client
	dialer websocket.Dialer
}

func New(config Config) (*Client, error) {
	base, err := url.Parse(config.BaseURL)
	if err != nil || base.Host == "" || (base.Scheme != "http" && base.Scheme != "https") || base.User != nil || base.RawQuery != "" || base.Fragment != "" || (base.Path != "" && base.Path != "/") {
		return nil, ErrInvalidInput
	}
	if (config.APIKey == "") != (config.HMACSecret == "") || config.APIKey == "" && config.TokenProvider == nil && config.AdminToken == "" {
		return nil, ErrInvalidInput
	}
	if strings.ContainsAny(config.APIKey, "\r\n") || strings.ContainsAny(config.AdminToken, "\r\n") || !utf8.ValidString(config.HMACSecret) {
		return nil, ErrInvalidInput
	}
	if config.ReconnectMin == 0 {
		config.ReconnectMin = 100 * time.Millisecond
	}
	if config.ReconnectMax == 0 {
		config.ReconnectMax = 5 * time.Second
	}
	if config.HandshakeTimeout == 0 {
		config.HandshakeTimeout = 10 * time.Second
	}
	if config.WriteTimeout == 0 {
		config.WriteTimeout = 5 * time.Second
	}
	if config.ReconnectMin < time.Millisecond || config.ReconnectMax < config.ReconnectMin || config.ReconnectMax > time.Minute || config.HandshakeTimeout <= 0 || config.WriteTimeout <= 0 {
		return nil, ErrInvalidInput
	}
	httpClient := http.Client{Timeout: 35 * time.Second}
	if config.HTTPClient != nil {
		httpClient = *config.HTTPClient
	}
	httpClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	dialer := *websocket.DefaultDialer
	if config.Dialer != nil {
		dialer = *config.Dialer
	}
	dialer.HandshakeTimeout = config.HandshakeTimeout
	return &Client{base: base, config: config, http: &httpClient, dialer: dialer}, nil
}

type APIError struct {
	StatusCode int
	Code       string
	Replayed   bool
}

func (e *APIError) Error() string   { return fmt.Sprintf("relayhub: HTTP %d (%s)", e.StatusCode, e.Code) }
func (e *APIError) Retryable() bool { return e.StatusCode == 429 || e.StatusCode >= 500 }

type transportError struct{ cause error }

func (e *transportError) Error() string { return "relayhub: transport failed" }
func (e *transportError) Unwrap() error { return e.cause }
func safeTransport(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return &transportError{err}
}

func sign(secret, timestamp, method, target string, body []byte) string {
	digest := sha256.Sum256(body)
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(timestamp + "\n" + method + "\n" + target + "\n" + hex.EncodeToString(digest[:])))
	return hex.EncodeToString(mac.Sum(nil))
}

var safeCode = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)

func (c *Client) request(ctx context.Context, method, path string, input any, key string, result any) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if c.config.APIKey == "" {
		return false, ErrInvalidInput
	}
	var body []byte
	var err error
	if input != nil {
		body, err = json.Marshal(input)
		if err != nil || len(body) > maxHTTPBody {
			return false, ErrInvalidInput
		}
	}
	u := *c.base
	target, err := url.ParseRequestURI(path)
	if err != nil || target.IsAbs() || !strings.HasPrefix(target.Path, "/") {
		return false, ErrInvalidInput
	}
	u.Path, u.RawPath, u.RawQuery = target.Path, target.RawPath, target.RawQuery
	req, err := http.NewRequestWithContext(ctx, method, u.String(), bytes.NewReader(body))
	if err != nil {
		return false, ErrInvalidInput
	}
	timestamp := strconv.FormatInt(time.Now().Unix(), 10)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-RelayHub-Api-Key", c.config.APIKey)
	req.Header.Set("X-RelayHub-Timestamp", timestamp)
	req.Header.Set("X-RelayHub-Signature", sign(c.config.HMACSecret, timestamp, method, req.URL.RequestURI(), body))
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	response, err := c.http.Do(req)
	if err != nil {
		return false, safeTransport(ctx, err)
	}
	defer response.Body.Close()
	replay := response.Header.Get("Idempotent-Replayed") == "true"
	raw, err := io.ReadAll(io.LimitReader(response.Body, maxResponse+1))
	if err != nil {
		return replay, safeTransport(ctx, err)
	}
	if len(raw) > maxResponse {
		return replay, ErrProtocol
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		var envelope struct {
			Error struct {
				Code string `json:"code"`
			} `json:"error"`
		}
		_ = json.Unmarshal(raw, &envelope)
		code := envelope.Error.Code
		if !safeCode.MatchString(code) {
			code = "request_failed"
		}
		return replay, &APIError{StatusCode: response.StatusCode, Code: code, Replayed: replay}
	}
	if result != nil && (!utf8.Valid(raw) || json.Unmarshal(raw, result) != nil) {
		return replay, ErrProtocol
	}
	return replay, nil
}

func (c *Client) adminRequest(ctx context.Context, method, path string, input any, result any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if c.config.AdminToken == "" {
		return ErrInvalidInput
	}
	var body []byte
	var err error
	if input != nil {
		body, err = json.Marshal(input)
		if err != nil || len(body) > maxHTTPBody {
			return ErrInvalidInput
		}
	}
	u := *c.base
	target, err := url.ParseRequestURI(path)
	if err != nil || target.IsAbs() || !strings.HasPrefix(target.Path, "/") {
		return ErrInvalidInput
	}
	u.Path, u.RawPath, u.RawQuery = target.Path, target.RawPath, target.RawQuery
	req, err := http.NewRequestWithContext(ctx, method, u.String(), bytes.NewReader(body))
	if err != nil {
		return ErrInvalidInput
	}
	req.Header.Set("Authorization", "Bearer "+c.config.AdminToken)
	if input != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	response, err := c.http.Do(req)
	if err != nil {
		return safeTransport(ctx, err)
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(response.Body, maxResponse+1))
	if err != nil {
		return safeTransport(ctx, err)
	}
	if len(raw) > maxResponse {
		return ErrProtocol
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		var envelope struct {
			Error struct {
				Code string `json:"code"`
			} `json:"error"`
		}
		_ = json.Unmarshal(raw, &envelope)
		code := envelope.Error.Code
		if !safeCode.MatchString(code) {
			code = "request_failed"
		}
		return &APIError{StatusCode: response.StatusCode, Code: code}
	}
	if result != nil && (!utf8.Valid(raw) || json.Unmarshal(raw, result) != nil) {
		return ErrProtocol
	}
	return nil
}

func (c *Client) socketToken(ctx context.Context, scope string) (string, error) {
	if c.config.TokenProvider != nil {
		token, err := c.config.TokenProvider(ctx, scope)
		if err != nil {
			return "", safeTransport(ctx, err)
		}
		if token == "" {
			return "", ErrInvalidInput
		}
		return token, nil
	}
	var result struct {
		Token string `json:"token"`
	}
	_, err := c.request(ctx, "POST", "/api/v1/socket/token", struct {
		Scopes []string `json:"scopes"`
		TTL    int      `json:"ttl_seconds"`
	}{[]string{scope}, 600}, "", &result)
	if err == nil && result.Token == "" {
		err = ErrProtocol
	}
	return result.Token, err
}

func object(raw json.RawMessage) bool {
	raw = bytes.TrimSpace(raw)
	return len(raw) > 0 && raw[0] == '{' && utf8.Valid(raw) && json.Valid(raw)
}

type callOptions struct{ key string }
type CallOption func(*callOptions)

func IdempotencyKey(key string) CallOption { return func(o *callOptions) { o.key = key } }
func callKey(options []CallOption) (string, error) {
	o := callOptions{}
	for _, option := range options {
		if option == nil {
			return "", ErrInvalidInput
		}
		option(&o)
	}
	if strings.TrimSpace(o.key) == "" || len(o.key) > 256 || strings.ContainsAny(o.key, "\r\n") {
		return "", ErrInvalidInput
	}
	return o.key, nil
}
