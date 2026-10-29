// Package relayhub is the experimental Rust v1 backend-only SDK, pending cutover.
package relayhub

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

type Headers [][2]string
type Body []byte
type EventInput struct {
	EndpointID string  `json:"endpoint_id"`
	EventType  string  `json:"event_type"`
	Method     string  `json:"method"`
	Path       string  `json:"path"`
	Headers    Headers `json:"headers"`
	Body       Body    `json:"body"`
}
type Accepted struct {
	EventID    string `json:"event_id"`
	DeliveryID string `json:"delivery_id"`
	Status     int    `json:"status"`
}
type DeliveryLease struct {
	DeliveryID   string `json:"delivery_id"`
	EventID      string `json:"event_id"`
	AttemptToken string `json:"attempt_token"`
	Generation   uint32 `json:"generation"`
	Attempt      uint8  `json:"attempt"`
}
type HTTPResponse struct {
	Status  int     `json:"status"`
	Headers Headers `json:"headers"`
	Body    Body    `json:"body"`
}
type Envelope struct {
	Version   int    `json:"version"`
	RequestID string `json:"request_id"`
	Type      string `json:"type"`
}

func (Envelope) isFrame() {}

type Frame interface{ isFrame() }
type Hello struct {
	Envelope
	AgentID     string `json:"agent_id"`
	MaxInFlight int    `json:"max_in_flight"`
}
type Delivery struct {
	Envelope
	Lease DeliveryLease `json:"lease"`
	Input EventInput    `json:"input"`
}
type Ack struct {
	Envelope
	EventID      string `json:"event_id"`
	DeliveryID   string `json:"delivery_id"`
	AttemptToken string `json:"attempt_token"`
}
type Response struct {
	Ack
	Response HTTPResponse `json:"response"`
}
type Heartbeat struct {
	Envelope
	Lease *DeliveryLease `json:"lease"`
}
type RealtimeFrame struct {
	Envelope
	Channel string `json:"channel"`
	Body    Body   `json:"body"`
}
type ProcessingResult struct{ Response *HTTPResponse }
type PublishReceipt struct {
	Status             int    `json:"status"`
	ObservedRecipients uint64 `json:"observed_recipients"`
}
type Options struct {
	AgentID     string
	MaxInFlight int
}
type Error struct {
	Status int
	Code   string
}

func (e *Error) Error() string { return e.Code }

const maxBody = 1_048_576
const maxFrame = 2_097_152

// Client holds backend credentials privately. Construct with NewClient; it is safe
// for concurrent use. Do not copy credentials into browser code or logs.
type Client struct {
	url         string
	token       string
	agentID     string
	maxInFlight int
	http        *http.Client
	// admissionHook is only used by package tests to force the check/dispatch boundary.
	admissionHook func()
}

func (c Client) Format(f fmt.State, verb rune) { fmt.Fprint(f, "relayhub.Client{redacted}") }

// NewClient requires HTTPS except for loopback development servers. Options has
// no mutable client state; zero MaxInFlight selects the protocol maximum of 16.
func NewClient(serverURL, credential string, options Options) (*Client, error) {
	u, err := url.Parse(serverURL)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || !(u.Scheme == "https" || (u.Scheme == "http" && (u.Hostname() == "localhost" || u.Hostname() == "127.0.0.1" || u.Hostname() == "::1"))) {
		return nil, invalid("invalid_url")
	}
	if credential == "" {
		return nil, invalid("invalid_credential")
	}
	for _, b := range []byte(credential) {
		if b < 33 || b > 126 {
			return nil, invalid("invalid_credential")
		}
	}
	if options.AgentID != "" && !validID(options.AgentID) {
		return nil, invalid("invalid_agent_id")
	}
	if options.MaxInFlight == 0 {
		options.MaxInFlight = 16
	}
	if options.MaxInFlight < 1 || options.MaxInFlight > 16 {
		return nil, invalid("invalid_max_in_flight")
	}
	return &Client{url: strings.TrimRight(u.String(), "/"), token: credential, agentID: options.AgentID, maxInFlight: options.MaxInFlight, http: &http.Client{Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}
func invalid(code string) *Error { return &Error{Status: 422, Code: code} }
func unavailable() *Error        { return &Error{Status: 503, Code: "transport_unavailable"} }

var idPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
var tokenPattern = regexp.MustCompile("^[!#$%&'*+.^_`|~0-9A-Za-z-]+$")
var channelPattern = regexp.MustCompile(`^[A-Za-z0-9_-]+(?:\.[A-Za-z0-9_-]+)*$`)

func validID(s string) bool { return idPattern.MatchString(s) }
func newID() string {
	var b [16]byte
	// crypto/rand.Read terminates on failure; it always fills the buffer.
	_, _ = rand.Read(b[:])
	b[6] = b[6]&15 | 64
	b[8] = b[8]&63 | 128
	s := hex.EncodeToString(b[:])
	return s[:8] + "-" + s[8:12] + "-" + s[12:16] + "-" + s[16:20] + "-" + s[20:]
}
func envelope(kind string) Envelope { return Envelope{Version: 1, RequestID: newID(), Type: kind} }
func validChannel(s string) error {
	if len(s) > 128 || !channelPattern.MatchString(s) {
		return invalid("invalid_channel")
	}
	return nil
}
func (b Body) MarshalJSON() ([]byte, error) {
	if len(b) > maxBody {
		return nil, &Error{413, "body_too_large"}
	}
	return json.Marshal(base64.StdEncoding.EncodeToString(b))
}
func (b *Body) UnmarshalJSON(data []byte) error {
	var s string
	if bytes.Equal(data, []byte("null")) || json.Unmarshal(data, &s) != nil {
		return invalid("invalid_body")
	}
	if len(s) > 1_398_104 {
		return &Error{413, "body_too_large"}
	}
	raw, err := base64.StdEncoding.Strict().DecodeString(s)
	if err != nil || base64.StdEncoding.EncodeToString(raw) != s {
		return invalid("invalid_body")
	}
	if len(raw) > maxBody {
		return &Error{413, "body_too_large"}
	}
	*b = raw
	return nil
}
func (h Headers) validate() error {
	for _, pair := range h {
		if !tokenPattern.MatchString(pair[0]) || !utf8.ValidString(pair[1]) {
			return invalid("invalid_headers")
		}
		for _, b := range []byte(pair[1]) {
			if (b < 32 && b != 9) || b == 127 {
				return invalid("invalid_headers")
			}
		}
	}
	return nil
}
func (h Headers) MarshalJSON() ([]byte, error) {
	if err := h.validate(); err != nil {
		return nil, err
	}
	if h == nil {
		return []byte("[]"), nil
	}
	return json.Marshal([][2]string(h))
}
func (h *Headers) UnmarshalJSON(data []byte) error {
	var pairs [][]*string
	if bytes.Equal(data, []byte("null")) || json.Unmarshal(data, &pairs) != nil {
		return invalid("invalid_headers")
	}
	result := make(Headers, 0, len(pairs))
	for _, p := range pairs {
		if len(p) != 2 || p[0] == nil || p[1] == nil {
			return invalid("invalid_headers")
		}
		result = append(result, [2]string{*p[0], *p[1]})
	}
	if err := result.validate(); err != nil {
		return err
	}
	*h = result
	return nil
}
func (v EventInput) validate() error {
	if len(v.Body) > maxBody {
		return &Error{413, "body_too_large"}
	}
	if !validID(v.EndpointID) || !utf8.ValidString(v.EventType) || !tokenPattern.MatchString(v.Method) || !strings.HasPrefix(v.Path, "/") {
		return invalid("invalid_input")
	}
	for _, b := range []byte(v.Path) {
		if b < 33 || b > 126 || b == '#' {
			return invalid("invalid_path")
		}
	}
	if !validPercentEscapes(v.Path) {
		return invalid("invalid_path")
	}
	if _, err := url.ParseRequestURI(v.Path); err != nil {
		return invalid("invalid_path")
	}
	return v.Headers.validate()
}

func validPercentEscapes(path string) bool {
	for i := 0; i < len(path); i++ {
		if path[i] != '%' {
			continue
		}
		if i+2 >= len(path) || !isHex(path[i+1]) || !isHex(path[i+2]) {
			return false
		}
		i += 2
	}
	return true
}

func isHex(b byte) bool {
	return b >= '0' && b <= '9' || b >= 'a' && b <= 'f' || b >= 'A' && b <= 'F'
}
func (l DeliveryLease) validate() error {
	if !validID(l.DeliveryID) || !validID(l.EventID) || !validID(l.AttemptToken) || l.Generation == 0 || l.Attempt < 1 || l.Attempt > 5 {
		return invalid("invalid_lease")
	}
	return nil
}
func (r HTTPResponse) validate() error {
	if len(r.Body) > maxBody {
		return &Error{413, "body_too_large"}
	}
	if r.Status < 100 || r.Status > 599 || r.Status == 101 {
		return invalid("invalid_response")
	}
	return r.Headers.validate()
}
func shape(data []byte, keys ...string) (map[string]json.RawMessage, error) {
	var fields map[string]json.RawMessage
	if json.Unmarshal(data, &fields) != nil || len(fields) != len(keys) {
		return nil, invalid("invalid_frame")
	}
	for _, k := range keys {
		v, ok := fields[k]
		if !ok || (k != "lease" && bytes.Equal(v, []byte("null"))) {
			return nil, invalid("invalid_frame")
		}
	}
	return fields, nil
}
func leaseShape(b []byte) error {
	_, err := shape(b, "delivery_id", "event_id", "attempt_token", "generation", "attempt")
	return err
}
func eventShape(b []byte) error {
	_, err := shape(b, "endpoint_id", "event_type", "method", "path", "headers", "body")
	return err
}
func responseShape(b []byte) error { _, err := shape(b, "status", "headers", "body"); return err }

// DecodeFrame validates the frozen JSON/base64 contract. Binary bodies and
// ordered duplicate headers remain lossless, including empty bodies.
func DecodeFrame(data []byte) (Frame, error) {
	if len(data) > maxFrame {
		return nil, &Error{413, "frame_too_large"}
	}
	if !utf8.Valid(data) {
		return nil, invalid("invalid_frame")
	}
	var base Envelope
	if json.Unmarshal(data, &base) != nil {
		return nil, invalid("invalid_frame")
	}
	if base.Version != 1 {
		return nil, invalid("unsupported_version")
	}
	if !validID(base.RequestID) {
		return nil, invalid("invalid_request_id")
	}
	keys := []string{"version", "request_id", "type"}
	var f Frame
	switch base.Type {
	case "hello":
		keys = append(keys, "agent_id", "max_in_flight")
		f = &Hello{}
	case "delivery":
		keys = append(keys, "lease", "input")
		f = &Delivery{}
	case "ack":
		keys = append(keys, "event_id", "delivery_id", "attempt_token")
		f = &Ack{}
	case "response":
		keys = append(keys, "event_id", "delivery_id", "attempt_token", "response")
		f = &Response{}
	case "heartbeat":
		keys = append(keys, "lease")
		f = &Heartbeat{}
	case "realtime":
		keys = append(keys, "channel", "body")
		f = &RealtimeFrame{}
	default:
		return nil, invalid("invalid_frame")
	}
	fields, err := shape(data, keys...)
	if err != nil {
		return nil, err
	}
	if b, ok := fields["lease"]; ok && (!bytes.Equal(b, []byte("null")) || base.Type == "delivery") {
		if err = leaseShape(b); err != nil {
			return nil, err
		}
	}
	if b, ok := fields["input"]; ok {
		if err = eventShape(b); err != nil {
			return nil, err
		}
	}
	if b, ok := fields["response"]; ok {
		if err = responseShape(b); err != nil {
			return nil, err
		}
	}
	if err = json.Unmarshal(data, f); err != nil {
		// JSON errors can include peer text. Preserve only our own static validation codes.
		var protocol *Error
		if errors.As(err, &protocol) {
			return nil, protocol
		}
		return nil, invalid("invalid_frame")
	}
	switch v := f.(type) {
	case *Hello:
		if !validID(v.AgentID) || v.MaxInFlight < 1 || v.MaxInFlight > 16 {
			err = invalid("invalid_hello")
		}
	case *Delivery:
		err = v.Lease.validate()
		if err == nil {
			err = v.Input.validate()
		}
	case *Ack:
		err = validateAck(*v)
	case *Response:
		err = validateAck(v.Ack)
		if err == nil {
			err = v.Response.validate()
		}
	case *Heartbeat:
		if v.Lease != nil {
			err = v.Lease.validate()
		}
	}
	if err != nil {
		return nil, err
	}
	return f, nil
}
func validateAck(v Ack) error {
	if !validID(v.EventID) || !validID(v.DeliveryID) || !validID(v.AttemptToken) {
		return invalid("invalid_correlation")
	}
	return nil
}
func EncodeFrame(frame Frame) ([]byte, error) {
	data, err := json.Marshal(frame)
	if err != nil {
		return nil, err
	}
	if _, err = DecodeFrame(data); err != nil {
		return nil, err
	}
	return data, nil
}
func (c *Client) post(ctx context.Context, path string, body []byte, key string) ([]byte, error) {
	if len(body) > maxFrame {
		return nil, &Error{413, "frame_too_large"}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url+path, bytes.NewReader(body))
	if err != nil {
		return nil, invalid("invalid_request")
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	response, err := c.http.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, unavailable()
	}
	defer response.Body.Close()
	if response.StatusCode != 202 {
		return nil, &Error{response.StatusCode, "request_rejected"}
	}
	b, err := io.ReadAll(io.LimitReader(response.Body, 4097))
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, unavailable()
	}
	if len(b) > 4096 {
		return nil, invalid("invalid_receipt")
	}
	return b, nil
}

// Publish returns durable acceptance, not processing success. On ambiguous HTTP
// failure, callers reuse the same key. The SDK never retries or persists events.
// The host must bind Events to POST /api/v1/events with Idempotency-Key.
func (c *Client) Publish(ctx context.Context, input EventInput, key string) (Accepted, error) {
	var accepted Accepted
	if key == "" || len(key) > 256 || strings.TrimSpace(key) != key || !utf8.ValidString(key) || strings.ContainsFunc(key, unicode.IsControl) {
		return accepted, invalid("invalid_idempotency_key")
	}
	if err := input.validate(); err != nil {
		return accepted, err
	}
	data, err := json.Marshal(input)
	if err != nil {
		return accepted, err
	}
	data, err = c.post(ctx, "/api/v1/events", data, key)
	if err != nil {
		return accepted, err
	}
	if _, err = shape(data, "event_id", "delivery_id", "status"); err != nil {
		return accepted, err
	}
	if json.Unmarshal(data, &accepted) != nil || accepted.Status != 202 || !validID(accepted.EventID) || !validID(accepted.DeliveryID) {
		return Accepted{}, invalid("invalid_receipt")
	}
	return accepted, nil
}

// PublishRealtime is ephemeral: observed recipients are not processing ACKs.
func (c *Client) PublishRealtime(ctx context.Context, channel string, body []byte) (PublishReceipt, error) {
	var receipt PublishReceipt
	if err := validChannel(channel); err != nil {
		return receipt, err
	}
	data, err := EncodeFrame(&RealtimeFrame{Envelope: envelope("realtime"), Channel: channel, Body: body})
	if err != nil {
		return receipt, err
	}
	data, err = c.post(ctx, "/api/v1/realtime", data, "")
	if err != nil {
		return receipt, err
	}
	if _, err = shape(data, "status", "observed_recipients"); err != nil {
		return receipt, err
	}
	if json.Unmarshal(data, &receipt) != nil || receipt.Status != 202 {
		return PublishReceipt{}, invalid("invalid_receipt")
	}
	return receipt, nil
}
