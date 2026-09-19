// Package realtime implements the RFC 6455 application protocol. Socket.IO is unsupported.
package realtime

import (
	"bytes"
	"encoding/json"
	"io"
	"regexp"
	"strings"

	"github.com/dungxbuif/RelayHub/internal/domain"
)

const MaxInboundBytes = 64 * 1024
const OutboundQueueSize = 64
const ProtocolV2 = "relayhub.realtime.v2"

const MaxChannelsPerFrame = 100
const MaxHistoryItems = 100
const MaxBatchPublishItems = 50
const MaxRewindChannels = 10

var realtimeClientIDPattern = regexp.MustCompile(`^[A-Za-z0-9_.:-]{1,128}$`)
var realtimeCursorPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)

type Audience struct {
	Type         string `json:"type"`
	ConnectionID string `json:"connection_id,omitempty"`
	ClientID     string `json:"client_id,omitempty"`
}

type HistoryRequest struct {
	Limit  int    `json:"limit"`
	Cursor string `json:"cursor,omitempty"`
}

type PublishItem struct {
	ID       string          `json:"id"`
	Channel  string          `json:"channel"`
	Audience *Audience       `json:"audience,omitempty"`
	Data     json.RawMessage `json:"data"`
}

type PublishOutcome struct {
	ID        string `json:"id"`
	Accepted  bool   `json:"accepted"`
	MessageID string `json:"message_id,omitempty"`
	Code      string `json:"code,omitempty"`
}

type ClientFrame struct {
	Type         string          `json:"type"`
	Topics       []string        `json:"topics,omitempty"`
	Channels     []string        `json:"channels,omitempty"`
	Channel      string          `json:"channel,omitempty"`
	Audience     *Audience       `json:"audience,omitempty"`
	Data         json.RawMessage `json:"data,omitempty"`
	Rewind       *HistoryRequest `json:"rewind,omitempty"`
	Limit        int             `json:"limit,omitempty"`
	Cursor       string          `json:"cursor,omitempty"`
	Items        []PublishItem   `json:"items,omitempty"`
	InvocationID string          `json:"invocation_id,omitempty"`
	OK           *bool           `json:"ok,omitempty"`
	Result       json.RawMessage `json:"result,omitempty"`
	Error        json.RawMessage `json:"error,omitempty"`
}
type EventPayload = domain.Event
type JobPayload = domain.Job
type ServerFrame struct {
	Type                  string           `json:"type"`
	Protocol              string           `json:"protocol,omitempty"`
	Capabilities          []string         `json:"capabilities,omitempty"`
	AppID                 string           `json:"app_id,omitempty"`
	ClientID              string           `json:"client_id,omitempty"`
	ConnectionID          string           `json:"connection_id,omitempty"`
	Topics                []string         `json:"topics,omitempty"`
	Channels              []string         `json:"channels,omitempty"`
	Channel               string           `json:"channel,omitempty"`
	PublisherAppID        string           `json:"publisher_app_id,omitempty"`
	PublisherClientID     string           `json:"publisher_client_id,omitempty"`
	PublisherConnectionID string           `json:"publisher_connection_id,omitempty"`
	MessageID             string           `json:"message_id,omitempty"`
	PublishedAt           string           `json:"published_at,omitempty"`
	Audience              *Audience        `json:"audience,omitempty"`
	Occupancy             int              `json:"occupancy,omitempty"`
	Data                  json.RawMessage  `json:"data,omitempty"`
	Cursor                string           `json:"cursor,omitempty"`
	NextCursor            string           `json:"next_cursor,omitempty"`
	ContinuityCursor      string           `json:"continuity_cursor,omitempty"`
	Items                 []ServerFrame    `json:"items,omitempty"`
	Outcomes              []PublishOutcome `json:"outcomes,omitempty"`
	Event                 *EventPayload    `json:"event,omitempty"`
	Job                   *JobPayload      `json:"job,omitempty"`
	Code                  string           `json:"code,omitempty"`
	Message               string           `json:"message,omitempty"`
	InvocationID          string           `json:"invocation_id,omitempty"`
	Function              string           `json:"function,omitempty"`
	Input                 json.RawMessage  `json:"input,omitempty"`
	Deadline              string           `json:"deadline,omitempty"`
}

// DecodeClientFrameV2 decodes only the version-negotiated channel protocol.
// Application and publisher identity fields are deliberately absent from the
// client contract so DisallowUnknownFields rejects identity spoofing.
func DecodeClientFrameV2(raw []byte) (ClientFrame, *ProtocolError) {
	var frame ClientFrame
	if len(raw) > MaxInboundBytes {
		return frame, protocolError("frame_too_large", "Frame exceeds the 64 KiB limit.")
	}
	if !json.Valid(raw) {
		return frame, protocolError("invalid_json", "Frame must contain one JSON object.")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&frame); err != nil {
		return frame, protocolError("invalid_frame", "Frame contains invalid fields.")
	}
	if decoder.Decode(new(any)) != io.EOF {
		return frame, protocolError("invalid_json", "Frame must contain one JSON object.")
	}
	switch frame.Type {
	case "subscribe":
		if err := validateChannels(frame.Channels); err != nil {
			return frame, err
		}
		if frame.Rewind != nil {
			if len(frame.Channels) > MaxRewindChannels {
				return frame, protocolError("invalid_history", "Rewind supports at most 10 channels per subscribe frame.")
			}
			if err := validateHistoryRequest(*frame.Rewind); err != nil {
				return frame, err
			}
		}
	case "unsubscribe":
		if err := validateChannels(frame.Channels); err != nil {
			return frame, err
		}
		if frame.Rewind != nil {
			return frame, protocolError("invalid_history", "Rewind is only valid when subscribing.")
		}
	case "channel.publish":
		if !domain.ValidRealtimeChannel(frame.Channel) || !domain.JSONObject(frame.Data) {
			return frame, protocolError("invalid_publish", "Publish requires a valid channel and JSON object data.")
		}
		if err := validateAudience(frame.Audience); err != nil {
			return frame, err
		}
	case "channel.publish.batch":
		if err := validatePublishItems(frame.Items); err != nil {
			return frame, err
		}
	case "history.get":
		if !domain.ValidRealtimeChannel(frame.Channel) {
			return frame, protocolError("invalid_history", "History requires a valid channel, limit, and optional cursor.")
		}
		if err := validateHistoryRequest(HistoryRequest{Limit: frame.Limit, Cursor: frame.Cursor}); err != nil {
			return frame, err
		}
	case "presence.update":
		if !domain.ValidRealtimeChannel(frame.Channel) || !domain.JSONObject(frame.Data) {
			return frame, protocolError("invalid_presence", "Presence requires a valid channel and JSON object data.")
		}
	case "ping":
	case "":
		return frame, protocolError("invalid_frame", "Frame type is required.")
	default:
		return frame, protocolError("unknown_type", "Frame type is not supported.")
	}
	return frame, nil
}

func validateHistoryRequest(request HistoryRequest) *ProtocolError {
	if request.Limit < 1 || request.Limit > MaxHistoryItems || request.Cursor != "" && !realtimeCursorPattern.MatchString(request.Cursor) {
		return protocolError("invalid_history", "History requires a limit between 1 and 100 and a valid cursor.")
	}
	return nil
}

func validatePublishItems(items []PublishItem) *ProtocolError {
	if len(items) < 1 || len(items) > MaxBatchPublishItems {
		return protocolError("invalid_batch", "Batch publish requires between 1 and 50 items.")
	}
	seen := make(map[string]bool, len(items))
	for _, item := range items {
		if !realtimeClientIDPattern.MatchString(item.ID) || seen[item.ID] || !domain.ValidRealtimeChannel(item.Channel) || !domain.JSONObject(item.Data) {
			return protocolError("invalid_batch", "Batch items require unique IDs, valid channels, audiences, and JSON object data.")
		}
		if err := validateAudience(item.Audience); err != nil {
			return protocolError("invalid_batch", "Batch items require unique IDs, valid channels, audiences, and JSON object data.")
		}
		seen[item.ID] = true
	}
	return nil
}

func validateChannels(channels []string) *ProtocolError {
	if len(channels) == 0 || len(channels) > MaxChannelsPerFrame {
		return protocolError("invalid_channels", "Supply between 1 and 100 exact channels without duplicates.")
	}
	seen := make(map[string]bool, len(channels))
	for _, channel := range channels {
		if !domain.ValidRealtimeChannel(channel) || seen[channel] {
			return protocolError("invalid_channels", "Supply between 1 and 100 exact channels without duplicates.")
		}
		seen[channel] = true
	}
	return nil
}

func validateAudience(audience *Audience) *ProtocolError {
	if audience == nil {
		return nil
	}
	switch audience.Type {
	case "all", "others":
		if audience.ConnectionID != "" || audience.ClientID != "" {
			return protocolError("invalid_audience", "Audience target fields must match its type.")
		}
	case "connection":
		if !strings.HasPrefix(audience.ConnectionID, "conn_") || audience.ClientID != "" {
			return protocolError("invalid_audience", "A connection audience requires connection_id.")
		}
	case "client":
		if !realtimeClientIDPattern.MatchString(audience.ClientID) || audience.ConnectionID != "" {
			return protocolError("invalid_audience", "A client audience requires client_id.")
		}
	default:
		return protocolError("invalid_audience", "Audience type must be all, others, connection, or client.")
	}
	return nil
}

type ProtocolError struct {
	Code    string
	Message string
}

func (e *ProtocolError) Error() string { return e.Code + ": " + e.Message }
func protocolError(code, message string) *ProtocolError {
	return &ProtocolError{Code: code, Message: message}
}
func ErrorFrame(e *ProtocolError) ServerFrame {
	return ServerFrame{Type: "error", Code: e.Code, Message: e.Message}
}
func DecodeClientFrame(raw []byte) (ClientFrame, *ProtocolError) {
	var frame ClientFrame
	if len(raw) > MaxInboundBytes {
		return frame, protocolError("frame_too_large", "Frame exceeds the 64 KiB limit.")
	}
	if !json.Valid(raw) {
		return frame, protocolError("invalid_json", "Frame must contain one JSON object.")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&frame); err != nil {
		return frame, protocolError("invalid_frame", "Frame contains invalid fields.")
	}
	if decoder.Decode(new(any)) != io.EOF {
		return frame, protocolError("invalid_json", "Frame must contain one JSON object.")
	}
	switch frame.Type {
	case "subscribe":
		if err := validateTopics(frame.Topics); err != nil {
			return frame, err
		}
	case "ping":
	case "rpc.result":
		if frame.OK == nil || !domain.ValidRPCResult(domain.RPCResult{InvocationID: frame.InvocationID, OK: frame.OK != nil && *frame.OK, Result: frame.Result, Error: frame.Error}) {
			return frame, protocolError("invalid_rpc_result", "RPC result requires invocation_id, ok, and result or a code/message error object.")
		}
	case "":
		return frame, protocolError("invalid_frame", "Frame type is required.")
	default:
		return frame, protocolError("unknown_type", "Frame type is not supported.")
	}
	return frame, nil
}
func validateTopics(topics []string) *ProtocolError {
	if len(topics) == 0 {
		return protocolError("invalid_topics", "Supply events, jobs, functions or channel:<name> topics without duplicates.")
	}
	seen := map[string]bool{}
	for _, topic := range topics {
		channel, ok := strings.CutPrefix(topic, "channel:")
		if (topic != "events" && topic != "jobs" && topic != "functions" && (!ok || !domain.ValidRealtimeChannel(channel))) || seen[topic] {
			return protocolError("invalid_topics", "Supply events, jobs, functions or channel:<name> topics without duplicates.")
		}
		seen[topic] = true
	}
	return nil
}
