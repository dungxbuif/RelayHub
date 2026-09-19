// Package realtime implements the RFC 6455 application protocol. Socket.IO is unsupported.
package realtime

import (
	"bytes"
	"encoding/json"
	"io"
	"strings"

	"github.com/dungxbuif/RelayHub/internal/domain"
)

const MaxInboundBytes = 64 * 1024
const OutboundQueueSize = 64

type ClientFrame struct {
	Type         string          `json:"type"`
	Topics       []string        `json:"topics,omitempty"`
	InvocationID string          `json:"invocation_id,omitempty"`
	OK           *bool           `json:"ok,omitempty"`
	Result       json.RawMessage `json:"result,omitempty"`
	Error        json.RawMessage `json:"error,omitempty"`
}
type EventPayload = domain.Event
type JobPayload = domain.Job
type ServerFrame struct {
	Type           string          `json:"type"`
	AppID          string          `json:"app_id,omitempty"`
	ConnectionID   string          `json:"connection_id,omitempty"`
	Topics         []string        `json:"topics,omitempty"`
	Channel        string          `json:"channel,omitempty"`
	PublisherAppID string          `json:"publisher_app_id,omitempty"`
	Data           json.RawMessage `json:"data,omitempty"`
	Event          *EventPayload   `json:"event,omitempty"`
	Job            *JobPayload     `json:"job,omitempty"`
	Code           string          `json:"code,omitempty"`
	Message        string          `json:"message,omitempty"`
	InvocationID   string          `json:"invocation_id,omitempty"`
	Function       string          `json:"function,omitempty"`
	Input          json.RawMessage `json:"input,omitempty"`
	Deadline       string          `json:"deadline,omitempty"`
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
