// Package streamprotocol defines RelayHub's broker-independent v1 durable stream
// wire contract. It deliberately contains no NATS subject or sequence fields.
package streamprotocol

import (
	"bytes"
	"encoding/json"
	"io"
	"time"
	"unicode/utf8"

	"github.com/dungxbuif/RelayHub/internal/domain"
)

const (
	Version         = 1
	Subprotocol     = "relayhub.stream.v1"
	DefaultConsumer = "default"
	MaxMessageBytes = 64 * 1024

	CloseProtocolViolation     = 4400
	CloseAuthenticationFailed  = 4401
	CloseForbidden             = 4403
	CloseUnsupportedVersion    = 4406
	CloseTimeout               = 4408
	CloseBackpressure          = 4429
	CloseDependencyUnavailable = 4503
)

var stableErrors = map[string]bool{
	"invalid_utf8": true, "frame_too_large": true, "invalid_json": true,
	"duplicate_key": true, "invalid_frame": true, "unknown_type": true,
	"unsupported_version": true, "delivery_not_assigned": true,
	"stale_delivery": true, "consumer_already_started": true,
	"consumer_unavailable": true, "backpressure": true,
	"function_not_assigned": true, "internal_error": true,
}

type ProtocolError struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
}

func (e *ProtocolError) Error() string { return e.Code + ": " + e.Message }
func protocolError(code, message string, retryable bool) *ProtocolError {
	return &ProtocolError{Code: code, Message: message, Retryable: retryable}
}

func StableErrorCode(code string) bool { return stableErrors[code] }

func CloseCodeFor(err *ProtocolError) int {
	if err == nil {
		return 1000
	}
	switch err.Code {
	case "frame_too_large":
		return 1009
	case "unsupported_version":
		return CloseUnsupportedVersion
	case "backpressure":
		return CloseBackpressure
	case "consumer_unavailable", "internal_error":
		return CloseDependencyUnavailable
	default:
		return CloseProtocolViolation
	}
}

type ClientFrame struct {
	Type            string
	ProtocolVersion int
	Consumer        string
	Topics          []string
	MaxInFlight     int
	DeliveryID      string
	DelayMS         int
	InvocationID    string
	OK              *bool
	Result          json.RawMessage
	Error           json.RawMessage
}

type Assignment struct {
	AppID        string
	ConnectionID string
}

type InvocationAssignment struct {
	AppID        string
	ConnectionID string
}

func DecodeClientFrame(raw []byte) (ClientFrame, *ProtocolError) {
	var frame ClientFrame
	object, err := decodeObject(raw)
	if err != nil {
		return frame, err
	}
	typeName, ok := stringField(object, "type")
	if !ok || typeName == "" {
		return frame, invalidFrame()
	}
	frame.Type = typeName
	switch typeName {
	case "consumer.start":
		if !only(object, "type", "protocol_version", "consumer", "max_in_flight") {
			return frame, invalidFrame()
		}
		version, vok := intField(object, "protocol_version")
		if vok && version != Version {
			return frame, protocolError("unsupported_version", "Protocol version is not supported.", false)
		}
		consumer, cok := stringField(object, "consumer")
		max, mok := intField(object, "max_in_flight")
		if !vok || !cok || consumer != DefaultConsumer || !mok || max < 1 || max > 256 {
			return frame, invalidFrame()
		}
		frame.ProtocolVersion, frame.Consumer, frame.MaxInFlight = version, consumer, max
	case "delivery.ack", "delivery.progress":
		if !only(object, "type", "delivery_id") {
			return frame, invalidFrame()
		}
		id, ok := stringField(object, "delivery_id")
		if !ok || !bounded(id, 128) {
			return frame, invalidFrame()
		}
		frame.DeliveryID = id
	case "delivery.nack":
		if !only(object, "type", "delivery_id", "delay_ms") {
			return frame, invalidFrame()
		}
		id, iok := stringField(object, "delivery_id")
		delay, dok := intField(object, "delay_ms")
		if !iok || !bounded(id, 128) || !dok || delay < 0 || delay > 300000 {
			return frame, invalidFrame()
		}
		frame.DeliveryID, frame.DelayMS = id, delay
	case "function.result":
		if !only(object, "type", "invocation_id", "ok", "result", "error") {
			return frame, invalidFrame()
		}
		id, iok := stringField(object, "invocation_id")
		okValue, ook := boolField(object, "ok")
		if !iok || !bounded(id, 128) || !ook {
			return frame, invalidFrame()
		}
		frame.InvocationID, frame.OK = id, &okValue
		result, hasResult := object["result"]
		errorValue, hasError := object["error"]
		if okValue {
			if !hasResult || hasError || !validJSONValue(result) {
				return frame, invalidFrame()
			}
			frame.Result = clone(result)
		} else {
			if hasResult || !hasError || !validHandlerError(errorValue) {
				return frame, invalidFrame()
			}
			frame.Error = clone(errorValue)
		}
	case "ping":
		if !only(object, "type") {
			return frame, invalidFrame()
		}
	default:
		return frame, protocolError("unknown_type", "Frame type is not supported.", false)
	}
	return frame, nil
}

func ValidateAssignment(frame ClientFrame, appID, connectionID string, assignments map[string]Assignment) *ProtocolError {
	if frame.DeliveryID == "" {
		return invalidFrame()
	}
	assignment, exists := assignments[frame.DeliveryID]
	if !exists || assignment.AppID != appID {
		return protocolError("delivery_not_assigned", "Delivery is not assigned to this connection.", false)
	}
	if assignment.ConnectionID != connectionID {
		return protocolError("stale_delivery", "Delivery assignment is no longer current.", true)
	}
	return nil
}

func ValidateInvocationAssignment(frame ClientFrame, appID, connectionID string, assignments map[string]InvocationAssignment) *ProtocolError {
	if frame.InvocationID == "" {
		return invalidFrame()
	}
	assignment, exists := assignments[frame.InvocationID]
	if !exists || assignment.AppID != appID || assignment.ConnectionID != connectionID {
		return protocolError("function_not_assigned", "Function invocation is not assigned to this connection.", false)
	}
	return nil
}

func ValidateServerFrame(raw []byte) *ProtocolError {
	object, err := decodeObject(raw)
	if err != nil {
		return err
	}
	typeName, ok := stringField(object, "type")
	if !ok {
		return invalidFrame()
	}
	switch typeName {
	case "ready":
		if !only(object, "type", "protocol_version", "app_id", "connection_id", "heartbeat_interval_ms", "max_in_flight_limit") {
			return invalidFrame()
		}
		version, vok := intField(object, "protocol_version")
		app, aok := stringField(object, "app_id")
		conn, cok := stringField(object, "connection_id")
		heartbeat, hok := intField(object, "heartbeat_interval_ms")
		max, mok := intField(object, "max_in_flight_limit")
		if !vok || version != Version || !aok || !bounded(app, 128) || !cok || !bounded(conn, 128) || !hok || heartbeat < 1000 || heartbeat > 120000 || !mok || max < 1 || max > 256 {
			return invalidFrame()
		}
	case "consumer.started":
		if !only(object, "type", "consumer", "max_in_flight") {
			return invalidFrame()
		}
		consumer, cok := stringField(object, "consumer")
		max, mok := intField(object, "max_in_flight")
		if !cok || consumer != DefaultConsumer || !mok || max < 1 || max > 256 {
			return invalidFrame()
		}
	case "event.delivery":
		if !only(object, "type", "delivery_id", "attempt", "event") {
			return invalidFrame()
		}
		id, iok := stringField(object, "delivery_id")
		attempt, aok := intField(object, "attempt")
		if !iok || !bounded(id, 128) || !aok || attempt < 1 || !validEvent(object["event"]) {
			return invalidFrame()
		}
	case "delivery.accepted":
		if !only(object, "type", "delivery_id", "state") {
			return invalidFrame()
		}
		id, iok := stringField(object, "delivery_id")
		state, sok := stringField(object, "state")
		if !iok || !bounded(id, 128) || !sok || (state != "acked" && state != "retrying" && state != "progress") {
			return invalidFrame()
		}
	case "function.invoke":
		if !only(object, "type", "invocation_id", "function", "input", "deadline") {
			return invalidFrame()
		}
		id, iok := stringField(object, "invocation_id")
		name, nok := stringField(object, "function")
		deadline, dok := stringField(object, "deadline")
		if !iok || !bounded(id, 128) || !nok || !domain.ValidFunctionName(name) || !jsonObject(object["input"]) || !dok || !validTime(deadline) {
			return invalidFrame()
		}
	case "error":
		if !only(object, "type", "code", "message", "retryable") {
			return invalidFrame()
		}
		code, cok := stringField(object, "code")
		message, mok := stringField(object, "message")
		_, rok := boolField(object, "retryable")
		if !cok || !StableErrorCode(code) || !mok || !bounded(message, 1024) || !rok {
			return invalidFrame()
		}
	case "pong":
		if !only(object, "type") {
			return invalidFrame()
		}
	default:
		return protocolError("unknown_type", "Frame type is not supported.", false)
	}
	return nil
}

func invalidFrame() *ProtocolError {
	return protocolError("invalid_frame", "Frame fields are invalid.", false)
}

func decodeObject(raw []byte) (map[string]json.RawMessage, *ProtocolError) {
	if len(raw) > MaxMessageBytes {
		return nil, protocolError("frame_too_large", "Frame exceeds the 64 KiB limit.", false)
	}
	if !utf8.Valid(raw) {
		return nil, protocolError("invalid_utf8", "Frame must contain valid UTF-8.", false)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	duplicate, rootObject, parseErr := inspectValue(decoder, true)
	if duplicate {
		return nil, protocolError("duplicate_key", "Frame contains a duplicate JSON object key.", false)
	}
	if parseErr != nil || !rootObject {
		return nil, protocolError("invalid_json", "Frame must contain exactly one JSON object.", false)
	}
	if _, err := decoder.Token(); err != io.EOF {
		return nil, protocolError("invalid_json", "Frame must contain exactly one JSON object.", false)
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil || object == nil {
		return nil, protocolError("invalid_json", "Frame must contain exactly one JSON object.", false)
	}
	return object, nil
}

func inspectValue(decoder *json.Decoder, root bool) (duplicate bool, rootObject bool, err error) {
	token, err := decoder.Token()
	if err != nil {
		return false, false, err
	}
	delim, isDelim := token.(json.Delim)
	if !isDelim {
		return false, false, nil
	}
	switch delim {
	case '{':
		seen := map[string]bool{}
		for decoder.More() {
			keyToken, keyErr := decoder.Token()
			if keyErr != nil {
				return false, root, keyErr
			}
			key, ok := keyToken.(string)
			if !ok {
				return false, root, io.ErrUnexpectedEOF
			}
			if seen[key] {
				return true, root, nil
			}
			seen[key] = true
			dup, _, childErr := inspectValue(decoder, false)
			if dup || childErr != nil {
				return dup, root, childErr
			}
		}
		end, endErr := decoder.Token()
		if endErr != nil || end != json.Delim('}') {
			return false, root, io.ErrUnexpectedEOF
		}
		return false, root, nil
	case '[':
		for decoder.More() {
			dup, _, childErr := inspectValue(decoder, false)
			if dup || childErr != nil {
				return dup, false, childErr
			}
		}
		end, endErr := decoder.Token()
		if endErr != nil || end != json.Delim(']') {
			return false, false, io.ErrUnexpectedEOF
		}
		return false, false, nil
	default:
		return false, false, io.ErrUnexpectedEOF
	}
}

func only(object map[string]json.RawMessage, fields ...string) bool {
	allowed := make(map[string]bool, len(fields))
	for _, field := range fields {
		allowed[field] = true
	}
	for field := range object {
		if !allowed[field] {
			return false
		}
	}
	return true
}
func stringField(object map[string]json.RawMessage, name string) (string, bool) {
	var value string
	raw, ok := object[name]
	if !ok || json.Unmarshal(raw, &value) != nil {
		return "", false
	}
	return value, true
}
func boolField(object map[string]json.RawMessage, name string) (bool, bool) {
	var value bool
	raw, ok := object[name]
	if !ok || json.Unmarshal(raw, &value) != nil {
		return false, false
	}
	return value, true
}
func intField(object map[string]json.RawMessage, name string) (int, bool) {
	var value int
	raw, ok := object[name]
	if !ok || json.Unmarshal(raw, &value) != nil {
		return 0, false
	}
	return value, true
}
func bounded(value string, max int) bool        { return value != "" && len([]byte(value)) <= max }
func clone(raw json.RawMessage) json.RawMessage { return append(json.RawMessage(nil), raw...) }
func jsonObject(raw json.RawMessage) bool {
	var value map[string]json.RawMessage
	return len(raw) > 0 && json.Unmarshal(raw, &value) == nil && value != nil
}
func validJSONValue(raw json.RawMessage) bool {
	return len(raw) > 0 && utf8.Valid(raw) && json.Valid(raw)
}
func validTime(value string) bool { _, err := time.Parse(time.RFC3339, value); return err == nil }

func validHandlerError(raw json.RawMessage) bool {
	var value map[string]json.RawMessage
	if json.Unmarshal(raw, &value) != nil || !only(value, "code", "message") || len(value) != 2 {
		return false
	}
	code, cok := stringField(value, "code")
	message, mok := stringField(value, "message")
	return cok && domain.ValidFunctionName(code) && mok && bounded(message, 1024)
}

func validEvent(raw json.RawMessage) bool {
	var event map[string]json.RawMessage
	if json.Unmarshal(raw, &event) != nil || !only(event, "id", "type", "source_app_id", "target_app_ids", "data", "created_at") || len(event) != 6 {
		return false
	}
	id, iok := stringField(event, "id")
	eventType, tok := stringField(event, "type")
	source, sok := stringField(event, "source_app_id")
	created, cok := stringField(event, "created_at")
	var targets []string
	if json.Unmarshal(event["target_app_ids"], &targets) != nil || len(targets) < 1 || len(targets) > 100 {
		return false
	}
	seen := map[string]bool{}
	for _, target := range targets {
		if !bounded(target, 128) || seen[target] {
			return false
		}
		seen[target] = true
	}
	return iok && bounded(id, 128) && tok && eventType != "" && sok && bounded(source, 128) && jsonObject(event["data"]) && cok && validTime(created)
}
