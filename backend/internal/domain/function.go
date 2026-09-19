package domain

import (
	"bytes"
	"encoding/json"
	"regexp"
	"time"
	"unicode/utf8"
)

const FunctionFrameLimit = 64 * 1024
const InvocationRetention = 24 * time.Hour

var functionName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_.-]{0,63}$`)

func ValidFunctionName(name string) bool { return functionName.MatchString(name) }
func JSONObject(raw json.RawMessage) bool {
	raw = bytes.TrimSpace(raw)
	return len(raw) > 0 && raw[0] == '{' && utf8.Valid(raw) && json.Valid(raw)
}

type Function struct {
	ID             string    `json:"id"`
	AppID          string    `json:"app_id"`
	Name           string    `json:"name"`
	TimeoutSeconds int       `json:"timeout_seconds"`
	Enabled        bool      `json:"enabled"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}
type RPCInvoke struct {
	Type         string          `json:"type"`
	InvocationID string          `json:"invocation_id"`
	Function     string          `json:"function"`
	Input        json.RawMessage `json:"input"`
	Deadline     string          `json:"deadline"`
}
type RPCResult struct {
	InvocationID string          `json:"invocation_id"`
	OK           bool            `json:"ok"`
	Result       json.RawMessage `json:"result,omitempty"`
	Error        json.RawMessage `json:"error,omitempty"`
}

// ValidRPCResult bounds the complete wire representation, including type. Handler
// errors are explicit caller-visible code/message objects; RelayHub adds no internals.
func ValidRPCResult(r RPCResult) bool {
	if r.InvocationID == "" || len(r.InvocationID) > 128 {
		return false
	}
	if r.OK {
		if len(r.Error) != 0 || !utf8.Valid(r.Result) || !json.Valid(r.Result) {
			return false
		}
	} else {
		if len(r.Result) != 0 || !JSONObject(r.Error) {
			return false
		}
		if _, ok := CanonicalRPCError(r.Error); !ok {
			return false
		}
	}
	wire := struct {
		Type string `json:"type"`
		RPCResult
	}{"rpc.result", r}
	raw, e := json.Marshal(wire)
	return e == nil && len(raw) <= FunctionFrameLimit
}

// CanonicalRPCError rejects duplicate, wrong-case, unknown and null fields before
// rebuilding the two caller-visible fields from validated strings.
func CanonicalRPCError(raw json.RawMessage) (json.RawMessage, bool) {
	if !JSONObject(raw) {
		return nil, false
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	_, _ = dec.Token()
	fields := make(map[string]string, 2)
	for dec.More() {
		key, err := dec.Token()
		if err != nil {
			return nil, false
		}
		name, ok := key.(string)
		if !ok || (name != "code" && name != "message") {
			return nil, false
		}
		if _, exists := fields[name]; exists {
			return nil, false
		}
		var value *string
		if dec.Decode(&value) != nil || value == nil {
			return nil, false
		}
		fields[name] = *value
	}
	if len(fields) != 2 || !ValidFunctionName(fields["code"]) || fields["message"] == "" || len(fields["message"]) > 1024 {
		return nil, false
	}
	result, err := json.Marshal(struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}{fields["code"], fields["message"]})
	return result, err == nil
}

type InvocationState string

const (
	InvocationPending      InvocationState = "pending"
	InvocationReserved     InvocationState = "reserved"
	InvocationClaimed      InvocationState = "claimed"
	InvocationSuccess      InvocationState = "success"
	InvocationHandlerError InvocationState = "handler_error"
	InvocationUnavailable  InvocationState = "unavailable"
	InvocationTimeout      InvocationState = "timeout"
)

type Invocation struct {
	ID           string          `json:"id"`
	FunctionID   string          `json:"function_id"`
	OwnerAppID   string          `json:"owner_app_id"`
	CallerAppID  string          `json:"caller_app_id"`
	Name         string          `json:"name"`
	Input        json.RawMessage `json:"input"`
	CreatedAt    time.Time       `json:"created_at"`
	Deadline     time.Time       `json:"deadline"`
	ClaimBy      time.Time       `json:"claim_by"`
	State        InvocationState `json:"state"`
	ConnectionID string          `json:"connection_id,omitempty"`
	Reply        *RPCResult      `json:"reply,omitempty"`
}

func (v Invocation) Terminal() bool {
	switch v.State {
	case InvocationSuccess, InvocationHandlerError, InvocationUnavailable, InvocationTimeout:
		return true
	}
	return false
}
func (v Invocation) Frame() RPCInvoke {
	return RPCInvoke{"rpc.invoke", v.ID, v.Name, v.Input, v.Deadline.UTC().Format(time.RFC3339Nano)}
}
