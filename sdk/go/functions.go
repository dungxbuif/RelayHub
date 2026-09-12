package relayhub

import (
	"context"
	"encoding/json"
	"strings"
	"time"
)

type Function struct {
	ID             string    `json:"id"`
	AppID          string    `json:"app_id"`
	Name           string    `json:"name"`
	TimeoutSeconds int       `json:"timeout_seconds"`
	Enabled        bool      `json:"enabled"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}
type FunctionError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e *FunctionError) Error() string { return "relayhub: function handler failed (" + e.Code + ")" }

type InvocationResult struct {
	InvocationID string          `json:"invocation_id"`
	OK           bool            `json:"ok"`
	Result       json.RawMessage `json:"result,omitempty"`
	Error        *FunctionError  `json:"error,omitempty"`
	Replayed     bool            `json:"-"`
}

func (c *Client) Invoke(ctx context.Context, id string, input json.RawMessage, options ...CallOption) (InvocationResult, error) {
	if ctx.Err() != nil {
		return InvocationResult{}, ctx.Err()
	}
	key, err := callKey(options)
	if err != nil {
		return InvocationResult{}, err
	}
	if id == "" || len(id) > 128 || strings.ContainsAny(id, "/\\?#%") || !object(input) {
		return InvocationResult{}, ErrInvalidInput
	}
	var result InvocationResult
	result.Replayed, err = c.request(ctx, "POST", "/api/v1/functions/"+id+"/invoke", struct {
		Input json.RawMessage `json:"input"`
	}{input}, key, &result)
	return result, err
}
