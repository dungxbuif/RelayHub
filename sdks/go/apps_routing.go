package relayhub

import (
	"context"
	"strings"
	"time"
)

type CreateAppInput struct {
	Name         string  `json:"name"`
	CallbackURL  *string `json:"callback_url,omitempty"`
	DeliveryMode string  `json:"delivery_mode"`
}

type AppCredentials struct {
	AppID      string `json:"app_id"`
	APIKey     string `json:"api_key"`
	HMACSecret string `json:"hmac_secret"`
}

type App struct {
	ID           string    `json:"id"`
	Name         string    `json:"name"`
	CallbackURL  *string   `json:"callback_url"`
	DeliveryMode string    `json:"delivery_mode"`
	Enabled      bool      `json:"enabled"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

func (c *Client) CreateApp(ctx context.Context, input CreateAppInput) (AppCredentials, error) {
	if strings.TrimSpace(input.Name) == "" || strings.TrimSpace(input.DeliveryMode) == "" {
		return AppCredentials{}, ErrInvalidInput
	}
	var result AppCredentials
	err := c.adminRequest(ctx, "POST", "/api/v1/apps", input, &result)
	return result, err
}

func (c *Client) ListApps(ctx context.Context) ([]App, error) {
	var result []App
	err := c.adminRequest(ctx, "GET", "/api/v1/apps", nil, &result)
	return result, err
}

type RoutingRuleInput struct {
	SourceAppID     *string `json:"source_app_id,omitempty"`
	EventType       string  `json:"event_type"`
	TargetAppID     string  `json:"target_app_id"`
	RealtimeChannel *string `json:"realtime_channel,omitempty"`
	Enabled         *bool   `json:"enabled,omitempty"`
}

type RoutingRule struct {
	ID              string    `json:"id"`
	SourceAppID     *string   `json:"source_app_id"`
	EventType       string    `json:"event_type"`
	TargetAppID     string    `json:"target_app_id"`
	RealtimeChannel *string   `json:"realtime_channel"`
	Enabled         bool      `json:"enabled"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
}

func (c *Client) CreateRoutingRule(ctx context.Context, input RoutingRuleInput) (RoutingRule, error) {
	if strings.TrimSpace(input.EventType) == "" || strings.TrimSpace(input.TargetAppID) == "" {
		return RoutingRule{}, ErrInvalidInput
	}
	var result RoutingRule
	err := c.adminRequest(ctx, "POST", "/api/v1/routing/rules", input, &result)
	return result, err
}

func (c *Client) ListRoutingRules(ctx context.Context) ([]RoutingRule, error) {
	var result []RoutingRule
	err := c.adminRequest(ctx, "GET", "/api/v1/routing/rules", nil, &result)
	return result, err
}
