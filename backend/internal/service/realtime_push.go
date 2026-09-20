package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"time"

	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/store"
	"github.com/google/uuid"
)

type PushAdapter interface {
	Send(context.Context, domain.PushDevice, domain.PushNotification) (string, error)
}
type RealtimePushOptions struct {
	Now      func() time.Time
	NewID    func(string) string
	Adapters map[string]PushAdapter
}
type RealtimePushService struct {
	repository store.RealtimePushRepository
	options    RealtimePushOptions
}
type RegisterPushDeviceInput struct {
	Provider string `json:"provider"`
	Token    string `json:"token"`
}

func NewRealtimePushService(repository store.RealtimePushRepository, options RealtimePushOptions) *RealtimePushService {
	if options.Now == nil {
		options.Now = time.Now
	}
	if options.NewID == nil {
		options.NewID = func(prefix string) string { return prefix + uuid.NewString() }
	}
	return &RealtimePushService{repository: repository, options: options}
}
func (service *RealtimePushService) Register(ctx context.Context, appID string, input RegisterPushDeviceInput) (domain.PushDevice, error) {
	input.Provider = strings.ToLower(strings.TrimSpace(input.Provider))
	input.Token = strings.TrimSpace(input.Token)
	if service == nil || nilDependency(service.repository) {
		return domain.PushDevice{}, ErrInvalidDependency
	}
	if appID == "" || (input.Provider != "apns" && input.Provider != "fcm") || len(input.Token) < 16 || len(input.Token) > 4096 || strings.ContainsAny(input.Token, "\x00\r\n") {
		return domain.PushDevice{}, ErrInvalidInput
	}
	hash := sha256.Sum256([]byte(input.Token))
	now := service.options.Now().UTC()
	device := domain.PushDevice{ID: service.options.NewID("device_"), AppID: appID, Provider: input.Provider, Token: []byte(input.Token), TokenHash: hex.EncodeToString(hash[:]), CreatedAt: now, UpdatedAt: now}
	device, err := service.repository.CreatePushDevice(ctx, device)
	device.Token = nil
	return device, mapStoreError(err)
}
func (service *RealtimePushService) Delete(ctx context.Context, appID, id string) error {
	if service == nil || nilDependency(service.repository) {
		return ErrInvalidDependency
	}
	return mapStoreError(service.repository.DeletePushDevice(ctx, appID, id))
}
func (service *RealtimePushService) Bind(ctx context.Context, appID, channel, deviceID string, bind bool) error {
	if service == nil || nilDependency(service.repository) {
		return ErrInvalidDependency
	}
	if appID == "" || !domain.ValidRealtimeChannel(channel) || deviceID == "" {
		return ErrInvalidInput
	}
	if bind {
		return mapStoreError(service.repository.BindPushDevice(ctx, appID, channel, deviceID, service.options.Now().UTC()))
	}
	return mapStoreError(service.repository.UnbindPushDevice(ctx, appID, channel, deviceID))
}
func (service *RealtimePushService) Publish(ctx context.Context, appID, channel string, notification domain.PushNotification) ([]domain.PushOutcome, error) {
	if service == nil || nilDependency(service.repository) {
		return nil, ErrInvalidDependency
	}
	if appID == "" || !domain.ValidRealtimeChannel(channel) || len(notification.Title) > 100 || len(notification.Body) > 500 || (!jsonObjectOrEmpty(notification.Data)) || len(notification.Data) > 4096 || containsSecretField(notification.Data) {
		return nil, ErrInvalidInput
	}
	devices, err := service.repository.ListPushDevicesForChannel(ctx, appID, channel, 100)
	if err != nil {
		return nil, mapStoreError(err)
	}
	outcomes := make([]domain.PushOutcome, 0, len(devices))
	for _, device := range devices {
		outcome := domain.PushOutcome{ID: service.options.NewID("push_"), AppID: appID, DeviceID: device.ID, Channel: channel, Provider: device.Provider, Status: "delivered", CreatedAt: service.options.Now().UTC()}
		adapter := service.options.Adapters[device.Provider]
		if adapter == nil {
			outcome.Status = "failed"
			outcome.Reason = "provider_unavailable"
		} else {
			providerID, sendErr := adapter.Send(ctx, device, notification)
			outcome.ProviderMessageID = providerID
			if sendErr != nil {
				outcome.Status = "failed"
				outcome.Reason = "provider_error"
			}
		}
		if persistErr := service.repository.CreatePushOutcome(ctx, outcome); persistErr != nil {
			return outcomes, persistErr
		}
		outcomes = append(outcomes, outcome)
	}
	return outcomes, nil
}
func jsonObjectOrEmpty(raw json.RawMessage) bool {
	return len(raw) == 0 || json.Valid(raw) && len(raw) >= 2 && raw[0] == '{' && raw[len(raw)-1] == '}'
}
func containsSecretField(raw json.RawMessage) bool {
	if len(raw) == 0 {
		return false
	}
	var value any
	if json.Unmarshal(raw, &value) != nil {
		return true
	}
	var inspect func(any) bool
	inspect = func(item any) bool {
		switch typed := item.(type) {
		case map[string]any:
			for key, child := range typed {
				lower := strings.ToLower(key)
				if strings.Contains(lower, "secret") || strings.Contains(lower, "token") || strings.Contains(lower, "password") || strings.Contains(lower, "authorization") || inspect(child) {
					return true
				}
			}
		case []any:
			for _, child := range typed {
				if inspect(child) {
					return true
				}
			}
		}
		return false
	}
	return inspect(value)
}
