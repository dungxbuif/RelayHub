package service

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/store"
)

var (
	ErrInvalidInput = errors.New("invalid input")
	ErrUnauthorized = errors.New("unauthorized")
	ErrNotFound     = errors.New("application not found")
	ErrConflict     = errors.New("application conflict")
)

type CreateApp struct {
	Name         string
	CallbackURL  *string
	DeliveryMode domain.DeliveryMode
}

type OptionalString struct {
	Set   bool
	Value *string
}

type UpdateApp struct {
	Name         *string
	CallbackURL  OptionalString
	DeliveryMode *domain.DeliveryMode
}

type AppCredentials struct {
	AppID      string `json:"app_id"`
	APIKey     string `json:"api_key"`
	HMACSecret string `json:"hmac_secret"`
}

type AuthenticatedApp struct {
	App        domain.App
	HMACSecret []byte
}

type AppOptions struct {
	Now                    func() time.Time
	Random                 io.Reader
	AllowInsecureCallbacks bool
}

type AppService struct {
	store                  store.ApplicationStore
	now                    func() time.Time
	random                 io.Reader
	allowInsecureCallbacks bool
}

func NewAppService(repository store.ApplicationStore, options AppOptions) *AppService {
	if options.Now == nil {
		options.Now = time.Now
	}
	if options.Random == nil {
		options.Random = rand.Reader
	}
	return &AppService{
		store:                  repository,
		now:                    options.Now,
		random:                 options.Random,
		allowInsecureCallbacks: options.AllowInsecureCallbacks,
	}
}

func (service *AppService) Create(ctx context.Context, input CreateApp) (domain.App, AppCredentials, error) {
	input.Name = strings.TrimSpace(input.Name)
	if err := service.validate(input.Name, input.CallbackURL, input.DeliveryMode); err != nil {
		return domain.App{}, AppCredentials{}, err
	}
	appID, err := service.randomValue("app_", 16)
	if err != nil {
		return domain.App{}, AppCredentials{}, err
	}
	apiKey, err := service.randomValue("rhk_", 32)
	if err != nil {
		return domain.App{}, AppCredentials{}, err
	}
	hmacSecret, err := service.randomValue("rhs_", 32)
	if err != nil {
		return domain.App{}, AppCredentials{}, err
	}
	now := service.now().UTC()
	app := domain.App{
		ID:           appID,
		Name:         input.Name,
		CallbackURL:  copyString(input.CallbackURL),
		DeliveryMode: input.DeliveryMode,
		Enabled:      true,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	credential := store.AppCredential{AppID: appID, APIKeyHash: hashAPIKey(apiKey), HMACSecret: []byte(hmacSecret)}
	if err := service.store.CreateApplication(ctx, app, credential); err != nil {
		return domain.App{}, AppCredentials{}, mapStoreError(err)
	}
	return app, AppCredentials{AppID: appID, APIKey: apiKey, HMACSecret: hmacSecret}, nil
}

func (service *AppService) List(ctx context.Context) ([]domain.App, error) {
	apps, err := service.store.ListApplications(ctx)
	if err != nil {
		return nil, mapStoreError(err)
	}
	sort.Slice(apps, func(left, right int) bool {
		if apps[left].CreatedAt.Equal(apps[right].CreatedAt) {
			return apps[left].ID < apps[right].ID
		}
		return apps[left].CreatedAt.Before(apps[right].CreatedAt)
	})
	return apps, nil
}

func (service *AppService) Get(ctx context.Context, appID string) (domain.App, error) {
	app, err := service.store.GetApplication(ctx, appID)
	if err != nil {
		return domain.App{}, mapStoreError(err)
	}
	return app, nil
}

func (service *AppService) Update(ctx context.Context, appID string, input UpdateApp) (domain.App, error) {
	if input.Name == nil && !input.CallbackURL.Set && input.DeliveryMode == nil {
		return domain.App{}, ErrInvalidInput
	}
	// Re-read and revalidate the complete merged configuration after contention.
	// The store compares editable fields, keeping disable/rotation independent.
	for attempt := 0; attempt < 16; attempt++ {
		if err := ctx.Err(); err != nil {
			return domain.App{}, err
		}
		app, err := service.Get(ctx, appID)
		if err != nil {
			return domain.App{}, err
		}
		expected := app
		if input.Name != nil {
			app.Name = strings.TrimSpace(*input.Name)
		}
		if input.CallbackURL.Set {
			app.CallbackURL = copyString(input.CallbackURL.Value)
		}
		if input.DeliveryMode != nil {
			app.DeliveryMode = *input.DeliveryMode
		}
		if err := service.validate(app.Name, app.CallbackURL, app.DeliveryMode); err != nil {
			return domain.App{}, err
		}
		app.UpdatedAt = service.now().UTC()
		updated, err := service.store.CompareAndSwapApplication(ctx, expected, app)
		if errors.Is(err, store.ErrConflict) {
			continue
		}
		if err != nil {
			return domain.App{}, mapStoreError(err)
		}
		return updated, nil
	}
	return domain.App{}, ErrConflict
}

func (service *AppService) Disable(ctx context.Context, appID string) (domain.App, error) {
	app, err := service.store.DisableApplication(ctx, appID, service.now().UTC())
	if err != nil {
		return domain.App{}, mapStoreError(err)
	}
	return app, nil
}

func (service *AppService) RotateSecret(ctx context.Context, appID string) (AppCredentials, error) {
	apiKey, err := service.randomValue("rhk_", 32)
	if err != nil {
		return AppCredentials{}, err
	}
	hmacSecret, err := service.randomValue("rhs_", 32)
	if err != nil {
		return AppCredentials{}, err
	}
	credential := store.AppCredential{AppID: appID, APIKeyHash: hashAPIKey(apiKey), HMACSecret: []byte(hmacSecret)}
	if err := service.store.RotateApplicationCredential(ctx, appID, credential, service.now().UTC()); err != nil {
		return AppCredentials{}, mapStoreError(err)
	}
	return AppCredentials{AppID: appID, APIKey: apiKey, HMACSecret: hmacSecret}, nil
}

func (service *AppService) AuthenticateAPIKey(ctx context.Context, apiKey string) (AuthenticatedApp, error) {
	if apiKey == "" {
		return AuthenticatedApp{}, ErrUnauthorized
	}
	credential, err := service.store.FindCredentialByAPIKeyHash(ctx, hashAPIKey(apiKey))
	if err != nil {
		return AuthenticatedApp{}, ErrUnauthorized
	}
	app, err := service.store.GetApplication(ctx, credential.AppID)
	if err != nil || !app.Enabled {
		return AuthenticatedApp{}, ErrUnauthorized
	}
	return AuthenticatedApp{App: app, HMACSecret: append([]byte(nil), credential.HMACSecret...)}, nil
}

func (service *AppService) validate(name string, callbackURL *string, mode domain.DeliveryMode) error {
	if name == "" || len(name) > 128 || !mode.Valid() {
		return ErrInvalidInput
	}
	if (mode == domain.DeliveryCallback || mode == domain.DeliveryAll) && callbackURL == nil {
		return ErrInvalidInput
	}
	if callbackURL == nil {
		return nil
	}
	parsed, err := url.Parse(*callbackURL)
	if err != nil || parsed.Hostname() == "" || parsed.User != nil || parsed.Fragment != "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return ErrInvalidInput
	}
	if parsed.Scheme == "https" {
		return nil
	}
	if !service.allowInsecureCallbacks || !internalHost(parsed.Hostname()) {
		return ErrInvalidInput
	}
	return nil
}

func internalHost(host string) bool {
	host = strings.ToLower(strings.TrimSuffix(host, "."))
	if host == "localhost" || strings.HasSuffix(host, ".localhost") || strings.HasSuffix(host, ".local") || strings.HasSuffix(host, ".internal") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && (ip.IsLoopback() || ip.IsPrivate())
}

func (service *AppService) randomValue(prefix string, bytesCount int) (string, error) {
	random := make([]byte, bytesCount)
	if _, err := io.ReadFull(service.random, random); err != nil {
		return "", err
	}
	return prefix + base64.RawURLEncoding.EncodeToString(random), nil
}

func hashAPIKey(apiKey string) string {
	hash := sha256.Sum256([]byte(apiKey))
	return hex.EncodeToString(hash[:])
}

func copyString(value *string) *string {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func mapStoreError(err error) error {
	switch {
	case errors.Is(err, store.ErrNotFound):
		return ErrNotFound
	case errors.Is(err, store.ErrConflict):
		return ErrConflict
	default:
		return err
	}
}
