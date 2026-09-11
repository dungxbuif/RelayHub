package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/store"
)

func TestAppCreateReturnsCredentialsOnceAndQueriesRedactThem(t *testing.T) {
	repository := newMemoryAppStore()
	service := newDeterministicAppService(repository, false)
	callbackURL := "https://orders.internal/events"

	created, credentials, err := service.Create(context.Background(), CreateApp{
		Name:         "orders-web",
		CallbackURL:  &callbackURL,
		DeliveryMode: domain.DeliveryQueue,
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if created.ID == "" || credentials.AppID != created.ID || credentials.APIKey == "" || credentials.HMACSecret == "" {
		t.Fatalf("Create() app = %#v credentials = %#v, want complete one-time credentials", created, credentials)
	}

	got, err := service.Get(context.Background(), created.ID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	listed, err := service.List(context.Background())
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(listed) != 1 || listed[0].ID != created.ID {
		t.Fatalf("List() = %#v, want created app", listed)
	}

	encoded, err := json.Marshal(struct {
		Get  domain.App   `json:"get"`
		List []domain.App `json:"list"`
	}{Get: got, List: listed})
	if err != nil {
		t.Fatalf("Marshal(query responses) error = %v", err)
	}
	if bytes.Contains(encoded, []byte(credentials.APIKey)) || bytes.Contains(encoded, []byte(credentials.HMACSecret)) || bytes.Contains(encoded, []byte("api_key")) || bytes.Contains(encoded, []byte("hmac_secret")) {
		t.Fatalf("query JSON exposed credentials: %s", encoded)
	}
	if repository.hasPlaintextAPIKey(credentials.APIKey) {
		t.Fatal("repository received plaintext API key instead of only its cryptographic hash")
	}
}

func TestAppRotateInvalidatesOldCredentialsAtomically(t *testing.T) {
	repository := newMemoryAppStore()
	service := newDeterministicAppService(repository, false)
	created, oldCredentials, err := service.Create(context.Background(), CreateApp{Name: "orders", DeliveryMode: domain.DeliveryQueue})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if _, err := service.AuthenticateAPIKey(context.Background(), oldCredentials.APIKey); err != nil {
		t.Fatalf("AuthenticateAPIKey(old before rotate) error = %v", err)
	}

	newCredentials, err := service.RotateSecret(context.Background(), created.ID)
	if err != nil {
		t.Fatalf("RotateSecret() error = %v", err)
	}
	if newCredentials.APIKey == oldCredentials.APIKey || newCredentials.HMACSecret == oldCredentials.HMACSecret {
		t.Fatal("RotateSecret() reused old credential material")
	}
	if _, err := service.AuthenticateAPIKey(context.Background(), oldCredentials.APIKey); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("AuthenticateAPIKey(old after rotate) error = %v, want ErrUnauthorized", err)
	}
	authenticated, err := service.AuthenticateAPIKey(context.Background(), newCredentials.APIKey)
	if err != nil {
		t.Fatalf("AuthenticateAPIKey(new) error = %v", err)
	}
	if authenticated.App.ID != created.ID || string(authenticated.HMACSecret) != newCredentials.HMACSecret {
		t.Fatalf("AuthenticateAPIKey(new) = %#v, want rotated app and secret", authenticated)
	}
}

func TestAppDisableInvalidatesAuthentication(t *testing.T) {
	repository := newMemoryAppStore()
	service := newDeterministicAppService(repository, false)
	created, credentials, err := service.Create(context.Background(), CreateApp{Name: "orders", DeliveryMode: domain.DeliveryQueue})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	disabled, err := service.Disable(context.Background(), created.ID)
	if err != nil {
		t.Fatalf("Disable() error = %v", err)
	}
	if disabled.Enabled {
		t.Fatal("Disable() app remained enabled")
	}
	if _, err := service.AuthenticateAPIKey(context.Background(), credentials.APIKey); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("AuthenticateAPIKey(disabled) error = %v, want ErrUnauthorized", err)
	}
}

func TestAppValidationRejectsInvalidDeliveryAndCallbackConfiguration(t *testing.T) {
	publicHTTP := "http://example.com/events"
	loopbackHTTP := "http://127.0.0.1:9000/events"
	relative := "/events"

	tests := []struct {
		name          string
		input         CreateApp
		allowInsecure bool
		wantError     bool
	}{
		{name: "missing name", input: CreateApp{DeliveryMode: domain.DeliveryQueue}, wantError: true},
		{name: "unknown delivery mode", input: CreateApp{Name: "orders", DeliveryMode: domain.DeliveryMode("email")}, wantError: true},
		{name: "callback mode requires URL", input: CreateApp{Name: "orders", DeliveryMode: domain.DeliveryCallback}, wantError: true},
		{name: "relative callback", input: CreateApp{Name: "orders", DeliveryMode: domain.DeliveryCallback, CallbackURL: &relative}, wantError: true},
		{name: "public HTTP rejected even when insecure enabled", input: CreateApp{Name: "orders", DeliveryMode: domain.DeliveryCallback, CallbackURL: &publicHTTP}, allowInsecure: true, wantError: true},
		{name: "loopback HTTP rejected by default", input: CreateApp{Name: "orders", DeliveryMode: domain.DeliveryCallback, CallbackURL: &loopbackHTTP}, wantError: true},
		{name: "loopback HTTP allowed by explicit policy", input: CreateApp{Name: "orders", DeliveryMode: domain.DeliveryCallback, CallbackURL: &loopbackHTTP}, allowInsecure: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			appService := newDeterministicAppService(newMemoryAppStore(), tt.allowInsecure)
			_, _, err := appService.Create(context.Background(), tt.input)
			if tt.wantError && !errors.Is(err, ErrInvalidInput) {
				t.Fatalf("Create() error = %v, want ErrInvalidInput", err)
			}
			if !tt.wantError && err != nil {
				t.Fatalf("Create() error = %v, want nil", err)
			}
		})
	}
}

func TestAppUpdateValidatesAndCanClearCallback(t *testing.T) {
	repository := newMemoryAppStore()
	service := newDeterministicAppService(repository, false)
	callback := "https://orders.internal/events"
	created, _, err := service.Create(context.Background(), CreateApp{Name: "orders", CallbackURL: &callback, DeliveryMode: domain.DeliveryAll})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	queue := domain.DeliveryQueue

	updated, err := service.Update(context.Background(), created.ID, UpdateApp{
		CallbackURL:  OptionalString{Set: true},
		DeliveryMode: &queue,
	})
	if err != nil {
		t.Fatalf("Update() error = %v", err)
	}
	if updated.CallbackURL != nil || updated.DeliveryMode != domain.DeliveryQueue {
		t.Fatalf("Update() = %#v, want cleared callback in queue mode", updated)
	}
	if _, err := service.Update(context.Background(), created.ID, UpdateApp{}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("Update(empty) error = %v, want ErrInvalidInput", err)
	}
}

func TestAppDisableWinsWhenUpdateReadPrecedesDisable(t *testing.T) {
	repository := newMemoryAppStore()
	service := newDeterministicAppService(repository, false)
	created, credentials, err := service.Create(context.Background(), CreateApp{Name: "orders", DeliveryMode: domain.DeliveryQueue})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	repository.updateStarted = make(chan struct{})
	repository.continueUpdate = make(chan struct{})
	newName := "orders-v2"
	type updateResult struct {
		app domain.App
		err error
	}
	result := make(chan updateResult, 1)
	go func() {
		app, err := service.Update(context.Background(), created.ID, UpdateApp{Name: &newName})
		result <- updateResult{app: app, err: err}
	}()

	select {
	case <-repository.updateStarted:
	case <-time.After(time.Second):
		t.Fatal("Update() did not reach store after reading the application")
	}
	if _, err := service.Disable(context.Background(), created.ID); err != nil {
		t.Fatalf("Disable() during Update error = %v", err)
	}
	close(repository.continueUpdate)
	var updated updateResult
	select {
	case updated = <-result:
	case <-time.After(time.Second):
		t.Fatal("Update() did not finish after store interleaving was released")
	}
	if updated.err != nil {
		t.Fatalf("Update() error = %v", updated.err)
	}
	if updated.app.Enabled {
		t.Fatal("Update() response re-enabled an application disabled after its initial read")
	}
	stored, err := service.Get(context.Background(), created.ID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if stored.Enabled {
		t.Fatal("Update() permanently overwrote the concurrent disable")
	}
	if _, err := service.AuthenticateAPIKey(context.Background(), credentials.APIKey); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("AuthenticateAPIKey() after interleaving error = %v, want ErrUnauthorized", err)
	}
}

func newDeterministicAppService(repository store.ApplicationStore, allowInsecure bool) *AppService {
	random := make([]byte, 2048)
	for index := range random {
		random[index] = byte(index%251 + 1)
	}
	return NewAppService(repository, AppOptions{
		Now:                    func() time.Time { return time.Date(2026, 9, 11, 10, 0, 0, 0, time.UTC) },
		Random:                 bytes.NewReader(random),
		AllowInsecureCallbacks: allowInsecure,
	})
}

type memoryAppStore struct {
	mu             sync.Mutex
	apps           map[string]domain.App
	credentials    map[string]store.AppCredential
	credentialHash map[string]string
	updateStarted  chan struct{}
	continueUpdate chan struct{}
}

func newMemoryAppStore() *memoryAppStore {
	return &memoryAppStore{
		apps:           make(map[string]domain.App),
		credentials:    make(map[string]store.AppCredential),
		credentialHash: make(map[string]string),
	}
}

func (memory *memoryAppStore) CreateApplication(_ context.Context, app domain.App, credential store.AppCredential) error {
	memory.mu.Lock()
	defer memory.mu.Unlock()
	if _, exists := memory.apps[app.ID]; exists {
		return store.ErrConflict
	}
	if _, exists := memory.credentialHash[credential.APIKeyHash]; exists {
		return store.ErrConflict
	}
	memory.apps[app.ID] = app
	memory.credentials[app.ID] = credential
	memory.credentialHash[credential.APIKeyHash] = app.ID
	return nil
}

func (memory *memoryAppStore) ListApplications(context.Context) ([]domain.App, error) {
	memory.mu.Lock()
	defer memory.mu.Unlock()
	result := make([]domain.App, 0, len(memory.apps))
	for _, app := range memory.apps {
		result = append(result, app)
	}
	return result, nil
}

func (memory *memoryAppStore) GetApplication(_ context.Context, appID string) (domain.App, error) {
	memory.mu.Lock()
	defer memory.mu.Unlock()
	app, exists := memory.apps[appID]
	if !exists {
		return domain.App{}, store.ErrNotFound
	}
	return app, nil
}

func (memory *memoryAppStore) UpdateApplication(_ context.Context, app domain.App) (domain.App, error) {
	if memory.updateStarted != nil {
		close(memory.updateStarted)
		<-memory.continueUpdate
	}
	memory.mu.Lock()
	defer memory.mu.Unlock()
	current, exists := memory.apps[app.ID]
	if !exists {
		return domain.App{}, store.ErrNotFound
	}
	current.Name = app.Name
	current.CallbackURL = app.CallbackURL
	current.DeliveryMode = app.DeliveryMode
	current.UpdatedAt = app.UpdatedAt
	memory.apps[app.ID] = current
	return current, nil
}

func (memory *memoryAppStore) DisableApplication(_ context.Context, appID string, updatedAt time.Time) (domain.App, error) {
	memory.mu.Lock()
	defer memory.mu.Unlock()
	app, exists := memory.apps[appID]
	if !exists {
		return domain.App{}, store.ErrNotFound
	}
	app.Enabled = false
	app.UpdatedAt = updatedAt
	memory.apps[appID] = app
	return app, nil
}

func (memory *memoryAppStore) FindCredentialByAPIKeyHash(_ context.Context, apiKeyHash string) (store.AppCredential, error) {
	memory.mu.Lock()
	defer memory.mu.Unlock()
	appID, exists := memory.credentialHash[apiKeyHash]
	if !exists {
		return store.AppCredential{}, store.ErrNotFound
	}
	return memory.credentials[appID], nil
}

func (memory *memoryAppStore) RotateApplicationCredential(_ context.Context, appID string, credential store.AppCredential, updatedAt time.Time) error {
	memory.mu.Lock()
	defer memory.mu.Unlock()
	app, exists := memory.apps[appID]
	if !exists {
		return store.ErrNotFound
	}
	if _, exists := memory.credentialHash[credential.APIKeyHash]; exists {
		return store.ErrConflict
	}
	old := memory.credentials[appID]
	delete(memory.credentialHash, old.APIKeyHash)
	memory.credentials[appID] = credential
	memory.credentialHash[credential.APIKeyHash] = appID
	app.UpdatedAt = updatedAt
	memory.apps[appID] = app
	return nil
}

func (memory *memoryAppStore) hasPlaintextAPIKey(apiKey string) bool {
	memory.mu.Lock()
	defer memory.mu.Unlock()
	hash := sha256.Sum256([]byte(apiKey))
	for storedHash := range memory.credentialHash {
		if storedHash == apiKey {
			return true
		}
	}
	_, hasExpectedHash := memory.credentialHash[hex.EncodeToString(hash[:])]
	return !hasExpectedHash
}
