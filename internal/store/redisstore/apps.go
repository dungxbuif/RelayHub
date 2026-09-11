package redisstore

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/store"
	"github.com/redis/go-redis/v9"
)

const (
	applicationsKey   = "relayhub:apps"
	applicationPrefix = "relayhub:app:"
	credentialPrefix  = "relayhub:credential:"
)

var createApplicationScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 1 or redis.call('EXISTS', KEYS[2]) == 1 then
  return 0
end
redis.call('HSET', KEYS[1],
  'id', ARGV[1],
  'name', ARGV[2],
  'callback_url', ARGV[3],
  'delivery_mode', ARGV[4],
  'enabled', ARGV[5],
  'created_at', ARGV[6],
  'updated_at', ARGV[7],
  'api_key_hash', ARGV[8],
  'hmac_secret', ARGV[9])
redis.call('SET', KEYS[2], ARGV[1])
redis.call('SADD', KEYS[3], ARGV[1])
return 1
`)

var updateApplicationScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 0 then
  return {}
end
redis.call('HSET', KEYS[1],
  'name', ARGV[1],
  'callback_url', ARGV[2],
  'delivery_mode', ARGV[3],
  'updated_at', ARGV[4])
return redis.call('HGETALL', KEYS[1])
`)

var disableApplicationScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 0 then
  return 0
end
redis.call('HSET', KEYS[1], 'enabled', '0', 'updated_at', ARGV[1])
return 1
`)

var rotateApplicationCredentialScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 0 then
  return -1
end
if redis.call('EXISTS', KEYS[2]) == 1 then
  return 0
end
local old_hash = redis.call('HGET', KEYS[1], 'api_key_hash')
if old_hash then
  redis.call('DEL', ARGV[1] .. old_hash)
end
redis.call('SET', KEYS[2], ARGV[2])
redis.call('HSET', KEYS[1],
  'api_key_hash', ARGV[3],
  'hmac_secret', ARGV[4],
  'updated_at', ARGV[5])
return 1
`)

var findCredentialScript = redis.NewScript(`
local app_id = redis.call('GET', KEYS[1])
if not app_id then
  return {}
end
local app_key = ARGV[1] .. app_id
if redis.call('HGET', app_key, 'api_key_hash') ~= ARGV[2] then
  return {}
end
local secret = redis.call('HGET', app_key, 'hmac_secret')
if not secret then
  return {}
end
return {app_id, secret}
`)

func (client *Client) CreateApplication(ctx context.Context, app domain.App, credential store.AppCredential) error {
	result, err := createApplicationScript.Run(ctx, client.client,
		[]string{client.applicationKey(app.ID), client.credentialKey(credential.APIKeyHash), client.key(applicationsKey)},
		app.ID,
		app.Name,
		callbackValue(app.CallbackURL),
		string(app.DeliveryMode),
		boolValue(app.Enabled),
		app.CreatedAt.UTC().Format(time.RFC3339Nano),
		app.UpdatedAt.UTC().Format(time.RFC3339Nano),
		credential.APIKeyHash,
		credential.HMACSecret,
	).Int()
	if err != nil {
		return err
	}
	if result == 0 {
		return store.ErrConflict
	}
	return nil
}

func (client *Client) ListApplications(ctx context.Context) ([]domain.App, error) {
	ids, err := client.client.SMembers(ctx, client.key(applicationsKey)).Result()
	if err != nil {
		return nil, err
	}
	apps := make([]domain.App, 0, len(ids))
	for _, id := range ids {
		app, err := client.GetApplication(ctx, id)
		if errors.Is(err, store.ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		apps = append(apps, app)
	}
	return apps, nil
}

func (client *Client) GetApplication(ctx context.Context, appID string) (domain.App, error) {
	values, err := client.client.HGetAll(ctx, client.applicationKey(appID)).Result()
	if err != nil {
		return domain.App{}, err
	}
	if len(values) == 0 {
		return domain.App{}, store.ErrNotFound
	}
	return decodeApplication(values)
}

func (client *Client) UpdateApplication(ctx context.Context, app domain.App) (domain.App, error) {
	result, err := updateApplicationScript.Run(ctx, client.client, []string{client.applicationKey(app.ID)},
		app.Name,
		callbackValue(app.CallbackURL),
		string(app.DeliveryMode),
		app.UpdatedAt.UTC().Format(time.RFC3339Nano),
	).StringSlice()
	if err != nil {
		return domain.App{}, err
	}
	if len(result) == 0 {
		return domain.App{}, store.ErrNotFound
	}
	values := make(map[string]string, len(result)/2)
	for index := 0; index+1 < len(result); index += 2 {
		values[result[index]] = result[index+1]
	}
	return decodeApplication(values)
}

func (client *Client) DisableApplication(ctx context.Context, appID string, updatedAt time.Time) (domain.App, error) {
	result, err := disableApplicationScript.Run(ctx, client.client, []string{client.applicationKey(appID)}, updatedAt.UTC().Format(time.RFC3339Nano)).Int()
	if err != nil {
		return domain.App{}, err
	}
	if result == 0 {
		return domain.App{}, store.ErrNotFound
	}
	return client.GetApplication(ctx, appID)
}

func (client *Client) FindCredentialByAPIKeyHash(ctx context.Context, apiKeyHash string) (store.AppCredential, error) {
	result, err := findCredentialScript.Run(ctx, client.client, []string{client.credentialKey(apiKeyHash)}, client.key(applicationPrefix), apiKeyHash).StringSlice()
	if err != nil {
		return store.AppCredential{}, err
	}
	if len(result) != 2 {
		return store.AppCredential{}, store.ErrNotFound
	}
	return store.AppCredential{AppID: result[0], APIKeyHash: apiKeyHash, HMACSecret: []byte(result[1])}, nil
}

func (client *Client) RotateApplicationCredential(ctx context.Context, appID string, credential store.AppCredential, updatedAt time.Time) error {
	result, err := rotateApplicationCredentialScript.Run(ctx, client.client,
		[]string{client.applicationKey(appID), client.credentialKey(credential.APIKeyHash)},
		client.key(credentialPrefix),
		appID,
		credential.APIKeyHash,
		credential.HMACSecret,
		updatedAt.UTC().Format(time.RFC3339Nano),
	).Int()
	if err != nil {
		return err
	}
	switch result {
	case -1:
		return store.ErrNotFound
	case 0:
		return store.ErrConflict
	default:
		return nil
	}
}

func decodeApplication(values map[string]string) (domain.App, error) {
	createdAt, err := time.Parse(time.RFC3339Nano, values["created_at"])
	if err != nil {
		return domain.App{}, fmt.Errorf("decode application created_at: %w", err)
	}
	updatedAt, err := time.Parse(time.RFC3339Nano, values["updated_at"])
	if err != nil {
		return domain.App{}, fmt.Errorf("decode application updated_at: %w", err)
	}
	enabled, err := strconv.ParseBool(values["enabled"])
	if err != nil {
		return domain.App{}, fmt.Errorf("decode application enabled: %w", err)
	}
	var callbackURL *string
	if callback := values["callback_url"]; callback != "" {
		callbackURL = &callback
	}
	return domain.App{
		ID:           values["id"],
		Name:         values["name"],
		CallbackURL:  callbackURL,
		DeliveryMode: domain.DeliveryMode(values["delivery_mode"]),
		Enabled:      enabled,
		CreatedAt:    createdAt,
		UpdatedAt:    updatedAt,
	}, nil
}

func applicationKey(appID string) string {
	return applicationPrefix + appID
}

func credentialKey(hash string) string {
	return credentialPrefix + hash
}

func callbackValue(callbackURL *string) string {
	if callbackURL == nil {
		return ""
	}
	return *callbackURL
}

func boolValue(value bool) string {
	if value {
		return "1"
	}
	return "0"
}
