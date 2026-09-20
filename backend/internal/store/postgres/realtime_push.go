package postgres

import (
	"context"
	"time"

	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/store"
)

func (client *Client) CreatePushDevice(ctx context.Context, device domain.PushDevice) (domain.PushDevice, error) {
	encrypted, err := client.cipher.Encrypt(device.Token)
	if err != nil {
		return domain.PushDevice{}, err
	}
	var result domain.PushDevice
	var stored string
	err = client.pool.QueryRow(ctx, `INSERT INTO realtime_push_devices(id,app_id,provider,token_hash,encrypted_token,created_at,updated_at) VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(app_id,provider,token_hash) DO UPDATE SET encrypted_token=EXCLUDED.encrypted_token,updated_at=EXCLUDED.updated_at RETURNING id,app_id,provider,token_hash,encrypted_token,created_at,updated_at`, device.ID, device.AppID, device.Provider, device.TokenHash, encrypted, device.CreatedAt, device.UpdatedAt).Scan(&result.ID, &result.AppID, &result.Provider, &result.TokenHash, &stored, &result.CreatedAt, &result.UpdatedAt)
	if err != nil {
		return result, err
	}
	result.Token, err = client.cipher.Decrypt(stored)
	return result, err
}
func (client *Client) DeletePushDevice(ctx context.Context, appID, id string) error {
	tag, err := client.pool.Exec(ctx, `DELETE FROM realtime_push_devices WHERE app_id=$1 AND id=$2`, appID, id)
	if err == nil && tag.RowsAffected() == 0 {
		return store.ErrNotFound
	}
	return err
}
func (client *Client) BindPushDevice(ctx context.Context, appID, channel, deviceID string, at time.Time) error {
	tag, err := client.pool.Exec(ctx, `INSERT INTO realtime_push_bindings(app_id,channel,device_id,created_at) SELECT $1,$2,$3,$4 FROM realtime_push_devices WHERE app_id=$1 AND id=$3 ON CONFLICT DO NOTHING`, appID, channel, deviceID, at)
	if err == nil && tag.RowsAffected() == 0 {
		var exists bool
		if scanErr := client.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM realtime_push_bindings WHERE app_id=$1 AND channel=$2 AND device_id=$3)`, appID, channel, deviceID).Scan(&exists); scanErr == nil && exists {
			return nil
		}
		return store.ErrNotFound
	}
	return err
}
func (client *Client) UnbindPushDevice(ctx context.Context, appID, channel, deviceID string) error {
	tag, err := client.pool.Exec(ctx, `DELETE FROM realtime_push_bindings WHERE app_id=$1 AND channel=$2 AND device_id=$3`, appID, channel, deviceID)
	if err == nil && tag.RowsAffected() == 0 {
		return store.ErrNotFound
	}
	return err
}
func (client *Client) ListPushDevicesForChannel(ctx context.Context, appID, channel string, limit int) ([]domain.PushDevice, error) {
	rows, err := client.pool.Query(ctx, `SELECT d.id,d.app_id,d.provider,d.token_hash,d.encrypted_token,d.created_at,d.updated_at FROM realtime_push_bindings b JOIN realtime_push_devices d ON d.id=b.device_id AND d.app_id=b.app_id WHERE b.app_id=$1 AND b.channel=$2 ORDER BY d.id LIMIT $3`, appID, channel, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []domain.PushDevice{}
	for rows.Next() {
		var device domain.PushDevice
		var encrypted string
		if err := rows.Scan(&device.ID, &device.AppID, &device.Provider, &device.TokenHash, &encrypted, &device.CreatedAt, &device.UpdatedAt); err != nil {
			return nil, err
		}
		device.Token, err = client.cipher.Decrypt(encrypted)
		if err != nil {
			return nil, err
		}
		result = append(result, device)
	}
	return result, rows.Err()
}
func (client *Client) CreatePushOutcome(ctx context.Context, outcome domain.PushOutcome) error {
	_, err := client.pool.Exec(ctx, `INSERT INTO realtime_push_outcomes(id,app_id,device_id,channel,provider,status,provider_message_id,reason,created_at) VALUES($1,$2,$3,$4,$5,$6,NULLIF($7,''),NULLIF($8,''),$9)`, outcome.ID, outcome.AppID, outcome.DeviceID, outcome.Channel, outcome.Provider, outcome.Status, outcome.ProviderMessageID, outcome.Reason, outcome.CreatedAt)
	return err
}
