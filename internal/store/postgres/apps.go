package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/store"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var _ store.ApplicationStore = (*Client)(nil)

func (client *Client) CreateApplication(ctx context.Context, app domain.App, credential store.AppCredential) error {
	if credential.AppID != app.ID {
		return store.ErrConflict
	}
	encrypted, err := client.cipher.Encrypt(credential.HMACSecret)
	if err != nil {
		return err
	}
	tx, err := client.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	_, err = tx.Exec(ctx, `INSERT INTO applications(id,name,callback_url,delivery_mode,enabled,created_at,updated_at) VALUES($1,$2,$3,$4,$5,$6,$7)`, app.ID, app.Name, app.CallbackURL, app.DeliveryMode, app.Enabled, app.CreatedAt, app.UpdatedAt)
	if err == nil {
		_, err = tx.Exec(ctx, `INSERT INTO application_credentials(app_id,api_key_hash,encrypted_hmac_secret,created_at,rotated_at,version) VALUES($1,$2,$3,$4,$4,1)`, app.ID, credential.APIKeyHash, encrypted, app.CreatedAt)
	}
	if err == nil && app.CallbackURL != nil {
		_, err = tx.Exec(ctx, `INSERT INTO callback_endpoints(app_id,url,enabled,created_at,updated_at) VALUES($1,$2,$3,$4,$5)`, app.ID, *app.CallbackURL, app.Enabled, app.CreatedAt, app.UpdatedAt)
	}
	if err != nil {
		if uniqueViolation(err) {
			return store.ErrConflict
		}
		return err
	}
	return tx.Commit(ctx)
}

func (client *Client) ListApplications(ctx context.Context) ([]domain.App, error) {
	rows, err := client.pool.Query(ctx, `SELECT id,name,callback_url,delivery_mode,enabled,created_at,updated_at FROM applications ORDER BY created_at,id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	apps := make([]domain.App, 0)
	for rows.Next() {
		app, err := scanApplication(rows)
		if err != nil {
			return nil, err
		}
		apps = append(apps, app)
	}
	return apps, rows.Err()
}

func (client *Client) GetApplication(ctx context.Context, appID string) (domain.App, error) {
	app, err := scanApplication(client.pool.QueryRow(ctx, `SELECT id,name,callback_url,delivery_mode,enabled,created_at,updated_at FROM applications WHERE id=$1`, appID))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.App{}, store.ErrNotFound
	}
	return app, err
}

func (client *Client) UpdateApplication(ctx context.Context, app domain.App) (domain.App, error) {
	return client.updateApplication(ctx, domain.App{}, app, false)
}

func (client *Client) CompareAndSwapApplication(ctx context.Context, expected, app domain.App) (domain.App, error) {
	if expected.ID != app.ID {
		return domain.App{}, store.ErrConflict
	}
	return client.updateApplication(ctx, expected, app, true)
}

func (client *Client) updateApplication(ctx context.Context, expected, app domain.App, compare bool) (domain.App, error) {
	tx, err := client.pool.Begin(ctx)
	if err != nil {
		return domain.App{}, err
	}
	defer tx.Rollback(ctx)
	query := `UPDATE applications SET name=$2,callback_url=$3,delivery_mode=$4,updated_at=GREATEST(updated_at,$5) WHERE id=$1`
	args := []any{app.ID, app.Name, app.CallbackURL, app.DeliveryMode, app.UpdatedAt}
	if compare {
		query += ` AND name=$6 AND callback_url IS NOT DISTINCT FROM $7 AND delivery_mode=$8`
		args = append(args, expected.Name, expected.CallbackURL, expected.DeliveryMode)
	}
	query += ` RETURNING id,name,callback_url,delivery_mode,enabled,created_at,updated_at`
	updated, err := scanApplication(tx.QueryRow(ctx, query, args...))
	if errors.Is(err, pgx.ErrNoRows) {
		var exists bool
		if e := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM applications WHERE id=$1)`, app.ID).Scan(&exists); e != nil {
			return domain.App{}, e
		}
		if exists && compare {
			return domain.App{}, store.ErrConflict
		}
		return domain.App{}, store.ErrNotFound
	}
	if err != nil {
		return domain.App{}, err
	}
	if err := syncCallback(ctx, tx, updated); err != nil {
		return domain.App{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.App{}, err
	}
	return updated, nil
}

func (client *Client) DisableApplication(ctx context.Context, appID string, updatedAt time.Time) (domain.App, error) {
	tx, err := client.pool.Begin(ctx)
	if err != nil {
		return domain.App{}, err
	}
	defer tx.Rollback(ctx)
	app, err := scanApplication(tx.QueryRow(ctx, `UPDATE applications SET enabled=false,updated_at=GREATEST(updated_at,$2) WHERE id=$1 RETURNING id,name,callback_url,delivery_mode,enabled,created_at,updated_at`, appID, updatedAt))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.App{}, store.ErrNotFound
	}
	if err != nil {
		return domain.App{}, err
	}
	if _, err := tx.Exec(ctx, `UPDATE callback_endpoints SET enabled=false,updated_at=GREATEST(updated_at,$2) WHERE app_id=$1`, appID, updatedAt); err != nil {
		return domain.App{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.App{}, err
	}
	return app, nil
}

func (client *Client) FindCredentialByAPIKeyHash(ctx context.Context, apiKeyHash string) (store.AppCredential, error) {
	var credential store.AppCredential
	var encrypted string
	err := client.pool.QueryRow(ctx, `SELECT app_id,api_key_hash,encrypted_hmac_secret,version,revoked_at FROM application_credentials WHERE api_key_hash=$1 AND revoked_at IS NULL`, apiKeyHash).Scan(&credential.AppID, &credential.APIKeyHash, &encrypted, &credential.Version, &credential.RevokedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return store.AppCredential{}, store.ErrNotFound
	}
	if err != nil {
		return store.AppCredential{}, err
	}
	credential.HMACSecret, err = client.cipher.Decrypt(encrypted)
	if err != nil {
		return store.AppCredential{}, err
	}
	return credential, nil
}

func (client *Client) RotateApplicationCredential(ctx context.Context, appID string, credential store.AppCredential, updatedAt time.Time) error {
	if credential.AppID != appID {
		return store.ErrConflict
	}
	encrypted, err := client.cipher.Encrypt(credential.HMACSecret)
	if err != nil {
		return err
	}
	tx, err := client.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var nextVersion int64
	var lockedAppID string
	err = tx.QueryRow(ctx, `SELECT id FROM applications WHERE id=$1 FOR UPDATE`, appID).Scan(&lockedAppID)
	if errors.Is(err, pgx.ErrNoRows) {
		return store.ErrNotFound
	}
	if err != nil {
		return err
	}
	err = tx.QueryRow(ctx, `SELECT COALESCE(MAX(version),0)+1 FROM application_credentials WHERE app_id=$1`, appID).Scan(&nextVersion)
	if err != nil {
		return err
	}
	result, err := tx.Exec(ctx, `UPDATE application_credentials SET revoked_at=$2 WHERE app_id=$1 AND revoked_at IS NULL`, appID, updatedAt)
	if err != nil {
		if uniqueViolation(err) {
			return store.ErrConflict
		}
		return err
	}
	if result.RowsAffected() == 0 {
		return store.ErrNotFound
	}
	_, err = tx.Exec(ctx, `INSERT INTO application_credentials(app_id,api_key_hash,encrypted_hmac_secret,created_at,rotated_at,version) SELECT $1,$2,$3,created_at,$4,$5 FROM application_credentials WHERE app_id=$1 ORDER BY version LIMIT 1`, appID, credential.APIKeyHash, encrypted, updatedAt, nextVersion)
	if err != nil {
		if uniqueViolation(err) {
			return store.ErrConflict
		}
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE applications SET updated_at=GREATEST(updated_at,$2) WHERE id=$1`, appID, updatedAt); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (client *Client) CurrentCredential(ctx context.Context, appID string) (store.AppCredential, error) {
	var credential store.AppCredential
	var encrypted string
	err := client.pool.QueryRow(ctx, `SELECT app_id,api_key_hash,encrypted_hmac_secret,version,revoked_at FROM application_credentials WHERE app_id=$1 AND revoked_at IS NULL`, appID).Scan(&credential.AppID, &credential.APIKeyHash, &encrypted, &credential.Version, &credential.RevokedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return store.AppCredential{}, store.ErrNotFound
	}
	if err != nil {
		return store.AppCredential{}, err
	}
	credential.HMACSecret, err = client.cipher.Decrypt(encrypted)
	return credential, err
}

type rowScanner interface{ Scan(...any) error }

func scanApplication(row rowScanner) (domain.App, error) {
	var app domain.App
	err := row.Scan(&app.ID, &app.Name, &app.CallbackURL, &app.DeliveryMode, &app.Enabled, &app.CreatedAt, &app.UpdatedAt)
	return app, err
}

func syncCallback(ctx context.Context, tx pgx.Tx, app domain.App) error {
	if app.CallbackURL == nil {
		_, err := tx.Exec(ctx, `DELETE FROM callback_endpoints WHERE app_id=$1`, app.ID)
		return err
	}
	_, err := tx.Exec(ctx, `INSERT INTO callback_endpoints(app_id,url,enabled,created_at,updated_at) VALUES($1,$2,$3,$4,$5) ON CONFLICT(app_id) DO UPDATE SET url=EXCLUDED.url,enabled=EXCLUDED.enabled,updated_at=GREATEST(callback_endpoints.updated_at,EXCLUDED.updated_at)`, app.ID, *app.CallbackURL, app.Enabled, app.CreatedAt, app.UpdatedAt)
	return err
}

func uniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}
