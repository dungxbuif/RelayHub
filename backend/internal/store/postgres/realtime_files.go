package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/store"
	"github.com/jackc/pgx/v5"
)

const realtimeFileColumns = `id, app_id, channel, name, mime_type, size_bytes, sha256, object_key, status, created_at, expires_at, completed_at`

func (client *Client) CreateRealtimeFile(ctx context.Context, file domain.RealtimeFile) error {
	_, err := client.pool.Exec(ctx, `INSERT INTO realtime_files (`+realtimeFileColumns+`) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`, file.ID, file.AppID, file.Channel, file.Name, file.MIMEType, file.SizeBytes, file.SHA256, file.ObjectKey, file.Status, file.CreatedAt, file.ExpiresAt, file.CompletedAt)
	if uniqueViolation(err) {
		return store.ErrConflict
	}
	return err
}

func (client *Client) GetRealtimeFile(ctx context.Context, appID, id string) (domain.RealtimeFile, error) {
	return scanRealtimeFile(client.pool.QueryRow(ctx, `SELECT `+realtimeFileColumns+` FROM realtime_files WHERE app_id=$1 AND id=$2`, appID, id))
}

func (client *Client) CompleteRealtimeFile(ctx context.Context, appID, id string, at time.Time) (domain.RealtimeFile, error) {
	file, err := scanRealtimeFile(client.pool.QueryRow(ctx, `UPDATE realtime_files SET status='ready', completed_at=$3 WHERE app_id=$1 AND id=$2 AND status='pending' AND expires_at>$3 RETURNING `+realtimeFileColumns, appID, id, at))
	if errors.Is(err, store.ErrNotFound) {
		if _, getErr := client.GetRealtimeFile(ctx, appID, id); getErr == nil {
			return domain.RealtimeFile{}, store.ErrConflict
		}
	}
	return file, err
}

type realtimeFileRow interface{ Scan(...any) error }

func scanRealtimeFile(row realtimeFileRow) (domain.RealtimeFile, error) {
	var file domain.RealtimeFile
	err := row.Scan(&file.ID, &file.AppID, &file.Channel, &file.Name, &file.MIMEType, &file.SizeBytes, &file.SHA256, &file.ObjectKey, &file.Status, &file.CreatedAt, &file.ExpiresAt, &file.CompletedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return file, store.ErrNotFound
	}
	return file, err
}
