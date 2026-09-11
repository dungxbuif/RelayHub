package postgres

import (
	"context"
	"errors"

	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/store"
	"github.com/jackc/pgx/v5"
)

var _ store.FunctionCatalog = (*Client)(nil)

func (client *Client) CreateFunction(ctx context.Context, function domain.Function) error {
	result, err := client.pool.Exec(ctx, `INSERT INTO functions(id,app_id,name,timeout_seconds,enabled,created_at,updated_at) SELECT $1,$2,$3,$4,$5,$6,$7 FROM applications WHERE id=$2 AND enabled=true`, function.ID, function.AppID, function.Name, function.TimeoutSeconds, function.Enabled, function.CreatedAt, function.UpdatedAt)
	if err != nil {
		if uniqueViolation(err) {
			return store.ErrConflict
		}
		return err
	}
	if result.RowsAffected() == 0 {
		return store.ErrNotFound
	}
	return nil
}

func (client *Client) GetFunction(ctx context.Context, functionID string) (domain.Function, error) {
	function, err := scanFunction(client.pool.QueryRow(ctx, `SELECT id,app_id,name,timeout_seconds,enabled,created_at,updated_at FROM functions WHERE id=$1`, functionID))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Function{}, store.ErrNotFound
	}
	return function, err
}

func (client *Client) ListFunctions(ctx context.Context, ownerAppID string) ([]domain.Function, error) {
	rows, err := client.pool.Query(ctx, `SELECT id,app_id,name,timeout_seconds,enabled,created_at,updated_at FROM functions WHERE app_id=$1 ORDER BY created_at,id`, ownerAppID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	functions := make([]domain.Function, 0)
	for rows.Next() {
		function, err := scanFunction(rows)
		if err != nil {
			return nil, err
		}
		functions = append(functions, function)
	}
	return functions, rows.Err()
}

func (client *Client) DeleteFunction(ctx context.Context, ownerAppID, functionID string) error {
	result, err := client.pool.Exec(ctx, `DELETE FROM functions WHERE id=$1 AND app_id=$2`, functionID, ownerAppID)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		return store.ErrNotFound
	}
	return nil
}

func scanFunction(row rowScanner) (domain.Function, error) {
	var function domain.Function
	err := row.Scan(&function.ID, &function.AppID, &function.Name, &function.TimeoutSeconds, &function.Enabled, &function.CreatedAt, &function.UpdatedAt)
	return function, err
}
