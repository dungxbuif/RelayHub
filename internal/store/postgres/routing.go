package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/store"
	"github.com/jackc/pgx/v5"
)

var _ store.RoutingRuleStore = (*Client)(nil)

func (client *Client) CreateRoutingRule(ctx context.Context, rule domain.RoutingRule) error {
	_, err := client.pool.Exec(ctx, `INSERT INTO routing_rules(id,source_app_id,event_type,target_app_id,realtime_channel,enabled,created_at,updated_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8)`, rule.ID, rule.SourceAppID, rule.EventType, rule.TargetAppID, rule.RealtimeChannel, rule.Enabled, rule.CreatedAt, rule.UpdatedAt)
	if err != nil {
		if uniqueViolation(err) {
			return store.ErrConflict
		}
		if foreignKeyViolation(err) {
			return store.ErrInvalidTarget
		}
	}
	return err
}

func (client *Client) ListRoutingRules(ctx context.Context) ([]domain.RoutingRule, error) {
	rows, err := client.pool.Query(ctx, `SELECT id,source_app_id,event_type,target_app_id,realtime_channel,enabled,created_at,updated_at,deleted_at FROM routing_rules WHERE deleted_at IS NULL ORDER BY created_at,id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var rules []domain.RoutingRule
	for rows.Next() {
		rule, err := scanRoutingRule(rows)
		if err != nil {
			return nil, err
		}
		rules = append(rules, rule)
	}
	return rules, rows.Err()
}

func (client *Client) GetRoutingRule(ctx context.Context, id string) (domain.RoutingRule, error) {
	rule, err := scanRoutingRule(client.pool.QueryRow(ctx, `SELECT id,source_app_id,event_type,target_app_id,realtime_channel,enabled,created_at,updated_at,deleted_at FROM routing_rules WHERE id=$1 AND deleted_at IS NULL`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.RoutingRule{}, store.ErrNotFound
	}
	return rule, err
}

func (client *Client) UpdateRoutingRule(ctx context.Context, rule domain.RoutingRule) (domain.RoutingRule, error) {
	updated, err := scanRoutingRule(client.pool.QueryRow(ctx, `UPDATE routing_rules SET source_app_id=$2,event_type=$3,target_app_id=$4,realtime_channel=$5,enabled=$6,updated_at=GREATEST(updated_at,$7) WHERE id=$1 AND deleted_at IS NULL RETURNING id,source_app_id,event_type,target_app_id,realtime_channel,enabled,created_at,updated_at,deleted_at`, rule.ID, rule.SourceAppID, rule.EventType, rule.TargetAppID, rule.RealtimeChannel, rule.Enabled, rule.UpdatedAt))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.RoutingRule{}, store.ErrNotFound
	}
	if err != nil {
		if foreignKeyViolation(err) {
			return domain.RoutingRule{}, store.ErrInvalidTarget
		}
		return domain.RoutingRule{}, err
	}
	return updated, nil
}

func (client *Client) DeleteRoutingRule(ctx context.Context, id string, now time.Time) error {
	result, err := client.pool.Exec(ctx, `UPDATE routing_rules SET deleted_at=$2,updated_at=GREATEST(updated_at,$2) WHERE id=$1 AND deleted_at IS NULL`, id, now)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		return store.ErrNotFound
	}
	return nil
}

func (client *Client) ResolveRoutingRules(ctx context.Context, sourceAppID, eventType string) ([]domain.RoutingRule, error) {
	rows, err := client.pool.Query(ctx, `SELECT id,source_app_id,event_type,target_app_id,realtime_channel,enabled,created_at,updated_at,deleted_at FROM routing_rules WHERE deleted_at IS NULL AND enabled=true AND event_type=$1 AND (source_app_id IS NULL OR source_app_id=$2) ORDER BY source_app_id NULLS LAST, created_at, id`, eventType, sourceAppID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var rules []domain.RoutingRule
	for rows.Next() {
		rule, err := scanRoutingRule(rows)
		if err != nil {
			return nil, err
		}
		rules = append(rules, rule)
	}
	return rules, rows.Err()
}

func scanRoutingRule(row rowScanner) (domain.RoutingRule, error) {
	var rule domain.RoutingRule
	err := row.Scan(&rule.ID, &rule.SourceAppID, &rule.EventType, &rule.TargetAppID, &rule.RealtimeChannel, &rule.Enabled, &rule.CreatedAt, &rule.UpdatedAt, &rule.DeletedAt)
	return rule, err
}
