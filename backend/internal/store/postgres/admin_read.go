package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"

	"github.com/dungxbuif/RelayHub/internal/adminread"
	"github.com/dungxbuif/RelayHub/internal/store"
	"github.com/jackc/pgx/v5"
)

func (client *Client) ListAdminEvents(ctx context.Context, query adminread.EventListQuery) (adminread.Page[adminread.EventSummary], error) {
	if err := query.Options.ValidateCursor(query.Filters.Fingerprint()); err != nil {
		return adminread.Page[adminread.EventSummary]{}, err
	}
	if err := query.Filters.Validate(); err != nil {
		return adminread.Page[adminread.EventSummary]{}, err
	}
	var cursorAt any
	var cursorID string
	if query.Options.Cursor != nil {
		cursorAt, cursorID = query.Options.Cursor.Timestamp, query.Options.Cursor.ID
	}
	limit := query.Options.NormalizedLimit()
	rows, err := client.pool.Query(ctx, `
		SELECT e.id,e.type,e.source_app_id,cardinality(e.target_app_ids),count(d.id),e.created_at
		FROM events e LEFT JOIN deliveries d ON d.event_id=e.id
		WHERE ($1='' OR e.type=$1) AND ($2='' OR e.source_app_id=$2)
		  AND ($3::timestamptz IS NULL OR e.created_at >= $3)
		  AND ($4::timestamptz IS NULL OR e.created_at <= $4)
		  AND ($5::timestamptz IS NULL OR (e.created_at,e.id) < ($5,$6))
		GROUP BY e.id
		ORDER BY e.created_at DESC,e.id DESC LIMIT $7`, query.Filters.Type, query.Filters.SourceAppID,
		query.Filters.From, query.Filters.To, cursorAt, cursorID, limit+1)
	if err != nil {
		return adminread.Page[adminread.EventSummary]{}, err
	}
	defer rows.Close()
	items := make([]adminread.EventSummary, 0, limit+1)
	for rows.Next() {
		var item adminread.EventSummary
		if err := rows.Scan(&item.ID, &item.Type, &item.SourceAppID, &item.TargetCount, &item.DeliveryCount, &item.CreatedAt); err != nil {
			return adminread.Page[adminread.EventSummary]{}, err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return adminread.Page[adminread.EventSummary]{}, err
	}
	return eventPage(items, limit, query.Filters.Fingerprint())
}

func eventPage(items []adminread.EventSummary, limit int, fingerprint string) (adminread.Page[adminread.EventSummary], error) {
	page := adminread.Page[adminread.EventSummary]{Items: items}
	if len(items) <= limit {
		return page, nil
	}
	page.Items = items[:limit]
	last := page.Items[len(page.Items)-1]
	next, err := adminread.EncodeCursor(adminread.Cursor{Timestamp: last.CreatedAt, ID: last.ID, FilterFingerprint: fingerprint})
	if err != nil {
		return adminread.Page[adminread.EventSummary]{}, err
	}
	page.NextCursor = next
	return page, nil
}

func (client *Client) GetAdminEvent(ctx context.Context, eventID string) (adminread.EventDetail, error) {
	if eventID == "" || len(eventID) > 256 {
		return adminread.EventDetail{}, adminread.ErrInvalidArgument
	}
	var result adminread.EventDetail
	var data []byte
	err := client.pool.QueryRow(ctx, `SELECT e.id,e.type,e.source_app_id,cardinality(e.target_app_ids),count(d.id),e.created_at,e.target_app_ids,e.data::text FROM events e LEFT JOIN deliveries d ON d.event_id=e.id WHERE e.id=$1 GROUP BY e.id`, eventID).Scan(
		&result.ID, &result.Type, &result.SourceAppID, &result.TargetCount, &result.DeliveryCount, &result.CreatedAt, &result.TargetAppIDs, &data)
	if errors.Is(err, pgx.ErrNoRows) {
		return adminread.EventDetail{}, store.ErrNotFound
	}
	if err != nil {
		return adminread.EventDetail{}, err
	}
	result.Data = json.RawMessage(data)
	return result, nil
}

func (client *Client) ListAdminDeadLetters(ctx context.Context, query adminread.DeadLetterListQuery) (adminread.Page[adminread.DeadLetterSummary], error) {
	if err := query.Options.ValidateCursor(query.Filters.Fingerprint()); err != nil {
		return adminread.Page[adminread.DeadLetterSummary]{}, err
	}
	if err := query.Filters.Validate(); err != nil {
		return adminread.Page[adminread.DeadLetterSummary]{}, err
	}
	var cursorAt any
	var cursorID string
	if query.Options.Cursor != nil {
		cursorAt, cursorID = query.Options.Cursor.Timestamp, query.Options.Cursor.ID
	}
	limit := query.Options.NormalizedLimit()
	rows, err := client.pool.Query(ctx, `
		SELECT d.id,d.public_job_id,d.event_id,d.source_app_id,d.target_app_id,d.sink,
		       COALESCE(d.callback_reason,o.last_error,a.reason,'unknown'),d.attempts,d.created_at,d.updated_at
		FROM deliveries d
		LEFT JOIN outbox o ON o.delivery_id=d.id
		LEFT JOIN LATERAL (SELECT reason FROM delivery_attempts WHERE delivery_id=d.id ORDER BY attempt DESC LIMIT 1) a ON true
		WHERE d.status='dead_letter' AND ($1='' OR d.source_app_id=$1) AND ($2='' OR d.target_app_id=$2)
		  AND ($3='' OR d.sink=$3) AND ($4='' OR COALESCE(d.callback_reason,o.last_error,a.reason,'unknown')=$4)
		  AND ($5::timestamptz IS NULL OR d.updated_at >= $5) AND ($6::timestamptz IS NULL OR d.updated_at <= $6)
		  AND ($7::timestamptz IS NULL OR (d.updated_at,d.id) < ($7,$8))
		ORDER BY d.updated_at DESC,d.id DESC LIMIT $9`, query.Filters.SourceAppID, query.Filters.TargetAppID,
		query.Filters.Sink, query.Filters.Reason, query.Filters.From, query.Filters.To, cursorAt, cursorID, limit+1)
	if err != nil {
		return adminread.Page[adminread.DeadLetterSummary]{}, err
	}
	defer rows.Close()
	items := make([]adminread.DeadLetterSummary, 0, limit+1)
	for rows.Next() {
		var item adminread.DeadLetterSummary
		if err := rows.Scan(&item.DeliveryID, &item.JobID, &item.EventID, &item.SourceAppID, &item.TargetAppID, &item.Sink, &item.Reason, &item.Attempts, &item.CreatedAt, &item.UpdatedAt); err != nil {
			return adminread.Page[adminread.DeadLetterSummary]{}, err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return adminread.Page[adminread.DeadLetterSummary]{}, err
	}
	page := adminread.Page[adminread.DeadLetterSummary]{Items: items}
	if len(items) > limit {
		page.Items = items[:limit]
		last := page.Items[len(page.Items)-1]
		page.NextCursor, err = adminread.EncodeCursor(adminread.Cursor{Timestamp: last.UpdatedAt, ID: last.DeliveryID, FilterFingerprint: query.Filters.Fingerprint()})
		if err != nil {
			return adminread.Page[adminread.DeadLetterSummary]{}, err
		}
	}
	return page, nil
}

func (client *Client) GetAdminDeadLetter(ctx context.Context, deliveryID string) (adminread.DeadLetterDetail, error) {
	if deliveryID == "" || len(deliveryID) > 256 {
		return adminread.DeadLetterDetail{}, adminread.ErrInvalidArgument
	}
	var result adminread.DeadLetterDetail
	err := client.pool.QueryRow(ctx, `
		SELECT d.id,d.public_job_id,d.event_id,d.source_app_id,d.target_app_id,d.sink,
		       COALESCE(d.callback_reason,o.last_error,a.reason,'unknown'),d.attempts,d.created_at,d.updated_at,e.type
		FROM deliveries d JOIN events e ON e.id=d.event_id LEFT JOIN outbox o ON o.delivery_id=d.id
		LEFT JOIN LATERAL (SELECT reason FROM delivery_attempts WHERE delivery_id=d.id ORDER BY attempt DESC LIMIT 1) a ON true
		WHERE d.id=$1 AND d.status='dead_letter'`, deliveryID).Scan(&result.DeliveryID, &result.JobID, &result.EventID,
		&result.SourceAppID, &result.TargetAppID, &result.Sink, &result.Reason, &result.Attempts, &result.CreatedAt, &result.UpdatedAt, &result.EventType)
	if errors.Is(err, pgx.ErrNoRows) {
		return adminread.DeadLetterDetail{}, store.ErrNotFound
	}
	return result, err
}

func (client *Client) ListAdminAudit(ctx context.Context, query adminread.AuditListQuery) (adminread.Page[adminread.AuditSummary], error) {
	if err := query.Options.ValidateCursor(query.Filters.Fingerprint()); err != nil {
		return adminread.Page[adminread.AuditSummary]{}, err
	}
	if err := query.Filters.Validate(); err != nil {
		return adminread.Page[adminread.AuditSummary]{}, err
	}
	var cursorAt any
	var cursorID any
	if query.Options.Cursor != nil {
		parsed, err := strconv.ParseInt(query.Options.Cursor.ID, 10, 64)
		if err != nil || parsed < 1 {
			return adminread.Page[adminread.AuditSummary]{}, adminread.ErrInvalidCursor
		}
		cursorAt, cursorID = query.Options.Cursor.Timestamp, parsed
	}
	limit := query.Options.NormalizedLimit()
	rows, err := client.pool.Query(ctx, `
		SELECT id,occurred_at,actor_type,COALESCE(actor_id,''),action,resource_type,COALESCE(resource_id,''),outcome,metadata
		FROM audit_log WHERE ($1='' OR actor_type=$1) AND ($2='' OR action=$2) AND ($3='' OR resource_type=$3)
		  AND ($4='' OR resource_id=$4) AND ($5='' OR outcome=$5)
		  AND ($6::timestamptz IS NULL OR occurred_at >= $6) AND ($7::timestamptz IS NULL OR occurred_at <= $7)
		  AND ($8::timestamptz IS NULL OR (occurred_at,id) < ($8,$9))
		ORDER BY occurred_at DESC,id DESC LIMIT $10`, query.Filters.ActorType, query.Filters.Action, query.Filters.ResourceType,
		query.Filters.ResourceID, query.Filters.Outcome, query.Filters.From, query.Filters.To, cursorAt, cursorID, limit+1)
	if err != nil {
		return adminread.Page[adminread.AuditSummary]{}, err
	}
	defer rows.Close()
	items := make([]adminread.AuditSummary, 0, limit+1)
	for rows.Next() {
		var item adminread.AuditSummary
		if err := rows.Scan(&item.ID, &item.OccurredAt, &item.ActorType, &item.ActorID, &item.Action, &item.ResourceType, &item.ResourceID, &item.Outcome, &item.Metadata); err != nil {
			return adminread.Page[adminread.AuditSummary]{}, err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return adminread.Page[adminread.AuditSummary]{}, err
	}
	page := adminread.Page[adminread.AuditSummary]{Items: items}
	if len(items) > limit {
		page.Items = items[:limit]
		last := page.Items[len(page.Items)-1]
		page.NextCursor, err = adminread.EncodeCursor(adminread.Cursor{Timestamp: last.OccurredAt, ID: strconv.FormatInt(last.ID, 10), FilterFingerprint: query.Filters.Fingerprint()})
		if err != nil {
			return adminread.Page[adminread.AuditSummary]{}, err
		}
	}
	return page, nil
}

func (client *Client) AdminDurableCounts(ctx context.Context) (adminread.DurableCounts, error) {
	var result adminread.DurableCounts
	err := client.pool.QueryRow(ctx, `SELECT
		count(*) FILTER (WHERE status IN ('pending','dispatched')),
		count(*) FILTER (WHERE status='retrying'),
		count(*) FILTER (WHERE status='dead_letter'),
		min(created_at) FILTER (WHERE status IN ('pending','dispatched','retrying')),
		percentile_cont(0.50) WITHIN GROUP (ORDER BY EXTRACT(epoch FROM (updated_at-created_at))*1000) FILTER (WHERE status IN ('delivered','acked')),
		percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(epoch FROM (updated_at-created_at))*1000) FILTER (WHERE status IN ('delivered','acked')),
		percentile_cont(0.99) WITHIN GROUP (ORDER BY EXTRACT(epoch FROM (updated_at-created_at))*1000) FILTER (WHERE status IN ('delivered','acked'))
		FROM deliveries`).Scan(&result.Pending, &result.Retrying, &result.DeadLetter, &result.OldestPendingAt,
		&result.DeliveryLatencyP50MS, &result.DeliveryLatencyP95MS, &result.DeliveryLatencyP99MS)
	return result, err
}

var _ store.AdminReadStore = (*Client)(nil)
