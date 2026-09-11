package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	natsbroker "github.com/dungxbuif/RelayHub/internal/broker/nats"
	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/store"
	"github.com/jackc/pgx/v5"
)

type targetPolicy struct {
	mode        domain.DeliveryMode
	callbackURL *string
}

type outboxEnvelope struct {
	DeliveryID string       `json:"delivery_id"`
	Event      domain.Event `json:"event"`
}

func (client *Client) FindPublication(ctx context.Context, source, key string) (store.Publication, error) {
	var raw []byte
	err := client.pool.QueryRow(ctx, `SELECT publication FROM event_idempotency WHERE source_app_id=$1 AND key_hash=$2 AND expires_at>clock_timestamp()`, source, idempotencyHash(source, key)).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return store.Publication{}, store.ErrNotFound
	}
	if err != nil {
		return store.Publication{}, err
	}
	var publication store.Publication
	if err := json.Unmarshal(raw, &publication); err != nil {
		return store.Publication{}, fmt.Errorf("decode stored publication: %w", err)
	}
	return publication, nil
}

func (client *Client) PublishEvent(ctx context.Context, publication store.Publication, key string, retention store.EventRetention) (store.Publication, bool, error) {
	if err := validatePublication(publication, key, retention); err != nil {
		return store.Publication{}, false, err
	}
	tx, err := client.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return store.Publication{}, false, err
	}
	defer tx.Rollback(ctx)
	hash := idempotencyHash(publication.Event.SourceAppID, key)
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, advisoryKey(publication.Event.SourceAppID, hash)); err != nil {
		return store.Publication{}, false, err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM event_idempotency WHERE source_app_id=$1 AND key_hash=$2 AND expires_at<=clock_timestamp()`, publication.Event.SourceAppID, hash); err != nil {
		return store.Publication{}, false, err
	}
	var replayRaw []byte
	err = tx.QueryRow(ctx, `SELECT publication FROM event_idempotency WHERE source_app_id=$1 AND key_hash=$2`, publication.Event.SourceAppID, hash).Scan(&replayRaw)
	if err == nil {
		var replay store.Publication
		if err := json.Unmarshal(replayRaw, &replay); err != nil {
			return store.Publication{}, false, err
		}
		if err := tx.Commit(ctx); err != nil {
			return store.Publication{}, false, err
		}
		return replay, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return store.Publication{}, false, err
	}

	policies, err := lockTargetPolicies(ctx, tx, publication.Event.TargetAppIDs)
	if err != nil {
		return store.Publication{}, false, err
	}
	for index := range publication.Jobs {
		policy := policies[publication.Jobs[index].TargetAppID]
		publication.Jobs[index].Callback = callbackEnabled(policy)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO events(id,type,source_app_id,target_app_ids,data,created_at,expires_at) VALUES($1,$2,$3,$4,$5::json,$6,$7)`, publication.Event.ID, publication.Event.Type, publication.Event.SourceAppID, publication.Event.TargetAppIDs, string(publication.Event.Data), publication.Event.CreatedAt, publication.Event.CreatedAt.Add(retention.Event)); err != nil {
		if uniqueViolation(err) {
			return store.Publication{}, false, store.ErrConflict
		}
		return store.Publication{}, false, err
	}
	for _, job := range publication.Jobs {
		policy := policies[job.TargetAppID]
		sinks := enabledSinks(policy)
		for sinkIndex, sink := range sinks {
			deliveryID := job.ID
			if sinkIndex > 0 {
				deliveryID = derivedCallbackDeliveryID(job.ID)
			}
			if err := client.insertDeliveryAndOutbox(ctx, tx, publication.Event, job, deliveryID, sink); err != nil {
				return store.Publication{}, false, err
			}
		}
	}
	response, err := json.Marshal(publication)
	if err != nil {
		return store.Publication{}, false, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO event_idempotency(source_app_id,key_hash,event_id,publication,created_at,expires_at) VALUES($1,$2,$3,$4,$5,$6)`, publication.Event.SourceAppID, hash, publication.Event.ID, response, publication.Event.CreatedAt, publication.Event.CreatedAt.Add(retention.Idempotency)); err != nil {
		return store.Publication{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return store.Publication{}, false, err
	}
	return publication, false, nil
}

func (client *Client) insertDeliveryAndOutbox(ctx context.Context, tx pgx.Tx, event domain.Event, job domain.Job, deliveryID, sink string) error {
	_, err := tx.Exec(ctx, `INSERT INTO deliveries(id,public_job_id,event_id,source_app_id,target_app_id,sink,status,attempts,created_at,updated_at) VALUES($1,$2,$3,$4,$5,$6,'pending',0,$7,$7)`, deliveryID, job.ID, event.ID, event.SourceAppID, job.TargetAppID, sink, event.CreatedAt)
	if err != nil {
		if uniqueViolation(err) {
			return store.ErrConflict
		}
		return err
	}
	subject, err := deliverySubject(job.TargetAppID, sink)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(outboxEnvelope{DeliveryID: deliveryID, Event: event})
	if err != nil {
		return err
	}
	outboxID, messageID := outboxIdentities(deliveryID)
	_, err = tx.Exec(ctx, `INSERT INTO outbox(id,event_id,delivery_id,subject,payload,message_id,available_at,created_at,updated_at) VALUES($1,$2,$3,$4,$5,$6,$7,$7,$7)`, outboxID, event.ID, deliveryID, subject, payload, messageID, event.CreatedAt)
	if uniqueViolation(err) {
		return store.ErrConflict
	}
	return err
}

func (client *Client) GetEvent(ctx context.Context, eventID string) (domain.Event, error) {
	var event domain.Event
	var data []byte
	err := client.pool.QueryRow(ctx, `SELECT id,type,source_app_id,target_app_ids,data::text,created_at FROM events WHERE id=$1`, eventID).Scan(&event.ID, &event.Type, &event.SourceAppID, &event.TargetAppIDs, &data, &event.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Event{}, store.ErrNotFound
	}
	if err != nil {
		return domain.Event{}, err
	}
	event.Data = append(json.RawMessage(nil), data...)
	return event, nil
}

func (client *Client) GetJob(ctx context.Context, jobID string) (domain.Job, error) {
	var job domain.Job
	var status string
	err := client.pool.QueryRow(ctx, `SELECT d.public_job_id,d.event_id,d.source_app_id,d.target_app_id,d.status,d.attempts,d.created_at,d.updated_at,EXISTS(SELECT 1 FROM deliveries c WHERE c.public_job_id=d.public_job_id AND c.sink='callback') FROM deliveries d WHERE d.public_job_id=$1 ORDER BY CASE d.sink WHEN 'stream' THEN 0 ELSE 1 END LIMIT 1`, jobID).Scan(&job.ID, &job.EventID, &job.SourceAppID, &job.TargetAppID, &status, &job.Attempts, &job.CreatedAt, &job.UpdatedAt, &job.Callback)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Job{}, store.ErrNotFound
	}
	if err != nil {
		return domain.Job{}, err
	}
	job.Status = publicJobStatus(status)
	return job, nil
}

func validatePublication(publication store.Publication, key string, retention store.EventRetention) error {
	event := publication.Event
	if strings.TrimSpace(key) == "" || event.ID == "" || event.Type == "" || event.SourceAppID == "" || len(event.TargetAppIDs) == 0 || !domain.JSONObject(event.Data) || retention.Event <= 0 || retention.Idempotency <= 0 || len(publication.Jobs) != len(event.TargetAppIDs) {
		return store.ErrConflict
	}
	targets := make(map[string]struct{}, len(event.TargetAppIDs))
	for _, target := range event.TargetAppIDs {
		if target == "" {
			return store.ErrConflict
		}
		targets[target] = struct{}{}
	}
	if len(targets) != len(event.TargetAppIDs) {
		return store.ErrConflict
	}
	jobs := make(map[string]struct{}, len(publication.Jobs))
	for _, job := range publication.Jobs {
		if job.ID == "" || job.EventID != event.ID || job.SourceAppID != event.SourceAppID {
			return store.ErrConflict
		}
		if _, ok := targets[job.TargetAppID]; !ok {
			return store.ErrConflict
		}
		if _, duplicate := jobs[job.TargetAppID]; duplicate {
			return store.ErrConflict
		}
		jobs[job.TargetAppID] = struct{}{}
	}
	return nil
}

func lockTargetPolicies(ctx context.Context, tx pgx.Tx, targetIDs []string) (map[string]targetPolicy, error) {
	ordered := append([]string(nil), targetIDs...)
	sort.Strings(ordered)
	rows, err := tx.Query(ctx, `SELECT id,delivery_mode,callback_url FROM applications WHERE id=ANY($1) AND enabled=true ORDER BY id FOR SHARE`, ordered)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	policies := make(map[string]targetPolicy, len(ordered))
	for rows.Next() {
		var id string
		var policy targetPolicy
		if err := rows.Scan(&id, &policy.mode, &policy.callbackURL); err != nil {
			return nil, err
		}
		policies[id] = policy
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(policies) != len(ordered) {
		return nil, store.ErrInvalidTarget
	}
	for _, target := range ordered {
		if len(enabledSinks(policies[target])) == 0 {
			return nil, store.ErrInvalidTarget
		}
	}
	return policies, nil
}

func enabledSinks(policy targetPolicy) []string {
	sinks := make([]string, 0, 2)
	if policy.mode == domain.DeliveryQueue || policy.mode == domain.DeliveryWebSocket || policy.mode == domain.DeliveryAll {
		sinks = append(sinks, "stream")
	}
	if callbackEnabled(policy) {
		sinks = append(sinks, "callback")
	}
	return sinks
}

func callbackEnabled(policy targetPolicy) bool {
	return policy.callbackURL != nil && strings.TrimSpace(*policy.callbackURL) != "" && (policy.mode == domain.DeliveryCallback || policy.mode == domain.DeliveryAll)
}

func deliverySubject(appID, sink string) (string, error) {
	if sink == "stream" {
		subjects, err := natsbroker.SubjectsForApp(appID)
		return subjects.Deliveries, err
	}
	digest := sha256.Sum256([]byte(appID))
	return natsbroker.CallbackSubject(int(binary.BigEndian.Uint16(digest[:2])) % 64)
}

func derivedCallbackDeliveryID(jobID string) string {
	digest := sha256.Sum256([]byte("callback\x00" + jobID))
	return "dlv_cb_" + hex.EncodeToString(digest[:16])
}
func outboxIdentities(deliveryID string) (string, string) {
	digest := sha256.Sum256([]byte("outbox\x00" + deliveryID))
	encoded := hex.EncodeToString(digest[:])
	return "obx_" + encoded[:32], "rh-v1-" + encoded
}
func idempotencyHash(source, key string) string {
	digest := sha256.Sum256([]byte(source + "\x00" + key))
	return hex.EncodeToString(digest[:])
}
func advisoryKey(source, hash string) int64 {
	digest := sha256.Sum256([]byte(source + "\x00" + hash))
	return int64(binary.BigEndian.Uint64(digest[:8]))
}

func publicJobStatus(status string) domain.JobStatus {
	switch status {
	case "acked":
		return domain.JobAcked
	case "dead_letter":
		return domain.JobDeadLetter
	default:
		return domain.JobPending
	}
}
