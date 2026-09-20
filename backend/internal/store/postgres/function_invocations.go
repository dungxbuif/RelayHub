package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"

	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/store"
	"github.com/jackc/pgx/v5"
)

var _ store.FunctionInvocationStore = (*Client)(nil)

func invocationKeyHash(caller, key string) []byte {
	digest := sha256.Sum256([]byte(caller + "\x00" + key))
	return digest[:]
}

func (client *Client) CreateInvocation(ctx context.Context, invocation domain.Invocation, key string) (domain.Invocation, bool, error) {
	if invocation.ID == "" || invocation.FunctionID == "" || invocation.OwnerAppID == "" || invocation.CallerAppID == "" || key == "" {
		return domain.Invocation{}, false, store.ErrNotFound
	}
	hash := invocationKeyHash(invocation.CallerAppID, key)
	if _, err := client.pool.Exec(ctx, `DELETE FROM function_invocations WHERE caller_app_id=$1 AND idempotency_hash=$2 AND expires_at<=clock_timestamp()`, invocation.CallerAppID, hash); err != nil {
		return domain.Invocation{}, false, err
	}
	var id string
	err := client.pool.QueryRow(ctx, `
		INSERT INTO function_invocations(id,function_id,owner_app_id,caller_app_id,idempotency_hash,function_name,input,state,created_at,claim_by,deadline,expires_at,updated_at)
		SELECT $1,f.id,f.app_id,$4,$5,$6,$7,'pending',$8,$9,$10,$11,$8
		FROM functions f
		JOIN applications owner ON owner.id=f.app_id AND owner.enabled=true
		JOIN applications caller ON caller.id=$4 AND caller.enabled=true
		WHERE f.id=$2 AND f.app_id=$3 AND f.enabled=true
		ON CONFLICT (caller_app_id,idempotency_hash) DO NOTHING
		RETURNING id`, invocation.ID, invocation.FunctionID, invocation.OwnerAppID, invocation.CallerAppID, hash, invocation.Name, invocation.Input, invocation.CreatedAt, invocation.ClaimBy, invocation.Deadline, invocation.CreatedAt.Add(domain.InvocationRetention)).Scan(&id)
	if err == nil {
		stored, getErr := client.GetInvocation(ctx, id)
		return stored, false, getErr
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return domain.Invocation{}, false, err
	}
	err = client.pool.QueryRow(ctx, `SELECT id FROM function_invocations WHERE caller_app_id=$1 AND idempotency_hash=$2`, invocation.CallerAppID, hash).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Invocation{}, false, store.ErrNotFound
	}
	if err != nil {
		return domain.Invocation{}, false, err
	}
	stored, err := client.GetInvocation(ctx, id)
	return stored, true, err
}

func (client *Client) FindInvocation(ctx context.Context, caller, key string) (domain.Invocation, error) {
	var id string
	err := client.pool.QueryRow(ctx, `SELECT id FROM function_invocations WHERE caller_app_id=$1 AND idempotency_hash=$2 AND expires_at>clock_timestamp()`, caller, invocationKeyHash(caller, key)).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Invocation{}, store.ErrNotFound
	}
	if err != nil {
		return domain.Invocation{}, err
	}
	return client.GetInvocation(ctx, id)
}

func (client *Client) GetInvocation(ctx context.Context, id string) (domain.Invocation, error) {
	_, err := client.pool.Exec(ctx, `
		UPDATE function_invocations
		SET state=CASE WHEN state='claimed' THEN 'timeout' ELSE 'unavailable' END,
		    connection_id=NULL,updated_at=clock_timestamp()
		WHERE id=$1 AND expires_at>clock_timestamp()
		  AND ((state='claimed' AND deadline<=clock_timestamp())
		    OR (state IN ('pending','reserved') AND claim_by<=clock_timestamp()))`, id)
	if err != nil {
		return domain.Invocation{}, err
	}
	invocation, err := scanInvocation(client.pool.QueryRow(ctx, `SELECT id,function_id,owner_app_id,caller_app_id,function_name,input,created_at,deadline,claim_by,state,COALESCE(connection_id,''),reply FROM function_invocations WHERE id=$1 AND expires_at>clock_timestamp()`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Invocation{}, store.ErrNotFound
	}
	return invocation, err
}

func (client *Client) ClaimInvocation(ctx context.Context, owner, connection, id string) error {
	return client.transitionInvocation(ctx, `
		UPDATE function_invocations i SET state='reserved',connection_id=$2,updated_at=clock_timestamp()
		WHERE i.id=$1 AND i.owner_app_id=$3 AND $2<>'' AND i.state='pending'
		  AND clock_timestamp()<i.claim_by AND clock_timestamp()<i.deadline
		  AND EXISTS (SELECT 1 FROM applications a WHERE a.id=i.owner_app_id AND a.enabled=true)`, id, connection, owner)
}

func (client *Client) AcknowledgeInvocation(ctx context.Context, owner, connection, id string) error {
	return client.transitionInvocation(ctx, `
		UPDATE function_invocations SET state='claimed',updated_at=clock_timestamp()
		WHERE id=$1 AND owner_app_id=$3 AND connection_id=$2 AND state='reserved'
		  AND clock_timestamp()<claim_by AND clock_timestamp()<deadline`, id, connection, owner)
}

func (client *Client) ReleaseInvocation(ctx context.Context, owner, connection, id string) error {
	return client.transitionInvocation(ctx, `
		UPDATE function_invocations SET state='pending',connection_id=NULL,updated_at=clock_timestamp()
		WHERE id=$1 AND owner_app_id=$3 AND connection_id=$2
		  AND ((state='reserved' AND clock_timestamp()<claim_by)
		    OR (state='claimed' AND clock_timestamp()<deadline))`, id, connection, owner)
}

func (client *Client) CompleteInvocation(ctx context.Context, owner, connection string, result domain.RPCResult) error {
	if owner == "" || connection == "" || !domain.ValidRPCResult(result) {
		return store.ErrInvalidResult
	}
	reply, err := json.Marshal(result)
	if err != nil {
		return store.ErrInvalidResult
	}
	state := domain.InvocationSuccess
	if !result.OK {
		state = domain.InvocationHandlerError
	}
	command, err := client.pool.Exec(ctx, `
		UPDATE function_invocations SET state=$4,reply=$5,updated_at=clock_timestamp()
		WHERE id=$1 AND owner_app_id=$2 AND connection_id=$3 AND state='claimed'
		  AND clock_timestamp()<deadline`, result.InvocationID, owner, connection, state, reply)
	if err != nil {
		return err
	}
	if command.RowsAffected() != 1 {
		_, _ = client.GetInvocation(ctx, result.InvocationID)
		return store.ErrInvalidResult
	}
	return nil
}

func (client *Client) transitionInvocation(ctx context.Context, sql, id, connection, owner string) error {
	if id == "" || owner == "" || connection == "" {
		return store.ErrInvalidResult
	}
	command, err := client.pool.Exec(ctx, sql, id, connection, owner)
	if err != nil {
		return err
	}
	if command.RowsAffected() != 1 {
		_, _ = client.GetInvocation(ctx, id)
		return store.ErrInvalidResult
	}
	return nil
}

func scanInvocation(row rowScanner) (domain.Invocation, error) {
	var invocation domain.Invocation
	var reply []byte
	err := row.Scan(&invocation.ID, &invocation.FunctionID, &invocation.OwnerAppID, &invocation.CallerAppID, &invocation.Name, &invocation.Input, &invocation.CreatedAt, &invocation.Deadline, &invocation.ClaimBy, &invocation.State, &invocation.ConnectionID, &reply)
	if err != nil {
		return domain.Invocation{}, err
	}
	invocation.Input = append(json.RawMessage(nil), invocation.Input...)
	if len(reply) != 0 {
		invocation.Reply = &domain.RPCResult{}
		if err := json.Unmarshal(reply, invocation.Reply); err != nil {
			return domain.Invocation{}, err
		}
	}
	return invocation, nil
}

type postgresInvocationWatch struct {
	updates chan struct{}
}

func (watch *postgresInvocationWatch) Updates() <-chan struct{} { return watch.updates }
func (watch *postgresInvocationWatch) Close()                   {}

func (client *Client) WatchInvocation(ctx context.Context, id string) (store.InvocationWatch, error) {
	// PostgreSQL does not own broker connections. FunctionService uses the Core
	// NATS notifier's watch when configured; this fallback waits only for the
	// persisted deadline, without periodic database reads.
	if id == "" {
		return nil, store.ErrNotFound
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &postgresInvocationWatch{updates: make(chan struct{})}, nil
}
