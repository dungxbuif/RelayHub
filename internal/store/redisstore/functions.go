package redisstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/realtime"
	"github.com/dungxbuif/RelayHub/internal/store"
	"github.com/redis/go-redis/v9"
)

const functionOperationTimeout = 250 * time.Millisecond

func (c *Client) functionKey(id string) string { return c.prefix + ":function:" + id }
func (c *Client) functionNamesKey(owner string) string {
	return c.prefix + ":function-names:" + hex.EncodeToString([]byte(owner))
}
func (c *Client) invocationKey(id string) string { return c.prefix + ":invocation:" + id }
func (c *Client) functionIdemKey(caller, key string) string {
	digest := sha256.Sum256([]byte(caller + "\x00" + key))
	return c.prefix + ":function-idem:" + hex.EncodeToString(digest[:])
}
func (c *Client) invocationChannel(id string) string { return c.prefix + ":rpc-state:" + id }

var createFunctionScript = redis.NewScript(`
if redis.call('HGET',KEYS[3],'enabled')~='1' then return -1 end
if redis.call('HEXISTS',KEYS[2],ARGV[1])==1 or redis.call('EXISTS',KEYS[1])==1 then return 0 end
redis.call('SET',KEYS[1],ARGV[3]);redis.call('HSET',KEYS[2],ARGV[1],ARGV[2]);return 1
`)

func (c *Client) CreateFunction(ctx context.Context, f domain.Function) error {
	ctx, cancel := context.WithTimeout(ctx, functionOperationTimeout)
	defer cancel()
	raw, e := json.Marshal(f)
	if e != nil {
		return e
	}
	n, e := createFunctionScript.Run(ctx, c.client, []string{c.functionKey(f.ID), c.functionNamesKey(f.AppID), c.applicationKey(f.AppID)}, f.Name, f.ID, raw).Int()
	if e != nil {
		return e
	}
	if n < 0 {
		return store.ErrNotFound
	}
	if n == 0 {
		return store.ErrConflict
	}
	return nil
}
func (c *Client) GetFunction(ctx context.Context, id string) (domain.Function, error) {
	ctx, cancel := context.WithTimeout(ctx, functionOperationTimeout)
	defer cancel()
	raw, e := c.client.Get(ctx, c.functionKey(id)).Bytes()
	if errors.Is(e, redis.Nil) {
		return domain.Function{}, store.ErrNotFound
	}
	if e != nil {
		return domain.Function{}, e
	}
	var f domain.Function
	e = json.Unmarshal(raw, &f)
	return f, e
}
func (c *Client) ListFunctions(ctx context.Context, owner string) ([]domain.Function, error) {
	ctx, cancel := context.WithTimeout(ctx, functionOperationTimeout)
	defer cancel()
	ids, e := c.client.HVals(ctx, c.functionNamesKey(owner)).Result()
	if e != nil {
		return nil, e
	}
	out := []domain.Function{}
	if len(ids) == 0 {
		return out, nil
	}
	keys := make([]string, len(ids))
	for i, id := range ids {
		keys[i] = c.functionKey(id)
	}
	rows, e := c.client.MGet(ctx, keys...).Result()
	if e != nil {
		return nil, e
	}
	for _, row := range rows {
		if row == nil {
			continue
		}
		var f domain.Function
		if e = json.Unmarshal([]byte(row.(string)), &f); e != nil {
			return nil, e
		}
		if f.AppID == owner {
			out = append(out, f)
		}
	}
	return out, nil
}

var deleteFunctionScript = redis.NewScript(`
local raw=redis.call('GET',KEYS[1]);if not raw then return 0 end
local f=cjson.decode(raw);if f.app_id~=ARGV[1] then return 0 end
redis.call('DEL',KEYS[1]);redis.call('HDEL',KEYS[2],f.name);return 1
`)

func (c *Client) DeleteFunction(ctx context.Context, owner, id string) error {
	ctx, cancel := context.WithTimeout(ctx, functionOperationTimeout)
	defer cancel()
	n, e := deleteFunctionScript.Run(ctx, c.client, []string{c.functionKey(id), c.functionNamesKey(owner)}, owner).Int()
	if e != nil {
		return e
	}
	if n == 0 {
		return store.ErrNotFound
	}
	return nil
}

var createInvocationScript = redis.NewScript(`
local old=redis.call('GET',KEYS[1]);if old then return {old,'1'} end
local raw=redis.call('GET',KEYS[3]);if not raw then return {} end
local f=cjson.decode(raw);if not f.enabled or f.app_id~=ARGV[4] then return {} end
if redis.call('HGET',KEYS[4],'enabled')~='1' then return {} end
if redis.call('EXISTS',KEYS[2])==1 then return {} end
redis.call('HSET',KEYS[2],'record',ARGV[2],'state','pending','owner',ARGV[4],'connection','','claim_by',ARGV[5],'deadline',ARGV[6],'dispatch',ARGV[7],'dispatch_channel',ARGV[8])
redis.call('PEXPIRE',KEYS[2],ARGV[3]);redis.call('SET',KEYS[1],ARGV[1],'PX',ARGV[3]);return {ARGV[1],'0'}
`)

func (c *Client) CreateInvocation(ctx context.Context, v domain.Invocation, key string) (domain.Invocation, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, functionOperationTimeout)
	defer cancel()
	raw, e := json.Marshal(v)
	if e != nil {
		return domain.Invocation{}, false, e
	}
	dispatch, e := json.Marshal(notification{AppID: v.OwnerAppID, Frame: realtime.InvocationFrame(v)})
	if e != nil {
		return domain.Invocation{}, false, e
	}
	values, e := createInvocationScript.Run(ctx, c.client, []string{c.functionIdemKey(v.CallerAppID, key), c.invocationKey(v.ID), c.functionKey(v.FunctionID), c.applicationKey(v.OwnerAppID)}, v.ID, raw, domain.InvocationRetention.Milliseconds(), v.OwnerAppID, v.ClaimBy.UnixMilli(), v.Deadline.UnixMilli(), dispatch, c.prefix+":pubsub:"+hex.EncodeToString([]byte(v.OwnerAppID))).StringSlice()
	if e != nil {
		return domain.Invocation{}, false, e
	}
	if len(values) != 2 {
		return domain.Invocation{}, false, store.ErrNotFound
	}
	stored, e := c.GetInvocation(ctx, values[0])
	return stored, values[1] == "1", e
}
func (c *Client) FindInvocation(ctx context.Context, caller, key string) (domain.Invocation, error) {
	ctx, cancel := context.WithTimeout(ctx, functionOperationTimeout)
	defer cancel()
	id, e := c.client.Get(ctx, c.functionIdemKey(caller, key)).Result()
	if errors.Is(e, redis.Nil) {
		return domain.Invocation{}, store.ErrNotFound
	}
	if e != nil {
		return domain.Invocation{}, e
	}
	return c.GetInvocation(ctx, id)
}

// Lua changes metadata only. Input/result bytes remain opaque strings, avoiding
// Lua/cjson double rounding for arbitrary JSON integer values.
const invocationExpiryLua = `
local clock=redis.call('TIME');local now=tonumber(clock[1])*1000+math.floor(tonumber(clock[2])/1000)
local state=redis.call('HGET',KEYS[1],'state')
local next=state
if state=='claimed' and now>=tonumber(redis.call('HGET',KEYS[1],'deadline')) then next='timeout'
elseif (state=='pending' or state=='reserved') and now>=tonumber(redis.call('HGET',KEYS[1],'claim_by')) then next='unavailable' end
if state~=next then state=next;redis.call('HSET',KEYS[1],'state',state);redis.call('PUBLISH',KEYS[2],'1') end
`

var getInvocationScript = redis.NewScript(`if redis.call('EXISTS',KEYS[1])==0 then return {} end
` + invocationExpiryLua + `return redis.call('HGETALL',KEYS[1])`)

func (c *Client) GetInvocation(ctx context.Context, id string) (domain.Invocation, error) {
	ctx, cancel := context.WithTimeout(ctx, functionOperationTimeout)
	defer cancel()
	rows, e := getInvocationScript.Run(ctx, c.client, []string{c.invocationKey(id), c.invocationChannel(id)}).StringSlice()
	if e != nil {
		return domain.Invocation{}, e
	}
	if len(rows) == 0 {
		return domain.Invocation{}, store.ErrNotFound
	}
	values := map[string]string{}
	for i := 0; i < len(rows); i += 2 {
		values[rows[i]] = rows[i+1]
	}
	var v domain.Invocation
	if e = json.Unmarshal([]byte(values["record"]), &v); e != nil {
		return v, e
	}
	v.State = domain.InvocationState(values["state"])
	v.ConnectionID = values["connection"]
	if raw := values["reply"]; raw != "" {
		v.Reply = &domain.RPCResult{}
		if e = json.Unmarshal([]byte(raw), v.Reply); e != nil {
			return v, e
		}
	}
	return v, nil
}

var transitionInvocationScript = redis.NewScript(`if redis.call('EXISTS',KEYS[1])==0 then return 0 end
` + invocationExpiryLua + `
if redis.call('HGET',KEYS[1],'owner')~=ARGV[2] or ARGV[2]=='' or ARGV[3]=='' then return 0 end
local action=ARGV[1]
if action=='claim' then
 if state~='pending' then return 0 end
 if redis.call('HGET',KEYS[3],'enabled')~='1' then return 0 end
 redis.call('HSET',KEYS[1],'state','reserved','connection',ARGV[3])
else
 if redis.call('HGET',KEYS[1],'connection')~=ARGV[3] then return 0 end
 if action=='ack' then
  if state~='reserved' then return 0 end
  redis.call('HSET',KEYS[1],'state','claimed')
 elseif action=='release' then
  if state~='reserved' and state~='claimed' then return 0 end
  redis.call('HSET',KEYS[1],'state','pending','connection','')
  redis.call('PUBLISH',redis.call('HGET',KEYS[1],'dispatch_channel'),redis.call('HGET',KEYS[1],'dispatch'))
 elseif action=='complete' then
  if state~='claimed' then return 0 end
  redis.call('HSET',KEYS[1],'state',ARGV[5],'reply',ARGV[4])
 else return 0 end
end
redis.call('PUBLISH',KEYS[2],'1');return 1
`)

func (c *Client) invocationTransition(ctx context.Context, action, owner, conn, id, reply, outcome string) error {
	ctx, cancel := context.WithTimeout(ctx, functionOperationTimeout)
	defer cancel()
	n, e := transitionInvocationScript.Run(ctx, c.client, []string{c.invocationKey(id), c.invocationChannel(id), c.applicationKey(owner)}, action, owner, conn, reply, outcome).Int()
	if e != nil {
		return e
	}
	if n != 1 {
		return store.ErrInvalidResult
	}
	return nil
}
func (c *Client) ClaimInvocation(ctx context.Context, owner, conn, id string) error {
	return c.invocationTransition(ctx, "claim", owner, conn, id, "", "")
}
func (c *Client) AcknowledgeInvocation(ctx context.Context, owner, conn, id string) error {
	return c.invocationTransition(ctx, "ack", owner, conn, id, "", "")
}
func (c *Client) ReleaseInvocation(ctx context.Context, owner, conn, id string) error {
	return c.invocationTransition(ctx, "release", owner, conn, id, "", "")
}
func (c *Client) CompleteInvocation(ctx context.Context, owner, conn string, r domain.RPCResult) error {
	if !domain.ValidRPCResult(r) {
		return store.ErrInvalidResult
	}
	raw, e := json.Marshal(r)
	if e != nil {
		return store.ErrInvalidResult
	}
	state := domain.InvocationSuccess
	if !r.OK {
		state = domain.InvocationHandlerError
	}
	return c.invocationTransition(ctx, "complete", owner, conn, r.InvocationID, string(raw), string(state))
}

type invocationWatch struct {
	updates chan struct{}
	cancel  context.CancelFunc
	sub     *redis.PubSub
	done    chan struct{}
	once    sync.Once
}

func (w *invocationWatch) Updates() <-chan struct{} { return w.updates }
func (w *invocationWatch) Close()                   { w.once.Do(func() { w.cancel(); _ = w.sub.Close(); <-w.done }) }
func (c *Client) WatchInvocation(ctx context.Context, id string) (store.InvocationWatch, error) {
	if e := ctx.Err(); e != nil {
		return nil, e
	}
	ctx, cancel := context.WithCancel(ctx)
	sub := c.client.Subscribe(ctx, c.invocationChannel(id))
	startup, stop := context.WithTimeout(ctx, functionOperationTimeout)
	_, e := sub.Receive(startup)
	stop()
	if e != nil {
		cancel()
		_ = sub.Close()
		return nil, e
	}
	w := &invocationWatch{updates: make(chan struct{}, 1), cancel: cancel, sub: sub, done: make(chan struct{})}
	messages := sub.Channel(redis.WithChannelSize(1))
	go func() {
		defer close(w.done)
		defer sub.Close()
		for {
			select {
			case <-ctx.Done():
				return
			case _, ok := <-messages:
				if !ok {
					return
				}
				select {
				case w.updates <- struct{}{}:
				default:
				}
			}
		}
	}()
	return w, nil
}
