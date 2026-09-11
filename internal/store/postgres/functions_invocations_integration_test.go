//go:build integration

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/store"
)

func TestPostgresFunctionInvocationFencesAndReplay(t *testing.T) {
	client := integrationPostgresClient(t)
	resetControlTables(t, client)
	ctx := context.Background()
	now := time.Now().UTC()
	for _, id := range []string{"owner", "caller", "other"} {
		app := domain.App{ID: id, Name: id, DeliveryMode: domain.DeliveryWebSocket, Enabled: true, CreatedAt: now, UpdatedAt: now}
		if err := client.CreateApplication(ctx, app, store.AppCredential{AppID: id, APIKeyHash: "hash-" + id, HMACSecret: []byte("secret")}); err != nil {
			t.Fatal(err)
		}
	}
	function := domain.Function{ID: "fn_postgres", AppID: "owner", Name: "calculate", TimeoutSeconds: 1, Enabled: true, CreatedAt: now, UpdatedAt: now}
	if err := client.CreateFunction(ctx, function); err != nil {
		t.Fatal(err)
	}
	invocation := domain.Invocation{ID: "inv_postgres", FunctionID: function.ID, OwnerAppID: "owner", CallerAppID: "caller", Name: function.Name, Input: json.RawMessage(`{"n":9007199254740993}`), CreatedAt: now, ClaimBy: now.Add(250 * time.Millisecond), Deadline: now.Add(time.Second), State: domain.InvocationPending}
	created, replay, err := client.CreateInvocation(ctx, invocation, "same-key")
	if err != nil || replay || created.ID != invocation.ID {
		t.Fatalf("CreateInvocation()=%#v replay=%v error=%v", created, replay, err)
	}
	duplicate := invocation
	duplicate.ID = "inv_duplicate"
	stored, replay, err := client.CreateInvocation(ctx, duplicate, "same-key")
	if err != nil || !replay || stored.ID != invocation.ID {
		t.Fatalf("duplicate=%#v replay=%v error=%v", stored, replay, err)
	}
	if found, err := client.FindInvocation(ctx, "caller", "same-key"); err != nil || found.ID != invocation.ID || string(found.Input) != string(invocation.Input) {
		t.Fatalf("FindInvocation()=%#v error=%v", found, err)
	}
	var createdCount, replayCount atomic.Int64
	var workers sync.WaitGroup
	ids := make(chan string, 12)
	for index := range 12 {
		workers.Add(1)
		go func(index int) {
			defer workers.Done()
			candidate := invocation
			candidate.ID = fmt.Sprintf("inv_concurrent_%d", index)
			stored, replay, err := client.CreateInvocation(ctx, candidate, "concurrent-key")
			if err != nil {
				t.Errorf("concurrent CreateInvocation()=%v", err)
				return
			}
			ids <- stored.ID
			if replay {
				replayCount.Add(1)
			} else {
				createdCount.Add(1)
			}
		}(index)
	}
	workers.Wait()
	close(ids)
	var winner string
	for id := range ids {
		if winner == "" {
			winner = id
		}
		if id != winner {
			t.Fatalf("concurrent caller-key diverged: %q != %q", id, winner)
		}
	}
	if createdCount.Load() != 1 || replayCount.Load() != 11 {
		t.Fatalf("created=%d replay=%d", createdCount.Load(), replayCount.Load())
	}
	if err := client.ClaimInvocation(ctx, "other", "conn-one", invocation.ID); !errors.Is(err, store.ErrInvalidResult) {
		t.Fatalf("cross-app claim=%v", err)
	}
	if err := client.ClaimInvocation(ctx, "owner", "conn-one", invocation.ID); err != nil {
		t.Fatal(err)
	}
	if err := client.AcknowledgeInvocation(ctx, "owner", "conn-two", invocation.ID); !errors.Is(err, store.ErrInvalidResult) {
		t.Fatalf("cross-connection ack=%v", err)
	}
	if err := client.AcknowledgeInvocation(ctx, "owner", "conn-one", invocation.ID); err != nil {
		t.Fatal(err)
	}
	result := domain.RPCResult{InvocationID: invocation.ID, OK: true, Result: json.RawMessage(`{"value":9007199254740993}`)}
	if err := client.CompleteInvocation(ctx, "other", "conn-one", result); !errors.Is(err, store.ErrInvalidResult) {
		t.Fatalf("cross-app result=%v", err)
	}
	if err := client.CompleteInvocation(ctx, "owner", "conn-two", result); !errors.Is(err, store.ErrInvalidResult) {
		t.Fatalf("cross-connection result=%v", err)
	}
	if err := client.CompleteInvocation(ctx, "owner", "conn-one", result); err != nil {
		t.Fatal(err)
	}
	if err := client.CompleteInvocation(ctx, "owner", "conn-one", result); !errors.Is(err, store.ErrInvalidResult) {
		t.Fatalf("duplicate result=%v", err)
	}
	terminal, err := client.GetInvocation(ctx, invocation.ID)
	if err != nil || terminal.State != domain.InvocationSuccess || terminal.Reply == nil || string(terminal.Reply.Result) != string(result.Result) {
		t.Fatalf("terminal=%#v error=%v", terminal, err)
	}
}

func TestPostgresFunctionInvocationDeadlineTransitions(t *testing.T) {
	client := integrationPostgresClient(t)
	resetControlTables(t, client)
	ctx := context.Background()
	now := time.Now().UTC()
	for _, id := range []string{"owner", "caller"} {
		app := domain.App{ID: id, Name: id, DeliveryMode: domain.DeliveryWebSocket, Enabled: true, CreatedAt: now, UpdatedAt: now}
		if err := client.CreateApplication(ctx, app, store.AppCredential{AppID: id, APIKeyHash: "deadline-" + id, HMACSecret: []byte("secret")}); err != nil {
			t.Fatal(err)
		}
	}
	function := domain.Function{ID: "fn_deadline", AppID: "owner", Name: "wait", TimeoutSeconds: 1, Enabled: true, CreatedAt: now, UpdatedAt: now}
	if err := client.CreateFunction(ctx, function); err != nil {
		t.Fatal(err)
	}
	pending := domain.Invocation{ID: "inv_unavailable", FunctionID: function.ID, OwnerAppID: "owner", CallerAppID: "caller", Name: function.Name, Input: json.RawMessage(`{}`), CreatedAt: now, ClaimBy: now.Add(30 * time.Millisecond), Deadline: now.Add(time.Second), State: domain.InvocationPending}
	if _, _, err := client.CreateInvocation(ctx, pending, "unavailable"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(40 * time.Millisecond)
	if got, err := client.GetInvocation(ctx, pending.ID); err != nil || got.State != domain.InvocationUnavailable {
		t.Fatalf("unavailable=%#v error=%v", got, err)
	}
	claimed := pending
	claimed.ID = "inv_timeout"
	claimed.ClaimBy = time.Now().Add(30 * time.Millisecond)
	claimed.Deadline = time.Now().Add(80 * time.Millisecond)
	if _, _, err := client.CreateInvocation(ctx, claimed, "timeout"); err != nil {
		t.Fatal(err)
	}
	if err := client.ClaimInvocation(ctx, "owner", "conn", claimed.ID); err != nil {
		t.Fatal(err)
	}
	if err := client.AcknowledgeInvocation(ctx, "owner", "conn", claimed.ID); err != nil {
		t.Fatal(err)
	}
	time.Sleep(90 * time.Millisecond)
	if got, err := client.GetInvocation(ctx, claimed.ID); err != nil || got.State != domain.InvocationTimeout {
		t.Fatalf("timeout=%#v error=%v", got, err)
	}
	if err := client.CompleteInvocation(ctx, "owner", "conn", domain.RPCResult{InvocationID: claimed.ID, OK: true, Result: json.RawMessage(`{}`)}); !errors.Is(err, store.ErrInvalidResult) {
		t.Fatalf("late result=%v", err)
	}
}
