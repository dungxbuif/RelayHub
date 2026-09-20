//go:build integration

package postgres

import (
	"context"
	"testing"
	"time"
)

func TestDeliveryGenerationSchemaDefaultsAndConstraints(t *testing.T) {
	client := integrationPostgresClient(t)
	ctx := context.Background()
	stamp := time.Date(2026, 9, 20, 4, 0, 0, 0, time.UTC)
	for _, app := range []string{"app_generation_source", "app_generation_target"} {
		if _, err := client.pool.Exec(ctx, `INSERT INTO applications(id,name,delivery_mode,enabled,created_at,updated_at) VALUES($1,$1,'websocket',true,$2,$2)`, app, stamp); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := client.pool.Exec(ctx, `INSERT INTO events(id,type,source_app_id,target_app_ids,data,created_at,expires_at) VALUES('evt_generation','generation.test','app_generation_source',ARRAY['app_generation_target'],'{}',$1,$2)`, stamp, stamp.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := client.pool.Exec(ctx, `INSERT INTO deliveries(id,public_job_id,event_id,source_app_id,target_app_id,sink,status,attempts,created_at,updated_at) VALUES('dlv_generation','job_generation','evt_generation','app_generation_source','app_generation_target','stream','dead_letter',1,$1,$1)`, stamp); err != nil {
		t.Fatal(err)
	}
	if _, err := client.pool.Exec(ctx, `INSERT INTO outbox(id,event_id,delivery_id,subject,payload,message_id,available_at,created_at,updated_at) VALUES('obx_generation','evt_generation','dlv_generation','rh.v1.delivery.app_generation_target','{}','msg_generation',$1,$1,$1)`, stamp); err != nil {
		t.Fatal(err)
	}
	if _, err := client.pool.Exec(ctx, `INSERT INTO delivery_attempts(delivery_id,attempt,outcome,created_at,updated_at) VALUES('dlv_generation',1,'dead_letter',$1,$1)`, stamp); err != nil {
		t.Fatal(err)
	}
	var deliveryGeneration, outboxGeneration, attemptGeneration int64
	var attemptUpdated time.Time
	if err := client.pool.QueryRow(ctx, `SELECT d.generation,o.generation,a.generation,a.updated_at FROM deliveries d JOIN outbox o ON o.delivery_id=d.id JOIN delivery_attempts a ON a.delivery_id=d.id WHERE d.id='dlv_generation'`).Scan(&deliveryGeneration, &outboxGeneration, &attemptGeneration, &attemptUpdated); err != nil {
		t.Fatal(err)
	}
	if deliveryGeneration != 1 || outboxGeneration != 1 || attemptGeneration != 1 || !attemptUpdated.Equal(stamp) {
		t.Fatalf("generation defaults = delivery:%d outbox:%d attempt:%d updated:%s", deliveryGeneration, outboxGeneration, attemptGeneration, attemptUpdated)
	}
	if _, err := client.pool.Exec(ctx, `INSERT INTO delivery_attempts(delivery_id,generation,attempt,outcome,created_at,updated_at) VALUES('dlv_generation',1,1,'duplicate',$1,$1)`, stamp); err == nil {
		t.Fatal("duplicate attempt in one generation was accepted")
	}
	if _, err := client.pool.Exec(ctx, `INSERT INTO delivery_attempts(delivery_id,generation,attempt,outcome,created_at,updated_at) VALUES('dlv_generation',2,1,'started',$1,$1)`, stamp); err != nil {
		t.Fatalf("same attempt number in next generation: %v", err)
	}
	if _, err := client.pool.Exec(ctx, `UPDATE deliveries SET generation=0 WHERE id='dlv_generation'`); err == nil {
		t.Fatal("non-positive delivery generation was accepted")
	}
}
