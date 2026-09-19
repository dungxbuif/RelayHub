package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dungxbuif/RelayHub/internal/adminread"
	"github.com/dungxbuif/RelayHub/internal/redisstate"
)

type adminReadRepositoryStub struct {
	counts    adminread.DurableCounts
	countsErr error
}

func (stub *adminReadRepositoryStub) ListAdminEvents(context.Context, adminread.EventListQuery) (adminread.Page[adminread.EventSummary], error) {
	return adminread.Page[adminread.EventSummary]{}, nil
}
func (stub *adminReadRepositoryStub) GetAdminEvent(context.Context, string) (adminread.EventDetail, error) {
	return adminread.EventDetail{}, nil
}
func (stub *adminReadRepositoryStub) ListAdminDeadLetters(context.Context, adminread.DeadLetterListQuery) (adminread.Page[adminread.DeadLetterSummary], error) {
	return adminread.Page[adminread.DeadLetterSummary]{}, nil
}
func (stub *adminReadRepositoryStub) GetAdminDeadLetter(context.Context, string) (adminread.DeadLetterDetail, error) {
	return adminread.DeadLetterDetail{}, nil
}
func (stub *adminReadRepositoryStub) ListAdminAudit(context.Context, adminread.AuditListQuery) (adminread.Page[adminread.AuditSummary], error) {
	return adminread.Page[adminread.AuditSummary]{}, nil
}
func (stub *adminReadRepositoryStub) AdminDurableCounts(context.Context) (adminread.DurableCounts, error) {
	return stub.counts, stub.countsErr
}

type adminMetricsStub struct {
	series       []redisstate.MetricBucket
	instances    []redisstate.DashboardInstanceState
	seriesErr    error
	instancesErr error
}

func (stub adminMetricsStub) Read(context.Context, time.Time, time.Duration, time.Duration) ([]redisstate.MetricBucket, error) {
	return stub.series, stub.seriesErr
}
func (stub adminMetricsStub) LiveInstances(context.Context, int64) ([]redisstate.DashboardInstanceState, error) {
	return stub.instances, stub.instancesErr
}

func TestAdminReadDashboardCombinesAndSortsSources(t *testing.T) {
	now := time.Date(2026, 9, 20, 3, 0, 0, 0, time.UTC)
	repository := &adminReadRepositoryStub{counts: adminread.DurableCounts{Pending: 2, Retrying: 3, DeadLetter: 4}}
	metrics := adminMetricsStub{
		series: []redisstate.MetricBucket{{At: now, Values: map[redisstate.MetricField]int64{redisstate.MetricRequestTotal: 12}}},
		instances: []redisstate.DashboardInstanceState{
			{InstanceID: "api_b", Connections: 5, NATSConnected: false, NATSChangedAt: now, HeartbeatAt: now},
			{InstanceID: "api_a", Connections: 3, NATSConnected: true, NATSChangedAt: now, HeartbeatAt: now},
		},
	}
	reads, err := NewAdminReadService(repository, metrics, AdminReadOptions{Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	result, err := reads.Dashboard(context.Background(), 15*time.Minute, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if result.GeneratedAt != now || result.Durable.DeadLetter != 4 || result.ActiveConnections != 8 || len(result.Series) != 1 || result.Series[0].Values["request_total"] != 12 {
		t.Fatalf("dashboard = %#v", result)
	}
	if result.Instances[0].InstanceID != "api_a" || result.Instances[1].InstanceID != "api_b" {
		t.Fatalf("instances not sorted: %#v", result.Instances)
	}
}

func TestAdminReadDashboardDegradesRedisButNotDurableTruth(t *testing.T) {
	reads, err := NewAdminReadService(&adminReadRepositoryStub{counts: adminread.DurableCounts{Pending: 7}}, adminMetricsStub{seriesErr: redisstate.ErrUnavailable, instancesErr: redisstate.ErrUnavailable}, AdminReadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	result, err := reads.Dashboard(context.Background(), 5*time.Minute, time.Minute)
	if err != nil || result.Durable.Pending != 7 || len(result.DegradedComponents) != 2 || result.DegradedComponents[0] != "instances" || result.DegradedComponents[1] != "rolling_metrics" {
		t.Fatalf("degraded dashboard = %#v, %v", result, err)
	}

	repositoryErr := errors.New("postgres unavailable sentinel")
	reads, _ = NewAdminReadService(&adminReadRepositoryStub{countsErr: repositoryErr}, adminMetricsStub{}, AdminReadOptions{})
	if _, err := reads.Dashboard(context.Background(), 5*time.Minute, time.Minute); !errors.Is(err, repositoryErr) {
		t.Fatalf("durable failure error = %v", err)
	}
}

func TestAdminReadServiceRejectsInvalidDependenciesAndTimeout(t *testing.T) {
	if _, err := NewAdminReadService(nil, nil, AdminReadOptions{}); !errors.Is(err, ErrInvalidDependency) {
		t.Fatalf("nil repository error = %v", err)
	}
	if _, err := NewAdminReadService(&adminReadRepositoryStub{}, nil, AdminReadOptions{Timeout: time.Minute}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("long timeout error = %v", err)
	}
}
