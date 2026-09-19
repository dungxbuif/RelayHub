package service

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/dungxbuif/RelayHub/internal/adminread"
	"github.com/dungxbuif/RelayHub/internal/redisstate"
	"github.com/dungxbuif/RelayHub/internal/store"
)

type AdminMetricsReader interface {
	Read(context.Context, time.Time, time.Duration, time.Duration) ([]redisstate.MetricBucket, error)
	LiveInstances(context.Context, int64) ([]redisstate.DashboardInstanceState, error)
}

type AdminReadOptions struct {
	Now     func() time.Time
	Timeout time.Duration
}

type AdminReadService struct {
	repository store.AdminReadStore
	metrics    AdminMetricsReader
	now        func() time.Time
	timeout    time.Duration
}

func NewAdminReadService(repository store.AdminReadStore, metrics AdminMetricsReader, options AdminReadOptions) (*AdminReadService, error) {
	if nilDependency(repository) {
		return nil, ErrInvalidDependency
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	if options.Timeout <= 0 {
		options.Timeout = 3 * time.Second
	}
	if options.Timeout > 30*time.Second {
		return nil, ErrInvalidInput
	}
	return &AdminReadService{repository: repository, metrics: metrics, now: options.Now, timeout: options.Timeout}, nil
}

func (service *AdminReadService) Dashboard(ctx context.Context, window, step time.Duration) (adminread.DashboardSnapshot, error) {
	metrics, counts, err := service.readSnapshot(ctx, window, step, true)
	if err != nil {
		return adminread.DashboardSnapshot{}, err
	}
	return adminread.DashboardSnapshot{MetricsSnapshot: metrics, Durable: counts}, nil
}

func (service *AdminReadService) Metrics(ctx context.Context, window, step time.Duration) (adminread.MetricsSnapshot, error) {
	metrics, _, err := service.readSnapshot(ctx, window, step, false)
	return metrics, err
}

func (service *AdminReadService) readSnapshot(parent context.Context, window, step time.Duration, includeDurable bool) (adminread.MetricsSnapshot, adminread.DurableCounts, error) {
	if service == nil || service.repository == nil || window <= 0 || step <= 0 {
		return adminread.MetricsSnapshot{}, adminread.DurableCounts{}, ErrInvalidInput
	}
	generatedAt := service.now().UTC()
	ctx, cancel := context.WithTimeout(parent, service.timeout)
	defer cancel()
	var series []redisstate.MetricBucket
	var instances []redisstate.DashboardInstanceState
	var counts adminread.DurableCounts
	var seriesErr, instancesErr, countsErr error
	var wait sync.WaitGroup
	if includeDurable {
		wait.Add(1)
		go func() { defer wait.Done(); counts, countsErr = service.repository.AdminDurableCounts(ctx) }()
	}
	if service.metrics == nil {
		seriesErr, instancesErr = redisstate.ErrUnavailable, redisstate.ErrUnavailable
	} else {
		wait.Add(2)
		go func() { defer wait.Done(); series, seriesErr = service.metrics.Read(ctx, generatedAt, window, step) }()
		go func() { defer wait.Done(); instances, instancesErr = service.metrics.LiveInstances(ctx, 1000) }()
	}
	wait.Wait()
	if countsErr != nil {
		if ctx.Err() != nil {
			return adminread.MetricsSnapshot{}, adminread.DurableCounts{}, context.DeadlineExceeded
		}
		return adminread.MetricsSnapshot{}, adminread.DurableCounts{}, countsErr
	}
	result := adminread.MetricsSnapshot{GeneratedAt: generatedAt, WindowSeconds: int64(window / time.Second), StepSeconds: int64(step / time.Second), Series: []adminread.MetricPoint{}, Instances: []adminread.InstanceSummary{}, DegradedComponents: []string{}}
	if seriesErr != nil {
		result.DegradedComponents = append(result.DegradedComponents, "rolling_metrics")
	} else {
		result.Series = make([]adminread.MetricPoint, len(series))
		for index, bucket := range series {
			values := make(map[string]int64, len(bucket.Values))
			for field, value := range bucket.Values {
				values[string(field)] = value
			}
			result.Series[index] = adminread.MetricPoint{At: bucket.At, Values: values}
		}
	}
	if instancesErr != nil {
		result.DegradedComponents = append(result.DegradedComponents, "instances")
	} else {
		result.Instances = make([]adminread.InstanceSummary, len(instances))
		for index, instance := range instances {
			result.ActiveConnections += instance.Connections
			result.Instances[index] = adminread.InstanceSummary{InstanceID: instance.InstanceID, Connections: instance.Connections, NATSConnected: instance.NATSConnected, NATSChangedAt: instance.NATSChangedAt, HeartbeatAt: instance.HeartbeatAt}
		}
		sort.Slice(result.Instances, func(i, j int) bool { return result.Instances[i].InstanceID < result.Instances[j].InstanceID })
	}
	sort.Strings(result.DegradedComponents)
	return result, counts, nil
}

func (service *AdminReadService) ListEvents(ctx context.Context, query adminread.EventListQuery) (adminread.Page[adminread.EventSummary], error) {
	return service.repository.ListAdminEvents(ctx, query)
}
func (service *AdminReadService) GetEvent(ctx context.Context, id string) (adminread.EventDetail, error) {
	return service.repository.GetAdminEvent(ctx, id)
}
func (service *AdminReadService) ListDeadLetters(ctx context.Context, query adminread.DeadLetterListQuery) (adminread.Page[adminread.DeadLetterSummary], error) {
	return service.repository.ListAdminDeadLetters(ctx, query)
}
func (service *AdminReadService) GetDeadLetter(ctx context.Context, id string) (adminread.DeadLetterDetail, error) {
	return service.repository.GetAdminDeadLetter(ctx, id)
}
func (service *AdminReadService) ListAudit(ctx context.Context, query adminread.AuditListQuery) (adminread.Page[adminread.AuditSummary], error) {
	return service.repository.ListAdminAudit(ctx, query)
}

func AdminReadError(err error) error {
	switch {
	case errors.Is(err, adminread.ErrInvalidArgument), errors.Is(err, adminread.ErrInvalidCursor):
		return ErrInvalidInput
	case errors.Is(err, store.ErrNotFound):
		return ErrNotFound
	default:
		return err
	}
}
