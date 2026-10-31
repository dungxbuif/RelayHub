package httpapi

import (
	"context"
	"net/http"
	"testing"
	"testing/fstest"
	"time"

	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/service"
	"github.com/dungxbuif/RelayHub/internal/store"
)

func TestAdminQueueFacadeCoversSubscriptionDrainAndScheduleLifecycle(t *testing.T) {
	router, apps, queue := newAdminQueueRouter(t)
	app, _, err := apps.Create(context.Background(), service.CreateApp{Name: "orders", DeliveryMode: domain.DeliveryQueue})
	if err != nil {
		t.Fatal(err)
	}
	admin := adminSessionHeaders(t, router)
	subscription, err := queue.Create(context.Background(), app.ID, service.QueueSubscriptionInput{Name: "jobs"})
	if err != nil {
		t.Fatal(err)
	}

	created := requestJSON(t, router, http.MethodPost, "/api/v1/admin/apps/"+app.ID+"/subscriptions", []byte(`{"name":"urgent"}`), admin)
	if created.Code != http.StatusCreated {
		t.Fatalf("create subscription status=%d body=%s", created.Code, created.Body.String())
	}
	updated := requestJSON(t, router, http.MethodPatch, "/api/v1/admin/apps/"+app.ID+"/subscriptions/"+subscription.ID, []byte(`{"policy_version":1,"name":"jobs-v2"}`), admin)
	if updated.Code != http.StatusOK {
		t.Fatalf("update subscription status=%d body=%s", updated.Code, updated.Body.String())
	}
	paused := requestJSON(t, router, http.MethodPost, "/api/v1/admin/apps/"+app.ID+"/subscriptions/"+subscription.ID+"/pause", nil, admin)
	if paused.Code != http.StatusOK {
		t.Fatalf("pause status=%d body=%s", paused.Code, paused.Body.String())
	}
	resumed := requestJSON(t, router, http.MethodPost, "/api/v1/admin/apps/"+app.ID+"/subscriptions/"+subscription.ID+"/resume", nil, admin)
	if resumed.Code != http.StatusOK {
		t.Fatalf("resume status=%d body=%s", resumed.Code, resumed.Body.String())
	}
	metrics := requestJSON(t, router, http.MethodGet, "/api/v1/admin/apps/"+app.ID+"/subscriptions/"+subscription.ID+"/metrics", nil, admin)
	if metrics.Code != http.StatusOK {
		t.Fatalf("metrics status=%d body=%s", metrics.Code, metrics.Body.String())
	}
	drain := requestJSON(t, router, http.MethodPost, "/api/v1/admin/apps/"+app.ID+"/subscriptions/"+subscription.ID+"/drain", []byte(`{"timeout_seconds":30}`), admin)
	if drain.Code != http.StatusAccepted {
		t.Fatalf("drain status=%d body=%s", drain.Code, drain.Body.String())
	}
	drainStatus := requestJSON(t, router, http.MethodGet, "/api/v1/admin/apps/"+app.ID+"/subscriptions/"+subscription.ID+"/drain", nil, admin)
	if drainStatus.Code != http.StatusOK {
		t.Fatalf("drain status read=%d body=%s", drainStatus.Code, drainStatus.Body.String())
	}

	schedule := requestJSON(t, router, http.MethodPost, "/api/v1/admin/apps/"+app.ID+"/subscriptions/"+subscription.ID+"/schedules", []byte(`{"name":"hourly","cron_expression":"*/5 * * * *","timezone":"UTC","event_type":"orders.tick","data":{}}`), admin)
	if schedule.Code != http.StatusCreated {
		t.Fatalf("schedule create status=%d body=%s", schedule.Code, schedule.Body.String())
	}
	list := requestJSON(t, router, http.MethodGet, "/api/v1/admin/apps/"+app.ID+"/subscriptions/"+subscription.ID+"/schedules", nil, admin)
	if list.Code != http.StatusOK {
		t.Fatalf("schedule list status=%d body=%s", list.Code, list.Body.String())
	}
}

func TestAdminQueueFacadeRequiresConfirmationAndRejectsDisabledApps(t *testing.T) {
	router, apps, queue := newAdminQueueRouter(t)
	app, _, err := apps.Create(context.Background(), service.CreateApp{Name: "orders", DeliveryMode: domain.DeliveryQueue})
	if err != nil {
		t.Fatal(err)
	}
	admin := adminSessionHeaders(t, router)
	subscription, err := queue.Create(context.Background(), app.ID, service.QueueSubscriptionInput{Name: "jobs"})
	if err != nil {
		t.Fatal(err)
	}
	withoutConfirmation := requestJSON(t, router, http.MethodPost, "/api/v1/admin/apps/"+app.ID+"/subscriptions/"+subscription.ID+"/dead-letters/delete", []byte(`{"delivery_ids":["qdl_1"]}`), admin)
	assertStatusAndJSON(t, withoutConfirmation, http.StatusBadRequest, `{"error":{"code":"invalid_request","message":"The request is invalid."}}`)

	if _, err := apps.Disable(context.Background(), app.ID); err != nil {
		t.Fatal(err)
	}
	disabled := requestJSON(t, router, http.MethodGet, "/api/v1/admin/apps/"+app.ID+"/subscriptions", nil, admin)
	if disabled.Code != http.StatusNotFound {
		t.Fatalf("disabled app status=%d body=%s", disabled.Code, disabled.Body.String())
	}
}

func newAdminQueueRouter(t *testing.T) (http.Handler, *service.AppService, *service.QueueService) {
	t.Helper()
	repository := &adminQueueRepository{subscriptions: map[string]domain.QueueSubscription{}, schedules: map[string]domain.QueueSchedule{}, drains: map[string]domain.QueueDrain{}}
	now := func() time.Time { return time.Date(2026, 10, 31, 10, 0, 0, 0, time.UTC) }
	apps := service.NewAppService(newHTTPMemoryStore(), service.AppOptions{Now: now})
	queue := service.NewQueueService(repository, service.QueueOptions{Now: now, NewID: func() string { return "sub_admin" }, NewScheduleID: func() string { return "qsch_admin" }})
	users := &adminUserMemoryStore{password: "admin-bootstrap"}
	router := NewRouter(Dependencies{Admin: fstest.MapFS{}, AdminSessions: testAdminSessions(t), AdminUsers: users, Apps: apps, Queue: queue, Metrics: http.NotFoundHandler(), Now: now})
	return router, apps, queue
}

type adminQueueRepository struct {
	store.QueueRepository
	subscriptions map[string]domain.QueueSubscription
	schedules     map[string]domain.QueueSchedule
	drains        map[string]domain.QueueDrain
}

func (r *adminQueueRepository) CreateQueueSubscription(_ context.Context, item domain.QueueSubscription) error {
	for _, current := range r.subscriptions {
		if current.AppID == item.AppID && current.Name == item.Name {
			return store.ErrConflict
		}
	}
	r.subscriptions[item.ID] = item
	return nil
}
func (r *adminQueueRepository) ListQueueSubscriptions(_ context.Context, appID string) ([]domain.QueueSubscription, error) {
	items := make([]domain.QueueSubscription, 0)
	for _, item := range r.subscriptions {
		if item.AppID == appID {
			items = append(items, item)
		}
	}
	return items, nil
}
func (r *adminQueueRepository) GetQueueSubscription(_ context.Context, appID, id string) (domain.QueueSubscription, error) {
	item, ok := r.subscriptions[id]
	if !ok || item.AppID != appID {
		return domain.QueueSubscription{}, store.ErrNotFound
	}
	return item, nil
}
func (r *adminQueueRepository) UpdateQueueSubscription(_ context.Context, item domain.QueueSubscription, version int64) (domain.QueueSubscription, error) {
	current, ok := r.subscriptions[item.ID]
	if !ok || current.AppID != item.AppID {
		return domain.QueueSubscription{}, store.ErrNotFound
	}
	if current.PolicyVersion != version {
		return domain.QueueSubscription{}, store.ErrConflict
	}
	item.PolicyVersion = version + 1
	r.subscriptions[item.ID] = item
	return item, nil
}
func (r *adminQueueRepository) DeleteQueueSubscription(_ context.Context, appID, id string) error {
	item, err := r.GetQueueSubscription(context.Background(), appID, id)
	if err != nil {
		return err
	}
	delete(r.subscriptions, item.ID)
	return nil
}
func (r *adminQueueRepository) QueueDepth(context.Context, string, string, time.Time) (domain.QueueDepth, error) {
	return domain.QueueDepth{}, nil
}
func (r *adminQueueRepository) BeginQueueDrain(_ context.Context, appID, subscriptionID string, startedAt, deadlineAt time.Time) (domain.QueueDrain, error) {
	if _, err := r.GetQueueSubscription(context.Background(), appID, subscriptionID); err != nil {
		return domain.QueueDrain{}, err
	}
	drain := domain.QueueDrain{SubscriptionID: subscriptionID, Status: "draining", StartedAt: &startedAt, DeadlineAt: &deadlineAt}
	r.drains[subscriptionID] = drain
	return drain, nil
}
func (r *adminQueueRepository) GetQueueDrain(_ context.Context, appID, subscriptionID string, _ time.Time) (domain.QueueDrain, error) {
	if _, err := r.GetQueueSubscription(context.Background(), appID, subscriptionID); err != nil {
		return domain.QueueDrain{}, err
	}
	drain, ok := r.drains[subscriptionID]
	if !ok {
		return domain.QueueDrain{}, store.ErrNotFound
	}
	return drain, nil
}
func (r *adminQueueRepository) CreateQueueSchedule(_ context.Context, item domain.QueueSchedule) error {
	r.schedules[item.ID] = item
	return nil
}
func (r *adminQueueRepository) ListQueueSchedules(_ context.Context, appID, subscriptionID string) ([]domain.QueueSchedule, error) {
	items := make([]domain.QueueSchedule, 0)
	for _, item := range r.schedules {
		if item.AppID == appID && item.SubscriptionID == subscriptionID {
			items = append(items, item)
		}
	}
	return items, nil
}
func (r *adminQueueRepository) GetQueueSchedule(_ context.Context, appID, subscriptionID, id string) (domain.QueueSchedule, error) {
	item, ok := r.schedules[id]
	if !ok || item.AppID != appID || item.SubscriptionID != subscriptionID {
		return domain.QueueSchedule{}, store.ErrNotFound
	}
	return item, nil
}
func (r *adminQueueRepository) UpdateQueueSchedule(_ context.Context, item domain.QueueSchedule, version int64) (domain.QueueSchedule, error) {
	current, ok := r.schedules[item.ID]
	if !ok || current.PolicyVersion != version {
		return domain.QueueSchedule{}, store.ErrConflict
	}
	item.PolicyVersion = version + 1
	r.schedules[item.ID] = item
	return item, nil
}
func (r *adminQueueRepository) DeleteQueueSchedule(_ context.Context, appID, subscriptionID, id string) error {
	item, err := r.GetQueueSchedule(context.Background(), appID, subscriptionID, id)
	if err != nil {
		return err
	}
	delete(r.schedules, item.ID)
	return nil
}
func (r *adminQueueRepository) ListQueueResultCallbacks(context.Context, string, string, int) ([]domain.QueueCallbackOutcome, error) {
	return []domain.QueueCallbackOutcome{}, nil
}
