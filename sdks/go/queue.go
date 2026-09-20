package relayhub

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

type QueueSubscriptionInput struct {
	Name                     string          `json:"name"`
	Enabled                  *bool           `json:"enabled,omitempty"`
	EventTypes               []string        `json:"event_types,omitempty"`
	MaxAttempts              int             `json:"max_attempts,omitempty"`
	DefaultVisibilitySeconds int             `json:"default_visibility_seconds,omitempty"`
	MaxVisibilitySeconds     int             `json:"max_visibility_seconds,omitempty"`
	MaxTotalLeaseSeconds     int             `json:"max_total_lease_seconds,omitempty"`
	RetentionSeconds         int             `json:"retention_seconds,omitempty"`
	MaxInFlight              int             `json:"max_in_flight,omitempty"`
	MaxBatchSize             int             `json:"max_batch_size,omitempty"`
	RetryDelaySeconds        *int            `json:"retry_delay_seconds,omitempty"`
	OrderingMode             string          `json:"ordering_mode,omitempty"`
	DeduplicationSeconds     int             `json:"deduplication_seconds,omitempty"`
	MaxDispatchRate          *int            `json:"max_dispatch_rate,omitempty"`
	SuccessCallbackURL       *string         `json:"success_callback_url,omitempty"`
	FailureCallbackURL       *string         `json:"failure_callback_url,omitempty"`
	ResultCallbackMetadata   json.RawMessage `json:"result_callback_metadata,omitempty"`
}

type QueueSubscription struct {
	QueueSubscriptionInput
	ID              string     `json:"id"`
	AppID           string     `json:"app_id"`
	PausedAt        *time.Time `json:"paused_at,omitempty"`
	DrainingAt      *time.Time `json:"draining_at,omitempty"`
	DrainDeadlineAt *time.Time `json:"drain_deadline_at,omitempty"`
	DrainedAt       *time.Time `json:"drained_at,omitempty"`
	PolicyVersion   int64      `json:"policy_version"`
	CreatedAt       time.Time  `json:"created_at"`
	UpdatedAt       time.Time  `json:"updated_at"`
}
type QueueDrain struct {
	SubscriptionID string     `json:"subscription_id"`
	Status         string     `json:"status"`
	InFlight       int64      `json:"in_flight"`
	StartedAt      *time.Time `json:"started_at,omitempty"`
	DeadlineAt     *time.Time `json:"deadline_at,omitempty"`
	CompletedAt    *time.Time `json:"completed_at,omitempty"`
}
type QueueScheduleInput struct {
	Name           string          `json:"name"`
	Enabled        *bool           `json:"enabled,omitempty"`
	CronExpression string          `json:"cron_expression"`
	Timezone       string          `json:"timezone"`
	EventType      string          `json:"event_type"`
	Data           json.RawMessage `json:"data"`
	OrderingKey    string          `json:"ordering_key,omitempty"`
	Priority       int             `json:"priority,omitempty"`
	Metadata       json.RawMessage `json:"metadata,omitempty"`
}
type QueueSchedule struct {
	QueueScheduleInput
	ID             string     `json:"id"`
	AppID          string     `json:"app_id"`
	SubscriptionID string     `json:"subscription_id"`
	NextRunAt      time.Time  `json:"next_run_at"`
	LastRunAt      *time.Time `json:"last_run_at,omitempty"`
	PolicyVersion  int64      `json:"policy_version"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
}

type QueueDelivery struct {
	ID             string          `json:"id"`
	SubscriptionID string          `json:"subscription_id"`
	Receipt        string          `json:"receipt"`
	Event          Event           `json:"event"`
	Attempt        int             `json:"attempt"`
	Generation     int64           `json:"generation"`
	LeaseExpiresAt time.Time       `json:"lease_expires_at"`
	OrderingKey    string          `json:"ordering_key,omitempty"`
	Priority       int             `json:"priority"`
	Metadata       json.RawMessage `json:"metadata,omitempty"`
}

type QueueSettlement struct {
	Receipt      string `json:"receipt"`
	Disposition  string `json:"disposition"`
	DelaySeconds int    `json:"delay_seconds,omitempty"`
	Reason       string `json:"reason,omitempty"`
}
type QueueSettlementResult struct {
	Receipt string `json:"receipt"`
	Status  string `json:"status"`
}
type QueueLeaseExtension struct {
	Receipt          string `json:"receipt"`
	ExtensionSeconds int    `json:"extension_seconds"`
}
type QueueDepth struct {
	Available         int64      `json:"available"`
	InFlight          int64      `json:"in_flight"`
	Acknowledged      int64      `json:"acknowledged"`
	DeadLetter        int64      `json:"dead_letter"`
	OldestAvailableAt *time.Time `json:"oldest_available_at,omitempty"`
}
type QueueDeadLetter struct {
	DeliveryID     string    `json:"delivery_id"`
	SubscriptionID string    `json:"subscription_id"`
	EventID        string    `json:"event_id"`
	Attempts       int       `json:"attempts"`
	Generation     int64     `json:"generation"`
	Reason         string    `json:"reason"`
	UpdatedAt      time.Time `json:"updated_at"`
}

func (c *Client) CreateQueueSubscription(ctx context.Context, input QueueSubscriptionInput) (QueueSubscription, error) {
	var result QueueSubscription
	if strings.TrimSpace(input.Name) == "" {
		return result, ErrInvalidInput
	}
	_, err := c.request(ctx, http.MethodPost, "/api/v2/subscriptions", input, "", &result)
	return result, err
}
func (c *Client) ListQueueSubscriptions(ctx context.Context) ([]QueueSubscription, error) {
	var result struct {
		Items []QueueSubscription `json:"items"`
	}
	_, err := c.request(ctx, http.MethodGet, "/api/v2/subscriptions", nil, "", &result)
	return result.Items, err
}
func (c *Client) GetQueueSubscription(ctx context.Context, id string) (QueueSubscription, error) {
	var result QueueSubscription
	_, err := c.request(ctx, http.MethodGet, queuePath(id), nil, "", &result)
	return result, err
}
func (c *Client) UpdateQueueSubscription(ctx context.Context, id string, version int64, input QueueSubscriptionInput) (QueueSubscription, error) {
	var result QueueSubscription
	body := struct {
		QueueSubscriptionInput
		PolicyVersion int64 `json:"policy_version"`
	}{input, version}
	_, err := c.request(ctx, http.MethodPut, queuePath(id), body, "", &result)
	return result, err
}
func (c *Client) DeleteQueueSubscription(ctx context.Context, id string) error {
	_, err := c.request(ctx, http.MethodDelete, queuePath(id), nil, "", nil)
	return err
}
func (c *Client) PauseQueueSubscription(ctx context.Context, id string) (QueueSubscription, error) {
	return c.queueControl(ctx, id, "pause")
}
func (c *Client) ResumeQueueSubscription(ctx context.Context, id string) (QueueSubscription, error) {
	return c.queueControl(ctx, id, "resume")
}
func (c *Client) queueControl(ctx context.Context, id, action string) (QueueSubscription, error) {
	var result QueueSubscription
	_, err := c.request(ctx, http.MethodPost, queuePath(id)+"/"+action, nil, "", &result)
	return result, err
}

func (c *Client) PullQueue(ctx context.Context, id string, maxMessages int, wait, visibility time.Duration) ([]QueueDelivery, error) {
	if maxMessages < 1 || maxMessages > 100 || wait < 0 || wait > 30*time.Second || visibility < 0 || visibility > time.Hour {
		return nil, ErrInvalidInput
	}
	var result struct {
		Items []QueueDelivery `json:"items"`
	}
	body := struct{ MaxMessages, WaitSeconds, VisibilitySeconds int }{maxMessages, int(wait / time.Second), int(visibility / time.Second)}
	raw := struct {
		MaxMessages       int `json:"max_messages"`
		WaitSeconds       int `json:"wait_seconds,omitempty"`
		VisibilitySeconds int `json:"visibility_seconds,omitempty"`
	}{body.MaxMessages, body.WaitSeconds, body.VisibilitySeconds}
	_, err := c.request(ctx, http.MethodPost, queuePath(id)+"/pull", raw, "", &result)
	return result.Items, err
}
func (c *Client) SettleQueue(ctx context.Context, id string, items []QueueSettlement) ([]QueueSettlementResult, error) {
	var result struct {
		Items []QueueSettlementResult `json:"items"`
	}
	_, err := c.request(ctx, http.MethodPost, queuePath(id)+"/settle", struct {
		Items []QueueSettlement `json:"items"`
	}{items}, "", &result)
	return result.Items, err
}
func (c *Client) ExtendQueueLeases(ctx context.Context, id string, items []QueueLeaseExtension) ([]QueueSettlementResult, error) {
	var result struct {
		Items []QueueSettlementResult `json:"items"`
	}
	_, err := c.request(ctx, http.MethodPost, queuePath(id)+"/leases/extend", struct {
		Items []QueueLeaseExtension `json:"items"`
	}{items}, "", &result)
	return result.Items, err
}
func (c *Client) QueueMetrics(ctx context.Context, id string) (QueueDepth, error) {
	var result QueueDepth
	_, err := c.request(ctx, http.MethodGet, queuePath(id)+"/metrics", nil, "", &result)
	return result, err
}
func (c *Client) DrainQueueSubscription(ctx context.Context, id string, timeout time.Duration) (QueueDrain, error) {
	var result QueueDrain
	if timeout < 0 || timeout > time.Hour {
		return result, ErrInvalidInput
	}
	_, err := c.request(ctx, http.MethodPost, queuePath(id)+"/drain", map[string]int{"timeout_seconds": int(timeout / time.Second)}, "", &result)
	return result, err
}
func (c *Client) QueueDrainStatus(ctx context.Context, id string) (QueueDrain, error) {
	var result QueueDrain
	_, err := c.request(ctx, http.MethodGet, queuePath(id)+"/drain", nil, "", &result)
	return result, err
}
func (c *Client) CreateQueueSchedule(ctx context.Context, id string, input QueueScheduleInput) (QueueSchedule, error) {
	var result QueueSchedule
	_, err := c.request(ctx, http.MethodPost, queuePath(id)+"/schedules", input, "", &result)
	return result, err
}
func (c *Client) ListQueueSchedules(ctx context.Context, id string) ([]QueueSchedule, error) {
	var result struct {
		Items []QueueSchedule `json:"items"`
	}
	_, err := c.request(ctx, http.MethodGet, queuePath(id)+"/schedules", nil, "", &result)
	return result.Items, err
}
func (c *Client) UpdateQueueSchedule(ctx context.Context, id, scheduleID string, version int64, input QueueScheduleInput) (QueueSchedule, error) {
	var result QueueSchedule
	body := struct {
		QueueScheduleInput
		PolicyVersion int64 `json:"policy_version"`
	}{input, version}
	_, err := c.request(ctx, http.MethodPut, queuePath(id)+"/schedules/"+urlPathEscape(scheduleID), body, "", &result)
	return result, err
}
func (c *Client) DeleteQueueSchedule(ctx context.Context, id, scheduleID string) error {
	_, err := c.request(ctx, http.MethodDelete, queuePath(id)+"/schedules/"+urlPathEscape(scheduleID), nil, "", nil)
	return err
}
func (c *Client) ListQueueDeadLetters(ctx context.Context, id string) ([]QueueDeadLetter, error) {
	var result struct {
		Items []QueueDeadLetter `json:"items"`
	}
	_, err := c.request(ctx, http.MethodGet, queuePath(id)+"/dead-letters", nil, "", &result)
	return result.Items, err
}
func (c *Client) ExportQueueDeadLetters(ctx context.Context, id, cursor string, limit int) ([]QueueDeadLetter, string, error) {
	if limit < 1 || limit > 100 {
		return nil, "", ErrInvalidInput
	}
	query := "?format=json&limit=" + strconv.Itoa(limit)
	if cursor != "" {
		query += "&cursor=" + url.QueryEscape(cursor)
	}
	var result struct {
		Items []QueueDeadLetter `json:"items"`
	}
	_, err := c.request(ctx, http.MethodGet, queuePath(id)+"/dead-letters/export"+query, nil, "", &result)
	next := ""
	if len(result.Items) == limit {
		next = result.Items[len(result.Items)-1].DeliveryID
	}
	return result.Items, next, err
}
func (c *Client) ReplayQueueDeadLetters(ctx context.Context, id string, deliveryIDs []string) (int, error) {
	var result struct {
		Replayed int `json:"replayed"`
	}
	_, err := c.request(ctx, http.MethodPost, queuePath(id)+"/dead-letters/replay", struct {
		IDs []string `json:"delivery_ids"`
	}{deliveryIDs}, "", &result)
	return result.Replayed, err
}
func (c *Client) DeleteQueueDeadLetters(ctx context.Context, id string, deliveryIDs []string) (int, error) {
	var result struct {
		Deleted int `json:"deleted"`
	}
	_, err := c.request(ctx, http.MethodPost, queuePath(id)+"/dead-letters/delete", struct {
		IDs []string `json:"delivery_ids"`
	}{deliveryIDs}, "", &result)
	return result.Deleted, err
}

func queuePath(id string) string { return "/api/v2/subscriptions/" + urlPathEscape(id) }

type QueueResult struct {
	Disposition string
	Delay       time.Duration
	Reason      string
}

func QueueACK() QueueResult { return QueueResult{Disposition: "ack"} }
func QueueRetry(delay time.Duration, reason string) QueueResult {
	return QueueResult{Disposition: "retry", Delay: delay, Reason: reason}
}
func QueueDeadLetterResult(reason string) QueueResult {
	return QueueResult{Disposition: "dead_letter", Reason: reason}
}

type QueueHandler func(context.Context, QueueDelivery) QueueResult
type QueueWorkerOptions struct {
	Concurrency, BatchSize                  int
	Wait, Visibility, Heartbeat, RetryDelay time.Duration
	OnError                                 func(error)
}
type QueueWorker struct {
	cancel context.CancelFunc
	done   chan struct{}
	once   sync.Once
}

func (c *Client) WorkQueue(parent context.Context, subscriptionID string, handler QueueHandler, options QueueWorkerOptions) (*QueueWorker, error) {
	if parent == nil || subscriptionID == "" || handler == nil {
		return nil, ErrInvalidInput
	}
	if options.Concurrency == 0 {
		options.Concurrency = 4
	}
	if options.BatchSize == 0 {
		options.BatchSize = 10
	}
	if options.Wait == 0 {
		options.Wait = 20 * time.Second
	}
	if options.Visibility == 0 {
		options.Visibility = time.Minute
	}
	if options.Heartbeat == 0 {
		options.Heartbeat = 20 * time.Second
	}
	if options.RetryDelay == 0 {
		options.RetryDelay = 5 * time.Second
	}
	if options.Concurrency < 1 || options.Concurrency > 100 || options.BatchSize < 1 || options.BatchSize > 100 || options.Wait < 0 || options.Wait > 30*time.Second || options.Visibility < time.Second || options.Visibility > time.Hour || options.Heartbeat < time.Second || options.Heartbeat > time.Hour {
		return nil, ErrInvalidInput
	}
	pullCtx, cancel := context.WithCancel(parent)
	worker := &QueueWorker{cancel: cancel, done: make(chan struct{})}
	go worker.run(parent, pullCtx, c, subscriptionID, handler, options)
	return worker, nil
}

func (worker *QueueWorker) run(handlerCtx, pullCtx context.Context, client *Client, subscriptionID string, handler QueueHandler, options QueueWorkerOptions) {
	defer close(worker.done)
	semaphore := make(chan struct{}, options.Concurrency)
	var active sync.WaitGroup
	for {
		capacity := options.Concurrency - len(semaphore)
		if capacity == 0 {
			select {
			case <-pullCtx.Done():
				active.Wait()
				return
			case <-time.After(10 * time.Millisecond):
				continue
			}
		}
		batch, err := client.PullQueue(pullCtx, subscriptionID, minInt(options.BatchSize, capacity), options.Wait, options.Visibility)
		if err != nil {
			if pullCtx.Err() != nil {
				active.Wait()
				return
			}
			if options.OnError != nil {
				options.OnError(err)
			}
			select {
			case <-pullCtx.Done():
				active.Wait()
				return
			case <-time.After(250 * time.Millisecond):
				continue
			}
		}
		for _, delivery := range batch {
			semaphore <- struct{}{}
			active.Add(1)
			go func(item QueueDelivery) {
				defer func() { <-semaphore; active.Done() }()
				processQueueDelivery(handlerCtx, client, subscriptionID, item, handler, options)
			}(delivery)
		}
	}
}

func processQueueDelivery(ctx context.Context, client *Client, subscriptionID string, delivery QueueDelivery, handler QueueHandler, options QueueWorkerOptions) {
	handlerCtx, cancelHandler := context.WithCancel(ctx)
	defer cancelHandler()
	stopHeartbeat := make(chan struct{})
	heartbeatStopped := make(chan struct{})
	go func() {
		defer close(heartbeatStopped)
		ticker := time.NewTicker(options.Heartbeat)
		defer ticker.Stop()
		for {
			select {
			case <-stopHeartbeat:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				items, err := client.ExtendQueueLeases(handlerCtx, subscriptionID, []QueueLeaseExtension{{Receipt: delivery.Receipt, ExtensionSeconds: int(options.Heartbeat / time.Second)}})
				if err == nil && (len(items) != 1 || items[0].Receipt != delivery.Receipt || items[0].Status != "extended") {
					err = ErrProtocol
				}
				if err != nil {
					cancelHandler()
					if options.OnError != nil {
						options.OnError(err)
					}
					return
				}
			}
		}
	}()
	result := safeQueueHandler(handlerCtx, handler, delivery, options.RetryDelay)
	close(stopHeartbeat)
	<-heartbeatStopped
	if handlerCtx.Err() != nil {
		return
	}
	if result.Delay < 0 {
		result.Delay = 0
	}
	if result.Delay > 24*time.Hour {
		result.Delay = 24 * time.Hour
	}
	if len(result.Reason) > 1024 {
		result.Reason = result.Reason[:1024]
	}
	delaySeconds := int((result.Delay + time.Second - 1) / time.Second)
	if result.Delay == 0 {
		delaySeconds = 0
	}
	settlement := QueueSettlement{Receipt: delivery.Receipt, Disposition: result.Disposition, DelaySeconds: delaySeconds, Reason: result.Reason}
	items, err := client.SettleQueue(ctx, subscriptionID, []QueueSettlement{settlement})
	if err == nil && (len(items) != 1 || items[0].Receipt != delivery.Receipt || items[0].Status == "invalid_receipt") {
		err = ErrProtocol
	}
	if err != nil && options.OnError != nil {
		options.OnError(err)
	}
}
func safeQueueHandler(ctx context.Context, handler QueueHandler, delivery QueueDelivery, retry time.Duration) (result QueueResult) {
	defer func() {
		if recover() != nil {
			result = QueueRetry(retry, "handler_panic")
		}
	}()
	result = handler(ctx, delivery)
	if result.Disposition == "" {
		result = QueueACK()
	}
	if result.Disposition != "ack" && result.Disposition != "retry" && result.Disposition != "dead_letter" {
		return QueueRetry(retry, "invalid_handler_result")
	}
	return result
}
func (worker *QueueWorker) Drain(ctx context.Context) error {
	worker.once.Do(worker.cancel)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-worker.done:
		return nil
	}
}
func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
