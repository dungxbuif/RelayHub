package store

import "context"

type HealthChecker interface {
	Ping(context.Context) error
}
