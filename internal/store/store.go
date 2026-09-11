package store

import (
	"context"
	"errors"
	"time"

	"github.com/dungxbuif/RelayHub/internal/domain"
)

var (
	ErrNotFound = errors.New("store record not found")
	ErrConflict = errors.New("store record conflict")
)

type HealthChecker interface {
	Ping(context.Context) error
}

type AppCredential struct {
	AppID      string
	APIKeyHash string
	HMACSecret []byte
}

type ApplicationStore interface {
	CreateApplication(context.Context, domain.App, AppCredential) error
	ListApplications(context.Context) ([]domain.App, error)
	GetApplication(context.Context, string) (domain.App, error)
	UpdateApplication(context.Context, domain.App) (domain.App, error)
	DisableApplication(context.Context, string, time.Time) (domain.App, error)
	FindCredentialByAPIKeyHash(context.Context, string) (AppCredential, error)
	RotateApplicationCredential(context.Context, string, AppCredential, time.Time) error
}

type Store interface {
	HealthChecker
	ApplicationStore
}
