package broker

import (
	"context"
	"errors"
	"testing"
)

type healthFunc func(context.Context) error

func (fn healthFunc) Ping(ctx context.Context) error { return fn(ctx) }

func TestCompositeHealthChecksEveryRequiredDependency(t *testing.T) {
	called := 0
	health := CompositeHealth{
		healthFunc(func(context.Context) error { called++; return nil }),
		healthFunc(func(context.Context) error { called++; return errors.New("nats unavailable") }),
	}
	if err := health.Ping(context.Background()); err == nil {
		t.Fatal("Ping() error = nil, want dependency failure")
	}
	if called != 2 {
		t.Fatalf("Ping() called %d dependencies, want 2", called)
	}
}

func TestCompositeHealthRejectsMissingDependency(t *testing.T) {
	if err := (CompositeHealth{nil}).Ping(context.Background()); err == nil {
		t.Fatal("Ping() error = nil, want missing dependency failure")
	}
}
