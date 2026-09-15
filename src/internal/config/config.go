package config

import (
	"fmt"
	"net"
	"strconv"
)

type Config struct {
	Addr          string
	BackendToken  string
	WorkerToken   string
	RealtimeToken string
}

const (
	defaultBackendToken  = "rh_backend_demo_token"
	defaultWorkerToken   = "rh_worker_demo_token"
	defaultRealtimeToken = "rh_realtime_demo_token"
)

func Load(getenv func(string) string) (Config, error) {
	addr := getenv("RELAYHUB_ADDR")
	if addr == "" {
		addr = "127.0.0.1:8080"
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil || host == "" {
		return Config{}, fmt.Errorf("RELAYHUB_ADDR must contain an explicit host and numeric port")
	}
	n, err := strconv.Atoi(port)
	if err != nil || n < 0 || n > 65535 {
		return Config{}, fmt.Errorf("RELAYHUB_ADDR port must be between 0 and 65535")
	}

	backend := getenv("RELAYHUB_BACKEND_TOKEN")
	if backend == "" {
		backend = defaultBackendToken
	}
	worker := getenv("RELAYHUB_WORKER_TOKEN")
	if worker == "" {
		worker = defaultWorkerToken
	}
	realtime := getenv("RELAYHUB_REALTIME_TOKEN")
	if realtime == "" {
		realtime = defaultRealtimeToken
	}

	if backend == worker || backend == realtime || worker == realtime {
		return Config{}, fmt.Errorf("RELAYHUB_*_TOKEN values must be distinct")
	}

	return Config{
		Addr:          addr,
		BackendToken:  backend,
		WorkerToken:   worker,
		RealtimeToken: realtime,
	}, nil
}
