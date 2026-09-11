// Package config loads local process settings. Provider policy lives elsewhere.
package config

import (
	"fmt"
	"net"
	"strconv"
)

type Config struct{ Addr string }

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
	return Config{Addr: addr}, nil
}
