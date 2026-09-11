//go:build ignore

package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestInheritedPipeFixture(t *testing.T) {
	mode := os.Getenv("RELAYHUB_COMMAND_FIXTURE")
	if mode == "" {
		return
	}
	signal.Ignore(syscall.SIGTERM)
	if mode == "child" {
		if err := os.WriteFile(os.Getenv("RELAYHUB_CHILD_PID"), []byte(strconv.Itoa(os.Getpid())), 0600); err != nil {
			os.Exit(2)
		}
		for {
			time.Sleep(time.Second)
		}
	}
	child := exec.Command(os.Args[0], "-test.run=^TestInheritedPipeFixture$")
	child.Env = append(os.Environ(), "RELAYHUB_COMMAND_FIXTURE=child")
	child.Stdout = os.Stdout
	child.Stderr = os.Stderr
	if child.Start() != nil {
		os.Exit(2)
	}
	if mode == "orphan" {
		os.Exit(0)
	}
	_ = child.Wait()
	os.Exit(0)
}

func TestKeptProjectCleanupNeedsNoComposeConfigOrSecrets(t *testing.T) {
	root := t.TempDir()
	log := filepath.Join(root, "calls")
	// The Docker boundary exposes only resources belonging to the expected label.
	fixture := "#!/bin/sh\nset -eu\nprintf '%s\\n' \"$*\" >> \"$CLEANUP_LOG\"\ncase \"$*\" in\n'ps -aq --filter label=com.docker.compose.project=relayhub-e2e-fixture') echo fixture-container;;\n'network ls -q --filter label=com.docker.compose.project=relayhub-e2e-fixture') echo fixture-network;;\n'volume ls -q --filter label=com.docker.compose.project=relayhub-e2e-fixture') echo fixture-volume;;\n'rm -f fixture-container'|'network rm fixture-network'|'volume rm fixture-volume'|'image rm relayhub-e2e-fixture:local') :;;\n*) exit 29;;\nesac\n"
	if err := os.WriteFile(filepath.Join(root, "docker"), []byte(fixture), 0700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	out, err := runCommand(ctx, []string{"PATH=" + root + ":/usr/bin:/bin", "CLEANUP_LOG=" + log}, nil, "sh", "-c", keptProjectCleanupCommand("relayhub-e2e-fixture"))
	if err != nil {
		t.Fatalf("cleanup depends on deleted config or credentials: %v %s", err, out)
	}
	got, err := os.ReadFile(log)
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{"rm -f fixture-container", "network rm fixture-network", "volume rm fixture-volume", "image rm relayhub-e2e-fixture:local"} {
		if !strings.Contains(string(got), expected+"\n") {
			t.Fatalf("cleanup missed %s: %s", expected, got)
		}
	}
}

func TestBoundedCommandKillsPipeHoldingDescendantsAndContinuesCleanup(t *testing.T) {
	for _, mode := range []string{"orphan", "parent"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			pidFile := filepath.Join(dir, "child.pid")
			marker := filepath.Join(dir, "cleanup")
			env := append(os.Environ(), "RELAYHUB_COMMAND_FIXTURE="+mode, "RELAYHUB_CHILD_PID="+pidFile)
			var childPID int
			t.Cleanup(func() {
				if childPID > 0 {
					_ = syscall.Kill(childPID, syscall.SIGKILL)
				}
			})
			ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
			defer cancel()
			done := make(chan error, 1)
			started := time.Now()
			go func() {
				defer func() { _ = os.WriteFile(marker, []byte("continued"), 0600) }()
				_, err := runCommand(ctx, env, nil, os.Args[0], "-test.run=^TestInheritedPipeFixture$")
				done <- err
			}()
			until := time.Now().Add(time.Second)
			for childPID == 0 && time.Now().Before(until) {
				if b, e := os.ReadFile(pidFile); e == nil {
					childPID, _ = strconv.Atoi(string(b))
				}
				time.Sleep(10 * time.Millisecond)
			}
			if childPID == 0 {
				t.Fatal("fixture did not publish child PID")
			}
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("orphan/deadline command reported success")
				}
			case <-time.After(2 * time.Second):
				_ = syscall.Kill(childPID, syscall.SIGKILL)
				<-done
				t.Fatal("command waited indefinitely for inherited pipes")
			}
			if time.Since(started) > 2*time.Second {
				t.Fatal("command deadline was not bounded")
			}
			until = time.Now().Add(time.Second)
			for time.Now().Before(until) {
				if errors.Is(syscall.Kill(childPID, 0), syscall.ESRCH) {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			if !errors.Is(syscall.Kill(childPID, 0), syscall.ESRCH) {
				t.Fatal("pipe-holding child survived cancellation")
			}
			childPID = 0
			until = time.Now().Add(time.Second)
			for time.Now().Before(until) {
				if b, e := os.ReadFile(marker); e == nil && string(b) == "continued" {
					return
				}
				time.Sleep(time.Millisecond)
			}
			t.Fatal("deferred cleanup did not continue")
		})
	}
}

func TestBoundedCommandNormalOutputAndInput(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	out, err := runCommand(ctx, nil, strings.NewReader("exact bytes"), "sh", "-c", "cat; printf stderr >&2")
	if err != nil || string(out) != "exact bytesstderr" {
		t.Fatal(fmt.Sprintf("normal command failed: %v", err))
	}
}
