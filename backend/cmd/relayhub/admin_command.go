package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/dungxbuif/RelayHub/internal/config"
	secretcrypto "github.com/dungxbuif/RelayHub/internal/crypto"
	postgresstore "github.com/dungxbuif/RelayHub/internal/store/postgres"
)

func runAdminCommand(ctx context.Context) error {
	if len(os.Args) < 3 || os.Args[2] != "seed-user" {
		return errors.New("usage: relayhub admin seed-user --email user@example.com --role admin --password-stdin")
	}
	flags := flag.NewFlagSet("seed-user", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	email := flags.String("email", "", "Admin user email")
	role := flags.String("role", "admin", "User role: admin or user")
	passwordStdin := flags.Bool("password-stdin", false, "Read password from stdin")
	if err := flags.Parse(os.Args[3:]); err != nil {
		return err
	}
	if strings.TrimSpace(*email) == "" || (*role != "admin" && *role != "user") || !*passwordStdin {
		return errors.New("usage: relayhub admin seed-user --email user@example.com --role admin --password-stdin")
	}
	password, err := readPasswordFromStdin()
	if err != nil {
		return err
	}
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load configuration: %w", err)
	}
	cipher, err := secretcrypto.NewSecretCipher(cfg.SecretEncryptionKey)
	if err != nil {
		return errors.New("configure PostgreSQL secret encryption")
	}
	client, err := postgresstore.NewClient(ctx, postgresstore.Config{DatabaseURL: cfg.PostgresURL, MaxConnections: 2, MinConnections: 0}, cipher)
	if err != nil {
		return errors.New("connect PostgreSQL: PostgreSQL unavailable")
	}
	defer client.Close()
	if err := client.Migrate(ctx); err != nil {
		return fmt.Errorf("migrate PostgreSQL: %w", err)
	}
	if err := client.EnsureSeedAdminUser(ctx, *email, password, time.Now().UTC(), *role); err != nil {
		return errors.New("seed Admin user")
	}
	fmt.Fprintln(os.Stdout, "Admin user ensured.")
	return nil
}

func readPasswordFromStdin() (string, error) {
	reader := bufio.NewReader(os.Stdin)
	password, err := reader.ReadString('\n')
	if err != nil && password == "" {
		return "", errors.New("read password from stdin")
	}
	password = strings.TrimRight(password, "\r\n")
	if strings.TrimSpace(password) == "" {
		return "", errors.New("password is required")
	}
	return password, nil
}
