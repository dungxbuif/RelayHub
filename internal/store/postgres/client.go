package postgres

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	secretcrypto "github.com/dungxbuif/RelayHub/internal/crypto"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const migrationLockID int64 = 734872944296891186

//go:embed migrations/*.sql
var migrationFiles embed.FS

type Config struct {
	DatabaseURL           string
	MaxConnections        int32
	MinConnections        int32
	MaxConnectionLifetime time.Duration
	MaxConnectionIdleTime time.Duration
	HealthCheckPeriod     time.Duration
}

func (config Config) Validate() error {
	parsed, err := url.Parse(config.DatabaseURL)
	if err != nil || (parsed.Scheme != "postgres" && parsed.Scheme != "postgresql") || parsed.Host == "" || strings.Trim(parsed.Path, "/") == "" {
		return errors.New("PostgreSQL URL is invalid")
	}
	if config.MaxConnections < 1 || config.MinConnections < 0 || config.MinConnections > config.MaxConnections {
		return errors.New("PostgreSQL pool bounds are invalid")
	}
	return nil
}

func (config Config) SafeDatabaseAddress() string {
	parsed, err := url.Parse(config.DatabaseURL)
	if err != nil {
		return "invalid PostgreSQL address"
	}
	if parsed.User != nil {
		parsed.User = url.User(parsed.User.Username())
	}
	parsed.RawQuery, parsed.Fragment = "", ""
	return parsed.String()
}

type Client struct {
	pool   *pgxpool.Pool
	cipher *secretcrypto.SecretCipher
}

func NewClient(ctx context.Context, config Config, cipher *secretcrypto.SecretCipher) (*Client, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	if cipher == nil {
		return nil, errors.New("secret cipher is required")
	}
	poolConfig, err := pgxpool.ParseConfig(config.DatabaseURL)
	if err != nil {
		return nil, errors.New("parse PostgreSQL configuration")
	}
	poolConfig.MaxConns = config.MaxConnections
	poolConfig.MinConns = config.MinConnections
	if config.MaxConnectionLifetime > 0 {
		poolConfig.MaxConnLifetime = config.MaxConnectionLifetime
	}
	if config.MaxConnectionIdleTime > 0 {
		poolConfig.MaxConnIdleTime = config.MaxConnectionIdleTime
	}
	if config.HealthCheckPeriod > 0 {
		poolConfig.HealthCheckPeriod = config.HealthCheckPeriod
	}
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, errors.New("create PostgreSQL pool")
	}
	client := &Client{pool: pool, cipher: cipher}
	if err := client.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("connect PostgreSQL at %s: %w", config.SafeDatabaseAddress(), err)
	}
	return client, nil
}

func (client *Client) Close()                         { client.pool.Close() }
func (client *Client) Ping(ctx context.Context) error { return client.pool.Ping(ctx) }

type migration struct {
	Version  int64
	Name     string
	Checksum string
	SQL      []byte
}

func loadMigrations() ([]migration, error) {
	entries, err := migrationFiles.ReadDir("migrations")
	if err != nil {
		return nil, err
	}
	result := make([]migration, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".sql" {
			continue
		}
		parts := strings.SplitN(strings.TrimSuffix(entry.Name(), ".sql"), "_", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid migration filename %q", entry.Name())
		}
		version, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil || version < 1 {
			return nil, fmt.Errorf("invalid migration filename %q", entry.Name())
		}
		sql, err := migrationFiles.ReadFile("migrations/" + entry.Name())
		if err != nil {
			return nil, err
		}
		result = append(result, migration{Version: version, Name: parts[1], Checksum: migrationChecksum(sql), SQL: sql})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Version < result[j].Version })
	for index, item := range result {
		if item.Version != int64(index+1) {
			return nil, fmt.Errorf("migration versions must be contiguous from 1")
		}
	}
	return result, nil
}

func migrationChecksum(sql []byte) string {
	digest := sha256.Sum256(sql)
	return hex.EncodeToString(digest[:])
}

func (client *Client) Migrate(ctx context.Context) error {
	items, err := loadMigrations()
	if err != nil {
		return fmt.Errorf("load PostgreSQL migrations: %w", err)
	}
	connection, err := client.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire PostgreSQL migration connection: %w", err)
	}
	defer connection.Release()
	if _, err := connection.Exec(ctx, `SELECT pg_advisory_lock($1)`, migrationLockID); err != nil {
		return fmt.Errorf("lock PostgreSQL migrations: %w", err)
	}
	defer func() { _, _ = connection.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, migrationLockID) }()
	if _, err := connection.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version bigint PRIMARY KEY,
			name text NOT NULL,
			checksum text NOT NULL,
			applied_at timestamptz NOT NULL DEFAULT now()
		)`); err != nil {
		return fmt.Errorf("create PostgreSQL migration ledger: %w", err)
	}
	var unknownVersion int64
	err = connection.QueryRow(ctx, `SELECT version FROM schema_migrations WHERE version < 1 OR version > $1 ORDER BY version DESC LIMIT 1`, len(items)).Scan(&unknownVersion)
	if err == nil {
		return fmt.Errorf("PostgreSQL schema version %d is newer than this RelayHub binary", unknownVersion)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("inspect PostgreSQL migration ledger: %w", err)
	}
	for _, item := range items {
		var checksum string
		err := connection.QueryRow(ctx, `SELECT checksum FROM schema_migrations WHERE version=$1`, item.Version).Scan(&checksum)
		if err == nil {
			if checksum != item.Checksum {
				return fmt.Errorf("PostgreSQL migration %d checksum changed", item.Version)
			}
			continue
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("read PostgreSQL migration %d: %w", item.Version, err)
		}
		if err := applyMigration(ctx, connection.Conn(), item); err != nil {
			return err
		}
	}
	return nil
}

type migrationBeginner interface {
	Begin(context.Context) (pgx.Tx, error)
}

func applyMigration(ctx context.Context, connection migrationBeginner, item migration) error {
	tx, err := connection.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin PostgreSQL migration %d: %w", item.Version, err)
	}
	if _, err = tx.Exec(ctx, string(item.SQL)); err == nil {
		_, err = tx.Exec(ctx, `INSERT INTO schema_migrations(version,name,checksum) VALUES($1,$2,$3)`, item.Version, item.Name, item.Checksum)
	}
	if err != nil {
		_ = tx.Rollback(ctx)
		return fmt.Errorf("apply PostgreSQL migration %d: %w", item.Version, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit PostgreSQL migration %d: %w", item.Version, err)
	}
	return nil
}
