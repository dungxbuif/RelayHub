package postgres

import (
	"context"
	"crypto/rand"
	"errors"
	"net/mail"
	"strings"
	"time"

	"github.com/dungxbuif/RelayHub/internal/auth"
	"github.com/dungxbuif/RelayHub/internal/store"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

var _ store.AdminUserStore = (*Client)(nil)
var _ store.AdminUserLifecycleStore = (*Client)(nil)

func normalizeAdminEmail(email string) (string, error) {
	email = strings.ToLower(strings.TrimSpace(email))
	parsed, err := mail.ParseAddress(email)
	if err != nil || parsed.Address != email || email == "" || len(email) > 320 {
		return "", store.ErrInvalidTarget
	}
	return email, nil
}

func (client *Client) AuthenticateAdminUser(ctx context.Context, email, password string) (store.AdminUser, error) {
	normalized, err := normalizeAdminEmail(email)
	if err != nil {
		return store.AdminUser{}, store.ErrNotFound
	}
	var user store.AdminUser
	var passwordHash string
	err = client.pool.QueryRow(ctx, `SELECT id,email,password_hash,role,enabled,created_at,updated_at FROM admin_users WHERE lower(email)=lower($1) AND enabled=true`, normalized).Scan(&user.ID, &user.Email, &passwordHash, &user.Role, &user.Enabled, &user.CreatedAt, &user.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return store.AdminUser{}, store.ErrNotFound
	}
	if err != nil {
		return store.AdminUser{}, err
	}
	if !auth.VerifyPassword(passwordHash, password) {
		return store.AdminUser{}, store.ErrNotFound
	}
	return user, nil
}

func (client *Client) CreateAdminUser(ctx context.Context, user store.AdminUser, password string) error {
	email, err := normalizeAdminEmail(user.Email)
	if err != nil || (user.Role != "admin" && user.Role != "user") || user.ID == "" || user.CreatedAt.IsZero() || user.UpdatedAt.IsZero() {
		return store.ErrInvalidTarget
	}
	hash, err := auth.HashPassword(password, rand.Reader)
	if err != nil {
		return store.ErrInvalidTarget
	}
	_, err = client.pool.Exec(ctx, `INSERT INTO admin_users(id,email,password_hash,role,enabled,created_at,updated_at) VALUES($1,$2,$3,$4,true,$5,$6)`, user.ID, email, hash, user.Role, user.CreatedAt, user.UpdatedAt)
	if err != nil {
		if uniqueViolation(err) {
			return store.ErrConflict
		}
		return err
	}
	return nil
}

func (client *Client) ListAdminUsers(ctx context.Context) ([]store.AdminUser, error) {
	rows, err := client.pool.Query(ctx, `SELECT id,email,role,enabled,created_at,updated_at FROM admin_users ORDER BY created_at,id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	users := make([]store.AdminUser, 0)
	for rows.Next() {
		user, err := scanAdminUser(rows)
		if err != nil {
			return nil, err
		}
		users = append(users, user)
	}
	return users, rows.Err()
}

func (client *Client) GetAdminUser(ctx context.Context, id string) (store.AdminUser, error) {
	user, err := scanAdminUser(client.pool.QueryRow(ctx, `SELECT id,email,role,enabled,created_at,updated_at FROM admin_users WHERE id=$1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return store.AdminUser{}, store.ErrNotFound
	}
	return user, err
}

func (client *Client) UpdateAdminUser(ctx context.Context, user store.AdminUser) (store.AdminUser, error) {
	if user.ID == "" || (user.Role != "admin" && user.Role != "user") || user.UpdatedAt.IsZero() {
		return store.AdminUser{}, store.ErrInvalidTarget
	}
	updated, err := scanAdminUser(client.pool.QueryRow(ctx, `UPDATE admin_users SET role=$2,enabled=$3,updated_at=$4 WHERE id=$1 RETURNING id,email,role,enabled,created_at,updated_at`, user.ID, user.Role, user.Enabled, user.UpdatedAt))
	if errors.Is(err, pgx.ErrNoRows) {
		return store.AdminUser{}, store.ErrNotFound
	}
	return updated, err
}

func (client *Client) DisableAdminUser(ctx context.Context, id string, updatedAt time.Time) (store.AdminUser, error) {
	if id == "" || updatedAt.IsZero() {
		return store.AdminUser{}, store.ErrInvalidTarget
	}
	updated, err := scanAdminUser(client.pool.QueryRow(ctx, `UPDATE admin_users SET enabled=false,updated_at=$2 WHERE id=$1 RETURNING id,email,role,enabled,created_at,updated_at`, id, updatedAt))
	if errors.Is(err, pgx.ErrNoRows) {
		return store.AdminUser{}, store.ErrNotFound
	}
	return updated, err
}

func (client *Client) RotateAdminUserPassword(ctx context.Context, id, password string, updatedAt time.Time) (store.AdminUser, error) {
	if id == "" || password == "" || updatedAt.IsZero() {
		return store.AdminUser{}, store.ErrInvalidTarget
	}
	hash, err := auth.HashPassword(password, rand.Reader)
	if err != nil {
		return store.AdminUser{}, store.ErrInvalidTarget
	}
	updated, err := scanAdminUser(client.pool.QueryRow(ctx, `UPDATE admin_users SET password_hash=$2,updated_at=$3 WHERE id=$1 RETURNING id,email,role,enabled,created_at,updated_at`, id, hash, updatedAt))
	if errors.Is(err, pgx.ErrNoRows) {
		return store.AdminUser{}, store.ErrNotFound
	}
	return updated, err
}

func (client *Client) EnsureSeedAdminUser(ctx context.Context, email, password string, now time.Time, role string) error {
	email, err := normalizeAdminEmail(email)
	if err != nil || (role != "admin" && role != "user") {
		return store.ErrInvalidTarget
	}
	var exists bool
	if err := client.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM admin_users WHERE lower(email)=lower($1))`, email).Scan(&exists); err != nil {
		return err
	}
	if exists {
		return nil
	}
	return client.CreateAdminUser(ctx, store.AdminUser{ID: "adm_" + uuid.NewString(), Email: email, Role: role, Enabled: true, CreatedAt: now.UTC(), UpdatedAt: now.UTC()}, password)
}

func scanAdminUser(row rowScanner) (store.AdminUser, error) {
	var user store.AdminUser
	err := row.Scan(&user.ID, &user.Email, &user.Role, &user.Enabled, &user.CreatedAt, &user.UpdatedAt)
	return user, err
}
