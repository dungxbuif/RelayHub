package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"golang.org/x/crypto/pbkdf2"
)

const (
	passwordAlgorithm  = "pbkdf2-sha256"
	passwordIterations = 210_000
	passwordSaltBytes  = 16
	passwordKeyBytes   = 32
)

func HashPassword(password string, random io.Reader) (string, error) {
	if len(strings.TrimSpace(password)) < 12 {
		return "", errors.New("password is too short")
	}
	if random == nil {
		random = rand.Reader
	}
	salt := make([]byte, passwordSaltBytes)
	if _, err := io.ReadFull(random, salt); err != nil {
		return "", errors.New("generate password salt")
	}
	key := pbkdf2.Key([]byte(password), salt, passwordIterations, passwordKeyBytes, sha256.New)
	return fmt.Sprintf("%s$%d$%s$%s", passwordAlgorithm, passwordIterations, base64.RawURLEncoding.EncodeToString(salt), base64.RawURLEncoding.EncodeToString(key)), nil
}

func VerifyPassword(encoded, password string) bool {
	parts := strings.Split(encoded, "$")
	if len(parts) != 4 || parts[0] != passwordAlgorithm {
		return false
	}
	iterations, err := strconv.Atoi(parts[1])
	if err != nil || iterations < 100_000 || iterations > 1_000_000 {
		return false
	}
	salt, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil || len(salt) < passwordSaltBytes {
		return false
	}
	want, err := base64.RawURLEncoding.DecodeString(parts[3])
	if err != nil || len(want) != passwordKeyBytes {
		return false
	}
	got := pbkdf2.Key([]byte(password), salt, iterations, len(want), sha256.New)
	return subtle.ConstantTimeCompare(got, want) == 1
}
