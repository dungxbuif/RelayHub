package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"strings"
)

const secretCipherPrefix = "rhsec:v1:"

// SecretCipher seals stored application secrets with an explicit format version.
// The master key is never retained as text after construction.
type SecretCipher struct{ aead cipher.AEAD }

func NewSecretCipher(encodedKey string) (*SecretCipher, error) {
	key, err := base64.StdEncoding.DecodeString(strings.TrimSpace(encodedKey))
	if err != nil || len(key) != 32 {
		return nil, errors.New("secret encryption key must be base64-encoded 32 bytes")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("create secret cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("create secret AEAD: %w", err)
	}
	return &SecretCipher{aead: aead}, nil
}

func (cipher *SecretCipher) Encrypt(plain []byte) (string, error) {
	if cipher == nil || cipher.aead == nil {
		return "", errors.New("secret cipher is not configured")
	}
	nonce := make([]byte, cipher.aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", fmt.Errorf("generate secret nonce: %w", err)
	}
	sealed := cipher.aead.Seal(nonce, nonce, plain, []byte(secretCipherPrefix))
	return secretCipherPrefix + base64.RawURLEncoding.EncodeToString(sealed), nil
}

func (cipher *SecretCipher) Decrypt(encoded string) ([]byte, error) {
	if cipher == nil || cipher.aead == nil {
		return nil, errors.New("secret cipher is not configured")
	}
	if !strings.HasPrefix(encoded, secretCipherPrefix) {
		return nil, errors.New("unsupported secret ciphertext version")
	}
	sealed, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(encoded, secretCipherPrefix))
	if err != nil || len(sealed) < cipher.aead.NonceSize()+cipher.aead.Overhead() {
		return nil, errors.New("invalid secret ciphertext")
	}
	nonce := sealed[:cipher.aead.NonceSize()]
	plain, err := cipher.aead.Open(nil, nonce, sealed[cipher.aead.NonceSize():], []byte(secretCipherPrefix))
	if err != nil {
		return nil, errors.New("invalid secret ciphertext")
	}
	return plain, nil
}
