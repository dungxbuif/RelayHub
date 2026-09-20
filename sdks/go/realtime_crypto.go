package relayhub

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
)

type RealtimeEncryptionKeyProvider interface {
	EncryptionKey(context.Context, string) (keyID string, key []byte, err error)
	DecryptionKey(context.Context, string, string) ([]byte, error)
}

func EncryptRealtimePayload(ctx context.Context, provider RealtimeEncryptionKeyProvider, channel string, value any) (RealtimeEncryptionEnvelope, error) {
	if provider == nil || !privateRealtimeChannel(channel) || ctx.Err() != nil {
		return RealtimeEncryptionEnvelope{}, ErrInvalidInput
	}
	plaintext, err := json.Marshal(value)
	if err != nil || len(plaintext) < 2 || plaintext[0] != '{' || plaintext[len(plaintext)-1] != '}' || len(plaintext) > 48*1024 {
		return RealtimeEncryptionEnvelope{}, ErrInvalidInput
	}
	keyID, key, err := provider.EncryptionKey(ctx, channel)
	if err != nil {
		return RealtimeEncryptionEnvelope{}, err
	}
	if !realtimeClient.MatchString(keyID) || len(key) != 32 {
		return RealtimeEncryptionEnvelope{}, ErrInvalidInput
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return RealtimeEncryptionEnvelope{}, ErrInvalidInput
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return RealtimeEncryptionEnvelope{}, ErrInvalidInput
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return RealtimeEncryptionEnvelope{}, err
	}
	aad := []byte(channel + "\n" + keyID)
	ciphertext := aead.Seal(nil, nonce, plaintext, aad)
	return RealtimeEncryptionEnvelope{Algorithm: "aes-256-gcm", KeyID: keyID, Nonce: base64.RawURLEncoding.EncodeToString(nonce), Ciphertext: base64.RawURLEncoding.EncodeToString(ciphertext)}, nil
}

func DecryptRealtimeFrame(ctx context.Context, provider RealtimeEncryptionKeyProvider, frame RealtimeFrame, target any) error {
	if provider == nil || target == nil || frame.Encryption == nil || !validRealtimeEncryptionEnvelope(frame.Channel, *frame.Encryption) || ctx.Err() != nil {
		return ErrInvalidInput
	}
	key, err := provider.DecryptionKey(ctx, frame.Channel, frame.Encryption.KeyID)
	if err != nil {
		return err
	}
	if len(key) != 32 {
		return ErrInvalidInput
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return ErrInvalidInput
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return ErrInvalidInput
	}
	nonce, _ := base64.RawURLEncoding.DecodeString(frame.Encryption.Nonce)
	ciphertext, _ := base64.RawURLEncoding.DecodeString(frame.Encryption.Ciphertext)
	plaintext, err := aead.Open(nil, nonce, ciphertext, []byte(frame.Channel+"\n"+frame.Encryption.KeyID))
	if err != nil || !json.Valid(plaintext) {
		return ErrProtocol
	}
	if err := json.Unmarshal(plaintext, target); err != nil {
		return ErrProtocol
	}
	return nil
}

func validRealtimeEncryptionEnvelope(channel string, envelope RealtimeEncryptionEnvelope) bool {
	if !privateRealtimeChannel(channel) || envelope.Algorithm != "aes-256-gcm" || !realtimeClient.MatchString(envelope.KeyID) {
		return false
	}
	nonce, nonceErr := base64.RawURLEncoding.DecodeString(envelope.Nonce)
	ciphertext, ciphertextErr := base64.RawURLEncoding.DecodeString(envelope.Ciphertext)
	return nonceErr == nil && len(nonce) == 12 && ciphertextErr == nil && len(ciphertext) >= 16 && len(ciphertext) <= 48*1024
}

func privateRealtimeChannel(channel string) bool {
	return realtimeChannel.MatchString(channel) && len(channel) > len("private:") && channel[:len("private:")] == "private:"
}
