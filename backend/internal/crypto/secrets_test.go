package crypto

import (
	"bytes"
	"encoding/base64"
	"strings"
	"testing"
)

func TestSecretCipherRoundTripUsesVersionedRandomCiphertext(t *testing.T) {
	key := bytes.Repeat([]byte{0x42}, 32)
	cipher, err := NewSecretCipher(base64.StdEncoding.EncodeToString(key))
	if err != nil {
		t.Fatal(err)
	}
	one, err := cipher.Encrypt([]byte("rhs_super-secret"))
	if err != nil {
		t.Fatal(err)
	}
	two, err := cipher.Encrypt([]byte("rhs_super-secret"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(one, "rhsec:v1:") || one == two || strings.Contains(one, "super-secret") {
		t.Fatalf("ciphertexts do not have the expected version/randomness: %q %q", one, two)
	}
	plain, err := cipher.Decrypt(one)
	if err != nil || string(plain) != "rhs_super-secret" {
		t.Fatalf("Decrypt() = %q, %v", plain, err)
	}
}

func TestSecretCipherRejectsInvalidKeyAndTampering(t *testing.T) {
	if _, err := NewSecretCipher("short"); err == nil {
		t.Fatal("NewSecretCipher(short) succeeded")
	}
	cipher, err := NewSecretCipher(base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{7}, 32)))
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := cipher.Encrypt([]byte("secret"))
	if err != nil {
		t.Fatal(err)
	}
	payload, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(sealed, secretCipherPrefix))
	if err != nil {
		t.Fatal(err)
	}
	payload[len(payload)-1] ^= 1
	sealed = secretCipherPrefix + base64.RawURLEncoding.EncodeToString(payload)
	if _, err := cipher.Decrypt(sealed); err == nil {
		t.Fatal("Decrypt(tampered) succeeded")
	}
	if _, err := cipher.Decrypt("rhsec:v2:AAAA"); err == nil {
		t.Fatal("Decrypt(unknown version) succeeded")
	}
}
