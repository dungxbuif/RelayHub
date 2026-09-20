package auth

import "testing"

func TestAdminPasswordHashVerifyAndRejectMutation(t *testing.T) {
	hash, err := HashPassword("correct horse battery staple", fixedReader{b: 0x42})
	if err != nil {
		t.Fatal(err)
	}
	if hash == "correct horse battery staple" || !VerifyPassword(hash, "correct horse battery staple") {
		t.Fatalf("hash did not verify: %q", hash)
	}
	if VerifyPassword(hash, "wrong") {
		t.Fatal("wrong password verified")
	}
	if VerifyPassword(hash+"x", "correct horse battery staple") {
		t.Fatal("mutated hash verified")
	}
}

func TestAdminPasswordRejectsWeakInputAndBadHash(t *testing.T) {
	for _, password := range []string{"", "short", "        "} {
		if _, err := HashPassword(password, fixedReader{b: 0x42}); err == nil {
			t.Fatalf("weak password %q accepted", password)
		}
	}
	for _, hash := range []string{"", "pbkdf2-sha256$bad", "bcrypt$10$salt$hash"} {
		if VerifyPassword(hash, "anything") {
			t.Fatalf("bad hash %q verified", hash)
		}
	}
}

type fixedReader struct{ b byte }

func (r fixedReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = r.b
	}
	return len(p), nil
}
