package objectstore

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestS3PresignedUploadBindsTypeAndChecksum(t *testing.T) {
	store, err := NewS3(S3Config{Endpoint: "https://objects.example", Bucket: "relayhub-files", Region: "us-east-1", AccessKey: "access", SecretKey: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	sha := strings.Repeat("a", 64)
	upload, headers, err := store.PresignUpload(context.Background(), "realtime/app/file", "image/png", 10, sha, 15*time.Minute)
	if err != nil || !strings.HasPrefix(upload, "https://objects.example/") || headers["x-amz-checksum-sha256"] == "" || headers["x-amz-meta-relayhub-sha256"] != sha {
		t.Fatalf("url=%q headers=%#v error=%v", upload, headers, err)
	}
}
