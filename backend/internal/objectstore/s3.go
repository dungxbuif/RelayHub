package objectstore

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/dungxbuif/RelayHub/internal/service"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type S3Config struct{ Endpoint, Bucket, Region, AccessKey, SecretKey string }
type S3 struct {
	client *minio.Client
	bucket string
}

func NewS3(config S3Config) (*S3, error) {
	endpoint, err := url.Parse(config.Endpoint)
	if err != nil || endpoint.Scheme != "https" || endpoint.Host == "" || endpoint.Path != "" || endpoint.RawQuery != "" || endpoint.User != nil || config.Bucket == "" || config.AccessKey == "" || config.SecretKey == "" {
		return nil, errors.New("invalid S3-compatible object storage configuration")
	}
	client, err := minio.New(endpoint.Host, &minio.Options{Creds: credentials.NewStaticV4(config.AccessKey, config.SecretKey, ""), Secure: true, Region: config.Region, MaxRetries: 2})
	if err != nil {
		return nil, errors.New("configure S3-compatible object storage")
	}
	return &S3{client: client, bucket: config.Bucket}, nil
}

func (store *S3) PresignUpload(ctx context.Context, key, mimeType string, size int64, sha256Hex string, ttl time.Duration) (string, map[string]string, error) {
	raw, err := hex.DecodeString(sha256Hex)
	if err != nil {
		return "", nil, err
	}
	checksum := base64.StdEncoding.EncodeToString(raw)
	headers := http.Header{"Content-Type": []string{mimeType}, "X-Amz-Checksum-Sha256": []string{checksum}, "X-Amz-Meta-Relayhub-Sha256": []string{sha256Hex}}
	signed, err := store.client.PresignHeader(ctx, http.MethodPut, store.bucket, key, ttl, nil, headers)
	if err != nil {
		return "", nil, errors.New("presign object upload")
	}
	return signed.String(), map[string]string{"content-type": mimeType, "x-amz-checksum-sha256": checksum, "x-amz-meta-relayhub-sha256": sha256Hex}, nil
}

func (store *S3) PresignDownload(ctx context.Context, key string, ttl time.Duration) (string, error) {
	signed, err := store.client.PresignedGetObject(ctx, store.bucket, key, ttl, nil)
	if err != nil {
		return "", errors.New("presign object download")
	}
	return signed.String(), nil
}

func (store *S3) Stat(ctx context.Context, key string) (service.ObjectInfo, error) {
	info, err := store.client.StatObject(ctx, store.bucket, key, minio.StatObjectOptions{Checksum: true})
	if err != nil {
		return service.ObjectInfo{}, errors.New("stat object")
	}
	checksum := strings.ToLower(info.UserMetadata["Relayhub-Sha256"])
	if info.ChecksumSHA256 != "" {
		raw, decodeErr := base64.StdEncoding.DecodeString(info.ChecksumSHA256)
		if decodeErr == nil {
			checksum = hex.EncodeToString(raw)
		}
	}
	return service.ObjectInfo{Size: info.Size, SHA256: checksum}, nil
}
