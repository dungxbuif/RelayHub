package service

import (
	"context"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/store"
	"github.com/google/uuid"
)

const maxRealtimeFileBytes int64 = 25 << 20

var fileSHA256 = regexp.MustCompile(`^[a-f0-9]{64}$`)
var allowedRealtimeMIME = map[string]bool{"image/jpeg": true, "image/png": true, "image/gif": true, "image/webp": true, "application/pdf": true, "text/plain": true, "application/zip": true}

type ObjectInfo struct {
	Size   int64
	SHA256 string
}
type RealtimeObjectStore interface {
	PresignUpload(context.Context, string, string, int64, string, time.Duration) (string, map[string]string, error)
	PresignDownload(context.Context, string, time.Duration) (string, error)
	Stat(context.Context, string) (ObjectInfo, error)
}
type RealtimeFileScanner interface {
	Scan(context.Context, string) error
}

type RealtimeFileOptions struct {
	Now       func() time.Time
	NewID     func() string
	Retention time.Duration
	URLTTL    time.Duration
	Scanner   RealtimeFileScanner
}
type RealtimeFileService struct {
	repository store.RealtimeFileRepository
	objects    RealtimeObjectStore
	options    RealtimeFileOptions
}
type CreateRealtimeFileInput struct {
	Channel   string `json:"channel"`
	Name      string `json:"name"`
	MIMEType  string `json:"mime_type"`
	SizeBytes int64  `json:"size_bytes"`
	SHA256    string `json:"sha256"`
}
type RealtimeFileUpload struct {
	File            domain.RealtimeFile `json:"file"`
	UploadURL       string              `json:"upload_url"`
	RequiredHeaders map[string]string   `json:"required_headers"`
}
type RealtimeFileDownload struct {
	File        domain.RealtimeFile `json:"file"`
	DownloadURL string              `json:"download_url"`
}

func NewRealtimeFileService(repository store.RealtimeFileRepository, objects RealtimeObjectStore, options RealtimeFileOptions) *RealtimeFileService {
	if options.Now == nil {
		options.Now = time.Now
	}
	if options.NewID == nil {
		options.NewID = func() string { return "file_" + uuid.NewString() }
	}
	if options.Retention <= 0 {
		options.Retention = 24 * time.Hour
	}
	if options.URLTTL <= 0 {
		options.URLTTL = 15 * time.Minute
	}
	return &RealtimeFileService{repository: repository, objects: objects, options: options}
}

func (service *RealtimeFileService) Create(ctx context.Context, appID string, input CreateRealtimeFileInput) (RealtimeFileUpload, error) {
	if service == nil || nilDependency(service.repository) || nilDependency(service.objects) {
		return RealtimeFileUpload{}, ErrInvalidDependency
	}
	input.Name = strings.TrimSpace(input.Name)
	input.MIMEType = strings.ToLower(strings.TrimSpace(input.MIMEType))
	input.SHA256 = strings.ToLower(strings.TrimSpace(input.SHA256))
	if appID == "" || !domain.ValidRealtimeChannel(input.Channel) || input.Name == "" || len(input.Name) > 255 || strings.ContainsAny(input.Name, "/\\\x00\r\n") || !allowedRealtimeMIME[input.MIMEType] || input.SizeBytes < 1 || input.SizeBytes > maxRealtimeFileBytes || !fileSHA256.MatchString(input.SHA256) {
		return RealtimeFileUpload{}, ErrInvalidInput
	}
	now := service.options.Now().UTC()
	id := service.options.NewID()
	file := domain.RealtimeFile{ID: id, AppID: appID, Channel: input.Channel, Name: input.Name, MIMEType: input.MIMEType, SizeBytes: input.SizeBytes, SHA256: input.SHA256, ObjectKey: "realtime/" + appID + "/" + id, Status: "pending", CreatedAt: now, ExpiresAt: now.Add(service.options.Retention)}
	if err := service.repository.CreateRealtimeFile(ctx, file); err != nil {
		return RealtimeFileUpload{}, mapStoreError(err)
	}
	uploadURL, headers, err := service.objects.PresignUpload(ctx, file.ObjectKey, file.MIMEType, file.SizeBytes, file.SHA256, service.options.URLTTL)
	if err != nil || !safePresignedURL(uploadURL) {
		return RealtimeFileUpload{}, ErrInvalidDependency
	}
	return RealtimeFileUpload{File: file, UploadURL: uploadURL, RequiredHeaders: headers}, nil
}

func (service *RealtimeFileService) Complete(ctx context.Context, appID, fileID string) (domain.RealtimeFile, error) {
	if service == nil || nilDependency(service.repository) || nilDependency(service.objects) {
		return domain.RealtimeFile{}, ErrInvalidDependency
	}
	file, err := service.repository.GetRealtimeFile(ctx, appID, fileID)
	if err != nil {
		return file, mapStoreError(err)
	}
	if file.Status == "ready" {
		return file, nil
	}
	if file.Status != "pending" || !file.ExpiresAt.After(service.options.Now()) {
		return domain.RealtimeFile{}, ErrConflict
	}
	info, err := service.objects.Stat(ctx, file.ObjectKey)
	if err != nil || info.Size != file.SizeBytes || !strings.EqualFold(info.SHA256, file.SHA256) {
		return domain.RealtimeFile{}, ErrConflict
	}
	if service.options.Scanner != nil {
		if err := service.options.Scanner.Scan(ctx, file.ObjectKey); err != nil {
			return domain.RealtimeFile{}, ErrConflict
		}
	}
	file, err = service.repository.CompleteRealtimeFile(ctx, appID, fileID, service.options.Now().UTC())
	return file, mapStoreError(err)
}

func (service *RealtimeFileService) Download(ctx context.Context, appID, fileID string) (RealtimeFileDownload, error) {
	if service == nil || nilDependency(service.repository) || nilDependency(service.objects) {
		return RealtimeFileDownload{}, ErrInvalidDependency
	}
	file, err := service.repository.GetRealtimeFile(ctx, appID, fileID)
	if err != nil {
		return RealtimeFileDownload{}, mapStoreError(err)
	}
	if file.Status != "ready" || !file.ExpiresAt.After(service.options.Now()) {
		return RealtimeFileDownload{}, ErrConflict
	}
	downloadURL, err := service.objects.PresignDownload(ctx, file.ObjectKey, service.options.URLTTL)
	if err != nil || !safePresignedURL(downloadURL) {
		return RealtimeFileDownload{}, ErrInvalidDependency
	}
	return RealtimeFileDownload{File: file, DownloadURL: downloadURL}, nil
}

func (service *RealtimeFileService) Resolve(ctx context.Context, appID, fileID, channel string) (domain.RealtimeFile, error) {
	if service == nil || nilDependency(service.repository) {
		return domain.RealtimeFile{}, ErrInvalidDependency
	}
	file, err := service.repository.GetRealtimeFile(ctx, appID, fileID)
	if err != nil {
		return file, mapStoreError(err)
	}
	if file.Channel != channel || file.Status != "ready" || !file.ExpiresAt.After(service.options.Now()) {
		return domain.RealtimeFile{}, ErrConflict
	}
	file.ObjectKey = ""
	return file, nil
}

func safePresignedURL(raw string) bool {
	parsed, err := url.Parse(raw)
	return err == nil && parsed.Scheme == "https" && parsed.Host != "" && parsed.User == nil
}
