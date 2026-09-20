package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dungxbuif/RelayHub/internal/domain"
	"github.com/dungxbuif/RelayHub/internal/store"
)

type fileRepositoryFake struct {
	files map[string]domain.RealtimeFile
}

func (fake *fileRepositoryFake) CreateRealtimeFile(_ context.Context, file domain.RealtimeFile) error {
	if fake.files == nil {
		fake.files = map[string]domain.RealtimeFile{}
	}
	fake.files[file.AppID+"/"+file.ID] = file
	return nil
}
func (fake *fileRepositoryFake) GetRealtimeFile(_ context.Context, appID, id string) (domain.RealtimeFile, error) {
	file, ok := fake.files[appID+"/"+id]
	if !ok {
		return file, store.ErrNotFound
	}
	return file, nil
}
func (fake *fileRepositoryFake) CompleteRealtimeFile(_ context.Context, appID, id string, at time.Time) (domain.RealtimeFile, error) {
	file, err := fake.GetRealtimeFile(context.Background(), appID, id)
	if err != nil {
		return file, err
	}
	file.Status = "ready"
	file.CompletedAt = &at
	fake.files[appID+"/"+id] = file
	return file, nil
}

type objectStoreFake struct{ info ObjectInfo }

func (fake objectStoreFake) PresignUpload(context.Context, string, string, int64, string, time.Duration) (string, map[string]string, error) {
	return "https://objects.example/upload", map[string]string{"content-type": "image/png"}, nil
}
func (fake objectStoreFake) PresignDownload(context.Context, string, time.Duration) (string, error) {
	return "https://objects.example/download", nil
}
func (fake objectStoreFake) Stat(context.Context, string) (ObjectInfo, error) { return fake.info, nil }

func TestRealtimeFilesRequireBoundedMetadataAndVerifiedObject(t *testing.T) {
	now := time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC)
	sha := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	repository := &fileRepositoryFake{}
	service := NewRealtimeFileService(repository, objectStoreFake{info: ObjectInfo{Size: 12, SHA256: sha}}, RealtimeFileOptions{Now: func() time.Time { return now }, NewID: func() string { return "file_1" }})
	upload, err := service.Create(context.Background(), "app_a", CreateRealtimeFileInput{Channel: "private:room", Name: "photo.png", MIMEType: "image/png", SizeBytes: 12, SHA256: sha})
	if err != nil || upload.File.ObjectKey == "" || upload.UploadURL == "" || upload.File.Status != "pending" {
		t.Fatalf("upload=%#v error=%v", upload, err)
	}
	ready, err := service.Complete(context.Background(), "app_a", "file_1")
	if err != nil || ready.Status != "ready" {
		t.Fatalf("ready=%#v error=%v", ready, err)
	}
	download, err := service.Download(context.Background(), "app_a", "file_1")
	if err != nil || download.DownloadURL == "" {
		t.Fatalf("download=%#v error=%v", download, err)
	}
	if _, err := service.Download(context.Background(), "app_b", "file_1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-app download=%v", err)
	}
	resolved, err := service.Resolve(context.Background(), "app_a", "file_1", "private:room")
	if err != nil || resolved.ObjectKey != "" {
		t.Fatalf("resolved=%#v error=%v", resolved, err)
	}
	if _, err := service.Resolve(context.Background(), "app_a", "file_1", "other"); !errors.Is(err, ErrConflict) {
		t.Fatalf("cross-channel resolve=%v", err)
	}
}

func TestRealtimeFilesRejectUnsafeOrOversizedMetadata(t *testing.T) {
	service := NewRealtimeFileService(&fileRepositoryFake{}, objectStoreFake{}, RealtimeFileOptions{})
	sha := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	for _, input := range []CreateRealtimeFileInput{{Channel: "room", Name: "../bad", MIMEType: "image/png", SizeBytes: 1, SHA256: sha}, {Channel: "room", Name: "a.exe", MIMEType: "application/octet-stream", SizeBytes: 1, SHA256: sha}, {Channel: "room", Name: "a.png", MIMEType: "image/png", SizeBytes: maxRealtimeFileBytes + 1, SHA256: sha}} {
		if _, err := service.Create(context.Background(), "app_a", input); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("input=%#v error=%v", input, err)
		}
	}
}
