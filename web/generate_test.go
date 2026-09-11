package web

import (
	"bytes"
	"io/fs"
	"os"
	"reflect"
	"testing"
)

func TestGeneratedDocsSnapshotIsCurrent(t *testing.T) {
	generated, err := Generate(os.DirFS("../public-docs"))
	if err != nil {
		t.Fatalf("Generate(public-docs) error = %v", err)
	}
	committed, err := os.ReadFile("embed.go")
	if err != nil {
		t.Fatalf("ReadFile(embed.go) error = %v", err)
	}
	if !bytes.Equal(generated, committed) {
		t.Fatal("web/embed.go differs from public-docs; run `go generate ./web`")
	}
}

func TestEmbeddedDocsMatchPublicDocs(t *testing.T) {
	want, err := regularFiles(os.DirFS("../public-docs"))
	if err != nil {
		t.Fatalf("read public-docs: %v", err)
	}
	got, err := regularFiles(Public)
	if err != nil {
		t.Fatalf("read web.Public: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("embedded docs differ from public-docs\nembedded files: %v\nsource files: %v", mapKeys(got), mapKeys(want))
	}
}

func regularFiles(source fs.FS) (map[string][]byte, error) {
	files := make(map[string][]byte)
	err := fs.WalkDir(source, ".", func(name string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		data, err := fs.ReadFile(source, name)
		if err != nil {
			return err
		}
		files[name] = data
		return nil
	})
	return files, err
}

func mapKeys(values map[string][]byte) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	return keys
}
