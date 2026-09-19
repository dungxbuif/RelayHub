package web

import (
	"bytes"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"reflect"
	"strconv"
	"testing"
	"testing/fstest"
)

func TestGeneratedDocsSnapshotIsCurrent(t *testing.T) {
	generated, err := Generate(os.DirFS("../../web/docs/static"))
	if err != nil {
		t.Fatalf("Generate(web/docs/static) error = %v", err)
	}
	committed, err := os.ReadFile("embed.go")
	if err != nil {
		t.Fatalf("ReadFile(embed.go) error = %v", err)
	}
	if !bytes.Equal(generated, committed) {
		t.Fatal("web/embed.go differs from web/docs/static; run `go -C backend generate ./web`")
	}
}

func TestEmbeddedDocsMatchPublicDocs(t *testing.T) {
	want, err := regularFiles(os.DirFS("../../web/docs/static"))
	if err != nil {
		t.Fatalf("read web/docs/static: %v", err)
	}
	got, err := regularFiles(Public)
	if err != nil {
		t.Fatalf("read web.Public: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("embedded docs differ from web/docs/static\nembedded files: %v\nsource files: %v", mapKeys(got), mapKeys(want))
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

// Catches binary corruption or host-dependent ordering when embedding downloads.
func TestGenerateBinaryAssetsDeterministically(t *testing.T) {
	binary := []byte{'P', 'K', 3, 4, 0, 255, 128, '\n'}
	first := fstest.MapFS{"z.zip": {Data: binary}, "a.md": {Data: []byte("# Docs\n")}}
	second := fstest.MapFS{"a.md": {Data: []byte("# Docs\n")}, "z.zip": {Data: binary}}
	a, err := Generate(first)
	if err != nil {
		t.Fatal(err)
	}
	b, err := Generate(second)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a, b) {
		t.Fatal("map insertion order changed embedded output")
	}
	parsed, err := parser.ParseFile(token.NewFileSet(), "embed.go", a, 0)
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	ast.Inspect(parsed, func(n ast.Node) bool {
		literal, ok := n.(*ast.BasicLit)
		if !ok || literal.Kind != token.STRING {
			return true
		}
		value, err := strconv.Unquote(literal.Value)
		if err == nil && value == string(binary) {
			found = true
		}
		return true
	})
	if !found {
		t.Fatal("binary asset did not round-trip through generated Go literal")
	}
}
