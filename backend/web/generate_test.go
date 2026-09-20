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
	"strings"
	"testing"
	"testing/fstest"
)

func TestGeneratedAdminSnapshotIsCurrent(t *testing.T) {
	generated, err := GenerateWithDocs(os.DirFS("../../web/admin/dist"), os.DirFS("../../web/docs/build"))
	if err != nil {
		t.Fatalf("Generate(web/admin/dist) error = %v", err)
	}
	committed, err := os.ReadFile("embed.go")
	if err != nil {
		t.Fatalf("ReadFile(embed.go) error = %v", err)
	}
	if !bytes.Equal(generated, committed) {
		t.Fatal("web/embed.go differs from web/admin/dist; run `npm --prefix web/admin run build && go -C backend generate ./web`")
	}
}

func TestPublicDocsAreEmbeddedSeparatelyFromAdmin(t *testing.T) {
	err := fs.WalkDir(Admin, ".", func(name string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if strings.HasSuffix(name, ".md") || name == "openapi.json" || name == "asyncapi.yaml" {
			t.Fatalf("public documentation embedded as %q", name)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"README.md", "openapi.json", "llms.txt", "llms-full.txt"} {
		if _, err := fs.Stat(Docs, name); err != nil {
			t.Fatalf("public docs missing %q: %v", name, err)
		}
	}
}

func TestEmbeddedAdminMatchesSource(t *testing.T) {
	want, err := regularFiles(os.DirFS("../../web/admin/dist"))
	if err != nil {
		t.Fatalf("read web/admin/dist: %v", err)
	}
	got, err := regularFiles(Admin)
	if err != nil {
		t.Fatalf("read web.Admin: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("embedded Admin differs from web/admin/dist\nembedded files: %v\nsource files: %v", mapKeys(got), mapKeys(want))
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
	manifest := []byte(`{"index.html":{"file":"assets/z.zip","isEntry":true,"css":["assets/a.css"]}}`)
	index := []byte(`<script src="/admin/assets/z.zip"></script><link href="/admin/assets/a.css">`)
	first := fstest.MapFS{"index.html": {Data: index}, ".vite/manifest.json": {Data: manifest}, "assets/z.zip": {Data: binary}, "assets/a.css": {Data: []byte("body{}")}}
	second := fstest.MapFS{"assets/a.css": {Data: []byte("body{}")}, "assets/z.zip": {Data: binary}, ".vite/manifest.json": {Data: manifest}, "index.html": {Data: index}}
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

func TestGenerateRejectsInvalidAdminBuilds(t *testing.T) {
	validManifest := []byte(`{"index.html":{"file":"assets/app.js","isEntry":true,"css":["assets/app.css"]}}`)
	validIndex := []byte(`<script src="/admin/assets/app.js"></script><link href="/admin/assets/app.css">`)
	base := func() fstest.MapFS {
		return fstest.MapFS{
			"index.html": {Data: validIndex}, ".vite/manifest.json": {Data: validManifest},
			"assets/app.js": {Data: []byte("export{}")}, "assets/app.css": {Data: []byte("body{}")},
		}
	}
	tests := map[string]func(fstest.MapFS){
		"missing manifest asset": func(files fstest.MapFS) { delete(files, "assets/app.js") },
		"external runtime": func(files fstest.MapFS) {
			files["index.html"].Data = []byte(`<script src="https://cdn.invalid/app.js"></script>`)
		},
		"source map":      func(files fstest.MapFS) { files["assets/app.js.map"] = &fstest.MapFile{Data: []byte("map")} },
		"oversized asset": func(files fstest.MapFS) { files["assets/app.js"].Data = make([]byte, 5<<20+1) },
		"symlink": func(files fstest.MapFS) {
			files["assets/link.js"] = &fstest.MapFile{Mode: fs.ModeSymlink | 0o777}
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			files := base()
			mutate(files)
			if _, err := Generate(files); err == nil {
				t.Fatal("Generate() accepted invalid Admin build")
			}
		})
	}
}
