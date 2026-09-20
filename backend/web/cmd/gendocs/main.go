package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/dungxbuif/RelayHub/web"
)

func main() {
	sourcePath := flag.String("source", "../../web/admin/dist", "Admin Vite build directory")
	docsPath := flag.String("docs-source", "../../web/docs/static", "public docs static directory")
	outputPath := flag.String("output", "embed.go", "generated Go output path")
	flag.Parse()

	generated, err := web.GenerateWithDocs(os.DirFS(*sourcePath), os.DirFS(*docsPath))
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "generate embedded Admin assets: %v\n", err)
		os.Exit(1)
	}
	if err := os.WriteFile(*outputPath, generated, 0o644); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "write embedded Admin assets: %v\n", err)
		os.Exit(1)
	}
}
