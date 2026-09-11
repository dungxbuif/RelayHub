package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/dungxbuif/RelayHub/web"
)

func main() {
	sourcePath := flag.String("source", "../public-docs", "public docs source directory")
	outputPath := flag.String("output", "embed.go", "generated Go output path")
	flag.Parse()

	generated, err := web.Generate(os.DirFS(*sourcePath))
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "generate embedded docs: %v\n", err)
		os.Exit(1)
	}
	if err := os.WriteFile(*outputPath, generated, 0o644); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "write embedded docs: %v\n", err)
		os.Exit(1)
	}
}
