package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/dungxbuif/RelayHub/internal/migrate"
)

func runMigrateCommand(ctx context.Context) error {
	if len(os.Args) < 3 {
		return errors.New("usage: relayhub migrate [plan|apply|verify|rollback-report] --source-url URL --target-url URL")
	}
	mode := os.Args[2]
	flags := flag.NewFlagSet("migrate", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	sourceURL := flags.String("source-url", os.Getenv("RELAYHUB_LEGACY_POSTGRES_URL"), "legacy PostgreSQL URL")
	targetURL := flags.String("target-url", os.Getenv("RELAYHUB_POSTGRES_URL"), "Rust PostgreSQL URL")
	confirm := flags.String("confirm", "", "required confirmation for apply")
	if err := flags.Parse(os.Args[3:]); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	report, err := migrate.Run(ctx, migrate.Options{SourceURL: *sourceURL, TargetURL: *targetURL, Mode: mode, Confirm: *confirm})
	encoded, encodeErr := json.MarshalIndent(report, "", "  ")
	if encodeErr != nil {
		return encodeErr
	}
	fmt.Fprintln(os.Stdout, string(encoded))
	if err != nil {
		return err
	}
	if mode == "verify" {
		if err := report.Validate(); err != nil {
			return err
		}
	}
	return nil
}
