package main

import (
	"context"
	"io"

	"github.com/sadrax4/kvorum/internal/telemetry"
)

func cmdVersion(_ context.Context, _ kvorumAPI, g globals, args []string, out io.Writer) error {
	pos, err := parseInterleaved(newFlagSet("version"), args)
	if err != nil {
		return err
	}
	if err := wantArgs(pos); err != nil {
		return err
	}
	if g.json {
		return printJSON(out, struct {
			Version   string `json:"version"`
			Commit    string `json:"commit"`
			BuildDate string `json:"build_date"`
			GoVersion string `json:"go_version"`
		}{Version: telemetry.Version, Commit: telemetry.Commit, BuildDate: telemetry.BuildDate, GoVersion: telemetry.GoVersion()})
	}
	return printFields(out, [][2]string{
		{"version", telemetry.Version},
		{"commit", telemetry.Commit},
		{"build_date", telemetry.BuildDate},
		{"go_version", telemetry.GoVersion()},
	})
}
