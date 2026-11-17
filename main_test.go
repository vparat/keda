package main

import (
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/vparat/keda/internal/util"
)


// TestMainModes checks version precedence, logger selection, and shutdown output.
// It takes the test handle and returns no values.
func TestMainModes(t *testing.T) {

	for _, tt := range []struct {
		name     string
		logType  string
		logLevel string
		args     []string
		text     bool
		version  bool
	}{
		{name: "default json", args: []string{"keda"}},
		{name: "explicit json", args: []string{"keda"}, logType: "json", logLevel: "info"},
		{name: "unknown format", args: []string{"keda"}, logType: "unknown", logLevel: "unknown"},
		{name: "debug level", args: []string{"keda"}, logLevel: "DEBUG"},
		{name: "error level", args: []string{"keda"}, logLevel: "ERROR"},
		{name: "text", args: []string{"keda"}, logType: "text", text: true},
		{name: "mixed case text", args: []string{"keda"}, logType: "TeXt", text: true},
		{name: "version before logger setup", args: []string{"keda", "version", "--unknown"}, version: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("LOG_TYPE", tt.logType)
			t.Setenv("LOG_LEVEL", tt.logLevel)
			t.Setenv("CACHE_TYPE", "noop")
			t.Setenv("CACHE_HOST", "")
			t.Setenv("CACHE_TTL", "30m")
			t.Setenv("KUBERNETES_SERVICE_HOST", "")
			t.Setenv("KUBERNETES_SERVICE_PORT", "")
			oldArgs, oldVersion := os.Args, Version
			oldOut, oldErr, oldLogger := os.Stdout, os.Stderr, slog.Default()
			stdout, err := os.Create(filepath.Join(t.TempDir(), "stdout"))

			if err != nil {
				t.Fatal(err)
			}

			defer stdout.Close()
			stderr, err := os.Create(filepath.Join(t.TempDir(), "stderr"))

			if err != nil {
				t.Fatal(err)
			}

			defer stderr.Close()
			defer func() {
				os.Args, Version = oldArgs, oldVersion
				os.Stdout, os.Stderr = oldOut, oldErr
				slog.SetDefault(oldLogger)
			}()
			os.Args, Version = tt.args, "test-build"
			os.Stdout, os.Stderr = stdout, stderr
			main()
			out, err := os.ReadFile(stdout.Name())

			if err != nil {
				t.Fatal(err)
			}

			logs, err := os.ReadFile(stderr.Name())

			if err != nil {
				t.Fatal(err)
			}

			if tt.version {

				if string(out) != "test-build\n" || len(logs) != 0 {
					t.Fatalf("version stdout=%q stderr=%q", out, logs)
				}

				return
			}

			if len(out) != 0 {
				t.Fatalf("unexpected stdout: %q", out)
			}

			if tt.text {

				for _, want := range []string{"level=ERROR", "component=eventcontroller", `msg="Shutting down"`} {

					if !strings.Contains(string(logs), want) {
						t.Errorf("logs %q missing %q", logs, want)
					}

				}

				return
			}

			decoder := json.NewDecoder(strings.NewReader(string(logs)))
			var records []map[string]any

			for {
				var record map[string]any
				err := decoder.Decode(&record)

				if err == io.EOF {
					break
				}

				if err != nil {
					t.Fatalf("invalid JSON log %q: %v", logs, err)
				}

				if record["level"] == "DEBUG" {
					continue
				}

				records = append(records, record)
			}

			if strings.EqualFold(tt.logLevel, "error") {

				if len(records) != 1 || records[0]["level"] != "ERROR" || records[0]["component"] != "eventcontroller" {
					t.Fatalf("unexpected error-only log records: %v", records)
				}

				return
			}

			if len(records) != 3 {
				t.Fatalf("JSON records = %v, want logger setup, startup error, and shutdown", records)
			}

			if records[0]["msg"] != "logger configured" || records[1]["level"] != "ERROR" || records[1]["component"] != "eventcontroller" || records[2]["msg"] != "Shutting down" {
				t.Fatalf("unexpected log records: %v", records)
			}

		})
	}

}


// TestVersionNonmatching checks that other positional arguments produce no version.
// It takes the test handle and returns no values.
func TestVersionNonmatching(t *testing.T) {
	oldArgs := os.Args
	defer func() {
		os.Args = oldArgs
	}()

	for _, tt := range []struct {
		name string
		args []string
	}{
		{name: "empty arguments", args: []string{}},
		{name: "not first", args: []string{"keda", "other", "version"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			os.Args = tt.args

			if version() {
				t.Fatal("nonmatching arguments triggered version")
			}

		})
	}

}


// TestBuildCache takes the test handle and checks configured and fallback TTLs.
func TestBuildCache(t *testing.T) {

	for _, tt := range []struct {
		name, ttl string
		want      time.Duration
	}{
		{name: "configured", ttl: "5m", want: 5 * time.Minute},
		{name: "unset", want: defaultTtl},
		{name: "invalid", ttl: "invalid", want: defaultTtl},
		{name: "no expiration", ttl: "0s"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			server := miniredis.RunT(t)
			t.Setenv("CACHE_TYPE", "redis")
			t.Setenv("CACHE_HOST", server.Addr())
			t.Setenv("CACHE_TTL", tt.ttl)
			store, err := buildCache()

			if err != nil {
				t.Fatal(err)
			}

			if err := store.Set(t.Context(), "key", []byte("value")); err != nil {
				t.Fatal(err)
			}

			if got := server.TTL("key"); got != tt.want {
				t.Fatalf("TTL = %v, want %v", got, tt.want)
			}

		})
	}

}


// TestEnv takes the test handle and verifies empty values use the supplied fallback.
func TestEnv(t *testing.T) {

	for _, tt := range []struct {
		name, value, want string
	}{
		{name: "empty", want: "fallback"},
		{name: "configured", value: "configured", want: "configured"},
		{name: "whitespace preserved", value: " ", want: " "},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("KEDA_TEST_ENV", tt.value)

			if got := util.Env("KEDA_TEST_ENV", "fallback"); got != tt.want {
				t.Fatalf("env = %q, want %q", got, tt.want)
			}

		})
	}

}


// TestConfigureLoggerFiltering takes the test handle and verifies emitted messages for both formats.
func TestConfigureLoggerFiltering(t *testing.T) {

	for _, format := range []string{"json", "text"} {
		for _, tt := range []struct {
			name, level string
			debug, info bool
		}{
			{name: "default", info: true},
			{name: "info", level: "INFO", info: true},
			{name: "debug", level: "DeBuG", debug: true, info: true},
			{name: "error", level: "ERROR"},
			{name: "invalid", level: "invalid", info: true},
		} {
			t.Run(format+"/"+tt.name, func(t *testing.T) {
				t.Setenv("LOG_TYPE", format)
				t.Setenv("LOG_LEVEL", tt.level)
				output, err := os.Create(filepath.Join(t.TempDir(), "logs"))

				if err != nil {
					t.Fatal(err)
				}

				defer output.Close()
				previousErr, previousLogger := os.Stderr, slog.Default()
				t.Cleanup(func() {
					os.Stderr = previousErr
					slog.SetDefault(previousLogger)
				})
				os.Stderr = output
				configureLogger()
				slog.Debug("debug-marker")
				slog.Info("info-marker")
				slog.Error("error-marker")
				data, err := os.ReadFile(output.Name())

				if err != nil {
					t.Fatal(err)
				}

				for _, message := range []struct {
					text string
					want bool
				}{
					{text: "debug-marker", want: tt.debug},
					{text: "info-marker", want: tt.info},
					{text: "error-marker", want: true},
				} {

					if got := strings.Contains(string(data), message.text); got != message.want {
						t.Errorf("message %q present = %v, want %v; logs: %s", message.text, got, message.want, data)
					}

				}

			})
		}
	}

}
