package action

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"golang.org/x/sync/semaphore"
)


// TestCommandDo verifies output, failures, and empty commands through log records.
// It takes t and returns no values.
func TestCommandDo(t *testing.T) {

	for _, tt := range []struct {
		name        string
		args        string
		wantMessage string
		wantLevel   string
		wantOutput  string
	}{
		{name: "empty", wantMessage: "no command args provided", wantLevel: "WARN"},
		{name: "success", args: "printf 'command output'", wantMessage: "command executed", wantLevel: "INFO", wantOutput: "command output"},
		{name: "failure", args: "printf 'failure output'; exit 7", wantMessage: "command failed", wantLevel: "ERROR", wantOutput: "failure output"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var output bytes.Buffer
			limit := semaphore.NewWeighted(1)
			command := &Command{
				Logger: *slog.New(slog.NewJSONHandler(&output, nil)),
				Args:   tt.args,
				slots:  limit,
			}
			command.Do(t.Context())
			var record map[string]any

			if err := json.Unmarshal(output.Bytes(), &record); err != nil {
				t.Fatalf("invalid log record %q: %v", output.String(), err)
			}

			if record["msg"] != tt.wantMessage || record["level"] != tt.wantLevel {
				t.Fatalf("log = %v, want message %q and level %q", record, tt.wantMessage, tt.wantLevel)
			}

			if tt.wantOutput != "" && record["output"] != tt.wantOutput {
				t.Errorf("output = %v, want %q", record["output"], tt.wantOutput)
			}

			if tt.wantLevel == "ERROR" && !strings.Contains(record["error"].(string), "exit status 7") {
				t.Errorf("error = %v, want exit status 7", record["error"])
			}

			if !limit.TryAcquire(1) {
				t.Fatal("command did not release its shared semaphore slot")
			}

			limit.Release(1)

		})
	}

}
