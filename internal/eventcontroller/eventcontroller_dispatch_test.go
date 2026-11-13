package eventcontroller

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/vparat/keda/internal/action"
	"github.com/vparat/keda/internal/event"
	"github.com/vparat/keda/internal/informers"
	"k8s.io/client-go/kubernetes"
)


// dispatchLogWriter delivers complete JSON log records to the test thread.
type dispatchLogWriter struct {
	records chan map[string]any
}


// Write decodes p and returns the number of bytes accepted, or a decoding error.
func (w dispatchLogWriter) Write(p []byte) (int, error) {
	var record map[string]any

	if err := json.Unmarshal(p, &record); err != nil {
		return 0, err
	}

	w.records <- record

	return len(p), nil
}


// captureDispatchLogs installs a JSON logger for t and returns its log records.
func captureDispatchLogs(t *testing.T) <-chan map[string]any {
	t.Helper()
	records := make(chan map[string]any, 100)
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(
		dispatchLogWriter{records: records},
		&slog.HandlerOptions{Level: slog.LevelDebug},
	)))
	t.Cleanup(func() {
		slog.SetDefault(previous)
	})

	return records
}


// expectDispatchLog waits for message using t and returns its structured record.
func expectDispatchLog(t *testing.T, records <-chan map[string]any, message string) map[string]any {
	t.Helper()
	timeout := time.NewTimer(5 * time.Second)
	defer timeout.Stop()

	for {

		select {
		case record := <-records:

			if record["msg"] == message {
				return record
			}

		case <-timeout.C:
			t.Fatalf("waiting for log message %q", message)

			return nil
		}

	}

}


// startDispatch runs rule with command as its runner constructor. It returns the
// event input channel and cancels and joins every worker when t completes.
func startDispatch(
	t *testing.T,
	rule Rule,
	command func(action.Config) (*action.Command, error),
) chan<- informers.Message {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	ready := make(chan chan<- informers.Message, 1)
	watch := func(
		ctx context.Context,
		_ kubernetes.Interface,
		kind, operation, selector, namespace, name string,
		out chan<- informers.Message,
	) error {

		if kind != rule.Inbound.Kind || operation != rule.Inbound.Action ||
			selector != rule.Inbound.Label || namespace != rule.Inbound.Namespace || name != rule.Name {
			t.Errorf("watch arguments do not match rule: %q %q %q %q %q", kind, operation, selector, namespace, name)
		}

		ready <- out
		<-ctx.Done()

		return nil
	}
	go func() {
		defer close(done)
		c := newController()
		c.watchResources = watch
		c.newCommand = command
		c.runRules(ctx, nil, []Rule{rule})
	}()
	t.Cleanup(func() {
		cancel()

		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("dispatcher did not stop and join its workers")
		}

	})

	select {
	case input := <-ready:
		return input
	case <-time.After(5 * time.Second):
		t.Fatal("watch did not start")

		return nil
	}

}


// TestRunRulesNilAction checks no-op dispatch and resource log attributes.
func TestRunRulesNilAction(t *testing.T) {
	records := captureDispatchLogs(t)
	rule := Rule{
		Name:     "pods",
		Inbound:  event.Event{Kind: "pod", Action: "create", Label: "app=test", Namespace: "default"},
		Outbound: event.Event{Kind: "NIL"},
	}
	input := startDispatch(t, rule, func(action.Config) (*action.Command, error) {
		t.Error("nil action tried to construct a command")

		return nil, errors.New("unexpected command")
	})
	input <- informers.Message{Key: "pods", Name: "one", Namespace: "default"}
	record := expectDispatchLog(t, records, "nil action triggered")

	if record["event"] != "pods" || record["name"] != "one" || record["namespace"] != "default" ||
		record["component"] != "eventcontroller" {
		t.Fatalf("nil action log attributes = %v", record)
	}

}


// TestRunRulesInformerError verifies watcher failures are logged with rule names.
func TestRunRulesInformerError(t *testing.T) {
	records := captureDispatchLogs(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan struct{})
	watch := func(
		context.Context,
		kubernetes.Interface,
		string,
		string,
		string,
		string,
		string,
		chan<- informers.Message,
	) error {
		return errors.New("watch failed")
	}
	go func() {
		defer close(done)
		c := newController()
		c.watchResources = watch
		c.runRules(ctx, nil, []Rule{{Name: "failed-rule"}})
	}()
	record := expectDispatchLog(t, records, "informer error")
	cancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("dispatcher did not stop")
	}

	if record["event"] != "failed-rule" || record["error"] != "watch failed" {
		t.Fatalf("informer error log = %v", record)
	}

}


// TestRunRulesCommandArguments verifies quoting and trailing-semicolon behavior.
func TestRunRulesCommandArguments(t *testing.T) {

	for _, tt := range []struct {
		name       string
		command    string
		wantOutput string
	}{
		{
			name:       "resource arguments",
			command:    "  printf '%s\\n'  ",
			wantOutput: "pod'one\nspace's name\n",
		},
		{
			name:       "semicolon suppresses arguments",
			command:    " printf 'standalone'; ",
			wantOutput: "standalone",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			records := captureDispatchLogs(t)
			rule := Rule{Name: "pods", Outbound: event.Event{Kind: "command", Action: tt.command}}
			input := startDispatch(t, rule, action.NewCommandAction)
			input <- informers.Message{Key: "pods", Name: "pod'one", Namespace: "space's name"}
			record := expectDispatchLog(t, records, "command executed")

			if record["output"] != tt.wantOutput {
				t.Fatalf("command output = %q, want %q", record["output"], tt.wantOutput)
			}

		})
	}

}


// TestRunRulesDuplicateCommand holds a command queued until the duplicate is
// skipped, then verifies the original command executes exactly once.
func TestRunRulesDuplicateCommand(t *testing.T) {
	t.Setenv("CMDRUNNER_SEMAPHORE_MAX", "1")
	t.Setenv("CMDRUNNER_SEMAPHORE_TIMEOUT", "30s")
	records := captureDispatchLogs(t)
	constructs := 0
	var release func()
	rule := Rule{Name: "pods", Outbound: event.Event{Kind: "command", Action: "printf once;"}}
	input := startDispatch(t, rule, func(config action.Config) (*action.Command, error) {
		constructs++

		if err := config.GlobalSemaphore.Acquire(t.Context(), 1); err != nil {
			return nil, err
		}

		release = func() {
			config.GlobalSemaphore.Release(1)
		}

		if config.AcquireTimeout != 30*time.Second {
			t.Errorf("acquire timeout = %v, want 30s", config.AcquireTimeout)
		}

		return action.NewCommandAction(config)
	})
	msg := informers.Message{Key: "pods", Name: "one", Namespace: "default"}
	input <- msg
	input <- msg
	expectDispatchLog(t, records, "duplicate job skipped")

	// Receiving the duplicate log synchronizes with the completed constructor.
	if constructs != 1 {
		t.Fatalf("constructed %d runners for one pending job", constructs)
	}

	release()
	record := expectDispatchLog(t, records, "command executed")

	if record["output"] != "once" {
		t.Fatalf("command output = %v, want once", record["output"])
	}

}


// TestRunRulesConstructorError verifies failed construction releases the job
// reservation so the same resource event can be retried successfully.
func TestRunRulesConstructorError(t *testing.T) {
	records := captureDispatchLogs(t)
	constructs := 0
	rule := Rule{Name: "pods", Outbound: event.Event{Kind: "command", Action: "printf retried;"}}
	input := startDispatch(t, rule, func(config action.Config) (*action.Command, error) {
		constructs++

		if constructs == 1 {
			return nil, errors.New("runner unavailable")
		}

		return action.NewCommandAction(config)
	})
	msg := informers.Message{Key: "pods", Name: "one", Namespace: "default"}
	input <- msg
	record := expectDispatchLog(t, records, "failed to create command runner")

	if !strings.Contains(record["error"].(string), "runner unavailable") {
		t.Fatalf("constructor failure log = %v", record)
	}

	input <- msg
	record = expectDispatchLog(t, records, "command executed")

	if record["output"] != "retried" || constructs != 2 {
		t.Fatalf("retry output = %v, constructor calls = %d", record["output"], constructs)
	}

}
