package action

import (
	"io"
	"log/slog"
	"strings"
	"testing"

	"golang.org/x/sync/semaphore"
)


// TestNew verifies the default constructor returns a no-op action using t.
func TestNew(t *testing.T) {
	action, err := New()

	if err != nil {
		t.Fatal(err)
	}

	if _, ok := action.(*Nil); !ok {
		t.Fatalf("New() action = %T, want *Nil", action)
	}

}


// TestNewWithConfig checks action selection and invalid-type errors using t.
func TestNewWithConfig(t *testing.T) {

	for _, tt := range []struct {
		name        string
		typeName    string
		wantCommand bool
		wantError   bool
	}{
		{name: "command", typeName: "COMMAND", wantCommand: true},
		{name: "no-op", typeName: "NIL"},
		{name: "unknown", typeName: "unknown", wantError: true},
		{name: "empty", wantError: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			action, err := NewWithConfig(Config{
				Type:            tt.typeName,
				Logger:          *slog.New(slog.NewTextHandler(io.Discard, nil)),
				GlobalSemaphore: semaphore.NewWeighted(1),
			})

			if tt.wantError {

				if err == nil || !strings.Contains(err.Error(), "invalid action type") || action != nil {
					t.Fatalf("NewWithConfig() = (%v, %v), want nil and invalid action type", action, err)
				}

				return
			}

			if err != nil {
				t.Fatal(err)
			}

			if tt.wantCommand {

				if _, ok := action.(*Command); !ok {
					t.Fatalf("action = %T, want *Command", action)
				}

			} else {

				if _, ok := action.(*Nil); !ok {
					t.Fatalf("action = %T, want *Nil", action)
				}

			}

		})
	}

}
