package action

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)


func TestNilDoLogsMessage(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	n := &Nil{Logger: *logger}

	n.Do(t.Context())

	if !strings.Contains(buf.String(), "nil action triggered") {
		t.Errorf("Do() log = %q, want message containing %q", buf.String(), "nil action triggered")
	}

}


func TestNewNilAction(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	n, err := NewNilAction(Config{Type: "nil", Logger: *logger})

	if err != nil {
		t.Fatalf("NewNilAction() unexpected error: %v", err)
	}

	n.Do(t.Context())

	if !strings.Contains(buf.String(), "nil action triggered") {
		t.Errorf("Do() log = %q, want message containing %q", buf.String(), "nil action triggered")
	}

}
