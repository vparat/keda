package eventcontroller

import (
	"context"
	"fmt"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
)

func TestParseConfig(t *testing.T) {
	for _, tt := range []struct {
		name, data string
		count      int
		fail       bool
	}{
		{"valid", `events: [{name: one, inbound: {kind: pod, action: create}, outbound: {kind: cmd, action: echo}}]`, 1, false},
		{"disabled", `events: [{disabled: true}]`, 0, false},
		{"empty rules", `events: []`, 0, false},
		{"unknown field", `evnts: []`, 0, true},
		{"missing name", `events: [{inbound: {kind: pod, action: create}}]`, 0, true},
		{"unknown kind", `events: [{name: one, inbound: {kind: nope, action: create}}]`, 0, true},
		{"unsupported informer", `events: [{name: one, inbound: {kind: verticalpodautoscaler, action: create}, outbound: {kind: nil}}]`, 0, true},
		{"bad action", `events: [{name: one, inbound: {kind: pod, action: read}}]`, 0, true},
		{"bad outbound", `events: [{name: one, inbound: {kind: pod, action: create}, outbound: {kind: nope}}]`, 0, true},
		{"empty command", `events: [{name: one, inbound: {kind: pod, action: create}, outbound: {kind: cmd}}]`, 0, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rules, err := parseConfig(tt.data)
			if (err != nil) != tt.fail {
				t.Fatalf("error = %v", err)
			}
			if len(rules) != tt.count {
				t.Fatalf("rules = %d", len(rules))
			}
		})
	}
}

func TestConfigMapLifecycle(t *testing.T) {
	client := fake.NewClientset(&corev1.ConfigMap{Name: "config", Namespace: "default", Data: map[string]string{"config.yaml": "events: []"}})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx, client, "default", "config", "config.yaml") }()
	// Wait until the configuration informer is watching before changing the map.
	deadline := time.After(5 * time.Second)
	for {
		watching := false
		for _, action := range client.Actions() {
			if action.GetVerb() == "watch" {
				watching = true
			}
		}
		if watching {
			break
		}
		select {
		case <-deadline:
			cancel()
			t.Fatal("watch did not start")
		case <-time.After(time.Millisecond):
		}
	}
	cm, _ := client.CoreV1().ConfigMaps("default").Get(ctx, "config", metav1.GetOptions{})
	cm.Data["config.yaml"] = "events: [{disabled: true}]"
	if _, err := client.CoreV1().ConfigMaps("default").Update(ctx, cm, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown blocked")
	}
}

func TestMissingConfigKey(t *testing.T) {
	if _, err := configRules(&corev1.ConfigMap{}, "config.yaml"); err == nil {
		t.Fatal("expected missing key error")
	}
}

func TestConfigMapReloadsRulesAndActions(t *testing.T) {
	configuration := func(command string) string {
		return fmt.Sprintf("events: [{name: pods, inbound: {kind: pod, action: create}, outbound: {kind: cmd, action: %s}}]", command)
	}
	client := fake.NewClientset(&corev1.ConfigMap{Name: "config", Namespace: "default", Data: map[string]string{"config.yaml": configuration("first")}})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	transitions := make(chan string, 10)
	execute := func(ctx context.Context, _ kubernetes.Interface, rules []Rule) {
		command := rules[0].Outbound.Action
		transitions <- "start:" + command
		<-ctx.Done()
		transitions <- "stop:" + command
	}
	done := make(chan error, 1)
	go func() { done <- runWithRules(ctx, client, "default", "config", "config.yaml", execute) }()
	expect := func(want string) {
		t.Helper()
		select {
		case got := <-transitions:
			if got != want {
				t.Fatalf("transition = %q, want %q", got, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("waiting for %s", want)
		}
	}
	update := func(data string) {
		t.Helper()
		cm, err := client.CoreV1().ConfigMaps("default").Get(ctx, "config", metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		cm.Data["config.yaml"] = data
		if _, err := client.CoreV1().ConfigMaps("default").Update(ctx, cm, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	expect("start:first")
	update(configuration("second"))
	expect("stop:first")
	expect("start:second")
	update("events: [{name: invalid}]")
	update(configuration("third"))
	// Invalid configuration must not stop or replace the second runner.
	expect("stop:second")
	expect("start:third")
	cancel()
	expect("stop:third")
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown blocked")
	}
}

func TestDuplicateRules(t *testing.T) {
	rules, err := parseConfig(`events:
- name: first
  inbound: {kind: Pod, action: create, label: "b=2,a=1"}
  outbound: {kind: cmd, action: "echo hello"}
- name: second
  inbound: {kind: pod, action: CREATE, label: "a=1,b=2"}
  outbound: {kind: command, action: " echo hello "}
- name: first
  inbound: {kind: pod, action: create, label: "a=1,b=2"}
  outbound: {kind: cmd, action: "echo hello"}
- name: different-action
  inbound: {kind: pod, action: create, label: "a=1,b=2"}
  outbound: {kind: cmd, action: "echo other"}
`)
	if err != nil {
		t.Fatal(err)
	}
	if len(rules) != 2 || rules[0].Name != "first" || rules[1].Name != "different-action" {
		t.Fatalf("rules = %+v", rules)
	}
	_, err = parseConfig(`events:
- name: same
  inbound: {kind: pod, action: create}
  outbound: {kind: cmd, action: echo}
- name: same
  inbound: {kind: pod, action: delete}
  outbound: {kind: cmd, action: echo}
`)
	if err == nil {
		t.Fatal("conflicting rules with same name accepted")
	}
}
