package eventcontroller

import (
	"context"
	"fmt"
	configmapsource "github.com/vparat/keda/internal/configwatcher/configmap"
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
		{
			name:  "valid",
			data:  `events: [{name: one, inbound: {kind: pod, action: create}, outbound: {kind: cmd, action: echo}}]`,
			count: 1,
			fail:  false,
		},
		{name: "disabled", data: `events: [{disabled: true}]`, count: 0, fail: false},
		{name: "empty rules", data: `events: []`, count: 0, fail: false},
		{name: "unknown field", data: `evnts: []`, count: 0, fail: true},
		{
			name:  "missing name",
			data:  `events: [{inbound: {kind: pod, action: create}}]`,
			count: 0,
			fail:  true,
		},
		{
			name:  "unknown kind",
			data:  `events: [{name: one, inbound: {kind: nope, action: create}}]`,
			count: 0,
			fail:  true,
		},
		{
			name:  "unsupported informer",
			data:  `events: [{name: one, inbound: {kind: verticalpodautoscaler, action: create}, outbound: {kind: nil}}]`,
			count: 0,
			fail:  true,
		},
		{
			name:  "bad action",
			data:  `events: [{name: one, inbound: {kind: pod, action: read}}]`,
			count: 0,
			fail:  true,
		},
		{
			name:  "bad outbound",
			data:  `events: [{name: one, inbound: {kind: pod, action: create}, outbound: {kind: nope}}]`,
			count: 0,
			fail:  true,
		},
		{
			name:  "empty command",
			data:  `events: [{name: one, inbound: {kind: pod, action: create}, outbound: {kind: cmd}}]`,
			count: 0,
			fail:  true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rules, err := parseConfig(tt.data)

			if (err != nil) != tt.fail {
				t.Fatalf(
					"parseConfig(%q) error = %v, want failure = %v",
					tt.data,
					err,
					tt.fail,
				)
			}

			if len(rules) != tt.count {
				t.Fatalf(
					"len(parseConfig(%q)) = %d, want %d",
					tt.data,
					len(rules),
					tt.count,
				)
			}

		})
	}

}


func TestConfigMapLifecycle(t *testing.T) {
	client := fake.NewClientset(&corev1.ConfigMap{
		Name:      "config",
		Namespace: "default",
		Data:      map[string]string{"config.yaml": "events: []"},
	})
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		c := newController()
		c.watchConfig = configmapsource.ConfigMap{Client: client, Namespace: "default", Name: "config", Key: "config.yaml"}
		done <- c.run(
			ctx,
			client,
		)
	}()

	// Wait until the configuration informer is watching before changing the map.
	deadline := time.After(5 * time.Second)

	for {
		var watching bool

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

	cm, err := client.CoreV1().ConfigMaps("default").Get(ctx, "config", metav1.GetOptions{})

	if err != nil {
		t.Fatal(err)
	}

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

	if _, err := parseConfig(""); err == nil {
		t.Fatal("expected missing key error")
	}

}


func TestConfigMapReloadsRulesAndActions(t *testing.T) {
	configuration := func(command string) string {
		const configFormat = "events: [{name: pods, inbound: {kind: pod, action: create}, " +
			"outbound: {kind: cmd, action: %s}}]"

		return fmt.Sprintf(configFormat, command)
	}
	client := fake.NewClientset(&corev1.ConfigMap{
		Name:      "config",
		Namespace: "default",
		Data:      map[string]string{"config.yaml": configuration("first")},
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	transitions := make(chan string, 10)
	execute := func(ctx context.Context, _ kubernetes.Interface, rules []Rule) {
		command := rules[0].Outbound.Action
		transitions <- "start:" + command
		<-ctx.Done()
		transitions <- "stop:" + command
	}
	done := make(chan error, 1)
	go func() {
		c := newController()
		c.execute = execute
		c.watchConfig = configmapsource.ConfigMap{Client: client, Namespace: "default", Name: "config", Key: "config.yaml"}
		done <- c.run(
			ctx,
			client,
		)
	}()
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

	if len(rules) != 2 {
		t.Fatalf("len(parseConfig(...)) = %d, want 2", len(rules))
	}

	if rules[0].Name != "first" || rules[1].Name != "different-action" {
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
