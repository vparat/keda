package eventcontroller

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"

	"github.com/vparat/keda/internal/action"
	"github.com/vparat/keda/internal/event"
	"github.com/vparat/keda/internal/informers"
	"golang.org/x/sync/semaphore"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/yaml"
)

var logger = slog.Default().With("component", "eventcontroller")

type Rule struct {
	Name     string      `json:"name"`
	Disabled bool        `json:"disabled,omitempty"`
	Inbound  event.Event `json:"inbound"`
	Outbound event.Event `json:"outbound"`
}

type Configuration struct {
	Events []Rule `json:"events"`
}

func parseConfig(data string) ([]Rule, error) {
	var config Configuration
	if err := yaml.UnmarshalStrict([]byte(data), &config); err != nil {
		return nil, err
	}
	seen := make(map[string]bool)
	unique := make(map[struct{ Inbound, Outbound event.Event }]bool)
	var rules []Rule
	for _, rule := range config.Events {
		if rule.Disabled {
			continue
		}
		if rule.Name == "" {
			return nil, fmt.Errorf("missing or duplicate event name %q", rule.Name)
		}
		if _, ok := informers.Resource(rule.Inbound.Kind); !ok {
			return nil, fmt.Errorf("event %s: unsupported kind %q", rule.Name, rule.Inbound.Kind)
		}
		switch strings.ToLower(rule.Inbound.Action) {
		case "create", "update", "delete":
		default:
			return nil, fmt.Errorf("event %s: invalid inbound action %q", rule.Name, rule.Inbound.Action)
		}
		if _, err := labels.Parse(rule.Inbound.Label); err != nil {
			return nil, fmt.Errorf("event %s: %w", rule.Name, err)
		}
		switch strings.ToLower(rule.Outbound.Kind) {
		case "cmd", "command":
			if strings.TrimSpace(rule.Outbound.Action) == "" {
				return nil, fmt.Errorf("event %s: empty command", rule.Name)
			}
		case "nil":
		default:
			return nil, fmt.Errorf("event %s: unsupported outbound kind %q", rule.Name, rule.Outbound.Kind)
		}
		// Names identify rules; behavior determines whether a rule is redundant.
		rule.Inbound.Kind = strings.ToLower(rule.Inbound.Kind)
		rule.Inbound.Action = strings.ToLower(rule.Inbound.Action)
		selector, _ := labels.Parse(rule.Inbound.Label)
		rule.Inbound.Label = selector.String()
		rule.Outbound.Kind = strings.ToLower(rule.Outbound.Kind)
		if rule.Outbound.Kind == "cmd" {
			rule.Outbound.Kind = "command"
		}
		rule.Outbound.Action = strings.TrimSpace(rule.Outbound.Action)
		identity := struct{ Inbound, Outbound event.Event }{rule.Inbound, rule.Outbound}
		if unique[identity] {
			continue
		}
		if seen[rule.Name] {
			return nil, fmt.Errorf("duplicate event name %q", rule.Name)
		}
		seen[rule.Name], unique[identity] = true, true
		rules = append(rules, rule)
	}
	return rules, nil
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}

	return fallback
}

func Start(ctx context.Context, cancelParent context.CancelFunc) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		logger.Error("failed to get in-cluster config", "error", err)
		cancelParent()
		return
	}

	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		logger.Error("failed to create kubernetes client", "error", err)
		cancelParent()
		return
	}

	if err := run(ctx, client, env("CONFIGMAP_NAMESPACE", "default"), env("CONFIGMAP_NAME", "keda-config"), env("CONFIGMAP_KEY", "config.yaml")); err != nil {
		logger.Error("controller failed", "error", err)
		cancelParent()
	}
}

func run(ctx context.Context, client kubernetes.Interface, namespace, name, key string) error {
	return runWithRules(ctx, client, namespace, name, key, runRules)
}

func runWithRules(ctx context.Context, client kubernetes.Interface, namespace, name, key string, execute func(context.Context, kubernetes.Interface, []Rule)) error {
	cm, err := client.CoreV1().ConfigMaps(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	rules, err := configRules(cm, key)
	if err != nil {
		return err
	}
	updates, shutdown, err := watchConfigMap(ctx, client, namespace, name)
	if err != nil {
		return err
	}
	defer shutdown()
	// Each runner owns its context and completion channel. Stop waits for all
	// informers and actions before the next configuration starts.
	start := func(rules []Rule) func() {
		ruleCtx, cancel := context.WithCancel(ctx)
		done := make(chan struct{})
		go func() {
			defer close(done)
			execute(ruleCtx, client, rules)
		}()
		return func() { cancel(); <-done }
	}
	stop := start(rules)
	defer func() { stop() }()
	current := cm.Data[key]
	for {
		select {
		case <-ctx.Done():
			return nil
		case cm := <-updates:
			if cm.Data[key] == current {
				continue
			}
			next, err := configRules(cm, key)
			if err != nil {
				logger.Error("invalid configuration; keeping current rules", "error", err)
				continue
			}
			stop()
			stop = start(next)
			current = cm.Data[key]
			logger.Info("configuration reloaded", "events", len(next))
		}
	}
}

func configRules(cm *corev1.ConfigMap, key string) ([]Rule, error) {
	data := cm.Data[key]
	if strings.TrimSpace(data) == "" {
		return nil, fmt.Errorf("ConfigMap %s/%s missing nonempty key %q", cm.Namespace, cm.Name, key)
	}
	return parseConfig(data)
}

func runRules(ctx context.Context, client kubernetes.Interface, rules []Rule) {
	messages := make(chan informers.Message, 100)
	var workers sync.WaitGroup
	defer workers.Wait()
	settings := action.ConcurrencyFromEnv()
	limit := semaphore.NewWeighted(settings.Max)
	var jobs action.Jobs
	byName := make(map[string]Rule)
	for _, rule := range rules {
		byName[rule.Name] = rule
		workers.Go(func() {
			if err := informers.CreateAndRun(ctx, client, rule.Inbound.Kind, rule.Inbound.Action, rule.Inbound.Label, rule.Inbound.Namespace, rule.Name, messages); err != nil {
				logger.Error("informer error", "event", rule.Name, "error", err)
			}
		})
	}
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-messages:
			rule := byName[msg.Key]
			l := logger.With("event", rule.Name, "name", msg.Name, "namespace", msg.Namespace)
			if strings.EqualFold(rule.Outbound.Kind, "nil") {
				(&action.Nil{Logger: *l}).Do()
				continue
			}
			command := strings.TrimSpace(rule.Outbound.Action)
			if before, ok := strings.CutSuffix(command, ";"); ok {
				command = before
			} else {
				command += " " + shellQuote(msg.Name) + " " + shellQuote(msg.Namespace)
			}
			key := action.JobKey{Rule: rule.Name, Name: msg.Name, Namespace: msg.Namespace}
			if !jobs.Claim(key) {
				l.Debug("duplicate job skipped")
				continue
			}
			runner, _ := action.NewCommandAction(action.Config{CTX: ctx, Logger: *l, GlobalSemaphore: limit, AcquireTimeout: settings.Timeout})
			runner.Args = command
			workers.Go(func() { defer jobs.Release(key); runner.Do() })
		}
	}
}

func shellQuote(value string) string { return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'" }
