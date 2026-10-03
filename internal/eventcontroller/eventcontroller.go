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
	var rules []Rule
	for _, rule := range config.Events {
		if rule.Disabled {
			continue
		}
		if rule.Name == "" || seen[rule.Name] {
			return nil, fmt.Errorf("missing or duplicate event name %q", rule.Name)
		}
		seen[rule.Name] = true
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
	ruleCtx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	start := func() {
		activeCtx, activeDone, activeRules := ruleCtx, done, rules
		go func() { defer close(activeDone); execute(activeCtx, client, activeRules) }()
	}
	start()
	defer func() { stop(); <-done }()
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
			<-done
			rules, current = next, cm.Data[key]
			ruleCtx, stop = context.WithCancel(ctx)
			done = make(chan struct{})
			start()
			logger.Info("configuration reloaded", "events", len(rules))
		}
	}
}

func configRules(cm *corev1.ConfigMap, key string) ([]Rule, error) {
	data, ok := cm.Data[key]
	if !ok || strings.TrimSpace(data) == "" {
		return nil, fmt.Errorf("ConfigMap %s/%s missing nonempty key %q", cm.Namespace, cm.Name, key)
	}
	return parseConfig(data)
}

func runRules(ctx context.Context, client kubernetes.Interface, rules []Rule) {
	messages := make(chan informers.Message, 100)
	errors := make(chan error, 100)
	var workers sync.WaitGroup
	defer workers.Wait()
	limit := semaphore.NewWeighted(5)
	byName := make(map[string]Rule)
	for _, rule := range rules {
		byName[rule.Name] = rule
		workers.Go(func() {
			informers.CreateAndRun(ctx, client, rule.Inbound.Kind, rule.Inbound.Action, rule.Inbound.Label, rule.Inbound.Namespace, rule.Name, messages, errors)
		})
	}
	for {
		select {
		case <-ctx.Done():
			return
		case err := <-errors:
			logger.Error("informer error", "error", err)
		case msg := <-messages:
			rule := byName[msg.Key]
			l := logger.With("event", rule.Name, "name", msg.Name, "namespace", msg.Namespace)
			if strings.EqualFold(rule.Outbound.Kind, "nil") {
				(&action.Nil{Logger: *l}).Do()
				continue
			}
			command := strings.TrimSpace(rule.Outbound.Action)
			if strings.HasSuffix(command, ";") {
				command = strings.TrimSuffix(command, ";")
			} else {
				command += " " + shellQuote(msg.Name) + " " + shellQuote(msg.Namespace)
			}
			runner, _ := action.NewCommandAction(action.Config{CTX: ctx, Logger: *l, GlobalSemaphore: limit})
			runner.Args = command
			workers.Go(runner.Do)
		}
	}
}

func shellQuote(value string) string { return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'" }
