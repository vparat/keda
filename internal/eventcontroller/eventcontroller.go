// Package eventcontroller loads configuration and manages event rules.
package eventcontroller

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"

	"github.com/vparat/keda/internal/action"
	"github.com/vparat/keda/internal/cache"
	"github.com/vparat/keda/internal/configwatcher"
	"github.com/vparat/keda/internal/event"
	"github.com/vparat/keda/internal/informers"
	"github.com/vparat/keda/internal/util"
	"golang.org/x/sync/semaphore"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/yaml"
)

// Rule connects an inbound resource event to an outbound action.
type Rule struct {
	Name     string      `json:"name"`
	Disabled bool        `json:"disabled,omitempty"`
	Inbound  event.Event `json:"inbound"`
	Outbound event.Event `json:"outbound"`
}

// Configuration contains the rules loaded from the ConfigMap.
type Configuration struct {
	Events []Rule `json:"events"`
}

// parseConfig validates and normalizes enabled rules. Equivalent behaviors are
// deduplicated before checking for conflicting rule names.
func parseConfig(data string) ([]Rule, error) {

	if strings.TrimSpace(data) == "" {
		return nil, fmt.Errorf("configuration must not be empty")
	}
	var config Configuration

	if err := yaml.UnmarshalStrict([]byte(data), &config); err != nil {
		return nil, fmt.Errorf("decode event configuration: %w", err)
	}

	seen := make(map[string]bool)
	unique := make(map[struct{ Inbound, Outbound event.Event }]bool)

	// Preserve the nil result when no rules are enabled; callers already observe it.
	var rules []Rule

	for _, rule := range config.Events {

		if rule.Disabled {
			continue
		}

		if rule.Name == "" {
			return nil, fmt.Errorf("missing event name %q", rule.Name)
		}

		if _, ok := informers.Resource(rule.Inbound.Kind); !ok {
			return nil, fmt.Errorf("event %q: unsupported kind %q", rule.Name, rule.Inbound.Kind)
		}

		switch strings.ToLower(rule.Inbound.Action) {
		case "create", "update", "delete":
		default:
			return nil, fmt.Errorf("event %q: invalid inbound action %q", rule.Name, rule.Inbound.Action)
		}

		selector, err := labels.Parse(rule.Inbound.Label)

		if err != nil {
			return nil, fmt.Errorf("event %q: %w", rule.Name, err)
		}

		switch strings.ToLower(rule.Outbound.Kind) {
		case "cmd", "command":

			if strings.TrimSpace(rule.Outbound.Action) == "" {
				return nil, fmt.Errorf("event %q: empty command", rule.Name)
			}

		case "nil":
		default:
			return nil, fmt.Errorf("event %q: unsupported outbound kind %q", rule.Name, rule.Outbound.Kind)
		}

		// Names identify rules; behavior determines whether a rule is redundant.
		rule.Inbound.Kind = strings.ToLower(rule.Inbound.Kind)
		rule.Inbound.Action = strings.ToLower(rule.Inbound.Action)
		rule.Inbound.Label = selector.String()
		rule.Outbound.Kind = strings.ToLower(rule.Outbound.Kind)

		if rule.Outbound.Kind == "cmd" {
			rule.Outbound.Kind = "command"
		}

		rule.Outbound.Action = strings.TrimSpace(rule.Outbound.Action)
		identity := struct{ Inbound, Outbound event.Event }{Inbound: rule.Inbound, Outbound: rule.Outbound}

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

// controller holds the external operations used by startup, reload, and dispatch.
// Tests replace these operations on their own instance.
type controller struct {
	configure      func() (*rest.Config, error)
	watchConfig    configwatcher.Watcher
	watchResources func(context.Context, kubernetes.Interface, string, string, string, string, string, chan<- informers.Message) error
	newCommand     func(action.Config) (*action.Command, error)
	execute        func(context.Context, kubernetes.Interface, []Rule)
}

// newController returns a controller wired to the production dependencies.
func newController() *controller {
	c := &controller{
		configure:      rest.InClusterConfig,
		watchResources: informers.CreateAndRun,
		newCommand:     action.NewCommandAction,
	}
	c.execute = c.runRules

	return c
}

// Start runs the in-cluster controller until ctx is canceled.
// It cancels the parent context if configuration or client setup fails.
func Start(ctx context.Context, cancelParent context.CancelFunc, c cache.Cacher) {
	informers.SetDataStore(c)
	newController().start(ctx, cancelParent)
}

// start loads the REST configuration and runs the controller with
// ctx. It calls cancelParent on setup or runtime failure and returns no values.
func (c *controller) start(ctx context.Context, cancelParent context.CancelFunc) {
	logger := slog.Default().With("component", "eventcontroller")
	cfg, err := c.configure()

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

	if err := c.run(
		ctx,
		client,
	); err != nil {
		logger.Error("controller failed", "error", err)
		cancelParent()
	}

}

// run consumes configuration snapshots and reloads their rules using client.
// It returns startup or unexpected stream errors and joins workers on exit.
func (c *controller) run(ctx context.Context, client kubernetes.Interface) error {
	logger := slog.Default().With("component", "eventcontroller")
	source := c.watchConfig

	if source == nil {
		var err error
		cfg := configwatcher.NewConfig(env("CONFIGWATCHER_TYPE", "configmap"))
		sourceOptions := util.StringToMap(env("CONFIGWATCHER_SOURCE_OPTIONS", ""))
		if len(sourceOptions) == 0 {
			return errors.New("invalid CONFIGWATCHER_SOURCE_OPTIONS")
		}
		for k, v := range sourceOptions {
			cfg.SetOption(k, v)
		}
		source, err = configwatcher.NewWithConfig(cfg)
		if err != nil {
			return err
		}
	}
	updates, shutdown, err := source.Watch(ctx)

	if err != nil {
		return fmt.Errorf("watch config: %w", err)
	}
	defer shutdown()
	var current string

	select {
	case <-ctx.Done():
		return nil
	case data, ok := <-updates:

		if !ok {

			if ctx.Err() != nil {
				return nil
			}

			return fmt.Errorf("configuration source closed before initial snapshot")
		}
		current = string(data)
	}
	rules, err := parseConfig(current)

	if err != nil {
		return fmt.Errorf("initial configuration: %w", err)
	}

	// Each runner owns its context and completion channel. Stop waits for all
	// informers and actions before the next configuration starts.
	start := func(rules []Rule) func() {
		ruleCtx, cancel := context.WithCancel(ctx)
		done := make(chan struct{})
		go func() {
			defer close(done)
			c.execute(ruleCtx, client, rules)
		}()

		return func() {
			cancel()
			<-done
		}
	}
	stop := start(rules)
	defer func() {
		stop()
	}()

	for {

		select {
		case <-ctx.Done():
			return nil
		case data, ok := <-updates:

			if !ok {

				if ctx.Err() != nil {
					return nil
				}

				return fmt.Errorf("configuration source closed")
			}
			snapshot := string(data)

			if snapshot == current {
				continue
			}

			next, err := parseConfig(snapshot)

			if err != nil {
				logger.Error("invalid configuration; keeping current rules", "error", err)
				continue
			}

			stop()
			stop = start(next)
			current = snapshot
			logger.Info("configuration reloaded", "events", len(next))
		}
	}
}

// runRules watches the supplied rules using client and dispatches their actions.
// It joins all workers when ctx is canceled and returns no values.
func (c *controller) runRules(ctx context.Context, client kubernetes.Interface, rules []Rule) {
	logger := slog.Default().With("component", "eventcontroller")
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
			if err := c.watchResources(
				ctx,
				client,
				rule.Inbound.Kind,
				rule.Inbound.Action,
				rule.Inbound.Label,
				rule.Inbound.Namespace,
				rule.Name,
				messages,
			); err != nil {
				logger.Error(
					"informer error",
					"event",
					rule.Name,
					"error",
					err,
				)
			}

		})
	}

	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-messages:
			rule := byName[msg.Key]
			eventLogger := logger.With(
				"event",
				rule.Name,
				"name",
				msg.Name,
				"namespace",
				msg.Namespace,
			)

			if strings.EqualFold(rule.Outbound.Kind, "nil") {
				(&action.Nil{Logger: *eventLogger}).Do(ctx)
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
				eventLogger.Debug("duplicate job skipped")
				continue
			}

			runner, err := c.newCommand(action.Config{
				Logger:          *eventLogger,
				GlobalSemaphore: limit,
				AcquireTimeout:  settings.Timeout,
			})

			if err != nil {
				jobs.Release(key)
				eventLogger.Error("failed to create command runner", "error", err)
				continue
			}

			runner.Args = command
			workers.Go(func() {
				defer jobs.Release(key)
				runner.Do(ctx)
			})
		}

	}

}

// shellQuote escapes a single shell argument, including embedded apostrophes.
func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}
