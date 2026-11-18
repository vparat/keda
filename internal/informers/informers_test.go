package informers

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	kinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"
)


// Every kind accepted during configuration validation must have an informer.
func TestSupportedKindsHaveInformers(t *testing.T) {
	factory := kinformers.NewSharedInformerFactory(fake.NewClientset(), 0)

	for kind := range kindToGVR {
		t.Run(kind, func(t *testing.T) {

			if _, err := createInformer(kind, factory); err != nil {
				t.Fatal(err)
			}

		})
	}

}

func TestCreateAndRun(t *testing.T) {
	client := fake.NewClientset(&corev1.Pod{Name: "example", Namespace: "default"})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	messages := make(chan Message)
	done := make(chan error, 1)
	go func() {
		done <- CreateAndRun(
			ctx,
			client,
			"pod",
			"create",
			"",
			"default",
			"pod-created",
			messages,
		)
	}()

	select {
	case got := <-messages:
		want := Message{Key: "pod-created", Name: "example", Namespace: "default", InitialList: true}

		if got != want {
			t.Fatalf("message = %+v, want %+v", got, want)
		}

	case <-time.After(5 * time.Second):
		t.Fatal("no event received")
	}

	cancel()

	select {
	case err := <-done:

		if err != nil {
			t.Fatal(err)
		}

	case <-time.After(5 * time.Second):
		t.Fatal("informer did not stop")
	}

}

func TestCreateAndRunInvalidConfiguration(t *testing.T) {

	for _, tt := range []struct{ kind, action string }{
		{kind: "unknown", action: "create"},
		{kind: "pod", action: "unknown"},
	} {
		t.Run(tt.kind+"/"+tt.action, func(t *testing.T) {

			if err := CreateAndRun(
				t.Context(),
				fake.NewClientset(),
				tt.kind,
				tt.action,
				"",
				"",
				"rule",
				nil,
			); err == nil {
				t.Fatal("expected configuration error")
			}

		})
	}

}


// failingFactory returns its configured error for any requested resource.
type failingFactory struct {
	err error
}


// ForResource accepts a resource and returns the configured lookup failure.
func (f failingFactory) ForResource(schema.GroupVersionResource) (kinformers.GenericInformer, error) {
	return nil, f.err
}


// controlledInformer exposes registered callbacks without running a reflector.
type controlledInformer struct {
	cache.SharedIndexInformer
	handler         cache.ResourceEventHandler
	registrationErr error
	dispatch        func(cache.ResourceEventHandler)
	ran             bool
}


// AddEventHandler records handler and returns the configured registration error.
func (i *controlledInformer) AddEventHandler(handler cache.ResourceEventHandler) (cache.ResourceEventHandlerRegistration, error) {
	i.handler = handler

	return nil, i.registrationErr
}


// Run marks the informer started and dispatches its configured callback sequence.
func (i *controlledInformer) Run(<-chan struct{}) {
	i.ran = true
	i.dispatch(i.handler)
}


// TestCreateInformerFactoryError verifies that factory failures retain their cause.
func TestCreateInformerFactoryError(t *testing.T) {
	cause := errors.New("factory unavailable")
	informer, err := createInformer("pod", failingFactory{err: cause})

	if informer != nil || !errors.Is(err, cause) || !strings.Contains(err.Error(), `"pod"`) {
		t.Fatalf("createInformer = %v, %v; want wrapped factory error", informer, err)
	}

}


// TestRunInformerCallbacks exercises actions, tombstones, malformed objects, and resyncs.
func TestRunInformerCallbacks(t *testing.T) {
	old := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "example", Namespace: "default", ResourceVersion: "1"}}
	next := old.DeepCopy()
	next.ResourceVersion = "2"
	tests := []struct {
		name     string
		action   string
		dispatch func(cache.ResourceEventHandler)
	}{
		{
			name:   "create ignores malformed object",
			action: "CREATE",
			dispatch: func(h cache.ResourceEventHandler) {
				h.OnAdd("invalid", false)
				h.OnAdd(old, false)
			},
		},
		{
			name:   "update ignores malformed objects and resyncs",
			action: "update",
			dispatch: func(h cache.ResourceEventHandler) {
				h.OnUpdate("invalid", next)
				h.OnUpdate(old, "invalid")
				h.OnUpdate(old, old.DeepCopy())
				h.OnUpdate(old, next)
			},
		},
		{
			name:   "delete unwraps tombstones",
			action: "delete",
			dispatch: func(h cache.ResourceEventHandler) {
				h.OnDelete(cache.DeletedFinalStateUnknown{Obj: old})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out := make(chan Message, 4)
			informer := &controlledInformer{dispatch: tt.dispatch}
			err := runInformer(t.Context(), informer, "pod", tt.action, "rule", out)

			if err != nil || !informer.ran {
				t.Fatalf("runInformer = %v, ran = %v", err, informer.ran)
			}

			if len(out) != 1 {
				t.Fatalf("received %d events, want 1", len(out))
			}

			want := Message{Key: "rule", Name: "example", Namespace: "default"}

			if got := <-out; got != want {
				t.Fatalf("message = %+v, want %+v", got, want)
			}

		})
	}

}


// TestRunInformerCanceled verifies a blocked emitter exits when ctx is canceled.
func TestRunInformerCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	informer := &controlledInformer{dispatch: func(h cache.ResourceEventHandler) {
		h.OnAdd(&corev1.Pod{}, false)
	}}

	if err := runInformer(ctx, informer, "pod", "create", "rule", nil); err != nil {
		t.Fatal(err)
	}

	if !informer.ran {
		t.Fatal("informer did not run")
	}

}


// TestRunInformerRegistrationError verifies registration failures prevent startup.
func TestRunInformerRegistrationError(t *testing.T) {
	cause := errors.New("informer stopped")
	informer := &controlledInformer{registrationErr: cause}
	err := runInformer(t.Context(), informer, "pod", "create", "rule", nil)

	if !errors.Is(err, cause) || !strings.Contains(err.Error(), `register "create" handler for "pod"`) {
		t.Fatalf("registration error = %v", err)
	}

	if informer.ran {
		t.Fatal("informer ran after registration failed")
	}

}


// memoryStore records cache operations and can simulate backend failures.
type memoryStore struct {
	values         map[string][]byte
	getErr, setErr error
	gets, sets     int
	getCtx, setCtx context.Context
}


// Get takes a context and key and returns the stored value or configured error.
func (s *memoryStore) Get(ctx context.Context, key string) ([]byte, error) {
	s.gets++
	s.getCtx = ctx

	return s.values[key], s.getErr
}


// Set takes a context, key, and value and returns the configured write error.
func (s *memoryStore) Set(ctx context.Context, key string, value []byte) error {
	s.sets++
	s.setCtx = ctx

	if s.setErr == nil {
		s.values[key] = value
	}

	return s.setErr
}


// Delete takes a context and key and returns nil after removing the entry.
func (s *memoryStore) Delete(_ context.Context, key string) error {
	delete(s.values, key)

	return nil
}


// TestProceed takes the test handle and checks duplicate suppression and fail-open behavior.
func TestProceed(t *testing.T) {
	previous := dataStore
	t.Cleanup(func() { SetDataStore(previous) })

	for _, tt := range []struct {
		name, key, value, cached string
		getErr, setErr           error
		noStore, want            bool
		gets, sets               int
	}{
		{name: "no store", key: "pod", value: "1", noStore: true, want: true},
		{name: "empty key", value: "1", want: true},
		{name: "empty version", key: "pod", want: true},
		{name: "cache miss", key: "pod", value: "1", want: true, gets: 1, sets: 1},
		{name: "duplicate", key: "pod", value: "1", cached: "1", gets: 1},
		{name: "new version", key: "pod", value: "2", cached: "1", want: true, gets: 1, sets: 1},
		{name: "read failure", key: "pod", value: "1", cached: "1", getErr: errors.New("read failed"), want: true, gets: 1, sets: 1},
		{name: "write failure", key: "pod", value: "1", setErr: errors.New("write failed"), want: true, gets: 1, sets: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			store := &memoryStore{values: map[string][]byte{tt.key: []byte(tt.cached)}, getErr: tt.getErr, setErr: tt.setErr}
			SetDataStore(store)

			if tt.noStore {
				SetDataStore(nil)
			}

			if got := proceed(t.Context(), tt.key, tt.value); got != tt.want {
				t.Fatalf("proceed = %v, want %v", got, tt.want)
			}

			if store.gets != tt.gets || store.sets != tt.sets {
				t.Fatalf("cache calls = (%d, %d), want (%d, %d)", store.gets, store.sets, tt.gets, tt.sets)
			}

			if store.gets > 0 && store.getCtx != t.Context() {
				t.Fatal("cache read did not receive the caller context")
			}

			if store.sets > 0 && store.setCtx != t.Context() {
				t.Fatal("cache write did not receive the caller context")
			}

			if tt.sets > 0 && tt.setErr == nil && string(store.values[tt.key]) != tt.value {
				t.Fatalf("cached value = %q, want %q", store.values[tt.key], tt.value)
			}

		})
	}

}


// TestRunInformerDeduplication takes the test handle and checks version and rule isolation.
func TestRunInformerDeduplication(t *testing.T) {
	previous := dataStore
	t.Cleanup(func() { SetDataStore(previous) })
	SetDataStore(&memoryStore{values: make(map[string][]byte)})
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "example", Namespace: "default", ResourceVersion: "1"}}
	out := make(chan Message, 10)

	for _, rule := range []string{"first", "second"} {
		informer := &controlledInformer{dispatch: func(h cache.ResourceEventHandler) {
			h.OnAdd(pod, false)
			h.OnAdd(pod.DeepCopy(), false)
			next := pod.DeepCopy()
			next.ResourceVersion = "2"
			h.OnAdd(next, false)
		}}

		if err := runInformer(t.Context(), informer, "pod", "create", rule, out); err != nil {
			t.Fatal(err)
		}

		for range 2 {

			select {
			case got := <-out:

				if got != (Message{Key: rule, Name: "example", Namespace: "default"}) {
					t.Fatalf("unexpected message: %+v", got)
				}

			default:
				t.Fatal("missing event")
			}
		}

		if len(out) != 0 {
			t.Fatal("duplicate event forwarded")
		}
	}

}
