// Package configmapwatcher watches a selected Kubernetes ConfigMap.
package configmapwatcher

import (
	"context"
	"fmt"
	"maps"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)


// objectWatch starts a ConfigMap watch whose shutdown function waits for cleanup.
type objectWatch func(context.Context, kubernetes.Interface, string, string) (<-chan *corev1.ConfigMap, func(), error)


// Watch emits copies of the named ConfigMap on addition, Data changes, and
// deletion, including recreation. Deletions carry the last known object.
// The informer reconnects and relists when its API watch expires.
//
// Canceling ctx stops the watch but leaves the channel open. The returned
// shutdown function cancels the watch and waits for the informer to stop.
// Registration failures return nil outputs and an error.
func Watch(
	ctx context.Context,
	client kubernetes.Interface,
	namespace string,
	name string,
) (<-chan *corev1.ConfigMap, func(), error) {
	factory := informers.NewSharedInformerFactoryWithOptions(
		client,
		0,
		informers.WithNamespace(namespace),
		informers.WithTweakListOptions(func(opts *metav1.ListOptions) {
			opts.FieldSelector = fields.OneTermEqualSelector("metadata.name", name).String()
		}),
	)

	return watch(
		ctx,
		factory,
		factory.Core().V1().ConfigMaps().Informer(),
		namespace,
		name,
	)
}


// WatchAsBytes forwards Watch notifications as bytes from the selected Data key.
// A missing key produces empty bytes; deletion carries the last known value.
//
// The output channel closes when forwarding stops, retaining buffered values.
// The returned shutdown function cancels forwarding and waits for both it and
// the informer to stop. Registration failures return nil outputs and an error.
func WatchAsBytes(
	ctx context.Context,
	client kubernetes.Interface,
	namespace string,
	name string,
	key string,
) (<-chan []byte, func(), error) {

	return watchAsBytes(
		ctx,
		client,
		namespace,
		name,
		key,
		Watch,
	)
}


// watchAsBytes converts notifications from start into values for key.
// Its shutdown function stops the object watch and waits for byte forwarding.
func watchAsBytes(
	ctx context.Context,
	client kubernetes.Interface,
	namespace string,
	name string,
	key string,
	start objectWatch,
) (<-chan []byte, func(), error) {
	watchCtx, cancel := context.WithCancel(ctx)
	objects, shutdown, err := start(
		watchCtx,
		client,
		namespace,
		name,
	)

	if err != nil {
		cancel()

		return nil, nil, fmt.Errorf("start configmap watch %s/%s: %w", namespace, name, err)
	}

	updates := make(chan []byte, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer close(updates)

		for {
			select {
			case <-watchCtx.Done():
				return
			case cm := <-objects:
				select {
				case updates <- []byte(cm.Data[key]):
				case <-watchCtx.Done():
					return
				}
			}
		}

	}()

	return updates, func() {
		cancel()
		shutdown()
		<-done
	}, nil
}


// watch registers handlers on informer and starts factory.
// Notifications are copied so callers cannot modify the informer cache.
func watch(
	ctx context.Context,
	factory informers.SharedInformerFactory,
	informer cache.SharedIndexInformer,
	namespace string,
	name string,
) (<-chan *corev1.ConfigMap, func(), error) {
	updates := make(chan *corev1.ConfigMap, 1)
	watchCtx, cancel := context.WithCancel(ctx)

	enqueue := func(obj any) {

		if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
			obj = tombstone.Obj
		}

		cm, ok := obj.(*corev1.ConfigMap)
		matches := ok && cm.Name == name && cm.Namespace == namespace

		if !matches {

			return
		}

		select {
		case updates <- cm.DeepCopy():
		case <-watchCtx.Done():
		}
	}

	if _, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    enqueue,
		DeleteFunc: enqueue,
		UpdateFunc: func(old, next any) {

			if !maps.Equal(old.(*corev1.ConfigMap).Data, next.(*corev1.ConfigMap).Data) {
				enqueue(next)
			}

		},
	}); err != nil {
		cancel()

		return nil, nil, fmt.Errorf("register configmap handler %s/%s: %w", namespace, name, err)
	}

	factory.Start(watchCtx.Done())

	return updates, func() {
		cancel()
		factory.Shutdown()
	}, nil
}
