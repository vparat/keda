package eventcontroller

import (
	"context"
	"maps"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

// watchConfigMap watches only the selected configuration object. The shared
// informer reconnects and relists automatically when its API watch expires.
// Add notifications also cover recreation after the ConfigMap is deleted.
func watchConfigMap(ctx context.Context, client kubernetes.Interface, namespace, name string) (<-chan *corev1.ConfigMap, func(), error) {
	updates := make(chan *corev1.ConfigMap, 1)
	watchCtx, cancel := context.WithCancel(ctx)
	factory := informers.NewSharedInformerFactoryWithOptions(client, 0, informers.WithNamespace(namespace), informers.WithTweakListOptions(func(opts *metav1.ListOptions) {
		opts.FieldSelector = fields.OneTermEqualSelector("metadata.name", name).String()
	}))
	informer := factory.Core().V1().ConfigMaps().Informer()
	enqueue := func(obj any) {
		cm, ok := obj.(*corev1.ConfigMap)
		if !ok || cm.Name != name || cm.Namespace != namespace {
			return
		}
		select {
		case updates <- cm.DeepCopy():
		case <-watchCtx.Done():
		}
	}
	if _, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{AddFunc: enqueue, UpdateFunc: func(old, next any) {
		if !maps.Equal(old.(*corev1.ConfigMap).Data, next.(*corev1.ConfigMap).Data) {
			enqueue(next)
		}
	}}); err != nil {
		cancel()
		return nil, nil, err
	}
	factory.Start(watchCtx.Done())
	return updates, func() { cancel(); factory.Shutdown() }, nil
}
