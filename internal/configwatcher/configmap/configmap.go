// Package configmap adapts Kubernetes ConfigMaps to configuration snapshots.
package configmap

import (
	"context"
	"fmt"

	"github.com/vparat/configmapwatcher"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// ConfigMap watches one Data key using an existing Kubernetes client.
type ConfigMap struct {
	Client    kubernetes.Interface
	Name      string
	Key       string
	Namespace string
}


// Watch reads the initial ConfigMap and then emits updates for its selected key.
// ctx controls all work. It returns snapshots, a joining shutdown function, and
// any setup error. A missing key emits empty bytes for the consumer to validate.
func (c ConfigMap) Watch(ctx context.Context) (<-chan []byte, func(), error) {

	if c.Client == nil {
		return nil, nil, fmt.Errorf("configmap source requires a Kubernetes client")
	}
	cm, err := c.Client.CoreV1().ConfigMaps(c.Namespace).Get(ctx, c.Name, metav1.GetOptions{})

	if err != nil {
		return nil, nil, fmt.Errorf("get configmap %s/%s: %w", c.Namespace, c.Name, err)
	}
	watchCtx, cancel := context.WithCancel(ctx)
	changes, stop, err := configmapwatcher.WatchAsBytes(watchCtx, c.Client, c.Namespace, c.Name, c.Key)

	if err != nil {
		cancel()

		return nil, nil, fmt.Errorf("watch configmap %s/%s: %w", c.Namespace, c.Name, err)
	}
	updates := make(chan []byte, 1)
	updates <- []byte(cm.Data[c.Key])
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer close(updates)
		defer stop()

		for {

			select {
			case <-watchCtx.Done():
				return
			case data, ok := <-changes:

				if !ok {
					return
				}

				select {
				case updates <- data:
				case <-watchCtx.Done():
					return
				}
			}
		}
	}()

	return updates, func() {
		cancel()
		<-done
	}, nil
}
