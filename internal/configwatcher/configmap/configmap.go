// Package configmap adapts Kubernetes ConfigMaps to configuration snapshots.
package configmap

import (
	"context"
	"errors"
	"fmt"

	"github.com/vparat/configmapwatcher"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// ConfigMap watches one Data key using an existing Kubernetes client.
type ConfigMap struct {
	Name      string
	Key       string
	Namespace string
	Client    kubernetes.Interface
}

func New(namespace, name, key string) (ConfigMap, error) {
	if name == "" {
		return ConfigMap{}, errors.New("name is required")
	}
	if key == "" {
		return ConfigMap{}, errors.New("key is required")
	}
	if namespace == "" {
		return ConfigMap{}, errors.New("namespace is required")
	}
	c := ConfigMap{
		Name:      name,
		Namespace: namespace,
		Key:       key,
	}

	if c.Client == nil {
		cfg, err := rest.InClusterConfig()
		if err != nil {
			return ConfigMap{}, errors.New("failed to get in-cluster config")
		}

		c.Client, err = kubernetes.NewForConfig(cfg)
		if err != nil {
			return ConfigMap{}, fmt.Errorf("failed to create kubernetes client:  %w ", err)
		}
	}

	return c, nil
}

// Watch reads the initial ConfigMap and then emits updates for its selected key.
// ctx controls all work. It returns snapshots, a joining shutdown function, and
// any setup error. A missing key emits empty bytes for the consumer to validate.
func (c ConfigMap) Watch(ctx context.Context) (<-chan []byte, func(), error) {

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
