package eventcontroller

import (
	"context"
	"fmt"
	"log/slog"

	authorizationv1 "k8s.io/api/authorization/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	authv1 "k8s.io/client-go/kubernetes/typed/authorization/v1"
	"k8s.io/client-go/rest"

	"github.com/vparat/keda/internal/informers"
)

const eventActionKind = "EventAction"

var logger = slog.Default().With("component", "eventcontroller")

func Start(ctx context.Context, cancelParent context.CancelFunc) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		logger.Error("failed to get in-cluster config", "error", err)
		cancelParent()
		return
	}

	extClient, err := clientset.NewForConfig(cfg)
	if err != nil {
		logger.Error("failed to create apiextensions clientset", "error", err)
		cancelParent()
		return
	}

	k8sClient, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		logger.Error("failed to create kubernetes clientset", "error", err)
		cancelParent()
		return
	}

	sar := k8sClient.AuthorizationV1().SelfSubjectAccessReviews()

	// List CRDs and process only those defining EventAction resources.
	crdList, err := extClient.ApiextensionsV1().CustomResourceDefinitions().List(ctx, metav1.ListOptions{})
	if err != nil {
		logger.Error("failed to list CRDs", "error", err)
		cancelParent()
		return
	}

	// validate rbac and create informers based on those
	informerLife := make(chan struct{})
	msgCh := make(chan informers.Message, 100)
	errCh := make(chan error, 100)

	for _, crd := range crdList.Items {
		if crd.Spec.Names.Kind != eventActionKind {
			continue
		}

		for _, v := range crd.Spec.Versions {
			if !validateRBAC(ctx, sar, crd.Spec.Group, v.Name, crd.Spec.Names.Plural) {
				continue
			}

			informers.RegisterCRD(crd.Spec.Names.Kind, crd.Spec.Group, v.Name, crd.Spec.Names.Plural)
			logger.Info("registered CRD",
				"kind", crd.Spec.Names.Kind,
				"group", crd.Spec.Group,
				"version", v.Name,
			)

			go informers.CreateAndRun(crd.Spec.Names.Kind, "create", "", "", crd.Name, msgCh, errCh, informerLife)
		}
	}

	// create crd watcher
	go watchCRDs(ctx, extClient, cfg, informerLife, msgCh, errCh)

	// consume messages and errors
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case msg := <-msgCh:
				logger.Info("event received", "kind", msg.Key, "name", msg.Name, "namespace", msg.Namespace)
			case err := <-errCh:
				logger.Error("informer error", "error", err)
			}
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down event controller")
	close(informerLife)
}

func validateRBAC(ctx context.Context, sar authv1.SelfSubjectAccessReviewInterface, group, version, resource string) bool {
	resp, err := sar.Create(ctx, &authorizationv1.SelfSubjectAccessReview{
		Spec: authorizationv1.SelfSubjectAccessReviewSpec{
			ResourceAttributes: &authorizationv1.ResourceAttributes{
				Group:    group,
				Version:  version,
				Resource: resource,
				Verb:     "list",
			},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		logger.Error("RBAC check failed", "resource", resource, "error", err)
		return false
	}
	return resp.Status.Allowed
}

func watchCRDs(ctx context.Context, extClient clientset.Interface, cfg *rest.Config, informerLife chan struct{}, msgCh chan informers.Message, errCh chan error) {
	watcher, err := extClient.ApiextensionsV1().CustomResourceDefinitions().Watch(ctx, metav1.ListOptions{})
	if err != nil {
		logger.Error("failed to start CRD watcher", "error", err)
		return
	}
	defer watcher.Stop()

	k8sClient, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		logger.Error("failed to create kubernetes clientset for watcher", "error", err)
		return
	}

	seen := make(map[string]bool)
	sar := k8sClient.AuthorizationV1().SelfSubjectAccessReviews()

	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-watcher.ResultChan():
			if !ok {
				logger.Warn("CRD watcher channel closed, restarting")
				go watchCRDs(ctx, extClient, cfg, informerLife, msgCh, errCh)
				return
			}

			crd, ok := event.Object.(*apiextensionsv1.CustomResourceDefinition)
			if !ok || crd.Spec.Names.Kind != eventActionKind {
				continue
			}

			key := fmt.Sprintf("%s/%s", crd.Spec.Group, crd.Name)
			if seen[key] {
				continue
			}

			for _, v := range crd.Spec.Versions {
				if !validateRBAC(ctx, sar, crd.Spec.Group, v.Name, crd.Spec.Names.Plural) {
					continue
				}

				informers.RegisterCRD(crd.Spec.Names.Kind, crd.Spec.Group, v.Name, crd.Spec.Names.Plural)
				logger.Info("registered new CRD",
					"kind", crd.Spec.Names.Kind,
					"group", crd.Spec.Group,
					"version", v.Name,
				)

				go informers.CreateAndRun(crd.Spec.Names.Kind, "create", "", "", crd.Name, msgCh, errCh, informerLife)
				seen[key] = true
			}
		}
	}
}
