// Package informers watches Kubernetes resources and emits matching events.
package informers

import (
	"context"
	"fmt"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

const resync = 10 * time.Minute


// Message is emitted when an informer observes a change.
type Message struct {
	Key       string
	Name      string
	Namespace string
}

// kindToGVR maps supported resource kinds to their API resources.
var kindToGVR = map[string]schema.GroupVersionResource{
	"pod":                   {Group: "", Version: "v1", Resource: "pods"},
	"secret":                {Group: "", Version: "v1", Resource: "secrets"},
	"service":               {Group: "", Version: "v1", Resource: "services"},
	"configmap":             {Group: "", Version: "v1", Resource: "configmaps"},
	"node":                  {Group: "", Version: "v1", Resource: "nodes"},
	"namespace":             {Group: "", Version: "v1", Resource: "namespaces"},
	"persistentvolumeclaim": {Group: "", Version: "v1", Resource: "persistentvolumeclaims"},
	"persistentvolume":      {Group: "", Version: "v1", Resource: "persistentvolumes"},
	"endpoints":             {Group: "", Version: "v1", Resource: "endpoints"},
	"event":                 {Group: "", Version: "v1", Resource: "events"},
	"limitrange":            {Group: "", Version: "v1", Resource: "limitranges"},
	"resourcequota":         {Group: "", Version: "v1", Resource: "resourcequotas"},
	"replicationcontroller": {Group: "", Version: "v1", Resource: "replicationcontrollers"},
	"serviceaccount":        {Group: "", Version: "v1", Resource: "serviceaccounts"},

	"deployment":         {Group: "apps", Version: "v1", Resource: "deployments"},
	"statefulset":        {Group: "apps", Version: "v1", Resource: "statefulsets"},
	"daemonset":          {Group: "apps", Version: "v1", Resource: "daemonsets"},
	"replicaset":         {Group: "apps", Version: "v1", Resource: "replicasets"},
	"controllerrevision": {Group: "apps", Version: "v1", Resource: "controllerrevisions"},

	"ingress":       {Group: "networking.k8s.io", Version: "v1", Resource: "ingresses"},
	"networkpolicy": {Group: "networking.k8s.io", Version: "v1", Resource: "networkpolicies"},
	"endpointslice": {Group: "discovery.k8s.io", Version: "v1", Resource: "endpointslices"},

	"job":     {Group: "batch", Version: "v1", Resource: "jobs"},
	"cronjob": {Group: "batch", Version: "v1", Resource: "cronjobs"},

	"role":               {Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"},
	"clusterrole":        {Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"},
	"rolebinding":        {Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"},
	"clusterrolebinding": {Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"},

	"horizontalpodautoscaler": {Group: "autoscaling", Version: "v1", Resource: "horizontalpodautoscalers"},
	"poddisruptionbudget":     {Group: "policy", Version: "v1", Resource: "poddisruptionbudgets"},
	"priorityclass":           {Group: "scheduling.k8s.io", Version: "v1", Resource: "priorityclasses"},
	"storageclass":            {Group: "storage.k8s.io", Version: "v1", Resource: "storageclasses"},
	"volumeattachment":        {Group: "storage.k8s.io", Version: "v1", Resource: "volumeattachments"},
	"csidriver":               {Group: "storage.k8s.io", Version: "v1", Resource: "csidrivers"},
	"csinode":                 {Group: "storage.k8s.io", Version: "v1", Resource: "csinodes"},
	"runtimeclass":            {Group: "node.k8s.io", Version: "v1", Resource: "runtimeclasses"},
	"lease":                   {Group: "coordination.k8s.io", Version: "v1", Resource: "leases"},
	"mutatingwebhookconfiguration": {
		Group:    "admissionregistration.k8s.io",
		Version:  "v1",
		Resource: "mutatingwebhookconfigurations",
	},
	"validatingwebhookconfiguration": {
		Group:    "admissionregistration.k8s.io",
		Version:  "v1",
		Resource: "validatingwebhookconfigurations",
	},
	"ingressclass":    {Group: "networking.k8s.io", Version: "v1", Resource: "ingressclasses"},
	"podtemplate":     {Group: "", Version: "v1", Resource: "podtemplates"},
	"componentstatus": {Group: "", Version: "v1", Resource: "componentstatuses"},
}


// Resource resolves a case-insensitive resource kind to its API resource.
func Resource(kind string) (schema.GroupVersionResource, bool) {
	gvr, ok := kindToGVR[strings.ToLower(kind)]

	return gvr, ok
}


// createInformer resolves kind through the factory so API resources share its
// namespace, selector, and resync settings.
func createInformer(kind string, factory informers.SharedInformerFactory) (cache.SharedIndexInformer, error) {
	gvr, ok := Resource(kind)

	if !ok {

		return nil, fmt.Errorf("unsupported kind: %q", kind)
	}

	genericInformer, err := factory.ForResource(gvr)

	if err != nil {

		return nil, fmt.Errorf("failed to create informer for %q: %w", kind, err)
	}

	return genericInformer.Informer(), nil
}


// CreateAndRun starts an informer for the given kind and forwards events
// matching the action to out until ctx is canceled.
func CreateAndRun(
	ctx context.Context,
	client kubernetes.Interface,
	kind string,
	action string,
	labels string,
	namespace string,
	name string,
	out chan<- Message,
) error {
	factory := informers.NewSharedInformerFactoryWithOptions(
		client,
		resync,
		informers.WithNamespace(namespace),
		informers.WithTweakListOptions(func(opts *metav1.ListOptions) {
			opts.LabelSelector = labels
		}),
	)
	informer, err := createInformer(kind, factory)

	if err != nil {

		return err
	}

	emit := func(obj any) {

		if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
			obj = tombstone.Obj
		}

		object, ok := obj.(metav1.Object)

		if !ok {

			return
		}

		msg := Message{Key: name, Name: object.GetName(), Namespace: object.GetNamespace()}
		select {
		case out <- msg:
		case <-ctx.Done():
		}
	}
	handler := cache.ResourceEventHandlerFuncs{}

	switch strings.ToLower(action) {
	case "create":
		handler.AddFunc = emit
	case "update":
		handler.UpdateFunc = func(oldObj, newObj any) {
			old, ok := oldObj.(metav1.Object)
			next, nextOK := newObj.(metav1.Object)

			if !ok || !nextOK {

				return
			}

			if old.GetResourceVersion() != next.GetResourceVersion() {
				emit(newObj)
			}

		}
	case "delete":
		handler.DeleteFunc = emit
	default:
		return fmt.Errorf("invalid action: %q", action)
	}

	if _, err := informer.AddEventHandler(handler); err != nil {

		return fmt.Errorf("register %q handler for %q: %w", action, kind, err)
	}

	informer.Run(ctx.Done())

	return nil
}
