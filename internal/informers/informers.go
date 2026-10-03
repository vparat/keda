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

var (
	// Map of kind to GroupVersionResource
	kindToGVR = map[string]schema.GroupVersionResource{
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

		"horizontalpodautoscaler":        {Group: "autoscaling", Version: "v1", Resource: "horizontalpodautoscalers"},
		"verticalpodautoscaler":          {Group: "autoscaling.k8s.io", Version: "v1", Resource: "verticalpodautoscalers"},
		"poddisruptionbudget":            {Group: "policy", Version: "v1", Resource: "poddisruptionbudgets"},
		"priorityclass":                  {Group: "scheduling.k8s.io", Version: "v1", Resource: "priorityclasses"},
		"storageclass":                   {Group: "storage.k8s.io", Version: "v1", Resource: "storageclasses"},
		"volumeattachment":               {Group: "storage.k8s.io", Version: "v1", Resource: "volumeattachments"},
		"csidriver":                      {Group: "storage.k8s.io", Version: "v1", Resource: "csidrivers"},
		"csinode":                        {Group: "storage.k8s.io", Version: "v1", Resource: "csinodes"},
		"runtimeclass":                   {Group: "node.k8s.io", Version: "v1", Resource: "runtimeclasses"},
		"lease":                          {Group: "coordination.k8s.io", Version: "v1", Resource: "leases"},
		"customresourcedefinition":       {Group: "apiextensions.k8s.io", Version: "v1", Resource: "customresourcedefinitions"},
		"mutatingwebhookconfiguration":   {Group: "admissionregistration.k8s.io", Version: "v1", Resource: "mutatingwebhookconfigurations"},
		"validatingwebhookconfiguration": {Group: "admissionregistration.k8s.io", Version: "v1", Resource: "validatingwebhookconfigurations"},
		"apiservice":                     {Group: "apiregistration.k8s.io", Version: "v1", Resource: "apiservices"},
		"tokenreview":                    {Group: "authentication.k8s.io", Version: "v1", Resource: "tokenreviews"},
		"selfsubjectaccessreview":        {Group: "authorization.k8s.io", Version: "v1", Resource: "selfsubjectaccessreviews"},
		"subjectaccessreview":            {Group: "authorization.k8s.io", Version: "v1", Resource: "subjectaccessreviews"},
		"localsubjectaccessreview":       {Group: "authorization.k8s.io", Version: "v1", Resource: "localsubjectaccessreviews"},
		"selfsubjectrulesreview":         {Group: "authorization.k8s.io", Version: "v1", Resource: "selfsubjectrulesreviews"},
		"ingressclass":                   {Group: "networking.k8s.io", Version: "v1", Resource: "ingressclasses"},
		"podtemplate":                    {Group: "", Version: "v1", Resource: "podtemplates"},
		"componentstatus":                {Group: "", Version: "v1", Resource: "componentstatuses"},
	}
)

func Resource(kind string) (schema.GroupVersionResource, bool) {
	gvr, ok := kindToGVR[strings.ToLower(kind)]
	return gvr, ok
}

func createInformer(kind string, i informers.SharedInformerFactory) (cache.SharedIndexInformer, error) {
	gvr, ok := kindToGVR[strings.ToLower(kind)]
	if !ok {
		return nil, fmt.Errorf("unsupported kind: %s", kind)
	}

	genericInformer, err := i.ForResource(gvr)
	if err != nil {
		return nil, fmt.Errorf("failed to create informer for %s: %w", kind, err)
	}

	return genericInformer.Informer(), nil
}

func newMessage(kind, action, name, namespace string) Message {
	return Message{
		Key:       fmt.Sprintf("%s:%s:%s:%s", kind, action, name, namespace),
		Name:      name,
		Namespace: namespace,
	}
}

// CreateAndRun starts an informer for the given kind and forwards events
// matching the action to out until informerLife is closed.
func CreateAndRun(ctx context.Context, client kubernetes.Interface, kind, action, labels, namespace, name string, out chan<- Message, errChan chan<- error) {
	factory := informers.NewSharedInformerFactoryWithOptions(client, resync, informers.WithNamespace(namespace), informers.WithTweakListOptions(func(opts *metav1.ListOptions) { opts.LabelSelector = labels }))
	si, err := createInformer(kind, factory)
	report := func(err error) {
		select {
		case errChan <- err:
		case <-ctx.Done():
		}
	}
	if err != nil {
		report(err)
		return
	}
	emit := func(obj any) {
		if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
			obj = tombstone.Obj
		}
		o, ok := obj.(metav1.Object)
		if !ok {
			return
		}
		msg := newMessage(kind, action, o.GetName(), o.GetNamespace())
		msg.Key = name
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
			if ok && nextOK && old.GetResourceVersion() != next.GetResourceVersion() {
				emit(newObj)
			}
		}
	case "delete":
		handler.DeleteFunc = emit
	default:
		report(fmt.Errorf("invalid action: %s", action))
		return
	}
	if _, err := si.AddEventHandler(handler); err != nil {
		report(err)
		return
	}
	factory.Start(ctx.Done())
	factory.WaitForCacheSync(ctx.Done())
	<-ctx.Done()
	factory.Shutdown()
}
