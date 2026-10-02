# EventAction CRD

Install the definition:

```sh
kubectl apply -f kubernetes/crd/eventactions.yaml
```

Example resource:

```yaml
apiVersion: keda.vparat.github.com/v1alpha1
kind: EventAction
metadata:
  name: deployment-created
  namespace: default
spec:
  disabled: false
  inbound:
    kind: deployment
    action: create
    namespace: default
    label: app=my-app
  outbound:
    kind: cmd
    action: echo "deployment created"
```

The schema follows the inbound/outbound configuration example in
`kubernetes/deployments/kind.yaml`. The current controller discovers CRDs
and logs events; reading EventAction specifications and executing their
commands still requires controller implementation.
