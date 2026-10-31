// Package event defines resource triggers and outbound actions.
package event

// Event describes a resource trigger or an outbound action.
type Event struct {
	Kind      string `json:"kind"`
	Action    string `json:"action"`
	Label     string `json:"label,omitempty"`
	Namespace string `json:"namespace,omitempty"`
	Key       string
	Name      string
}
