package event

type Event struct {
	Kind      string `json:"kind"`
	Action    string `json:"action"`
	Label     string `json:"label,omitempty"`
	Namespace string `json:"namespace,omitempty"`
	Key       string
	Name      string
}
