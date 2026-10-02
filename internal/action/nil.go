package action

import "log/slog"

type Nil struct {
	Logger slog.Logger
}

func (n *Nil) Do() {
	n.Logger.Info("nil action triggered")
}

func NewNilAction(c Config) (*Nil, error) {
	return &Nil{
		Logger: c.Logger,
	}, nil
}
