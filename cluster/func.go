package cluster

type Outcome func(addr string, payload []byte) error

type Income func(payload []byte) error
