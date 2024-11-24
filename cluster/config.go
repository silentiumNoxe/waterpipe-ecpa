package cluster

import (
	"github.com/silentiumNoxe/goripple/sm"
	"log/slog"
	"sync"
)

type Config struct {
	ClusterId byte
	ReplicaId uint32
	Out       Outcome
	DB        sm.MessageDB

	Addr string

	Peers     []string
	Port      string
	WaitGroup *sync.WaitGroup
	Logger    *slog.Logger
}
