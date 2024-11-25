package cluster

import (
	"github.com/silentiumNoxe/waterpipe-ecpa/sm"
	"log/slog"
	"sync"
)

type Config struct {
	ClusterId byte
	ReplicaId uint32
	Out       Outcome
	DB        sm.MessageDB

	Addr string

	Peers     map[uint32]string
	Port      string
	WaitGroup *sync.WaitGroup
	Logger    *slog.Logger
}
