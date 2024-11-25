package cluster

import (
	"github.com/silentiumNoxe/goripple/sm"
	"log/slog"
	"sync"
	"time"
)

type Config struct {
	ClusterId byte
	ReplicaId uint32
	Out       Outcome
	DB        sm.MessageDB
	WaitTimeout time.Duration

	Addr string

	Peers     map[uint32]string
	Port      string
	WaitGroup *sync.WaitGroup
	Logger    *slog.Logger
}
