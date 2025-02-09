package cluster

import (
	"log/slog"
	"net"
	"sync"
	"waterpipe-ecpa/sm"
)

type Config struct {
	ClusterId byte
	ReplicaId uint32
	Out       Outcome
	DB        sm.MessageDB
	Addr      *net.UDPAddr

	Peers     map[uint32]string
	Port      string
	WaitGroup *sync.WaitGroup
	Logger    *slog.Logger

	Secret string
}
