package cluster

import (
	"github.com/silentiumNoxe/waterpipe-ecpa/sm"
	"log/slog"
	"net"
	"sync"
)

type Config struct {
	// ClusterId separate replicas by cluster
	ClusterId byte

	// ReplicaId server id in cluster network
	ReplicaId uint32

	// Out
	// Deprecated
	Out Outcome

	// DB Database of messages in cluster
	DB sm.MessageDB

	// Addr host address
	Addr *net.UDPAddr

	// Peers members in cluster. You can pre-define member on startup
	//
	// server will ping peers with heathbeat. This action register the server in cluster for other members
	Peers map[uint32]string

	// Port
	// Deprecated
	Port string

	// WaitGroup cluster uses goroutines. Required for graceful shut down
	WaitGroup *sync.WaitGroup

	// Logger set for customization logging
	Logger *slog.Logger

	// Secret required for auth in cluster. All members must have the same secret
	Secret string
}
