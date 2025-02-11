package cluster

import (
	"context"
	"encoding/hex"
	"fmt"
	"github.com/silentiumNoxe/waterpipe-ecpa/sm"
	"log/slog"
	"sync"
)

var pool = map[byte]*Cluster{}

func New(cfg *Config) (*Cluster, error) {
	if _, ok := pool[cfg.ClusterId]; ok {
		return nil, fmt.Errorf("cluster already exists")
	}

	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	s := sm.New(cfg.DB, cfg.Logger)
	for id, addr := range cfg.Peers {
		s.AddPeer(id, addr)
	}

	if cfg.WaitGroup == nil {
		cfg.WaitGroup = &sync.WaitGroup{}
	}

	secret, err := hex.DecodeString(cfg.Secret)
	if err != nil {
		panic(fmt.Errorf("Failed to decode secret: %w ", err))
	}

	var c = &Cluster{
		clusterId: cfg.ClusterId,
		replicaId: cfg.ReplicaId,
		addr:      cfg.Addr,
		state:     s,
		wg:        cfg.WaitGroup,
		log:       cfg.Logger,
		secret:    secret,
	}

	pool[cfg.ClusterId] = c

	return c, nil
}

func SyncStore(ctx context.Context, clusterId byte, message []byte) (uint32, error) {
	c := pool[clusterId]
	if c == nil {
		return 0, fmt.Errorf("cluster not found")
	}

	return c.SyncStore(ctx, message)
}

func AsyncStore(clusterId byte, message []byte) (uint32, error) {
	c := pool[clusterId]
	if c == nil {
		return 0, fmt.Errorf("cluster not found")
	}

	return c.AsyncStore(message)
}
