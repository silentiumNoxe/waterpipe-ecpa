package trans

type Transport interface {
	Broadcast(peers []string, data []byte) error
}
