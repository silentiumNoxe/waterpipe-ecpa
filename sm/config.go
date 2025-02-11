package sm

type Config struct {
	// Quorum value between 1-100 meaning a percentage of the cluster network that commit the message guarantee
	Quorum int
}
