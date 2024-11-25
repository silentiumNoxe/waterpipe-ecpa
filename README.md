# Waterpipe Echo Consensus Protocol Algorithm

Network with dynamic client count and without leader synchronize data between each other.
The initiator broadcasts to all peers, each peer must reply with the same message (make the same broadcast) if he accepts the intention.
If a quorum of peers broadcasted the same message we can commit intention.
