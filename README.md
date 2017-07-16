# ccmp1

This is an implementation of the gossip-style fault detector in [this paper](https://www.cs.cornell.edu/home/rvr/papers/GossipFD.pdf)

Each member maintains a list with for each known member its address and an integer which
is going to be used for failure detection. We call the integer the heartbeat counter. Every Tgossip
seconds, each member increments its own heartbeat counter, and selects one other member at
random to send its list to. Upon receipt of such a gossip message, a member merges the list in the
message with its own list, and adopts the maximum heartbeat counter for each member.

Each member occasionally broadcasts its list in order to be located initially and also to recover
from network partitions (see Section 5). In the absence of a broadcast capability, the network could
be equiped with a few gossip servers, that differ from other members only in that they are hosted
at well known addresses, and placed so that they are unlikely to be unavailable due to network
partitioning.

Each member also maintains, for each other member in the list, the last time that its corresponding
heartbeat counter has increased. If the heartbeat counter has not increased for more
than Tfail seconds, then the member is considered failed. Tfail is selected so that the probability
that anybody makes an erroneous failure detection is less than some small threshold Pmistake.

After a member is considered faulty, it cannot immediately be forgotten about. The problem
is that not all members will detect failures at the same time, and thus a member A may receive a
gossip about another member B that A has previously detected as faulty. If A had forgotten about
B, it would reinstall B in its membership list, since A would think that it was seeing B’s heartbeat
for the first time. A would continue to gossip this information on to other members, and, in effect,
the faulty member B never quite disappears from the membership.

Therefore, the failure detector does not remove a member from its membership list until after
Tcleanup seconds (Tcleanup ≥ Tfail). Tcleanup is chosen so that the probability that a gossip is received
about this member, after it has been detected as faulty, is less than some small threshold Pcleanup.
We can make Pcleanup equal to Pfail by setting Tcleanup to 2 × Tfail.

Importantly, when a node deems an entry failed and a heartbeat from the corresponding server reaches the node, the entry should not be deemed failed anymore.
