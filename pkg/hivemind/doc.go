// Package hivemind is the cluster's own coordination layer. Its membership
// is SWIM with Lifeguard's extensions: deterministic components a node
// composes, a versioned codec, and a runtime that drives a node over a
// transport.
//
// # Trust
//
// The transport decides who may talk to a node; the protocol decides what
// to believe of what they say. An authenticating transport, as mTLS on node
// identities, admits only the cluster's members and refuses removed ones,
// tells the node which member sent each message (transport.Message's
// Peer), and hands each message only to the member it is meant for
// (transport.Dest), not to whoever answers at the member's address, and
// drops a message for a seed known only by its address. The runtime drops a
// message that carries no proven peer unless it is told its transport
// proves nobody (runtime.Options.Unauthenticated). A transport that proves
// nobody, so allowed, leaves everything below open to anyone
// who can reach the node, not only to members.
//
// What a node enforces, whoever sends:
//
//   - A message's sender entry is the proven peer's own: one that names
//     another member is dropped whole, and reported (node.Impersonated).
//     A member can't pose as the sender of a message it didn't send; what
//     it relays of other members, in gossip and syncs, is another matter,
//     below.
//   - Its own entry is its own to the end: an entry saying it is suspect,
//     dead or left, at its incarnation or above, or alive at its
//     incarnation but different, makes it raise its incarnation and gossip
//     itself, which supersedes the claim wherever it went.
//   - A probe is acked only by its target or a helper it asked, and nacked
//     only by a helper it asked. As a helper, a node pings only members its
//     own view has alive, at the addresses it has for them.
//   - Bounds: incarnations within the clock in milliseconds plus 2^32, IDs
//     and addresses of at most 255 bytes, at most MaxMembers members, a
//     capped number of graves and of indirect probes for others, and
//     packets and syncs within the sizes and counts the codec reads.
//
// What a node believes without proof, from any member it admits:
//
//   - What members say of others, in gossip and syncs: that a member is
//     alive, suspect, dead or left, at any incarnation within the bound,
//     at any address. A running member refutes what is false of it once it
//     hears it; one that stopped can't, so a forged alive entry of a
//     crashed member stands until its probes fail again, and a forged dead
//     entry of a running member until the member hears of it and refutes.
//   - Who raised a suspicion. Confirmations from others shorten it, so one
//     member that names several raisers can bring a suspicion to its
//     shortest timeout, though never below it.
//   - A helper's account of its indirect probe: a helper asked can keep a
//     dead target from failing its probe, or make a live one fail.
package hivemind
