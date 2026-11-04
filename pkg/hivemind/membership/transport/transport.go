// Package transport is the network port the membership runtime sends and
// receives through. It carries bytes in two classes of delivery, whatever
// carries them underneath, connections or datagrams: packets, the small
// protocol messages, which may be lost, duplicated, late or out of order;
// and streams, each one larger message, for whole views, which may fail as
// well.
package transport

// Message is a packet or a stream's message that arrived. From is the
// address the sender listens at, where answers go: a transport over
// connections whose source addresses differ from their listen addresses
// must carry the listen address with each message.
//
// Peer is the member ID the transport proved the sender to be, as by its
// certificate on an mTLS connection, or empty from a transport that proves
// none. A node drops a message whose sender entry names another member.
// The runtime drops one with no Peer unless told its transport proves
// nobody. See package hivemind's doc for what the protocol trusts and
// what it doesn't.
type Message struct {
	From   string
	Peer   string
	Data   []byte
	Stream bool
}

// Dest is where a message goes: Addr, and Peer, the member the node means
// it for. An authenticating transport must hand it only to an other end
// that proves it is Peer, as by its certificate, and drop it otherwise, as
// a lost packet, as it drops one with no Peer. A transport that proves no
// identity hands it to whoever is at Addr.
type Dest struct {
	Addr string
	Peer string
}

// Transport is one node's network endpoint. Sending never blocks: a
// message that can't go is dropped, as a lost packet is. A send takes a
// copy of b: the caller may reuse b once it returns.
type Transport interface {
	// Addr is the address other nodes reach this one at.
	Addr() string
	SendPacket(to Dest, b []byte)
	SendStream(to Dest, b []byte)
	// Receive delivers what arrives. The transport closes the channel at
	// Close, or when it can receive no more; the runtime then stops
	// receiving.
	Receive() <-chan Message
	Close() error
}
