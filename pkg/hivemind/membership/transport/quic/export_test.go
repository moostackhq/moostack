package quic

import "time"

// SetStreamReadTimeout sets how long a stream from a peer is read, and
// returns a function that sets it back.
func SetStreamReadTimeout(d time.Duration) (restore func()) {
	was := streamReadTimeout
	streamReadTimeout = d
	return func() { streamReadTimeout = was }
}

// SetSyncTimeout sets how long a sync waits for a stream and is written,
// and returns a function that sets it back.
func SetSyncTimeout(d time.Duration) (restore func()) {
	was := syncTimeout
	syncTimeout = d
	return func() { syncTimeout = was }
}

// IdleTimeout is the idle timeout t runs with.
func (t *Transport) IdleTimeout() time.Duration { return t.cfg.IdleTimeout }

// StreamSlots is how many members t keeps stream slots for.
func (t *Transport) StreamSlots() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.streams)
}

// Crash closes t's socket, then t, so that no other end hears of it.
func (t *Transport) Crash() {
	t.udp.Close()
	t.Close()
}
