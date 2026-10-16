package sshclient

import (
	"context"
	"errors"
	"sync"
)

// Pool keeps one connection per node for an operation. Its clients
// reconnect once when their connection drops, and Close closes them all.
type Pool struct {
	opts Options

	mu      sync.Mutex
	entries map[string]*poolEntry
	closed  bool
}

// poolEntry is a node's connection, which is ready once done is closed.
type poolEntry struct {
	done   chan struct{}
	client *Client
	err    error
	// abandoned says the connection failed because the context of the
	// call that made it ended.
	abandoned bool
}

// NewPool returns an empty pool that connects with opts.
func NewPool(opts Options) *Pool {
	return &Pool{opts: opts, entries: map[string]*poolEntry{}}
}

// Client returns the connection to n, connecting on first use. Nodes are
// told apart by their targets alone, so a node's host keys must be the
// same on every call. A failed connection is tried again on the next
// call. The caller doesn't close the client; Close does.
func (p *Pool) Client(ctx context.Context, n Node) (*Client, error) {
	for {
		e, err := p.entry(ctx, n)
		if err != nil {
			return nil, err
		}
		// The caller who connected may have given up, and this one hasn't.
		if !e.abandoned || ctx.Err() != nil {
			return e.client, e.err
		}
	}
}

// entry returns n's entry once it is ready, connecting if no other call
// is.
func (p *Pool) entry(ctx context.Context, n Node) (*poolEntry, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, errClosed
	}
	e, ok := p.entries[n.Target]
	if !ok {
		e = &poolEntry{done: make(chan struct{})}
		p.entries[n.Target] = e
	}
	p.mu.Unlock()

	if !ok {
		e.client, e.err = Dial(ctx, n, p.opts)
		e.abandoned = e.err != nil && ctx.Err() != nil
		p.mu.Lock()
		if e.err != nil {
			delete(p.entries, n.Target)
		} else if p.closed {
			e.client.Close()
			e.client, e.err = nil, errClosed
		}
		// Under the lock, so that Close either sees the entry ready or
		// leaves it to this call, which then sees the pool closed.
		close(e.done)
		p.mu.Unlock()
	}
	select {
	case <-e.done:
		return e, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Close closes every connection in the pool. The pool can't be used
// afterwards. A connection still being made is closed when it is made,
// without Close waiting for it.
func (p *Pool) Close() error {
	p.mu.Lock()
	p.closed = true
	entries := p.entries
	p.entries = nil
	p.mu.Unlock()
	var errs []error
	for _, e := range entries {
		select {
		case <-e.done:
			if e.client != nil {
				errs = append(errs, e.client.Close())
			}
		default:
			// entry closes it once Dial returns, as the pool is closed.
		}
	}
	return errors.Join(errs...)
}
