package console

// IssueToken issues a login token, as the token socket does.
func (c *Console) IssueToken() string {
	return c.tokens.issue()
}

// Kept returns how many login tokens and sessions c holds.
func (c *Console) Kept() (tokens, sessions int) {
	c.tokens.mu.Lock()
	defer c.tokens.mu.Unlock()
	c.sessions.mu.Lock()
	defer c.sessions.mu.Unlock()
	return len(c.tokens.ends), len(c.sessions.ends)
}
