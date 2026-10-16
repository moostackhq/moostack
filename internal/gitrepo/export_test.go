package gitrepo

import "time"

// SetFetchTimeout sets the fetch timeout until restore.
func SetFetchTimeout(d time.Duration) (restore func()) {
	old := fetchTimeout
	fetchTimeout = d
	return func() { fetchTimeout = old }
}
