package release

import "time"

// SetStallTimeout sets how long a download may go without data, for the
// test, and returns what puts it back.
func SetStallTimeout(d time.Duration) (restore func()) {
	before := stallTimeout
	stallTimeout = d
	return func() { stallTimeout = before }
}

// SetMaxFileSize sets the size a downloaded file may have, for the test,
// and returns what puts it back.
func SetMaxFileSize(n int64) (restore func()) {
	before := maxFileSize
	maxFileSize = n
	return func() { maxFileSize = before }
}

// SetWaitNotice sets how long a download waits for another before it says
// so, for the test, and returns what puts it back.
func SetWaitNotice(d time.Duration) (restore func()) {
	before := waitNotice
	waitNotice = d
	return func() { waitNotice = before }
}
