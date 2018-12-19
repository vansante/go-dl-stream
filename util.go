package dlstream

import (
	"net"
	"syscall"
)

// shouldRetryRequest analyzes a given request error and determines whether its a good idea to retry the request
func shouldRetryRequest(err error) (shouldRetry bool) {
	netErr, ok := err.(net.Error)
	if ok {
		return netErr.Temporary() || netErr.Timeout()
	}

	switch t := err.(type) {
	case *net.OpError:
		if t.Op == "dial" {
			// Unknown host
			return false
		}
		if t.Op == "read" {
			// Connection refused
			return true
		}

	case syscall.Errno:
		if t == syscall.ECONNREFUSED {
			// Connection refused
			return true
		}
		if t == syscall.ECONNRESET {
			// Connection reset
			return true
		}
		if t == syscall.ECONNABORTED {
			// Connection aborted
			return true
		}
	}

	return false
}
