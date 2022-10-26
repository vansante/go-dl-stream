package dlstream

import (
	"errors"
	"io"
	"net"
	"syscall"
)

// shouldRetryRequest analyzes a given request error and determines whether its a good idea to retry the request
func shouldRetryRequest(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}

	var netError net.Error
	if errors.As(err, &netError) && netError.Timeout() {
		return true
	}

	var netOpError *net.OpError
	if errors.As(err, &netOpError) {
		switch netOpError.Op {
		case "dial":
			return false
		case "read":
			return true
		}
	}

	var errNo *syscall.Errno
	if errors.As(err, &errNo) {
		if *errNo == syscall.ECONNREFUSED {
			// Connection refused
			return true
		}
		if *errNo == syscall.ECONNRESET {
			// Connection reset
			return true
		}
		if *errNo == syscall.ECONNABORTED {
			// Connection aborted
			return true
		}
	}

	return false
}
