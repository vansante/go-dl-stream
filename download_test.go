package dlstream

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const (
	fileSize    = 5 * 1337 * 1337
	interruptAt = 1999999
)

func TestDownloadStreamNormal(t *testing.T) {
	serverURL, hash, cleanup := serveInterruptedTestFile(t, fileSize, 0)
	defer cleanup()

	hasherStream := sha256.New()

	dlPath := filepath.Join(os.TempDir(), "dl-normal-test")
	_ = os.Remove(dlPath) // Remove if it already exists
	defer os.Remove(dlPath)

	options := DefaultOptions()
	options.Logger = &testLogger{t}
	// Speed up the test
	options.RetryWait = time.Millisecond * 200
	options.RetryWaitMultiplier = 1

	err := DownloadStreamOpts(context.Background(), serverURL+"/random.rnd", dlPath, hasherStream, options)
	assert.NoError(t, err)

	assert.Equal(t, hash, hasherStream.Sum(nil))

	file, err := os.Open(dlPath)
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, file.Close())
	}()

	fi, err := file.Stat()
	assert.NoError(t, err)
	assert.EqualValues(t, fileSize, fi.Size())

	hasherFile := sha256.New()
	_, err = io.Copy(hasherFile, file)
	assert.NoError(t, err)

	assert.Equal(t, hash, hasherFile.Sum(nil))
}

func TestDownloadStreamInterrupted(t *testing.T) {
	serverURL, hash, cleanup := serveInterruptedTestFile(t, fileSize, interruptAt)
	defer cleanup()

	hasherStream := sha256.New()

	dlPath := filepath.Join(os.TempDir(), "dl-interrupt-test")
	_ = os.Remove(dlPath) // Remove if it already exists
	defer os.Remove(dlPath)

	options := DefaultOptions()
	options.Logger = &testLogger{t}
	// Speed up the test
	options.RetryWait = time.Millisecond * 200
	options.RetryWaitMultiplier = 1

	err := DownloadStreamOpts(context.Background(), serverURL+"/random.rnd", dlPath, hasherStream, options)
	assert.NoError(t, err)

	assert.Equal(t, hash, hasherStream.Sum(nil))

	file, err := os.Open(dlPath)
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, file.Close())
	}()

	fi, err := file.Stat()
	assert.NoError(t, err)
	assert.EqualValues(t, fileSize, fi.Size())

	hasherFile := sha256.New()
	_, err = io.Copy(hasherFile, file)
	assert.NoError(t, err)

	assert.Equal(t, hash, hasherFile.Sum(nil))
}

func TestDownloadStreamManualResume(t *testing.T) {
	serverURL, hash, cleanup := serveInterruptedTestFile(t, fileSize, interruptAt)
	defer cleanup()

	hasherStream := sha256.New()

	dlPath := filepath.Join(os.TempDir(), "dl-manual-resume-test")
	_ = os.Remove(dlPath) // Remove if it already exists
	defer os.Remove(dlPath)

	options := DefaultOptions()
	options.Logger = &testLogger{t}
	// Speed up the test
	options.RetryWait = time.Millisecond * 200
	options.RetryWaitMultiplier = 1
	options.Retries = 2

	err := DownloadStreamOpts(context.Background(), serverURL+"/random.rnd", dlPath, io.Discard, options)
	assert.Error(t, err)
	assert.EqualValues(t, ErrNoMoreRetries, err)
	assert.EqualValues(t, options.RetryWait, time.Millisecond*200)
	assert.EqualValues(t, options.Retries, 2)

	err = DownloadStreamOpts(context.Background(), serverURL+"/random.rnd", dlPath, io.Discard, options)
	assert.Error(t, err)
	assert.EqualValues(t, ErrNoMoreRetries, err)
	assert.EqualValues(t, options.RetryWait, time.Millisecond*200)
	assert.EqualValues(t, options.Retries, 2)

	err = DownloadStreamOpts(context.Background(), serverURL+"/random.rnd", dlPath, hasherStream, options)
	assert.NoError(t, err)

	assert.Equal(t, hash, hasherStream.Sum(nil))

	file, err := os.Open(dlPath)
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, file.Close())
	}()

	fi, err := file.Stat()
	assert.NoError(t, err)
	assert.EqualValues(t, fileSize, fi.Size())

	hasherFile := sha256.New()
	_, err = io.Copy(hasherFile, file)
	assert.NoError(t, err)

	assert.Equal(t, hash, hasherFile.Sum(nil))
}

func TestDownloadNonExistingServer(t *testing.T) {
	dlPath := filepath.Join(os.TempDir(), "dl-manual-resume-test")
	_ = os.Remove(dlPath) // Remove if it already exists
	defer os.Remove(dlPath)

	options := DefaultOptions()
	options.Logger = &testLogger{t}

	err := DownloadStreamOpts(context.Background(), "http://127.0.0.1:1337/random.rnd", dlPath, io.Discard, options)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "connection refused")
}

func serveInterruptedTestFile(t *testing.T, fileSize, interruptAt int64) (serverURL string, sha1Hash []byte, cleanup func()) {
	rndFile, err := os.CreateTemp(os.TempDir(), "random_file_*.rnd")
	assert.NoError(t, err)
	filePath := rndFile.Name()

	hasher := sha256.New()
	_, err = io.Copy(io.MultiWriter(hasher, rndFile), io.LimitReader(rand.Reader, fileSize))
	assert.NoError(t, err)
	assert.NoError(t, rndFile.Close())

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
		log.Printf("Serving random interrupted file (size: %d, interuptAt: %d), Range: %s", fileSize, interruptAt, request.Header.Get(rangeHeader))

		http.ServeFile(&interruptibleHTTPWriter{
			ResponseWriter: writer,
			writer:         writer,
			interruptAt:    interruptAt,
		}, request, filePath)

	})
	server := httptest.NewServer(mux)

	return server.URL, hasher.Sum(nil), func() {
		_ = os.Remove(filePath)
	}
}

type interruptibleHTTPWriter struct {
	http.ResponseWriter

	writer      io.Writer
	written     int64
	interruptAt int64
	mu          sync.Mutex
}

// Write interrupts after writing a certain size
func (w *interruptibleHTTPWriter) Write(buf []byte) (n int, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.written += int64(len(buf))
	if w.interruptAt > 0 && w.written > w.interruptAt {
		offset := len(buf) - int(w.written-w.interruptAt)
		n, err = w.writer.Write(buf[:offset])
		if err != nil {
			log.Printf("Error writing response: %v", err)
			return n, err
		}
		log.Printf("Interrupting download at %d bytes", w.interruptAt)
		return n, fmt.Errorf("interrupt size (%d bytes) reached", w.interruptAt)
	}
	return w.writer.Write(buf)
}

type testLogger struct {
	*testing.T
}

func (tl *testLogger) Infof(format string, args ...interface{}) {
	log.Printf("[INFO] "+format, args...)
	//tl.Logf("[INFO] "+format, args...)
}

func (tl *testLogger) Errorf(format string, args ...interface{}) {
	log.Printf("[ERROR] "+format, args...)
	//tl.Logf("[ERROR] "+format, args...)
}
