package dlstream

import (
	"context"
	"crypto/rand"
	"crypto/sha1"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const (
	fileSize    = 4 * 1337 * 1337
	interruptAt = 1999999
)

func TestDownloadStreamInterrupted(t *testing.T) {
	_, hash, cleanup := serveInterruptedTestFile(t, fileSize, interruptAt, 1337)
	defer cleanup()

	hasherStream := sha1.New()

	dlPath := filepath.Join(os.TempDir(), "dl-interrupt-test")
	_ = os.Remove(dlPath) // Remove if it already exists
	defer os.Remove(dlPath)

	options := DefaultOptions()
	options.Logger = &testLogger{t}
	options.RetryWaitMultiplier = 0.8 // Speed up the test

	err := DownloadStreamOpts(context.Background(), "http://127.0.0.1:1337/random.rnd", dlPath, hasherStream, options)
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

	hasherFile := sha1.New()
	_, err = io.Copy(hasherFile, file)
	assert.NoError(t, err)

	assert.Equal(t, hash, hasherFile.Sum(nil))
}

func serveInterruptedTestFile(t *testing.T, fileSize, interruptAt int64, port int) (filePath string, sha1Hash []byte, cleanup func()) {
	rndFile, err := ioutil.TempFile(os.TempDir(), "random_file_*.rnd")
	assert.NoError(t, err)
	filePath = rndFile.Name()

	hasher := sha1.New()
	_, err = io.Copy(io.MultiWriter(hasher, rndFile), io.LimitReader(rand.Reader, fileSize))
	assert.NoError(t, err)
	assert.NoError(t, rndFile.Close())

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
		log.Printf("Serving random interrupted file (size: %d, interuptAt: %d), Range: %s", fileSize, interruptAt, request.Header.Get(rangeHeader))
		// https://stackoverflow.com/questions/27187617/how-would-i-limit-upload-and-download-speed-from-the-server-in-golang

		http.ServeFile(&interruptibleHTTPWriter{
			ResponseWriter: writer,
			writer:         writer,
			interruptAt:    interruptAt,
		}, request, filePath)

	})

	go func() {
		log.Printf("Starting http server on port %d", port)
		_ = http.ListenAndServe(fmt.Sprintf(":%d", port), mux)
	}()

	time.Sleep(time.Second / 3) // Wait for HTTP server

	return filePath, hasher.Sum(nil), func() {
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
	if w.written > w.interruptAt {
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

func (tl *testLogger) Printf(format string, args ...interface{}) {
	log.Printf(format, args...)
	//tl.Logf(format, args...)
}
