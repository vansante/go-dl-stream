package dlstream

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/pkg/errors"
)

const (
	acceptRangeHeader = "Accept-Ranges"
)

var (
	// ErrNoMoreRetries is returned when all retry attempts at downloading have failed
	ErrNoMoreRetries = errors.New("no download attempts remaining")
	// ErrInconsistentDownload is returned when the Content-Length header is not equal to the bytes received
	ErrInconsistentDownload = errors.New("inconsistent download")
)

// Logger is an optional interface used for outputting debug logging
type Logger interface {
	Printf(format string, args ...interface{})
}

// Options are a set of options used while downloading a stream
type Options struct {
	Timeout             time.Duration
	InitialHeadTimeout  time.Duration
	Retries             uint
	RetryWaitSeconds    uint
	RetryWaitMultiplier float64
	FileMode            os.FileMode
	BufferSize          uint
	Logger              Logger
}

// DefaultOptions are the default options used when no options are specified by users of the library
var DefaultOptions = Options{
	Timeout:             time.Hour,
	InitialHeadTimeout:  time.Second * 3,
	Retries:             5,
	RetryWaitSeconds:    10,
	RetryWaitMultiplier: 1.61803398875, // Bonus points to who gets it
	FileMode:            0755,
	BufferSize:          128 * 1024 * 1024,
}

// Printf checks if a logger is present and logs to it if it is
func (o *Options) Printf(format string, args ...interface{}) {
	if o.Logger == nil {
		return
	}
	o.Logger.Printf(format, args...)
}

// DownloadStream downloads the file at the given URL to the filepath, while also sending a stream
// to the given writer while retrying any broken connections.
// Consecutive calls with the same URL and filePath will attempt to resume the download.
// The download stream written to the writer will then be replayed from the beginning of the download.
// With the given context the whole operation can be aborted.
func DownloadStream(ctx context.Context, url, filePath string, writer io.Writer) (err error) {
	return DownloadStreamOpts(ctx, filePath, url, writer, &DefaultOptions)
}

// DownloadStreamOpts is the same as DownloadStream, but allows you to override the default options with own values.
// See DownloadStream for more information.
func DownloadStreamOpts(ctx context.Context, url, filePath string, writer io.Writer, options *Options) (err error) {
	contentLength, resumable, err := fetchURLInfo(url, options)
	if err != nil {
		options.Printf("DownloadStreamOpts: URL invalid: %v", err)
		return err
	}
	options.Printf("DownloadStreamOpts: Download size: %d, Resumable: %v", contentLength, resumable)

	// Truncate the file if we cannot resume the http download
	file, written, err := openFile(filePath, !resumable, options)
	if err != nil {
		options.Printf("DownloadStreamOpts: Could not open file: %v", err)
		return err
	}
	defer func() {
		closeErr := file.Close()
		if closeErr != nil {
			options.Printf("DownloadStreamOpts: Error closing file: %v", err)
		}
	}()

	if !resumable {
		options.Printf("DownloadStreamOpts: Download not resumable")
	} else {
		options.Printf("DownloadStreamOpts: Current file size: %d, resuming", written)
	}

	// If we are resuming a download, copy the existing file contents to the writer for replay
	if written > 0 {
		_, err = file.Seek(0, io.SeekStart)
		if err != nil {
			options.Printf("DownloadStreamOpts: Error seeking file to start: %v", err)
			return errors.Wrap(err, "error seeking file to start")
		}
		written, err = io.Copy(writer, file)
		if err != nil {
			options.Printf("DownloadStreamOpts: Error replaying file stream: %v", err)
			return errors.Wrap(err, "error replaying file stream")
		}
	}

	return startDownloadRetries(ctx, url, contentLength, written, file, writer, options)
}

// startDownloadRetries starts a loop that retries the download until it either finishes or the retries are depleted
func startDownloadRetries(ctx context.Context, url string, contentLength, written int64, file *os.File, writer io.Writer, options *Options) (err error) {
	buffer := make([]byte, options.BufferSize)
	waitTime := time.Duration(options.RetryWaitSeconds) * time.Second

	// Loop that retries the download
	for i := 0; i < int(options.Retries); i++ {
		options.Printf("DownloadStreamOpts: Downloading %s from offset %d, total size: %d, attempt %d", url, written, contentLength, i)

		var bodyReader io.ReadCloser
		contentLength, bodyReader, err = doDownloadRequest(ctx, url, written, options)
		if err != nil {
			options.Printf("DownloadStreamOpts: Error retrieving URL: %v", err)
			waitTime = retryWait(options, waitTime)
			continue
		}

		// Byte loop that copies from the download reader to the file and writer
		for {
			var n int
			var writerErr, fileErr error
			n, err = bodyReader.Read(buffer)
			if written > 0 {
				_, writerErr = writer.Write(buffer[:n])
				if writerErr != nil {
					// If the writer at any point returns an error, we should abort and do nothing further
					closeErr := bodyReader.Close()
					if closeErr != nil {
						options.Printf("DownloadStreamOpts: Error closing body reader: %v", err)
					}
					return writerErr // Bounce back the error
				}
				_, fileErr = file.Write(buffer[:n])
				if fileErr != nil {
					// When the file returns an error, this is also pretty fatal, so abort
					closeErr := bodyReader.Close()
					if closeErr != nil {
						options.Printf("DownloadStreamOpts: Error closing body reader: %v", err)
					}
					return errors.Wrap(err, "error writing to file")
				}
			}

			written += int64(n)

			if err == io.EOF {
				_ = bodyReader.Close()
				if written != contentLength {
					options.Printf("DownloadStreamOpts: Download done yet incomplete, total: %d, expected: %d", written, contentLength)
					return ErrInconsistentDownload
				}
				options.Printf("DownloadStreamOpts: Download complete, %d bytes", written)
				return nil // YES, we have a complete download :)
			} else if err != nil {
				_ = bodyReader.Close()
				options.Printf("DownloadStreamOpts: Error reading from response body: %v", err)
				waitTime = retryWait(options, waitTime)
				continue
			}
		}
	}

	return ErrNoMoreRetries
}

func retryWait(options *Options, currentWait time.Duration) time.Duration {
	time.Sleep(currentWait)
	return time.Duration(float64(currentWait) * options.RetryWaitMultiplier)
}

// doDownloadRequest sends an actual download request and returns the content length (again) and response body reader
func doDownloadRequest(ctx context.Context, url string, downloadFrom int64, options *Options) (contentLength int64, body io.ReadCloser, err error) {
	client := http.Client{
		Timeout: options.Timeout,
	}

	// See: https://stackoverflow.com/a/29200933/3536354
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	req = req.WithContext(ctx)

	if downloadFrom > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", downloadFrom))
	}

	resp, err := client.Do(req)
	if err != nil {
		return -1, nil, errors.Wrap(err, "error requesting url")
	}

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return -1, nil, errors.Errorf("unexpected download http status code %d", resp.StatusCode)
	}

	return resp.ContentLength, resp.Body, nil
}

// verifyDownloadURL does a HEAD request to see if the download URL is valid and returns the size of the file
// Also checks if the download can be resumed
func fetchURLInfo(url string, options *Options) (contentLength int64, resumable bool, err error) {
	client := http.Client{
		Timeout: options.InitialHeadTimeout,
	}

	ctx, cancel := context.WithTimeout(context.Background(), options.InitialHeadTimeout)
	defer cancel()

	req, err := http.NewRequest(http.MethodHead, url, nil)
	if err != nil {
		return -1, false, errors.Wrap(err, "error creating head request")
	}
	req = req.WithContext(ctx)

	resp, err := client.Do(req)
	if err != nil {
		return -1, false, errors.Wrap(err, "error requesting url")
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return -1, false, errors.Errorf("unexpected head status code %d", resp.StatusCode)
	}

	err = resp.Body.Close()
	if err != nil {
		return -1, false, errors.Wrap(err, "error closing response body")
	}

	// Resumable is only possible if we can request ranges and know how big the file is gonna be.
	resumable = resp.Header.Get(acceptRangeHeader) == "bytes" && resp.ContentLength > 0

	return resp.ContentLength, resumable, nil
}

// openFile opens the file and seeks to the end, finding out if data was already
// written to it and thus a resume should be attempted.
// If truncate is true, the file is truncated.
func openFile(filePath string, truncate bool, options *Options) (file *os.File, resumeFrom int64, err error) {
	flags := os.O_CREATE
	if truncate {
		flags |= os.O_TRUNC | os.O_WRONLY
	} else {
		flags |= os.O_APPEND | os.O_RDWR
	}

	file, err = os.OpenFile(filePath, flags, options.FileMode)
	if err != nil {
		return nil, -1, errors.Wrap(err, "error opening download file")
	}

	resumeFrom, err = file.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, -1, errors.Wrap(err, "error seeking download file")
	}
	return file, resumeFrom, nil
}
