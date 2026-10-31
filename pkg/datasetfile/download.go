package datasetfile

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"bifract/pkg/egress"
	"bifract/pkg/exchange"
)

// stallTimeout fails a download that receives nothing for this long. The whole
// download has no deadline, since a large file on a slow link takes a while.
var stallTimeout = 2 * time.Minute

var (
	// ErrDownload reports a failed request, without addresses or remote text.
	ErrDownload = errors.New("download failed")
	// ErrChecksum reports a file that does not match the size or hash the Exchange lists.
	ErrChecksum = errors.New("the downloaded file does not match the Exchange")
)

// Download writes the dataset's file to w, failing unless it has exactly the
// listed size and SHA-256. progress, when set, gets the bytes received so far.
// Nothing written to w may be used until Download returns nil.
func Download(ctx context.Context, client *http.Client, d *exchange.Dataset, w io.Writer, progress func(int64)) error {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	stall := time.AfterFunc(stallTimeout, func() {
		cancel(fmt.Errorf("no data received for %s", stallTimeout))
	})
	defer stall.Stop()

	resp, err := egress.Get(ctx, client, d.URL)
	if err != nil {
		return fmt.Errorf("%w: %s", ErrDownload, describe(ctx, err))
	}
	defer resp.Body.Close()

	var got atomic.Int64
	h := sha256.New()
	body := &progressReader{r: io.LimitReader(resp.Body, d.Bytes+1), onRead: func(n int) {
		stall.Reset(stallTimeout)
		total := got.Add(int64(n))
		if progress != nil {
			progress(total)
		}
	}}
	if _, err := io.Copy(io.MultiWriter(w, h), body); err != nil {
		return fmt.Errorf("%w: %s", ErrDownload, describe(ctx, err))
	}
	if n := got.Load(); n != d.Bytes {
		return fmt.Errorf("%w: it is %d bytes, not %d", ErrChecksum, n, d.Bytes)
	}
	if sum := hex.EncodeToString(h.Sum(nil)); sum != d.SHA256 {
		return fmt.Errorf("%w: its SHA-256 is %s, not %s", ErrChecksum, sum, d.SHA256)
	}
	return nil
}

// describe says why a request failed without echoing addresses or remote text,
// naming a stall rather than a bare cancellation.
func describe(ctx context.Context, err error) string {
	if cause := context.Cause(ctx); cause != nil && !errors.Is(cause, context.Canceled) {
		return cause.Error()
	}
	return egress.Describe(err)
}

type progressReader struct {
	r      io.Reader
	onRead func(int)
}

func (p *progressReader) Read(b []byte) (int, error) {
	n, err := p.r.Read(b)
	if n > 0 {
		p.onRead(n)
	}
	return n, err
}

// tempPattern names a download until it is verified.
const tempPattern = "bifract-dataset-*.jsonl.zst"

// DownloadFile downloads the dataset into a new file in dir and returns its path.
// The file is removed if the download fails or does not verify.
func DownloadFile(ctx context.Context, client *http.Client, d *exchange.Dataset, dir string, progress func(int64)) (string, error) {
	f, err := os.CreateTemp(dir, tempPattern)
	if err != nil {
		return "", fmt.Errorf("create download file: %w", err)
	}
	err = Download(ctx, client, d, f, progress)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		os.Remove(f.Name())
		return "", err
	}
	return f.Name(), nil
}

// RemoveStale deletes downloads in dir last written more than age ago, left by a
// process that stopped before it could remove them.
func RemoveStale(dir string, age time.Duration) {
	paths, _ := filepath.Glob(filepath.Join(dir, tempPattern))
	for _, p := range paths {
		if info, err := os.Stat(p); err == nil && time.Since(info.ModTime()) > age {
			os.Remove(p)
		}
	}
}
