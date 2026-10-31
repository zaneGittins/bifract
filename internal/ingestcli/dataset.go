package ingestcli

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"bifract/pkg/datasetfile"
	"bifract/pkg/egress"
	"bifract/pkg/exchange"
)

// datasetPrefix marks an argument naming an Exchange dataset rather than a file.
const datasetPrefix = "dataset:"

// maxIndexBytes matches what the server reads of an index.
const maxIndexBytes = 1 << 20

// exchangeIndexURL is the index dataset:<id> resolves against: --exchange, then
// BIFRACT_EXCHANGE_URL, then the Bifract repository. custom reports an index the
// user chose, which may live inside their network.
func exchangeIndexURL(flag string) (url string, custom bool, err error) {
	url = strings.TrimSpace(flag)
	if url == "" {
		url = strings.TrimSpace(os.Getenv("BIFRACT_EXCHANGE_URL"))
	}
	switch {
	case url == "":
		return exchange.DefaultIndexURL, false, nil
	case strings.EqualFold(url, "off"):
		return "", false, fmt.Errorf("the Exchange is turned off (BIFRACT_EXCHANGE_URL=off)")
	}
	return url, true, nil
}

// datasetCacheDir keeps downloaded datasets, so a rerun skips the download.
func datasetCacheDir() (string, error) {
	base, err := os.UserCacheDir()
	if err != nil {
		return "", err
	}
	dir := filepath.Join(base, "bifract", "datasets")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", err
	}
	// Downloads cut off by a kill; a day is longer than any of them runs.
	datasetfile.RemoveStale(dir, 24*time.Hour)
	return dir, nil
}

// fetchDataset returns the path of a verified local copy of an Exchange dataset,
// and the number of logs in it, downloading it into cacheDir unless a copy there
// already matches its hash.
func fetchDataset(ctx context.Context, client *http.Client, indexURL, id, cacheDir string, out io.Writer) (string, int64, error) {
	d, name, err := findDataset(ctx, client, indexURL, id)
	if err != nil {
		return "", 0, err
	}
	path := filepath.Join(cacheDir, d.SHA256+".jsonl.zst")
	if fileMatches(path, d) {
		fmt.Fprintf(out, "Using the downloaded copy of %s\n", name)
		return path, d.Logs, nil
	}

	fmt.Fprintf(out, "Downloading %s (%s)\n", name, FormatBytes(d.Bytes))
	last := -1
	tmp, err := datasetfile.DownloadFile(ctx, client, d, cacheDir, func(n int64) {
		if pct := int(n * 100 / d.Bytes); pct != last && pct%5 == 0 {
			last = pct
			fmt.Fprintf(out, "\r  %3d%%", pct)
		}
	})
	fmt.Fprintln(out)
	if err != nil {
		return "", 0, err
	}
	// The hash pins the file; the count catches an index that lists it wrongly.
	n, err := CountLogs(tmp)
	if err == nil && n != d.Logs {
		err = fmt.Errorf("%s holds %d logs, but the Exchange lists %d", name, n, d.Logs)
	}
	if err == nil {
		err = os.Rename(tmp, path)
	}
	if err != nil {
		os.Remove(tmp)
		return "", 0, err
	}
	return path, d.Logs, nil
}

func findDataset(ctx context.Context, client *http.Client, indexURL, id string) (*exchange.Dataset, string, error) {
	resp, err := egress.Get(ctx, client, indexURL)
	if err != nil {
		return nil, "", fmt.Errorf("reading the Exchange index failed: %s", egress.Describe(err))
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxIndexBytes+1))
	if err != nil {
		return nil, "", fmt.Errorf("reading the Exchange index failed: %w", err)
	}
	if len(data) > maxIndexBytes {
		return nil, "", fmt.Errorf("the Exchange index is larger than %d bytes", maxIndexBytes)
	}
	idx, err := exchange.ReadIndex(data)
	if err != nil {
		return nil, "", err
	}
	it := idx.Find(id)
	switch {
	case it == nil:
		return nil, "", fmt.Errorf("the Exchange has no dataset %q", id)
	case it.Kind != exchange.KindDataset || it.Dataset == nil:
		return nil, "", fmt.Errorf("%q is a %s, not a dataset", id, it.Kind)
	}
	return it.Dataset, it.Name, nil
}

func fileMatches(path string, d *exchange.Dataset) bool {
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	return err == nil && n == d.Bytes && hex.EncodeToString(h.Sum(nil)) == d.SHA256
}
