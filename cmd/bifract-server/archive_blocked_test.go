package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"bifract/pkg/objstore"
)

func TestArchiveReadBlockedReason(t *testing.T) {
	tests := []struct {
		name    string
		obj     objstore.Config
		managed bool
		blocked bool
	}{
		// ClickHouse cannot read a pod-local directory, so restore and recall
		// must say so rather than queue jobs nothing will run.
		{"disk", objstore.Config{Backend: objstore.BackendDisk}, false, true},
		{"unset", objstore.Config{}, false, true},
		{"minio, self-hosted clickhouse", objstore.Config{Backend: objstore.BackendMinIO, S3Endpoint: "http://minio:9000"}, false, false},
		{"minio, managed clickhouse", objstore.Config{Backend: objstore.BackendMinIO, S3Endpoint: "http://minio:9000"}, true, true},
		{"aws s3, managed clickhouse", objstore.Config{Backend: objstore.BackendS3}, true, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := archiveReadBlockedReason(tc.obj, tc.managed); (got != "") != tc.blocked {
				t.Errorf("reason = %q, want blocked=%v", got, tc.blocked)
			}
		})
	}
}

// A restore or recall the archive cannot serve is refused with the reason,
// instead of queueing a job no worker will ever claim.
func TestArchiveJobsRefusedWhenReadBlocked(t *testing.T) {
	prev := archiveCHReadBlocked
	archiveCHReadBlocked = "the disk backend cannot be read by ClickHouse"
	t.Cleanup(func() { archiveCHReadBlocked = prev })

	for name, h := range map[string]http.HandlerFunc{
		"restore": routerDeps{}.handleCreateRestore,
		"recall":  routerDeps{}.handleCreateRecall,
	} {
		t.Run(name, func(t *testing.T) {
			w := httptest.NewRecorder()
			h(w, httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}")))
			if w.Code != http.StatusConflict || !strings.Contains(w.Body.String(), "disk backend") {
				t.Errorf("got %d %s, want 409 naming the reason", w.Code, w.Body.String())
			}
		})
	}
}

// The admin setting wins once saved; until then BIFRACT_ARCHIVE_ENABLED does,
// matching the ingest tee and the archiver. Reading the setting alone reported
// an env-enabled archive as off and let the clear guards run mid-write.
func TestResolveArchiveEnabled(t *testing.T) {
	tests := []struct {
		setting string
		seed    bool
		want    bool
	}{
		{"", true, true},
		{"", false, false},
		{"false", true, false},
		{"true", false, true},
	}
	for _, tc := range tests {
		if got := resolveArchiveEnabled(tc.setting, tc.seed); got != tc.want {
			t.Errorf("resolveArchiveEnabled(%q, %v) = %v, want %v", tc.setting, tc.seed, got, tc.want)
		}
	}
}
