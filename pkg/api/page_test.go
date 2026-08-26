package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestPageParams(t *testing.T) {
	cases := []struct {
		query              string
		defLimit, maxLimit int
		wantLimit          int
		wantOffset         int
	}{
		{"", 50, 100, 50, 0},
		{"?limit=10&offset=20", 50, 100, 10, 20},
		{"?limit=999", 50, 100, 100, 0},
		{"?limit=0", 50, 100, 50, 0},
		{"?limit=-5", 50, 100, 50, 0},
		{"?offset=-5", 50, 100, 50, 0},
		{"?limit=abc", 50, 100, 50, 0},
	}
	for _, tc := range cases {
		r := httptest.NewRequest(http.MethodGet, "/x"+tc.query, nil)
		limit, offset := PageParams(r, tc.defLimit, tc.maxLimit)
		if limit != tc.wantLimit || offset != tc.wantOffset {
			t.Errorf("PageParams(%q) = (%d, %d), want (%d, %d)",
				tc.query, limit, offset, tc.wantLimit, tc.wantOffset)
		}
	}
}

func TestWritePageShape(t *testing.T) {
	rec := httptest.NewRecorder()
	WritePage(rec, []string{"a"}, Page{Total: 42, Limit: 10, Offset: 20})

	want := `{"success":true,"data":["a"],"page":{"total":42,"limit":10,"offset":20}}` + "\n"
	if rec.Body.String() != want {
		t.Errorf("body = %q, want %q", rec.Body.String(), want)
	}
}

func TestSlice(t *testing.T) {
	items := []int{0, 1, 2, 3, 4}
	cases := []struct {
		limit, offset int
		want          []int
	}{
		{2, 0, []int{0, 1}},
		{2, 3, []int{3, 4}},
		{2, 4, []int{4}},
		{2, 5, []int{}},
		{2, 99, []int{}},
		{99, 0, []int{0, 1, 2, 3, 4}},
		{0, 0, []int{0, 1, 2, 3, 4}},
	}
	for _, tc := range cases {
		got, page := Slice(items, tc.limit, tc.offset)
		if len(got) != len(tc.want) {
			t.Errorf("Slice(limit=%d, offset=%d) = %v, want %v", tc.limit, tc.offset, got, tc.want)
			continue
		}
		for i := range got {
			if got[i] != tc.want[i] {
				t.Errorf("Slice(limit=%d, offset=%d) = %v, want %v", tc.limit, tc.offset, got, tc.want)
				break
			}
		}
		if page.Total != len(items) {
			t.Errorf("Slice total = %d, want %d", page.Total, len(items))
		}
	}
}

// An empty page must still carry data as [], so a client can tell an empty
// collection from a field the server did not send.
func TestWritePageEmptyKeepsDataArray(t *testing.T) {
	rec := httptest.NewRecorder()
	WritePage(rec, []string(nil), Page{Total: 0, Limit: 50, Offset: 0})

	want := `{"success":true,"data":[],"page":{"total":0,"limit":50,"offset":0}}` + "\n"
	if rec.Body.String() != want {
		t.Errorf("body = %q, want %q", rec.Body.String(), want)
	}
}

func TestWriteListAlwaysCarriesAnArray(t *testing.T) {
	rec := httptest.NewRecorder()
	WriteList(rec, []string(nil))

	want := `{"success":true,"data":[]}` + "\n"
	if rec.Body.String() != want {
		t.Errorf("body = %q, want %q", rec.Body.String(), want)
	}
}
