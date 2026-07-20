package settings

import "testing"

func TestToSnakeCase(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"camelCase", "queryName", "query_name"},
		{"PascalCase", "EventID", "event_id"},
		{"acronym", "HTTPStatusCode", "http_status_code"},
		{"userId", "userId", "user_id"},
		{"pre-underscored space-normalized header", "Action_Time", "action_time"},
		{"already snake_case", "action_time", "action_time"},
		{"multiple pre-existing underscores", "Full_Path_Name", "full_path_name"},
		{"acronym immediately after existing underscore", "HTTP_Status", "http_status"},
		{"acronym at end after existing underscore", "data_ID", "data_id"},
		{"leading underscore before uppercase", "_Time", "_time"},
		{"empty", "", ""},

		// Separator handling. Without it a hyphen survives and the uppercase rule
		// fires on the letter after it, producing names like "accept-_encoding".
		{"hyphenated header", "Accept-Encoding", "accept_encoding"},
		{"hyphenated header lowercase", "accept-encoding", "accept_encoding"},
		{"multi hyphen header", "X-Frame-Options", "x_frame_options"},
		{"all lowercase multi hyphen", "sec-ch-ua-platform", "sec_ch_ua_platform"},
		{"space separated", "Process Name", "process_name"},
		{"slash separated", "bytes/sec", "bytes_sec"},
		{"screaming snake", "ACCEPT_ENCODING", "accept_encoding"},
		{"mixed separators collapse", "Content--Type", "content_type"},
		{"separator adjacent to underscore", "X-Frame_Options", "x_frame_options"},
		{"leading separator dropped", "-Foo", "foo"},
		{"trailing separator dropped", "foo-", "foo"},
		{"degenerate all separators", "_", "_"},

		// Sigils are not separators: "@timestamp" must not collapse onto "timestamp".
		{"at-prefixed field preserved", "@timestamp", "@timestamp"},
		{"at-prefixed camel", "@versionNumber", "@version_number"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ToSnakeCase(tc.in)
			if got != tc.want {
				t.Errorf("ToSnakeCase(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// TestToSnakeCaseConverges is the point of the separator handling: however a
// shipper capitalises or delimits a header, it must land in one ClickHouse
// column rather than fragmenting across several.
func TestToSnakeCaseConverges(t *testing.T) {
	groups := map[string][]string{
		"accept_encoding": {"Accept-Encoding", "accept-encoding", "ACCEPT_ENCODING", "accept_encoding", "AcceptEncoding", "accept encoding"},
		"user_agent":      {"User-Agent", "user-agent", "userAgent", "User_Agent"},
		"process_name":    {"Process Name", "ProcessName", "process_name", "process-name"},
	}

	for want, spellings := range groups {
		t.Run(want, func(t *testing.T) {
			for _, in := range spellings {
				if got := ToSnakeCase(in); got != want {
					t.Errorf("ToSnakeCase(%q) = %q, want %q (fragments the field)", in, got, want)
				}
			}
		})
	}
}
