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
