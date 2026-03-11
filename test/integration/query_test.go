//go:build integration

package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"
)

type queryRequest struct {
	Query     string `json:"query"`
	Start     string `json:"start"`
	End       string `json:"end"`
	FractalID string `json:"fractal_id,omitempty"`
}

type queryResponse struct {
	Success      bool                     `json:"success"`
	Results      []map[string]interface{} `json:"results,omitempty"`
	Count        int                      `json:"count"`
	Query        string                   `json:"query,omitempty"`
	SQL          string                   `json:"sql,omitempty"`
	Error        string                   `json:"error,omitempty"`
	ExecutionMs  int64                    `json:"execution_ms,omitempty"`
	FieldOrder   []string                 `json:"field_order,omitempty"`
	IsAggregated bool                     `json:"is_aggregated,omitempty"`
}

type testCase struct {
	Name             string
	Query            string
	ShouldSucceed    bool
	ExpectCount      *int
	ExpectAggregated *bool
	Description      string
	ExpectMinCount       *int
	ExpectMaxCount       *int
	ExpectedFields       []string
	ExpectedSQLPatterns  []string
	ForbiddenSQLPatterns []string
	ResultValidator      func(queryResponse) (bool, string)
}

func intPtr(v int) *int    { return &v }
func boolPtr(v bool) *bool { return &v }

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func runQuery(t *testing.T, apiURL, apiKey, fractalID string, tc testCase) queryResponse {
	t.Helper()

	reqData := queryRequest{
		Query:     tc.Query,
		Start:     "2025-02-01T00:00:00Z",
		End:       "2026-02-28T23:59:59Z",
		FractalID: fractalID,
	}

	jsonData, err := json.Marshal(reqData)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	req, err := http.NewRequest("POST", apiURL, bytes.NewBuffer(jsonData))
	if err != nil {
		t.Fatalf("create request: %v", err)
	}

	req.Header.Set("X-API-Key", apiKey)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("execute request: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}

	var qr queryResponse
	if err := json.Unmarshal(body, &qr); err != nil {
		t.Fatalf("parse response: %v\nraw: %s", err, string(body))
	}

	return qr
}

func validateResponse(t *testing.T, tc testCase, resp queryResponse) {
	t.Helper()

	if tc.ShouldSucceed && !resp.Success {
		t.Fatalf("expected success, got error: %s", resp.Error)
	}
	if !tc.ShouldSucceed && resp.Success {
		t.Fatal("expected failure, but query succeeded")
	}

	if !resp.Success {
		return
	}

	if tc.ExpectCount != nil && resp.Count != *tc.ExpectCount {
		t.Errorf("expected count %d, got %d", *tc.ExpectCount, resp.Count)
	}
	if tc.ExpectMinCount != nil && resp.Count < *tc.ExpectMinCount {
		t.Errorf("expected min count %d, got %d", *tc.ExpectMinCount, resp.Count)
	}
	if tc.ExpectMaxCount != nil && resp.Count > *tc.ExpectMaxCount {
		t.Errorf("expected max count %d, got %d", *tc.ExpectMaxCount, resp.Count)
	}
	if tc.ExpectAggregated != nil && resp.IsAggregated != *tc.ExpectAggregated {
		t.Errorf("expected aggregated=%v, got %v", *tc.ExpectAggregated, resp.IsAggregated)
	}

	for _, field := range tc.ExpectedFields {
		found := false
		for _, f := range resp.FieldOrder {
			if f == field {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("expected field %q in field order %v", field, resp.FieldOrder)
		}
	}

	for _, pattern := range tc.ExpectedSQLPatterns {
		if !strings.Contains(resp.SQL, pattern) {
			t.Errorf("expected SQL pattern %q not found in: %s", pattern, resp.SQL)
		}
	}

	for _, pattern := range tc.ForbiddenSQLPatterns {
		matched, err := regexp.MatchString(pattern, resp.SQL)
		if err == nil && matched {
			t.Errorf("forbidden SQL pattern %q found in: %s", pattern, resp.SQL)
		}
	}

	if tc.ResultValidator != nil {
		if ok, msg := tc.ResultValidator(resp); !ok {
			t.Errorf("result validation failed: %s", msg)
		}
	}
}

func TestQueryIntegration(t *testing.T) {
	apiURL := getEnv("BIFRACT_API_URL", "http://127.0.0.1:8080/api/v1/query")
	apiKey := getEnv("BIFRACT_API_KEY", "bifract_default_d0a2354bb90cee7b63f81acc7246a08964541d2a7f94aa41c094cb482eeee67d")
	fractalID := getEnv("BIFRACT_FRACTAL_ID", "352184f1-96ef-49fc-acf1-c67af9c4ec44")

	// Verify the server is reachable before running tests
	client := &http.Client{Timeout: 5 * time.Second}
	healthURL := strings.TrimSuffix(strings.TrimSuffix(apiURL, "/query"), "/v1/query")
	if !strings.HasSuffix(healthURL, "/api/v1") {
		healthURL = strings.TrimSuffix(healthURL, "/") + "/api/v1"
	}
	healthURL += "/health"
	if _, err := client.Get(healthURL); err != nil {
		t.Skipf("Bifract server not reachable at %s: %v", healthURL, err)
	}

	tests := []testCase{
		{
			Name:                 "Basic Regex Field Match",
			Query:                "image=/powershell/i",
			ShouldSucceed:        true,
			Description:          "Basic case-insensitive regex field matching",
			ExpectMinCount:       intPtr(100),
			ExpectedFields:       []string{"timestamp", "fields"},
			ExpectedSQLPatterns:  []string{"match(fields.`image`.:String", "(?i)powershell"},
			ForbiddenSQLPatterns: []string{"GROUP BY", "HAVING"},
			ResultValidator: func(resp queryResponse) (bool, string) {
				if len(resp.Results) == 0 {
					return false, "no results returned for PowerShell query"
				}
				if fields, ok := resp.Results[0]["fields"].(map[string]interface{}); ok {
					if image, exists := fields["image"]; exists {
						imageStr := fmt.Sprintf("%v", image)
						if strings.Contains(strings.ToLower(imageStr), "powershell") {
							return true, ""
						}
						return false, fmt.Sprintf("image field doesn't contain 'powershell': %s", imageStr)
					}
				}
				return false, "results don't contain expected fields structure"
			},
		},

		{
			Name:                "AND Operator with Admin User",
			Query:               "image=/powershell/i AND user=/admin/i",
			ShouldSucceed:       true,
			Description:         "Multiple conditions with AND operator",
			ExpectMinCount:      intPtr(50),
			ExpectedSQLPatterns: []string{"AND", "match(fields.`image`.:String", "match(fields.`user`.:String", "(?i)admin"},
			ResultValidator: func(resp queryResponse) (bool, string) {
				if len(resp.Results) == 0 {
					return false, "no results for PowerShell + admin query"
				}
				if fields, ok := resp.Results[0]["fields"].(map[string]interface{}); ok {
					image := strings.ToLower(fmt.Sprintf("%v", fields["image"]))
					user := strings.ToLower(fmt.Sprintf("%v", fields["user"]))
					if !strings.Contains(image, "powershell") {
						return false, fmt.Sprintf("expected PowerShell image, got: %s", image)
					}
					if !strings.Contains(user, "admin") {
						return false, fmt.Sprintf("expected admin user, got: %s", user)
					}
					return true, ""
				}
				return false, "results missing fields data"
			},
		},

		{
			Name:                "Mathematical Expression Validation",
			Query:               "image=/powershell/i | result:=1+2 | result=3",
			ShouldSucceed:       true,
			Description:         "Mathematical expression evaluation with filtering",
			ExpectMinCount:      intPtr(100),
			ExpectedFields:      []string{"result"},
			ExpectedSQLPatterns: []string{"toString(1+2)", "result", "= '3'"},
			ResultValidator: func(resp queryResponse) (bool, string) {
				if len(resp.Results) == 0 {
					return false, "no results for math expression query"
				}
				if result, exists := resp.Results[0]["result"]; exists {
					resultStr := fmt.Sprintf("%v", result)
					if resultStr == "3" {
						return true, ""
					}
					return false, fmt.Sprintf("expected result=3, got: %s", resultStr)
				}
				return false, "result field not found in response"
			},
		},

		{
			Name:                "Case Statement Result Validation",
			Query:               "image=/powershell/i | case { user=/admin/i | check:=\"admin_user\"; * | check:=\"other_user\"; }",
			ShouldSucceed:       true,
			Description:         "Case statement with field assignments",
			ExpectMinCount:      intPtr(100),
			ExpectedFields:      []string{"check"},
			ExpectedSQLPatterns: []string{"CASE WHEN", "admin_user", "other_user"},
			ResultValidator: func(resp queryResponse) (bool, string) {
				if len(resp.Results) == 0 {
					return false, "no results for case statement query"
				}
				adminCount := 0
				otherCount := 0
				for _, result := range resp.Results {
					if check, exists := result["check"]; exists {
						checkStr := fmt.Sprintf("%v", check)
						if checkStr == "admin_user" {
							adminCount++
						} else if checkStr == "other_user" {
							otherCount++
						}
					}
				}
				if adminCount > 0 && otherCount >= 0 {
					return true, ""
				}
				return false, fmt.Sprintf("unexpected case results: admin=%d, other=%d", adminCount, otherCount)
			},
		},

		{
			Name:                 "Bare Regex Raw Log Search",
			Query:                "/powershell/i",
			ShouldSucceed:        true,
			Description:          "Bare regex query searching raw_log content",
			ExpectMinCount:       intPtr(100),
			ExpectedSQLPatterns:  []string{"match(raw_log", "(?i)powershell"},
			ForbiddenSQLPatterns: []string{"fields.`raw_log`.:String"},
		},

		{
			Name:                 "Pipeline Bare String Validation",
			Query:                "image=/powershell/i | \"namtws006\"",
			ShouldSucceed:        true,
			Description:          "Field condition followed by pipeline bare string",
			ExpectMinCount:       intPtr(10),
			ExpectMaxCount:       intPtr(1000),
			ExpectedSQLPatterns:  []string{"match(raw_log", "namtws006", "match(fields.`image`.:String"},
			ForbiddenSQLPatterns: []string{"fields.`raw_log`.:String"},
		},

		{
			Name:                "GroupBy Count Validation",
			Query:               "image=/powershell/i | groupby(user)",
			ShouldSucceed:       true,
			Description:         "GroupBy with implicit count",
			ExpectAggregated:    boolPtr(true),
			ExpectMinCount:      intPtr(2),
			ExpectedFields:      []string{"user", "_count"},
			ExpectedSQLPatterns: []string{"GROUP BY", "COUNT(*) AS _count", "fields.`user`.:String AS user"},
			ResultValidator: func(resp queryResponse) (bool, string) {
				if len(resp.Results) == 0 {
					return false, "no aggregation results"
				}
				for _, result := range resp.Results {
					if _, hasUser := result["user"]; !hasUser {
						return false, "result missing user field"
					}
					if count, hasCount := result["_count"]; hasCount {
						countVal := int(count.(float64))
						if countVal <= 0 {
							return false, fmt.Sprintf("invalid count value: %d", countVal)
						}
					} else {
						return false, "result missing _count field"
					}
				}
				return true, ""
			},
		},

		{
			Name:                 "HAVING Count Greater Than",
			Query:                "image=/powershell/i | groupby(user) | count > 100",
			ShouldSucceed:        true,
			Description:          "HAVING condition with count > operator",
			ExpectAggregated:     boolPtr(true),
			ExpectMinCount:       intPtr(1),
			ExpectedSQLPatterns:  []string{"HAVING", "_count > 100", "GROUP BY"},
			ForbiddenSQLPatterns: []string{"WHERE.*toFloat64.*count.*100"},
			ResultValidator: func(resp queryResponse) (bool, string) {
				if len(resp.Results) == 0 {
					return false, "no results for HAVING condition"
				}
				for _, result := range resp.Results {
					if count, hasCount := result["_count"]; hasCount {
						countVal := int(count.(float64))
						if countVal <= 100 {
							return false, fmt.Sprintf("HAVING filter failed: found count %d <= 100", countVal)
						}
					} else {
						return false, "result missing _count field for HAVING validation"
					}
				}
				return true, ""
			},
		},

		{
			Name:             "Complex Pipeline Validation",
			Query:            "image=/powershell/i AND user=/admin/i | \"namtws006.mtfin.local\" | groupby(user) | count >= 1",
			ShouldSucceed:    true,
			Description:      "Complex query with field filters, pipeline bare string, and aggregation",
			ExpectAggregated: boolPtr(true),
			ExpectCount:      intPtr(1),
			ExpectedSQLPatterns: []string{
				"match(fields.`image`.:String", "(?i)powershell",
				"match(fields.`user`.:String", "(?i)admin",
				"match(raw_log", "namtws006.mtfin.local",
				"GROUP BY user", "HAVING _count >= 1",
			},
			ResultValidator: func(resp queryResponse) (bool, string) {
				if resp.Count != 1 {
					return false, fmt.Sprintf("expected exactly 1 result, got %d", resp.Count)
				}
				result := resp.Results[0]
				if user, hasUser := result["user"]; hasUser {
					userStr := strings.ToLower(fmt.Sprintf("%v", user))
					if !strings.Contains(userStr, "admin") {
						return false, fmt.Sprintf("expected admin user, got: %s", userStr)
					}
				}
				if count, hasCount := result["_count"]; hasCount {
					countVal := int(count.(float64))
					if countVal < 1 {
						return false, fmt.Sprintf("expected count >= 1, got: %d", countVal)
					}
				}
				return true, ""
			},
		},

		{
			Name:                "SQL Injection Protection",
			Query:               "user=\"admin'; DROP TABLE logs; --\"",
			ShouldSucceed:       true,
			Description:         "Ensure SQL injection attempts are properly escaped",
			ExpectCount:         intPtr(0),
			ExpectedSQLPatterns: []string{"admin\\'; DROP TABLE logs; --"},
		},

		{
			Name:                "SelectFirst Function Validation",
			Query:               "user=\"Administrator\" | groupby(computer) | selectfirst(event_id)",
			ShouldSucceed:       true,
			Description:         "SelectFirst should get the earliest event_id value for each computer group",
			ExpectAggregated:    boolPtr(true),
			ExpectMinCount:      intPtr(1),
			ExpectedFields:      []string{"computer", "first_event_id"},
			ExpectedSQLPatterns: []string{"argMin(fields.`event_id`.:String, timestamp) AS first_event_id", "GROUP BY computer"},
			ResultValidator: func(resp queryResponse) (bool, string) {
				if len(resp.Results) == 0 {
					return false, "no results returned for selectfirst query"
				}
				result := resp.Results[0]
				if _, hasComputer := result["computer"]; !hasComputer {
					return false, "result missing computer field"
				}
				if _, hasFirst := result["first_event_id"]; !hasFirst {
					return false, "result missing first_event_id field"
				}
				return true, ""
			},
		},

		{
			Name:                "SelectLast Function Validation",
			Query:               "user=\"Administrator\" | groupby(computer) | selectlast(event_id)",
			ShouldSucceed:       true,
			Description:         "SelectLast should get the latest event_id value for each computer group",
			ExpectAggregated:    boolPtr(true),
			ExpectMinCount:      intPtr(1),
			ExpectedFields:      []string{"computer", "last_event_id"},
			ExpectedSQLPatterns: []string{"argMax(fields.`event_id`.:String, timestamp) AS last_event_id", "GROUP BY computer"},
			ResultValidator: func(resp queryResponse) (bool, string) {
				if len(resp.Results) == 0 {
					return false, "no results returned for selectlast query"
				}
				result := resp.Results[0]
				if _, hasComputer := result["computer"]; !hasComputer {
					return false, "result missing computer field"
				}
				if _, hasLast := result["last_event_id"]; !hasLast {
					return false, "result missing last_event_id field"
				}
				return true, ""
			},
		},

		{
			Name:                 "SelectFirst Timestamp Validation",
			Query:                "user=\"Administrator\" | groupby(computer) | selectfirst(timestamp)",
			ShouldSucceed:        true,
			Description:          "SelectFirst with timestamp should use min(timestamp) instead of argMin",
			ExpectAggregated:     boolPtr(true),
			ExpectMinCount:       intPtr(1),
			ExpectedFields:       []string{"computer", "first_timestamp"},
			ExpectedSQLPatterns:  []string{"min(timestamp) AS first_timestamp", "GROUP BY computer"},
			ForbiddenSQLPatterns: []string{"argMin(timestamp"},
			ResultValidator: func(resp queryResponse) (bool, string) {
				if len(resp.Results) == 0 {
					return false, "no results returned for selectfirst timestamp query"
				}
				result := resp.Results[0]
				if firstTime, hasFirst := result["first_timestamp"]; hasFirst {
					timeStr := fmt.Sprintf("%v", firstTime)
					if timeStr == "" || timeStr == "<nil>" {
						return false, "first_timestamp field is empty or nil"
					}
					return true, ""
				}
				return false, "result missing first_timestamp field"
			},
		},

		{
			Name:                "Non-existent Field Query",
			Query:               "nonexistent_field_xyz_123=\"impossible_value\"",
			ShouldSucceed:       true,
			Description:         "Query with non-existent field should succeed but return 0 results",
			ExpectCount:         intPtr(0),
			ExpectedSQLPatterns: []string{"fields.`nonexistent_field_xyz_123`.:String"},
		},

		{
			Name:           "OR Group with AND",
			Query:          "(event_id=1 OR event_id=3) AND image=/powershell/i",
			ShouldSucceed:  true,
			Description:    "Parenthetical OR group combined with AND condition",
			ExpectMinCount: intPtr(10),
			ExpectedSQLPatterns: []string{
				"(fields.`event_id`.:String = '1' OR fields.`event_id`.:String = '3')",
				"AND match(fields.`image`.:String",
				"(?i)powershell",
			},
			ResultValidator: func(resp queryResponse) (bool, string) {
				if len(resp.Results) == 0 {
					return false, "no results for parenthetical grouping query"
				}
				for _, result := range resp.Results {
					if fields, ok := result["fields"].(map[string]interface{}); ok {
						eventID := fmt.Sprintf("%v", fields["event_id"])
						image := strings.ToLower(fmt.Sprintf("%v", fields["image"]))
						if eventID != "1" && eventID != "3" {
							return false, fmt.Sprintf("expected event_id 1 or 3, got: %s", eventID)
						}
						if !strings.Contains(image, "powershell") {
							return false, fmt.Sprintf("expected powershell image, got: %s", image)
						}
					}
				}
				return true, ""
			},
		},

		{
			Name:           "Complex Parenthetical Grouping",
			Query:          "(user=/admin/i OR user=/system/i) AND (image=/powershell/i OR image=/cmd/i)",
			ShouldSucceed:  true,
			Description:    "Complex parenthetical grouping with multiple OR conditions",
			ExpectMinCount: intPtr(5),
			ExpectedSQLPatterns: []string{
				"(match(fields.`user`.:String, '(?i)admin') OR match(fields.`user`.:String, '(?i)system'))",
				"AND (match(fields.`image`.:String, '(?i)powershell') OR match(fields.`image`.:String, '(?i)cmd'))",
			},
			ResultValidator: func(resp queryResponse) (bool, string) {
				if len(resp.Results) == 0 {
					return false, "no results for complex parenthetical grouping"
				}
				if fields, ok := resp.Results[0]["fields"].(map[string]interface{}); ok {
					user := strings.ToLower(fmt.Sprintf("%v", fields["user"]))
					image := strings.ToLower(fmt.Sprintf("%v", fields["image"]))
					userValid := strings.Contains(user, "admin") || strings.Contains(user, "system")
					imageValid := strings.Contains(image, "powershell") || strings.Contains(image, "cmd")
					if !userValid {
						return false, fmt.Sprintf("user doesn't match admin or system: %s", user)
					}
					if !imageValid {
						return false, fmt.Sprintf("image doesn't match powershell or cmd: %s", image)
					}
				}
				return true, ""
			},
		},

		{
			Name:          "Impossible AND Condition",
			Query:         "(event_id=1 OR event_id=3) AND (event_id=10 OR event_id=7)",
			ShouldSucceed: true,
			Description:   "Logically impossible condition: should parse correctly but return 0 results",
			ExpectCount:   intPtr(0),
			ExpectedSQLPatterns: []string{
				"(fields.`event_id`.:String = '1' OR fields.`event_id`.:String = '3')",
				"AND (fields.`event_id`.:String = '10' OR fields.`event_id`.:String = '7')",
			},
		},

		{
			Name:           "Nested Parentheses with Grouping",
			Query:          "((user=/admin/i OR user=/administrator/i) AND event_id=1) OR (image=/powershell/i AND event_id=3)",
			ShouldSucceed:  true,
			Description:    "Nested parentheses and complex logical grouping",
			ExpectMinCount: intPtr(1),
			ExpectedSQLPatterns: []string{
				"((match(fields.`user`.:String, '(?i)admin') OR match(fields.`user`.:String, '(?i)administrator')) AND fields.`event_id`.:String = '1')",
				"OR (match(fields.`image`.:String, '(?i)powershell') AND fields.`event_id`.:String = '3')",
			},
		},

		{
			Name:           "Mixed Simple and Complex Parenthetical Grouping",
			Query:          "(event_id=1) OR (user=/admin/i AND image=/powershell/i)",
			ShouldSucceed:  true,
			Description:    "Simple condition OR compound condition in parentheses",
			ExpectMinCount: intPtr(5),
			ExpectedSQLPatterns: []string{
				"(fields.`event_id`.:String = '1')",
				"OR (match(fields.`user`.:String, '(?i)admin') AND match(fields.`image`.:String, '(?i)powershell'))",
			},
			ResultValidator: func(resp queryResponse) (bool, string) {
				if len(resp.Results) == 0 {
					return false, "no results for mixed parenthetical grouping"
				}
				validResults := 0
				for _, result := range resp.Results {
					if fields, ok := result["fields"].(map[string]interface{}); ok {
						eventID := fmt.Sprintf("%v", fields["event_id"])
						user := strings.ToLower(fmt.Sprintf("%v", fields["user"]))
						image := strings.ToLower(fmt.Sprintf("%v", fields["image"]))
						hasEventID1 := eventID == "1"
						hasAdminAndPowershell := strings.Contains(user, "admin") && strings.Contains(image, "powershell")
						if hasEventID1 || hasAdminAndPowershell {
							validResults++
						}
					}
				}
				if validResults == len(resp.Results) {
					return true, ""
				}
				return false, fmt.Sprintf("only %d of %d results matched the OR condition", validResults, len(resp.Results))
			},
		},

		{
			Name:             "HAVING Compound AND Condition",
			Query:            "event_id=10 | target_image=/lsass/i | granted_access != \"0x1000\" AND granted_access != \"0x1410\" | groupby(source_image,user,granted_access)",
			ShouldSucceed:    true,
			Description:      "Compound AND condition in HAVING clause",
			ExpectAggregated: boolPtr(true),
			ExpectMinCount:   intPtr(1),
			ExpectedSQLPatterns: []string{
				"fields.`event_id`.:String = '10'",
				"match(raw_log", "/lsass/i",
				"granted_access", "!= '0x1000'", "AND", "!= '0x1410'",
				"GROUP BY source_image, user, granted_access",
			},
			ResultValidator: func(resp queryResponse) (bool, string) {
				if len(resp.Results) == 0 {
					return false, "no results for HAVING compound condition"
				}
				return true, ""
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.Name, func(t *testing.T) {
			resp := runQuery(t, apiURL, apiKey, fractalID, tc)
			t.Logf("query=%q success=%v count=%d aggregated=%v time=%dms",
				tc.Query, resp.Success, resp.Count, resp.IsAggregated, resp.ExecutionMs)
			if resp.Success && len(resp.SQL) > 0 {
				sql := resp.SQL
				if len(sql) > 120 {
					sql = sql[:120] + "..."
				}
				t.Logf("sql: %s", sql)
			}
			validateResponse(t, tc, resp)
		})
	}
}
