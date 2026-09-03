package chat

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"bifract/pkg/attack"
)

// SuggestLabelsRequest is the rule as it stands in the editor.
type SuggestLabelsRequest struct {
	Name        string   `json:"name"`
	Description string   `json:"description"`
	QueryString string   `json:"query_string"`
	Labels      []string `json:"labels"`
}

// SuggestedLabel is one ATT&CK label the model proposed and the matrix confirmed.
type SuggestedLabel struct {
	Label string `json:"label"` // attack.t1059.001, attack.execution
	ID    string `json:"id"`    // T1059.001, TA0002
	Name  string `json:"name"`
	Kind  string `json:"kind"` // technique or tactic
}

// SuggestLabelsResponse lists labels not already on the rule.
type SuggestLabelsResponse struct {
	Labels []SuggestedLabel `json:"labels"`
}

const suggestLabelsPrompt = `You map detection rules to MITRE ATT&CK Enterprise.
Given a rule's name, description and query, answer with JSON only, no prose:
{"techniques":["T1059.001"],"tactics":["execution"]}
Rules: prefer the most specific sub-technique when the rule clearly targets it, otherwise the parent technique.
List at most 4 techniques and at most 2 tactics, most relevant first. Tactics are lowercase slugs such as
execution, persistence, privilege-escalation, defense-evasion, credential-access, discovery, lateral-movement,
collection, command-and-control, exfiltration, impact, initial-access, reconnaissance, resource-development.
If the rule does not describe adversary behaviour, return empty arrays.`

const suggestLabelsTimeout = 30 * time.Second

// SuggestAttackLabels asks the model for ATT&CK mappings and keeps only those the
// embedded matrix recognises, in the lowercase attack.* form Sigma tags use.
func (m *Manager) SuggestAttackLabels(ctx context.Context, req SuggestLabelsRequest) ([]SuggestedLabel, error) {
	if strings.TrimSpace(req.Name) == "" && strings.TrimSpace(req.QueryString) == "" && strings.TrimSpace(req.Description) == "" {
		return nil, fmt.Errorf("nothing to map yet: give the rule a name, a query or a description")
	}
	matrix, err := attack.Get()
	if err != nil {
		return nil, fmt.Errorf("ATT&CK matrix unavailable: %w", err)
	}

	var sb strings.Builder
	fmt.Fprintf(&sb, "Name: %s\n", strings.TrimSpace(req.Name))
	if d := strings.TrimSpace(req.Description); d != "" {
		fmt.Fprintf(&sb, "Description: %s\n", d)
	}
	fmt.Fprintf(&sb, "Query (BQL): %s\n", strings.TrimSpace(req.QueryString))

	ctx, cancel := context.WithTimeout(ctx, suggestLabelsTimeout)
	defer cancel()
	content, err := m.completeFromLiteLLM(ctx, []llmMessage{
		{Role: "system", Content: suggestLabelsPrompt},
		{Role: "user", Content: sb.String()},
	})
	if err != nil {
		return nil, err
	}

	var parsed struct {
		Techniques []string `json:"techniques"`
		Tactics    []string `json:"tactics"`
	}
	if err := json.Unmarshal([]byte(extractJSONObject(content)), &parsed); err != nil {
		return nil, fmt.Errorf("the model did not answer with a mapping")
	}

	have := make(map[string]bool, len(req.Labels))
	for _, l := range req.Labels {
		have[strings.ToLower(strings.TrimSpace(l))] = true
	}
	var out []SuggestedLabel
	add := func(s SuggestedLabel) {
		if have[s.Label] {
			return
		}
		have[s.Label] = true
		out = append(out, s)
	}
	for _, id := range parsed.Techniques {
		kind, canon := matrix.ParseLabel("attack." + id)
		if kind != attack.KindTechnique {
			continue
		}
		t := matrix.Technique(canon)
		if t == nil || t.Deprecated {
			continue
		}
		add(SuggestedLabel{Label: "attack." + strings.ToLower(canon), ID: canon, Name: t.Name, Kind: "technique"})
	}
	for _, slug := range parsed.Tactics {
		kind, short := matrix.ParseLabel("attack." + slug)
		if kind != attack.KindTactic {
			continue
		}
		t := matrix.Tactic(short)
		if t == nil {
			continue
		}
		add(SuggestedLabel{Label: "attack." + short, ID: t.ID, Name: t.Name, Kind: "tactic"})
	}
	return out, nil
}

// completeFromLiteLLM is the one-shot, non-streaming counterpart of the chat stream.
func (m *Manager) completeFromLiteLLM(ctx context.Context, messages []llmMessage) (string, error) {
	body, err := json.Marshal(llmRequest{Model: "bifract-chat", Messages: messages, Tools: []llmTool{}})
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, m.litellmURL+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	if m.litellmKey != "" {
		req.Header.Set("Authorization", "Bearer "+m.litellmKey)
	}
	resp, err := m.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("AI provider unreachable: %w", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == http.StatusUnauthorized {
			return "", fmt.Errorf("AI provider API key is not configured")
		}
		return "", fmt.Errorf("AI provider error %d", resp.StatusCode)
	}
	var parsed struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(raw, &parsed); err != nil || len(parsed.Choices) == 0 {
		return "", fmt.Errorf("AI provider returned an empty answer")
	}
	return parsed.Choices[0].Message.Content, nil
}

// extractJSONObject returns the first {...} in s, so a model that wraps its answer
// in a code fence or a sentence still parses.
func extractJSONObject(s string) string {
	start := strings.Index(s, "{")
	end := strings.LastIndex(s, "}")
	if start < 0 || end <= start {
		return s
	}
	return s[start : end+1]
}

// HandleSuggestLabels maps the rule in the editor to ATT&CK labels (analyst+).
func (h *Handler) HandleSuggestLabels(w http.ResponseWriter, r *http.Request) {
	var req SuggestLabelsRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 64<<10)).Decode(&req); err != nil {
		h.respondError(w, http.StatusBadRequest, "Invalid request body")
		return
	}
	labels, err := h.manager.SuggestAttackLabels(r.Context(), req)
	if err != nil {
		h.respondError(w, http.StatusBadGateway, err.Error())
		return
	}
	if labels == nil {
		labels = []SuggestedLabel{}
	}
	h.respondSuccess(w, SuggestLabelsResponse{Labels: labels})
}
