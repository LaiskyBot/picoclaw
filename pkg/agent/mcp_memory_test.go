package agent

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/providers"
	"github.com/stretchr/testify/require"
)

// TestHasLaiskyMemoryMCPRemote verifies mcp.laisky.com endpoint detection from remote MCP map.
// The t parameter controls test lifecycle and the function returns no value.
func TestHasLaiskyMemoryMCPRemote(t *testing.T) {
	remote := map[string]config.RemoteMCPServerConfig{
		"other": {
			Type: "http",
			URL:  "https://example.com/mcp",
		},
		"laisky": {
			Type: "http",
			URL:  "https://mcp.laisky.com",
		},
		"laisky-auth": {
			Type: "http",
			URL:  "https://mcp.laisky.com",
			Headers: map[string]string{
				"Authorization": "Bearer token",
			},
		},
	}

	require.True(t, hasLaiskyMemoryMCPRemote(remote))
	require.True(t, hasLaiskyMemoryMCPRemote(map[string]config.RemoteMCPServerConfig{"laisky": remote["laisky"]}))
	require.False(t, hasLaiskyMemoryMCPRemote(map[string]config.RemoteMCPServerConfig{"other": remote["other"]}))
}

// TestPickLaiskyMemoryMCPRemoteDeterministicByName verifies remote selection is deterministic by sorted map key.
// The t parameter controls test lifecycle and the function returns no value.
func TestPickLaiskyMemoryMCPRemoteDeterministicByName(t *testing.T) {
	remote := map[string]config.RemoteMCPServerConfig{
		"b-with-auth": {
			Type: "http",
			URL:  "https://mcp.laisky.com",
			Headers: map[string]string{
				"Authorization": "Bearer token",
			},
		},
		"a-without-auth": {
			Type: "http",
			URL:  "https://mcp.laisky.com",
		},
	}

	name, selected, ok := pickLaiskyMemoryMCPRemote(remote)
	require.True(t, ok)
	require.Equal(t, "a-without-auth", name)
	require.Equal(t, "https://mcp.laisky.com", selected.URL)
}

// TestMCPMemoryClientCallJSONRPCWithSessionSendsAuthorization verifies memory client forwards configured Authorization header.
// The t parameter controls test lifecycle and the function returns no value.
func TestMCPMemoryClientCallJSONRPCWithSessionSendsAuthorization(t *testing.T) {
	const expectedAuthorization = "Bearer test-key"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		require.Equal(t, expectedAuthorization, r.Header.Get("Authorization"))

		var req map[string]any
		err := json.NewDecoder(r.Body).Decode(&req)
		require.NoError(t, err)

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0",
			"id":      req["id"],
			"result":  map[string]any{"ok": true},
		})
	}))
	defer server.Close()

	client := &mcpTurnMemoryClient{
		endpoint: server.URL,
		headers: map[string]string{
			"Authorization": expectedAuthorization,
		},
		client: server.Client(),
	}

	result, _, err := client.callJSONRPCWithSession(context.Background(), "", "initialize", map[string]any{})
	require.NoError(t, err)
	require.NotNil(t, result)
}

// TestHasAuthorizationHeader verifies case-insensitive Authorization detection with non-empty values.
// The t parameter controls test lifecycle and the function returns no value.
func TestHasAuthorizationHeader(t *testing.T) {
	require.True(t, hasAuthorizationHeader(map[string]string{"Authorization": "Bearer token"}))
	require.True(t, hasAuthorizationHeader(map[string]string{"authorization": "  Bearer token  "}))
	require.False(t, hasAuthorizationHeader(map[string]string{"Authorization": "   "}))
	require.False(t, hasAuthorizationHeader(map[string]string{"X-Token": "abc"}))
}

// TestExtractRecallText verifies only non-duplicate non-user recall text is injected.
// The t parameter controls test lifecycle and the function returns no value.
func TestExtractRecallText(t *testing.T) {
	items := []memoryTurnItem{
		{
			Type: "message",
			Role: "developer",
			Content: []memoryTurnItemContent{{
				Type: "input_text",
				Text: "Memory recall: User prefers concise answers.",
			}},
		},
		{
			Type: "message",
			Role: "user",
			Content: []memoryTurnItemContent{{
				Type: "input_text",
				Text: "What should I focus on today?",
			}},
		},
	}

	recall := extractRecallText(items, "What should I focus on today?")
	require.Equal(t, "Memory recall: User prefers concise answers.", recall)
}

// TestInjectMCPMemoryRecall verifies memory recall is inserted before latest user message.
// The t parameter controls test lifecycle and the function returns no value.
func TestInjectMCPMemoryRecall(t *testing.T) {
	messages := []providers.Message{
		{Role: "system", Content: "base-system"},
		{Role: "assistant", Content: "prior answer"},
		{Role: "user", Content: "hello"},
	}

	updated := injectMCPMemoryRecall(messages, "Memory recall: timezone is UTC")
	require.Len(t, updated, 4)
	require.Equal(t, "system", updated[0].Role)
	require.Equal(t, "assistant", updated[1].Role)
	require.Equal(t, "assistant", updated[2].Role)
	require.Equal(t, "user", updated[3].Role)
	require.Equal(t, "hello", updated[3].Content)
	require.Contains(t, updated[2].Content, "MCP Memory Recall")
	require.Contains(t, updated[2].Content, "timezone is UTC")
}
