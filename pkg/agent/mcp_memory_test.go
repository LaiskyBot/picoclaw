package agent

import (
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
	}

	require.True(t, hasLaiskyMemoryMCPRemote(remote))
	require.False(t, hasLaiskyMemoryMCPRemote(map[string]config.RemoteMCPServerConfig{"other": remote["other"]}))
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

// TestInjectMCPMemoryRecall verifies memory recall is appended into first system prompt block.
// The t parameter controls test lifecycle and the function returns no value.
func TestInjectMCPMemoryRecall(t *testing.T) {
	messages := []providers.Message{
		{Role: "system", Content: "base-system"},
		{Role: "user", Content: "hello"},
	}

	updated := injectMCPMemoryRecall(messages, "Memory recall: timezone is UTC")
	require.Contains(t, updated[0].Content, "base-system")
	require.Contains(t, updated[0].Content, "MCP Memory Recall")
	require.Contains(t, updated[0].Content, "timezone is UTC")
}
