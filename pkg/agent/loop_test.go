package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/providers"
	"github.com/sipeed/picoclaw/pkg/session"
	"github.com/sipeed/picoclaw/pkg/tools"
	"github.com/stretchr/testify/require"
)

// TestBuildConfiguredRemoteMCPServers verifies config remote MCP entries are converted in sorted order.
// The t parameter controls test lifecycle.
// It returns no value and fails the test on assertion errors.
func TestBuildConfiguredRemoteMCPServers(t *testing.T) {
	servers := buildConfiguredRemoteMCPServers(map[string]config.RemoteMCPServerConfig{
		"zeta": {
			Type: "http",
			URL:  "https://zeta.example.com/mcp",
		},
		"alpha": {
			Type: "http",
			URL:  "https://alpha.example.com/mcp?APIKEY=token",
		},
	})

	require.Len(t, servers, 2)
	require.Equal(t, "alpha", servers[0].Name)
	require.Equal(t, "zeta", servers[1].Name)
	require.Equal(t, "https://alpha.example.com/mcp", servers[0].URL)
	require.Equal(t, "Bearer token", servers[0].Headers["Authorization"])
}

// TestBuildDesiredRemoteProxyNames verifies proxy naming rules for unique and duplicated remote tools.
// The t parameter controls test lifecycle.
// It returns no value and fails the test on assertion errors.
func TestBuildDesiredRemoteProxyNames(t *testing.T) {
	desired := buildDesiredRemoteProxyNames([]tools.RemoteMCPDiscoveredTool{
		{ServerName: "alpha", Name: "file_write"},
		{ServerName: "beta", Name: "file_write"},
		{ServerName: "alpha", Name: "read_file"},
	})

	require.Contains(t, desired, "alpha__file_write")
	require.Contains(t, desired, "beta__file_write")
	require.Contains(t, desired, "read_file")
}

// TestBuildDesiredRemoteProxyNames_TrimmedDuplicateNames verifies duplicate detection uses trimmed names.
// The t parameter controls test lifecycle.
// It returns no value and fails the test on assertion errors.
func TestBuildDesiredRemoteProxyNames_TrimmedDuplicateNames(t *testing.T) {
	desired := buildDesiredRemoteProxyNames([]tools.RemoteMCPDiscoveredTool{
		{ServerName: "alpha", Name: "memory_after_turn"},
		{ServerName: "beta", Name: " memory_after_turn "},
	})

	require.Contains(t, desired, "alpha__memory_after_turn")
	require.Contains(t, desired, "beta__memory_after_turn")
	require.NotContains(t, desired, "memory_after_turn")
}

// TestApplyRemoteMCPToolsToRegistry verifies dynamic remote tools are registered and stale tools are removed.
// The t parameter controls test lifecycle.
// It returns no value and fails the test on assertion errors.
func TestApplyRemoteMCPToolsToRegistry(t *testing.T) {
	registry := tools.NewToolRegistry()
	remoteClient := tools.NewRemoteMCPTool(t.TempDir())
	injectedStore := map[string]map[string]struct{}{}

	applyRemoteMCPToolsToRegistry("main", registry, remoteClient, []tools.RemoteMCPDiscoveredTool{{
		ServerName:  "alpha",
		Name:        "file_write",
		Description: "write file",
		InputSchema: map[string]any{"type": "object"},
	}}, injectedStore)

	_, exists := registry.Get("file_write")
	require.True(t, exists)

	applyRemoteMCPToolsToRegistry("main", registry, remoteClient, nil, injectedStore)
	_, exists = registry.Get("file_write")
	require.False(t, exists)
}

// TestApplyRemoteMCPToolsToRegistryRegistersCollisionAlias verifies collisions preserve local tools and inject remote aliases.
// The t parameter controls test lifecycle.
// It returns no value and fails the test on assertion errors.
func TestApplyRemoteMCPToolsToRegistryRegistersCollisionAlias(t *testing.T) {
	registry := tools.NewToolRegistry()
	registry.Register(&mockCustomTool{})

	remoteClient := tools.NewRemoteMCPTool(t.TempDir())
	injectedStore := map[string]map[string]struct{}{}

	applyRemoteMCPToolsToRegistry("main", registry, remoteClient, []tools.RemoteMCPDiscoveredTool{{
		ServerName:  "alpha",
		Name:        "mock_custom",
		Description: "collision",
		InputSchema: map[string]any{"type": "object"},
	}}, injectedStore)

	toolObj, exists := registry.Get("mock_custom")
	require.True(t, exists)
	_, isProxy := toolObj.(*tools.RemoteMCPProxyTool)
	require.False(t, isProxy)

	aliasToolObj, aliasExists := registry.Get("alpha__mock_custom")
	require.True(t, aliasExists)
	_, aliasIsProxy := aliasToolObj.(*tools.RemoteMCPProxyTool)
	require.True(t, aliasIsProxy)
}

// TestApplyRemoteMCPToolsToRegistryCollisionAliasSuffix verifies alias suffix fallback when preferred alias already exists.
// The t parameter controls test lifecycle.
// It returns no value and fails the test on assertion errors.
func TestApplyRemoteMCPToolsToRegistryCollisionAliasSuffix(t *testing.T) {
	registry := tools.NewToolRegistry()
	registry.Register(&mockCustomTool{})
	registry.Register(&mockToolWithName{name: "alpha__mock_custom"})

	remoteClient := tools.NewRemoteMCPTool(t.TempDir())
	injectedStore := map[string]map[string]struct{}{}

	applyRemoteMCPToolsToRegistry("main", registry, remoteClient, []tools.RemoteMCPDiscoveredTool{{
		ServerName:  "alpha",
		Name:        "mock_custom",
		Description: "collision",
		InputSchema: map[string]any{"type": "object"},
	}}, injectedStore)

	_, aliasExists := registry.Get("alpha__mock_custom")
	require.True(t, aliasExists)

	aliasToolObj, alias2Exists := registry.Get("alpha__mock_custom__2")
	require.True(t, alias2Exists)
	_, alias2IsProxy := aliasToolObj.(*tools.RemoteMCPProxyTool)
	require.True(t, alias2IsProxy)
}

// TestRunAgentLoop_UsesMCPMemoryLifecycle verifies laisky MCP memory hooks run before/after model turns.
// It configures mcp.laisky.com, injects mock MCP responses, and ensures recall is appended while file memory is disabled.
// It returns no value and fails when before/after lifecycle behavior is incorrect.
func TestRunAgentLoop_UsesMCPMemoryLifecycle(t *testing.T) {
	tmpDir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(tmpDir, "memory"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "memory", "MEMORY.md"), []byte("FILE_MEMORY_SHOULD_NOT_APPEAR"), 0o644))

	var mu sync.Mutex
	var capturedBeforeArgs map[string]any
	var capturedAfterArgs map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		var req map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))

		method, _ := req["method"].(string)
		switch method {
		case "initialize":
			w.Header().Set("Mcp-Session-Id", "memory-session-1")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result":  map[string]any{"protocolVersion": "2024-11-05"},
			})
		case "tools/call":
			params, _ := req["params"].(map[string]any)
			name, _ := params["name"].(string)
			arguments, _ := params["arguments"].(map[string]any)

			switch name {
			case memoryBeforeToolName:
				mu.Lock()
				capturedBeforeArgs = arguments
				mu.Unlock()

				_ = json.NewEncoder(w).Encode(map[string]any{
					"jsonrpc": "2.0",
					"id":      req["id"],
					"result": map[string]any{
						"content": []map[string]any{{
							"type": "text",
							"text": `{"input_items":[{"type":"message","role":"developer","content":[{"type":"input_text","text":"Memory recall: project prefers concise updates."}]},{"type":"message","role":"user","content":[{"type":"input_text","text":"hello memory"}]}]}`,
						}},
					},
				})
			case memoryAfterToolName:
				mu.Lock()
				capturedAfterArgs = arguments
				mu.Unlock()

				_ = json.NewEncoder(w).Encode(map[string]any{
					"jsonrpc": "2.0",
					"id":      req["id"],
					"result": map[string]any{
						"content": []map[string]any{{
							"type": "text",
							"text": `{"ok":true}`,
						}},
					},
				})
			default:
				w.WriteHeader(http.StatusBadRequest)
				_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": req["id"], "error": map[string]any{"code": -32601, "message": "method not found"}})
			}
		default:
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()

	recordingProvider := &recordingMockProvider{response: "done"}
	cfg := config.DefaultConfig()
	cfg.Agents.Defaults.Workspace = tmpDir
	cfg.Agents.Defaults.Model = "test-model"
	cfg.Tools.MCP.Remote = map[string]config.RemoteMCPServerConfig{
		"laisky": {
			Type: "http",
			URL:  "https://mcp.laisky.com",
			Headers: map[string]string{
				"Authorization": "Bearer test-token",
			},
		},
	}

	al := NewAgentLoop(cfg, bus.NewMessageBus(), recordingProvider)
	require.NotNil(t, al.memoryMCP)
	al.memoryMCP.endpoint = server.URL

	defaultAgent := al.registry.GetDefaultAgent()
	require.NotNil(t, defaultAgent)

	response, err := al.runAgentLoop(context.Background(), defaultAgent, processOptions{
		SessionKey:      "session-1",
		Channel:         "telegram",
		ChatID:          "chat-1",
		UserMessage:     "hello memory",
		DefaultResponse: "empty",
		EnableSummary:   false,
		SendResponse:    false,
	})
	require.NoError(t, err)
	require.Equal(t, "done", response)

	require.NotEmpty(t, recordingProvider.lastMessages)
	systemPrompt := recordingProvider.lastMessages[0].Content
	require.NotContains(t, systemPrompt, "MCP Memory Recall")
	require.NotContains(t, systemPrompt, "FILE_MEMORY_SHOULD_NOT_APPEAR")

	var recallInserted bool
	for idx, msg := range recordingProvider.lastMessages {
		if msg.Role != "assistant" || !strings.Contains(msg.Content, "MCP Memory Recall") {
			continue
		}
		require.Contains(t, msg.Content, "project prefers concise updates")
		require.Less(t, idx, len(recordingProvider.lastMessages)-1)
		require.Equal(t, "user", recordingProvider.lastMessages[len(recordingProvider.lastMessages)-1].Role)
		recallInserted = true
	}
	require.True(t, recallInserted)

	mu.Lock()
	defer mu.Unlock()
	require.NotNil(t, capturedBeforeArgs)
	require.NotNil(t, capturedAfterArgs)
	require.Equal(t, capturedBeforeArgs["turn_id"], capturedAfterArgs["turn_id"])
	require.Equal(t, "session-1", capturedBeforeArgs["session_id"])
	require.Equal(t, "session-1", capturedAfterArgs["session_id"])
}

// TestRunAgentLoop_MemoryLifecycleUsesToolChatID verifies memory user scope uses stable ToolChatID in delegated planner runs.
// The t parameter controls assertions and returns no value.
func TestRunAgentLoop_MemoryLifecycleUsesToolChatID(t *testing.T) {
	tmpDir := t.TempDir()
	var mu sync.Mutex
	var capturedBeforeArgs map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		var req map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))

		method, _ := req["method"].(string)
		switch method {
		case "initialize":
			w.Header().Set("Mcp-Session-Id", "memory-session-2")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result":  map[string]any{"protocolVersion": "2024-11-05"},
			})
		case "tools/call":
			params, _ := req["params"].(map[string]any)
			name, _ := params["name"].(string)
			arguments, _ := params["arguments"].(map[string]any)

			switch name {
			case memoryBeforeToolName:
				mu.Lock()
				capturedBeforeArgs = arguments
				mu.Unlock()

				_ = json.NewEncoder(w).Encode(map[string]any{
					"jsonrpc": "2.0",
					"id":      req["id"],
					"result": map[string]any{
						"content": []map[string]any{{
							"type": "text",
							"text": `{"input_items":[{"type":"message","role":"user","content":[{"type":"input_text","text":"hello"}]}]}`,
						}},
					},
				})
			case memoryAfterToolName:
				_ = json.NewEncoder(w).Encode(map[string]any{
					"jsonrpc": "2.0",
					"id":      req["id"],
					"result": map[string]any{
						"content": []map[string]any{{
							"type": "text",
							"text": `{"ok":true}`,
						}},
					},
				})
			default:
				w.WriteHeader(http.StatusBadRequest)
			}
		default:
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()

	provider := &recordingMockProvider{response: "ok"}
	cfg := config.DefaultConfig()
	cfg.Agents.Defaults.Workspace = tmpDir
	cfg.Agents.Defaults.Model = "test-model"
	cfg.Tools.MCP.Remote = map[string]config.RemoteMCPServerConfig{
		"laisky": {
			Type: "http",
			URL:  "https://mcp.laisky.com",
			Headers: map[string]string{
				"Authorization": "Bearer test-token",
			},
		},
	}

	al := NewAgentLoop(cfg, bus.NewMessageBus(), provider)
	require.NotNil(t, al.memoryMCP)
	al.memoryMCP.endpoint = server.URL

	defaultAgent := al.registry.GetDefaultAgent()
	require.NotNil(t, defaultAgent)

	_, err := al.runAgentLoop(context.Background(), defaultAgent, processOptions{
		SessionKey:      "agent:main:pipeline:task-2026-02-26-1234",
		Channel:         plannerTaskChannel,
		ChatID:          "task-2026-02-26-1234",
		ToolChannel:     "telegram",
		ToolChatID:      "telegram-chat-42",
		UserMessage:     "hello",
		DefaultResponse: "empty",
		EnableSummary:   false,
		SendResponse:    false,
		NoHistory:       true,
	})
	require.NoError(t, err)

	mu.Lock()
	defer mu.Unlock()
	require.NotNil(t, capturedBeforeArgs)
	require.Equal(t, "telegram-chat-42", capturedBeforeArgs["user_id"])
}

// TestRunAgentLoop_MemoryBeforeTurnFailureKeepsRecentHistoryMessages verifies recent session turns are included as actual history messages when remote memory recall fails.
// The t parameter controls assertions and returns no value.
func TestRunAgentLoop_MemoryBeforeTurnFailureKeepsRecentHistoryMessages(t *testing.T) {
	tmpDir := t.TempDir()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		var req map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		method, _ := req["method"].(string)

		switch method {
		case "initialize":
			w.Header().Set("Mcp-Session-Id", "memory-session-3")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result":  map[string]any{"protocolVersion": "2024-11-05"},
			})
		case "tools/call":
			params, _ := req["params"].(map[string]any)
			name, _ := params["name"].(string)
			if name == memoryBeforeToolName {
				_ = json.NewEncoder(w).Encode(map[string]any{
					"jsonrpc": "2.0",
					"id":      req["id"],
					"result": map[string]any{
						"isError": true,
						"error": map[string]any{
							"code":      "internal_error",
							"message":   "simulated failure",
							"retryable": false,
						},
					},
				})
				return
			}

			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"content": []map[string]any{{
						"type": "text",
						"text": `{"ok":true}`,
					}},
				},
			})
		default:
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()

	provider := &recordingMockProvider{response: "ok"}
	cfg := config.DefaultConfig()
	cfg.Agents.Defaults.Workspace = tmpDir
	cfg.Agents.Defaults.Model = "test-model"
	cfg.Tools.MCP.Remote = map[string]config.RemoteMCPServerConfig{
		"laisky": {
			Type: "http",
			URL:  "https://mcp.laisky.com",
			Headers: map[string]string{
				"Authorization": "Bearer test-token",
			},
		},
	}

	al := NewAgentLoop(cfg, bus.NewMessageBus(), provider)
	require.NotNil(t, al.memoryMCP)
	al.memoryMCP.endpoint = server.URL

	defaultAgent := al.registry.GetDefaultAgent()
	require.NotNil(t, defaultAgent)
	sessionKey := "agent:main:direct:user-1"
	defaultAgent.Sessions.AddMessage(sessionKey, "user", "我住在加拿大渥太华的 Barrhaven")
	defaultAgent.Sessions.AddMessage(sessionKey, "assistant", "已记录：你住在加拿大渥太华的 Barrhaven。")

	_, err := al.runAgentLoop(context.Background(), defaultAgent, processOptions{
		SessionKey:      sessionKey,
		Channel:         "telegram",
		ChatID:          "chat-1",
		UserMessage:     "附近的 costco 有卖中式料理的料酒吗",
		DefaultResponse: "empty",
		EnableSummary:   false,
		SendResponse:    false,
		NoHistory:       false,
	})
	require.NoError(t, err)

	require.NotEmpty(t, provider.lastMessages)
	systemPrompt := provider.lastMessages[0].Content
	require.NotContains(t, systemPrompt, "Recent Session Context")
	require.NotContains(t, systemPrompt, "Local Context Fallback")

	var hasUserHistory bool
	var hasAssistantHistory bool
	for _, msg := range provider.lastMessages {
		if msg.Role == "user" && strings.Contains(msg.Content, "Barrhaven") {
			hasUserHistory = true
		}
		if msg.Role == "assistant" && strings.Contains(msg.Content, "已记录") {
			hasAssistantHistory = true
		}
	}
	require.True(t, hasUserHistory)
	require.True(t, hasAssistantHistory)
}

// TestRunAgentLoop_RecentHistoryLimitApplied verifies only recent N history messages are included in LLM request.
// The t parameter controls assertions and returns no value.
func TestRunAgentLoop_RecentHistoryLimitApplied(t *testing.T) {
	tmpDir := t.TempDir()
	provider := &recordingMockProvider{response: "ok"}

	cfg := config.DefaultConfig()
	cfg.Agents.Defaults.Workspace = tmpDir
	cfg.Agents.Defaults.Model = "test-model"
	cfg.Agents.Defaults.RecentHistoryLimit = 3

	al := NewAgentLoop(cfg, bus.NewMessageBus(), provider)
	defaultAgent := al.registry.GetDefaultAgent()
	require.NotNil(t, defaultAgent)

	sessionKey := "agent:main:direct:user-2"
	defaultAgent.Sessions.AddMessage(sessionKey, "user", "u1")
	defaultAgent.Sessions.AddMessage(sessionKey, "assistant", "a1")
	defaultAgent.Sessions.AddMessage(sessionKey, "user", "u2")
	defaultAgent.Sessions.AddMessage(sessionKey, "assistant", "a2")
	defaultAgent.Sessions.AddMessage(sessionKey, "user", "u3")

	_, err := al.runAgentLoop(context.Background(), defaultAgent, processOptions{
		SessionKey:      sessionKey,
		Channel:         "telegram",
		ChatID:          "chat-2",
		UserMessage:     "u4",
		DefaultResponse: "empty",
		EnableSummary:   false,
		SendResponse:    false,
		NoHistory:       false,
	})
	require.NoError(t, err)

	require.Len(t, provider.lastMessages, 5)
	require.Equal(t, "system", provider.lastMessages[0].Role)
	require.Equal(t, "user", provider.lastMessages[1].Role)
	require.Equal(t, "assistant", provider.lastMessages[2].Role)
	require.Equal(t, "user", provider.lastMessages[3].Role)
	require.Equal(t, "user", provider.lastMessages[4].Role)
	require.Equal(t, "u2", provider.lastMessages[1].Content)
	require.Equal(t, "a2", provider.lastMessages[2].Content)
	require.Equal(t, "u3", provider.lastMessages[3].Content)
	require.Equal(t, "u4", provider.lastMessages[4].Content)
}

// TestRunAgentLoop_EmptyDefaultResponseStillReturnsNonEmpty verifies global non-empty fallback when model and default output are blank.
// The t parameter controls assertions for response fallback stability.
// It returns no value and fails when an empty final response is returned.
func TestRunAgentLoop_EmptyDefaultResponseStillReturnsNonEmpty(t *testing.T) {
	tmpDir := t.TempDir()
	provider := &simpleMockProvider{response: "   "}
	agentInstance := &AgentInstance{
		ID:             "main",
		Model:          "test-model",
		MaxIterations:  1,
		MaxTokens:      1024,
		Temperature:    0,
		Provider:       provider,
		Sessions:       session.NewSessionManager(filepath.Join(tmpDir, "sessions")),
		ContextBuilder: NewContextBuilder(tmpDir),
		Tools:          tools.NewToolRegistry(),
	}

	al := &AgentLoop{bus: bus.NewMessageBus()}
	response, err := al.runAgentLoop(context.Background(), agentInstance, processOptions{
		SessionKey:      "agent:main:test:empty-default",
		Channel:         "telegram",
		ChatID:          "861999008",
		UserMessage:     "hello",
		DefaultResponse: "   ",
		EnableSummary:   false,
		SendResponse:    false,
		NoHistory:       true,
	})
	require.NoError(t, err)
	require.Equal(t, nonEmptyDefaultResponse, response)
}

// TestRunAgentLoop_ForceFinalTextAfterToolLoopExhaustion verifies loop exhaustion still yields textual output via no-tool finalization call.
// The t parameter controls test lifecycle and assertions.
// It returns no value and fails when final text is lost after max iterations with only tool calls.
func TestRunAgentLoop_ForceFinalTextAfterToolLoopExhaustion(t *testing.T) {
	tmpDir := t.TempDir()
	provider := &toolLoopExhaustionProvider{forcedFinalResponse: "Ottawa potholes can be reported via 311 web portal or phone."}
	agentInstance := &AgentInstance{
		ID:             "main",
		Model:          "test-model",
		MaxIterations:  1,
		MaxTokens:      1024,
		Temperature:    0,
		Provider:       provider,
		Sessions:       session.NewSessionManager(filepath.Join(tmpDir, "sessions")),
		ContextBuilder: NewContextBuilder(tmpDir),
		Tools:          tools.NewToolRegistry(),
	}

	al := &AgentLoop{bus: bus.NewMessageBus()}
	response, err := al.runAgentLoop(context.Background(), agentInstance, processOptions{
		SessionKey:      "agent:main:test:tool-loop-finalize",
		Channel:         plannerTaskChannel,
		ChatID:          "task-2026-02-26-0099",
		UserMessage:     "Research where to report road potholes in Ottawa",
		DefaultResponse: plannerEmptySummaryText,
		EnableSummary:   false,
		SendResponse:    false,
		NoHistory:       true,
	})
	require.NoError(t, err)
	require.True(t, provider.sawNoToolsCall)
	require.Equal(t, "Ottawa potholes can be reported via 311 web portal or phone.", response)
}

// TestRunAgentLoop_ForceFinalTextFallbackToDefault verifies fallback response is preserved when forced no-tool call still has empty content.
// The t parameter controls test lifecycle and assertions.
// It returns no value and fails when backward-compatible default fallback behavior regresses.
func TestRunAgentLoop_ForceFinalTextFallbackToDefault(t *testing.T) {
	tmpDir := t.TempDir()
	provider := &toolLoopExhaustionProvider{forcedFinalResponse: "   "}
	agentInstance := &AgentInstance{
		ID:             "main",
		Model:          "test-model",
		MaxIterations:  1,
		MaxTokens:      1024,
		Temperature:    0,
		Provider:       provider,
		Sessions:       session.NewSessionManager(filepath.Join(tmpDir, "sessions")),
		ContextBuilder: NewContextBuilder(tmpDir),
		Tools:          tools.NewToolRegistry(),
	}

	al := &AgentLoop{bus: bus.NewMessageBus()}
	response, err := al.runAgentLoop(context.Background(), agentInstance, processOptions{
		SessionKey:      "agent:main:test:tool-loop-fallback",
		Channel:         plannerTaskChannel,
		ChatID:          "task-2026-02-26-0100",
		UserMessage:     "Research where to report road potholes in Ottawa",
		DefaultResponse: plannerEmptySummaryText,
		EnableSummary:   false,
		SendResponse:    false,
		NoHistory:       true,
	})
	require.NoError(t, err)
	require.True(t, provider.sawNoToolsCall)
	require.Equal(t, plannerEmptySummaryText, response)
}

// TestBuildDelegationReference_IncludesSummaryHistoryMemory verifies delegated reference payload contains key context blocks.
// It seeds summary, recent messages, and long-term memory then validates formatted output.
// It returns no value and fails test on missing sections.
func TestBuildDelegationReference_IncludesSummaryHistoryMemory(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "delegation-reference-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	cb := NewContextBuilder(tmpDir)
	require.NoError(t, cb.memory.WriteLongTerm("User prefers concise status updates."))

	agent := &AgentInstance{ContextBuilder: cb}
	history := []providers.Message{
		{Role: "system", Content: "ignored"},
		{Role: "user", Content: "Please summarize deployment risk."},
		{Role: "assistant", Content: "I will inspect release notes first."},
		{Role: "tool", Content: "ignored tool output"},
	}

	ref := buildDelegationReference(agent, history, "Need deployment risk summary for today")
	require.Contains(t, ref, "Conversation summary:")
	require.Contains(t, ref, "Recent interaction history:")
	require.Contains(t, ref, "Memory highlights:")
	require.Contains(t, ref, "deployment risk")
	require.Contains(t, ref, "concise status updates")
	require.NotContains(t, ref, "ignored tool output")
}

// TestNewAgentLoop_BootstrapsConfiguredRemoteMCPServers verifies remote MCP config is persisted to workspace registry.
// The t parameter controls test lifecycle.
// It returns no value and fails the test on assertion errors.
func TestNewAgentLoop_BootstrapsConfiguredRemoteMCPServers(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := config.DefaultConfig()
	cfg.Agents.Defaults.Workspace = tmpDir
	cfg.Agents.Defaults.Model = "test-model"
	cfg.Tools.MCP.Remote = map[string]config.RemoteMCPServerConfig{
		"laisky": {
			Type: "http",
			URL:  "https://mcp.laisky.com",
			Headers: map[string]string{
				"Authorization": "Bearer test-token",
			},
		},
	}

	msgBus := bus.NewMessageBus()
	provider := &mockProvider{}
	_ = NewAgentLoop(cfg, msgBus, provider)

	registryPath := filepath.Join(tmpDir, "memory", "mcp_servers.json")
	data, err := os.ReadFile(registryPath)
	require.NoError(t, err)

	var payload struct {
		Servers []struct {
			Name    string            `json:"name"`
			URL     string            `json:"url"`
			Headers map[string]string `json:"headers"`
		} `json:"servers"`
	}
	require.NoError(t, json.Unmarshal(data, &payload))
	require.Len(t, payload.Servers, 1)
	require.Equal(t, "laisky", payload.Servers[0].Name)
	require.Equal(t, "https://mcp.laisky.com", payload.Servers[0].URL)
	require.Equal(t, "Bearer test-token", payload.Servers[0].Headers["Authorization"])
}

// TestNewAgentLoop_DoesNotPreRegisterLocalWebTools verifies web tools are not preloaded locally.
// The t parameter controls test lifecycle.
// It returns no value and fails when local web_search/web_fetch are registered before MCP discovery.
func TestNewAgentLoop_DoesNotPreRegisterLocalWebTools(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := config.DefaultConfig()
	cfg.Agents.Defaults.Workspace = tmpDir
	cfg.Agents.Defaults.Model = "test-model"

	msgBus := bus.NewMessageBus()
	provider := &mockProvider{}
	al := NewAgentLoop(cfg, msgBus, provider)

	defaultAgent := al.registry.GetDefaultAgent()
	require.NotNil(t, defaultAgent)

	_, hasWebSearch := defaultAgent.Tools.Get("web_search")
	_, hasWebFetch := defaultAgent.Tools.Get("web_fetch")
	require.False(t, hasWebSearch)
	require.False(t, hasWebFetch)
}

func TestRecordLastChannel(t *testing.T) {
	// Create temp workspace
	tmpDir, err := os.MkdirTemp("", "agent-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create test config
	cfg := &config.Config{
		Agents: config.AgentsConfig{
			Defaults: config.AgentDefaults{
				Workspace:         tmpDir,
				Model:             "test-model",
				MaxTokens:         4096,
				MaxToolIterations: 10,
			},
		},
	}

	// Create agent loop
	msgBus := bus.NewMessageBus()
	provider := &mockProvider{}
	al := NewAgentLoop(cfg, msgBus, provider)

	// Test RecordLastChannel
	testChannel := "test-channel"
	err = al.RecordLastChannel(testChannel)
	if err != nil {
		t.Fatalf("RecordLastChannel failed: %v", err)
	}

	// Verify channel was saved
	lastChannel := al.state.GetLastChannel()
	if lastChannel != testChannel {
		t.Errorf("Expected channel '%s', got '%s'", testChannel, lastChannel)
	}

	// Verify persistence by creating a new agent loop
	al2 := NewAgentLoop(cfg, msgBus, provider)
	if al2.state.GetLastChannel() != testChannel {
		t.Errorf("Expected persistent channel '%s', got '%s'", testChannel, al2.state.GetLastChannel())
	}
}

func TestRecordLastChatID(t *testing.T) {
	// Create temp workspace
	tmpDir, err := os.MkdirTemp("", "agent-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create test config
	cfg := &config.Config{
		Agents: config.AgentsConfig{
			Defaults: config.AgentDefaults{
				Workspace:         tmpDir,
				Model:             "test-model",
				MaxTokens:         4096,
				MaxToolIterations: 10,
			},
		},
	}

	// Create agent loop
	msgBus := bus.NewMessageBus()
	provider := &mockProvider{}
	al := NewAgentLoop(cfg, msgBus, provider)

	// Test RecordLastChatID
	testChatID := "test-chat-id-123"
	err = al.RecordLastChatID(testChatID)
	if err != nil {
		t.Fatalf("RecordLastChatID failed: %v", err)
	}

	// Verify chat ID was saved
	lastChatID := al.state.GetLastChatID()
	if lastChatID != testChatID {
		t.Errorf("Expected chat ID '%s', got '%s'", testChatID, lastChatID)
	}

	// Verify persistence by creating a new agent loop
	al2 := NewAgentLoop(cfg, msgBus, provider)
	if al2.state.GetLastChatID() != testChatID {
		t.Errorf("Expected persistent chat ID '%s', got '%s'", testChatID, al2.state.GetLastChatID())
	}
}

func TestNewAgentLoop_StateInitialized(t *testing.T) {
	// Create temp workspace
	tmpDir, err := os.MkdirTemp("", "agent-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create test config
	cfg := &config.Config{
		Agents: config.AgentsConfig{
			Defaults: config.AgentDefaults{
				Workspace:         tmpDir,
				Model:             "test-model",
				MaxTokens:         4096,
				MaxToolIterations: 10,
			},
		},
	}

	// Create agent loop
	msgBus := bus.NewMessageBus()
	provider := &mockProvider{}
	al := NewAgentLoop(cfg, msgBus, provider)

	// Verify state manager is initialized
	if al.state == nil {
		t.Error("Expected state manager to be initialized")
	}

	// Verify state directory was created
	stateDir := filepath.Join(tmpDir, "state")
	if _, err := os.Stat(stateDir); os.IsNotExist(err) {
		t.Error("Expected state directory to exist")
	}
}

// TestToolRegistry_ToolRegistration verifies tools can be registered and retrieved
func TestToolRegistry_ToolRegistration(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "agent-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := &config.Config{
		Agents: config.AgentsConfig{
			Defaults: config.AgentDefaults{
				Workspace:         tmpDir,
				Model:             "test-model",
				MaxTokens:         4096,
				MaxToolIterations: 10,
			},
		},
	}

	msgBus := bus.NewMessageBus()
	provider := &mockProvider{}
	al := NewAgentLoop(cfg, msgBus, provider)

	// Register a custom tool
	customTool := &mockCustomTool{}
	al.RegisterTool(customTool)

	// Verify tool is registered by checking it doesn't panic on GetStartupInfo
	// (actual tool retrieval is tested in tools package tests)
	info := al.GetStartupInfo()
	toolsInfo := info["tools"].(map[string]any)
	toolsList := toolsInfo["names"].([]string)

	// Check that our custom tool name is in the list
	found := false
	for _, name := range toolsList {
		if name == "mock_custom" {
			found = true
			break
		}
	}
	if !found {
		t.Error("Expected custom tool to be registered")
	}
}

// TestToolContext_Updates verifies tool context is updated with channel/chatID
func TestToolContext_Updates(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "agent-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := &config.Config{
		Agents: config.AgentsConfig{
			Defaults: config.AgentDefaults{
				Workspace:         tmpDir,
				Model:             "test-model",
				MaxTokens:         4096,
				MaxToolIterations: 10,
			},
		},
	}

	msgBus := bus.NewMessageBus()
	provider := &simpleMockProvider{response: "OK"}
	_ = NewAgentLoop(cfg, msgBus, provider)

	// Verify that ContextualTool interface is defined and can be implemented
	// This test validates the interface contract exists
	ctxTool := &mockContextualTool{}

	// Verify the tool implements the interface correctly
	var _ tools.ContextualTool = ctxTool
}

// TestToolRegistry_GetDefinitions verifies tool definitions can be retrieved
func TestToolRegistry_GetDefinitions(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "agent-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := &config.Config{
		Agents: config.AgentsConfig{
			Defaults: config.AgentDefaults{
				Workspace:         tmpDir,
				Model:             "test-model",
				MaxTokens:         4096,
				MaxToolIterations: 10,
			},
		},
	}

	msgBus := bus.NewMessageBus()
	provider := &mockProvider{}
	al := NewAgentLoop(cfg, msgBus, provider)

	// Register a test tool and verify it shows up in startup info
	testTool := &mockCustomTool{}
	al.RegisterTool(testTool)

	info := al.GetStartupInfo()
	toolsInfo := info["tools"].(map[string]any)
	toolsList := toolsInfo["names"].([]string)

	// Check that our custom tool name is in the list
	found := false
	for _, name := range toolsList {
		if name == "mock_custom" {
			found = true
			break
		}
	}
	if !found {
		t.Error("Expected custom tool to be registered")
	}
}

// TestAgentLoop_GetStartupInfo verifies startup info contains tools
func TestAgentLoop_GetStartupInfo(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "agent-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := &config.Config{
		Agents: config.AgentsConfig{
			Defaults: config.AgentDefaults{
				Workspace:         tmpDir,
				Model:             "test-model",
				MaxTokens:         4096,
				MaxToolIterations: 10,
			},
		},
	}

	msgBus := bus.NewMessageBus()
	provider := &mockProvider{}
	al := NewAgentLoop(cfg, msgBus, provider)

	info := al.GetStartupInfo()

	// Verify tools info exists
	toolsInfo, ok := info["tools"]
	if !ok {
		t.Fatal("Expected 'tools' key in startup info")
	}

	toolsMap, ok := toolsInfo.(map[string]any)
	if !ok {
		t.Fatal("Expected 'tools' to be a map")
	}

	count, ok := toolsMap["count"]
	if !ok {
		t.Fatal("Expected 'count' in tools info")
	}

	// Should have default tools registered
	if count.(int) == 0 {
		t.Error("Expected at least some tools to be registered")
	}
}

// TestAgentLoop_Stop verifies Stop() sets running to false
func TestAgentLoop_Stop(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "agent-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := &config.Config{
		Agents: config.AgentsConfig{
			Defaults: config.AgentDefaults{
				Workspace:         tmpDir,
				Model:             "test-model",
				MaxTokens:         4096,
				MaxToolIterations: 10,
			},
		},
	}

	msgBus := bus.NewMessageBus()
	provider := &mockProvider{}
	al := NewAgentLoop(cfg, msgBus, provider)

	// Note: running is only set to true when Run() is called
	// We can't test that without starting the event loop
	// Instead, verify the Stop method can be called safely
	al.Stop()

	// Verify running is false (initial state or after Stop)
	if al.running.Load() {
		t.Error("Expected agent to be stopped (or never started)")
	}
}

// Mock implementations for testing

type simpleMockProvider struct {
	response string
}

type toolLoopExhaustionProvider struct {
	forcedFinalResponse string
	callCount           int
	sawNoToolsCall      bool
}

type recordingMockProvider struct {
	response     string
	lastMessages []providers.Message
}

// Chat records the latest message list and returns static response text.
// The messages parameter contains provider prompt input for assertions, and return value is a successful response.
func (m *recordingMockProvider) Chat(
	ctx context.Context,
	messages []providers.Message,
	tools []providers.ToolDefinition,
	model string,
	opts map[string]any,
) (*providers.LLMResponse, error) {
	m.lastMessages = append([]providers.Message(nil), messages...)
	return &providers.LLMResponse{Content: m.response}, nil
}

// GetDefaultModel returns the mock provider default model name.
// It takes no parameters and returns a static model identifier.
func (m *recordingMockProvider) GetDefaultModel() string {
	return "mock-model"
}

func (m *simpleMockProvider) Chat(
	ctx context.Context,
	messages []providers.Message,
	tools []providers.ToolDefinition,
	model string,
	opts map[string]any,
) (*providers.LLMResponse, error) {
	return &providers.LLMResponse{
		Content:   m.response,
		ToolCalls: []providers.ToolCall{},
	}, nil
}

func (m *simpleMockProvider) GetDefaultModel() string {
	return "mock-model"
}

// Chat returns perpetual tool calls for normal iterations and a textual answer for forced no-tool finalization.
// The ctx, messages, model, and opts parameters satisfy provider interface requirements for test execution.
// It returns a tool call when tools are allowed and forcedFinalResponse when tools are disabled.
func (m *toolLoopExhaustionProvider) Chat(
	ctx context.Context,
	messages []providers.Message,
	tools []providers.ToolDefinition,
	model string,
	opts map[string]any,
) (*providers.LLMResponse, error) {
	m.callCount++
	if len(tools) == 0 {
		m.sawNoToolsCall = true
		return &providers.LLMResponse{Content: m.forcedFinalResponse}, nil
	}

	return &providers.LLMResponse{
		Content: "",
		ToolCalls: []providers.ToolCall{{
			ID:        "loop-tool-1",
			Type:      "function",
			Name:      "unknown_tool",
			Arguments: map[string]any{},
		}},
	}, nil
}

// GetDefaultModel returns a static model name for this test provider.
// It accepts no parameters and returns deterministic model metadata.
func (m *toolLoopExhaustionProvider) GetDefaultModel() string {
	return "mock-model"
}

// mockCustomTool is a simple mock tool for registration testing
type mockCustomTool struct{}

func (m *mockCustomTool) Name() string {
	return "mock_custom"
}

func (m *mockCustomTool) Description() string {
	return "Mock custom tool for testing"
}

func (m *mockCustomTool) Parameters() map[string]any {
	return map[string]any{
		"type":       "object",
		"properties": map[string]any{},
	}
}

func (m *mockCustomTool) Execute(ctx context.Context, args map[string]any) *tools.ToolResult {
	return tools.SilentResult("Custom tool executed")
}

// mockToolWithName is a mock tool with a configurable tool name.
type mockToolWithName struct {
	name string
}

// Name returns the configured tool name.
// It accepts no parameters and returns the deterministic registration identifier.
func (m *mockToolWithName) Name() string {
	return m.name
}

// Description returns a generic mock tool description.
// It accepts no parameters and returns a static test string.
func (m *mockToolWithName) Description() string {
	return "Mock configurable tool for testing"
}

// Parameters returns an empty object schema.
// It accepts no parameters and returns a minimal tool parameter definition.
func (m *mockToolWithName) Parameters() map[string]any {
	return map[string]any{
		"type":       "object",
		"properties": map[string]any{},
	}
}

// Execute returns a static success result.
// The ctx and args parameters are unused in this mock implementation.
// It returns a non-error tool result.
func (m *mockToolWithName) Execute(ctx context.Context, args map[string]any) *tools.ToolResult {
	return tools.SilentResult("Named mock tool executed")
}

// mockContextualTool tracks context updates
type mockContextualTool struct {
	lastChannel string
	lastChatID  string
}

// mockMessageContextTool tracks context updates for the message tool slot.
type mockMessageContextTool struct {
	lastChannel      string
	lastChatID       string
	execChannel      string
	execChatID       string
	executeCallCount int
}

func (m *mockContextualTool) Name() string {
	return "mock_contextual"
}

func (m *mockContextualTool) Description() string {
	return "Mock contextual tool"
}

func (m *mockContextualTool) Parameters() map[string]any {
	return map[string]any{
		"type":       "object",
		"properties": map[string]any{},
	}
}

func (m *mockContextualTool) Execute(ctx context.Context, args map[string]any) *tools.ToolResult {
	return tools.SilentResult("Contextual tool executed")
}

func (m *mockContextualTool) SetContext(channel, chatID string) {
	m.lastChannel = channel
	m.lastChatID = chatID
}

// Name returns the registered tool name.
// It accepts no parameters and returns the tool identifier.
func (m *mockMessageContextTool) Name() string {
	return "message"
}

// Description describes the test tool behavior.
// It accepts no parameters and returns a static description.
func (m *mockMessageContextTool) Description() string {
	return "Mock message contextual tool"
}

// Parameters returns JSON schema for test tool arguments.
// It accepts no parameters and returns an empty object schema.
func (m *mockMessageContextTool) Parameters() map[string]any {
	return map[string]any{
		"type":       "object",
		"properties": map[string]any{},
	}
}

// Execute returns a silent result for this mock.
// The ctx and args parameters are ignored in this test implementation.
// It returns a successful silent tool result.
func (m *mockMessageContextTool) Execute(ctx context.Context, args map[string]any) *tools.ToolResult {
	m.execChannel = m.lastChannel
	m.execChatID = m.lastChatID
	m.executeCallCount++
	return tools.SilentResult("ok")
}

// SetContext captures the last channel/chat pair passed by the agent runtime.
// The channel and chatID parameters are stored for assertions and no value is returned.
func (m *mockMessageContextTool) SetContext(channel, chatID string) {
	m.lastChannel = channel
	m.lastChatID = chatID
}

// testHelper executes a message and returns the response
type testHelper struct {
	al *AgentLoop
}

func (h testHelper) executeAndGetResponse(tb testing.TB, ctx context.Context, msg bus.InboundMessage) string {
	// Use a short timeout to avoid hanging
	timeoutCtx, cancel := context.WithTimeout(ctx, responseTimeout)
	defer cancel()

	response, err := h.al.processMessage(timeoutCtx, msg)
	if err != nil {
		tb.Fatalf("processMessage failed: %v", err)
	}
	return response
}

const responseTimeout = 3 * time.Second

// TestRunAgentLoop_UsesToolContextOverrides verifies planner runtime can keep internal session IDs while tools target user chat.
// It runs one loop iteration and asserts message tool context receives ToolChannel/ToolChatID values.
// It returns no value and fails when override routing is not applied.
func TestRunAgentLoop_UsesToolContextOverrides(t *testing.T) {
	tmpDir := t.TempDir()
	provider := &simpleMockProvider{response: "done"}
	messageTool := &mockMessageContextTool{}

	agentInstance := &AgentInstance{
		ID:             "main",
		Model:          "test-model",
		MaxIterations:  1,
		MaxTokens:      1024,
		Temperature:    0,
		Provider:       provider,
		Sessions:       session.NewSessionManager(filepath.Join(tmpDir, "sessions")),
		ContextBuilder: NewContextBuilder(tmpDir),
		Tools:          tools.NewToolRegistry(),
	}
	agentInstance.Tools.Register(messageTool)

	al := &AgentLoop{bus: bus.NewMessageBus()}
	_, err := al.runAgentLoop(context.Background(), agentInstance, processOptions{
		SessionKey:      "agent:main:pipeline:task-2026-02-26-0014",
		Channel:         plannerTaskChannel,
		ChatID:          "task-2026-02-26-0014",
		ToolChannel:     "telegram",
		ToolChatID:      "861999008",
		UserMessage:     "send screenshot",
		DefaultResponse: "empty",
		EnableSummary:   false,
		SendResponse:    false,
		NoHistory:       true,
	})
	require.NoError(t, err)
	require.Equal(t, "telegram", messageTool.lastChannel)
	require.Equal(t, "861999008", messageTool.lastChatID)
}

type oneToolCallThenDoneProvider struct {
	called int
}

// Chat returns one message tool call on first turn, then a final textual response.
// The messages, tools, model, and opts parameters are accepted to satisfy the provider contract.
// It returns a deterministic response sequence for tool-context regression tests.
func (m *oneToolCallThenDoneProvider) Chat(
	ctx context.Context,
	messages []providers.Message,
	tools []providers.ToolDefinition,
	model string,
	opts map[string]any,
) (*providers.LLMResponse, error) {
	m.called++
	if m.called == 1 {
		return &providers.LLMResponse{
			ToolCalls: []providers.ToolCall{
				{
					ID:   "tool-1",
					Type: "function",
					Name: "message",
					Arguments: map[string]any{
						"content": "hello",
					},
				},
			},
		}, nil
	}

	return &providers.LLMResponse{Content: "done"}, nil
}

// GetDefaultModel returns the provider's static default model identifier.
// It accepts no parameters and returns a deterministic value.
func (m *oneToolCallThenDoneProvider) GetDefaultModel() string {
	return "mock-model"
}

type fixedToolCallThenDoneProvider struct {
	called   int
	toolCall providers.ToolCall
}

// Chat returns the configured tool call on first turn, then a final textual response.
// The parameters satisfy provider contract and are not otherwise used in this test provider.
// It returns deterministic responses to validate planner-to-message behavior.
func (m *fixedToolCallThenDoneProvider) Chat(
	ctx context.Context,
	messages []providers.Message,
	tools []providers.ToolDefinition,
	model string,
	opts map[string]any,
) (*providers.LLMResponse, error) {
	m.called++
	if m.called == 1 {
		return &providers.LLMResponse{ToolCalls: []providers.ToolCall{m.toolCall}}, nil
	}

	return &providers.LLMResponse{Content: "done"}, nil
}

// GetDefaultModel returns a static model identifier for this test provider.
// It accepts no parameters and always returns a deterministic model name.
func (m *fixedToolCallThenDoneProvider) GetDefaultModel() string {
	return "mock-model"
}

// TestRunAgentLoop_ExecutesToolCallsWithOverrideContext verifies execution-time contextual injection
// uses ToolChannel/ToolChatID instead of internal planner channel/task identifiers.
// It runs one real tool-call turn and asserts the message tool observed external Telegram context in Execute.
func TestRunAgentLoop_ExecutesToolCallsWithOverrideContext(t *testing.T) {
	tmpDir := t.TempDir()
	provider := &oneToolCallThenDoneProvider{}
	messageTool := &mockMessageContextTool{}

	agentInstance := &AgentInstance{
		ID:             "main",
		Model:          "test-model",
		MaxIterations:  2,
		MaxTokens:      1024,
		Temperature:    0,
		Provider:       provider,
		Sessions:       session.NewSessionManager(filepath.Join(tmpDir, "sessions")),
		ContextBuilder: NewContextBuilder(tmpDir),
		Tools:          tools.NewToolRegistry(),
	}
	agentInstance.Tools.Register(messageTool)

	al := &AgentLoop{bus: bus.NewMessageBus()}
	_, err := al.runAgentLoop(context.Background(), agentInstance, processOptions{
		SessionKey:      "agent:main:pipeline:task-2026-02-26-0037",
		Channel:         plannerTaskChannel,
		ChatID:          "task-2026-02-26-0037",
		ToolChannel:     "telegram",
		ToolChatID:      "861999008",
		UserMessage:     "send screenshot",
		DefaultResponse: "empty",
		EnableSummary:   false,
		SendResponse:    false,
		NoHistory:       true,
	})
	require.NoError(t, err)
	require.Equal(t, 1, messageTool.executeCallCount)
	require.Equal(t, "telegram", messageTool.execChannel)
	require.Equal(t, "861999008", messageTool.execChatID)
}

// TestRunAgentLoop_Behavior_RemapsPipelineTaskIDForTelegramAttachments verifies end-to-end planner behavior
// where model emits Telegram channel plus leaked task chat ID while sending attachments.
// It ensures outbound delivery still targets user chat injected by ToolChannel/ToolChatID overrides.
func TestRunAgentLoop_Behavior_RemapsPipelineTaskIDForTelegramAttachments(t *testing.T) {
	tmpDir := t.TempDir()
	provider := &fixedToolCallThenDoneProvider{toolCall: providers.ToolCall{
		ID:   "tool-attachment-1",
		Type: "function",
		Name: "message",
		Arguments: map[string]any{
			"content": "Here are your screenshots",
			"channel": "telegram",
			"chat_id": "task-2026-02-26-0037",
			"attachments": []any{
				map[string]any{"type": "photo", "path": "/tmp/a.png"},
			},
		},
	}}

	var sent bus.OutboundMessage
	messageTool := tools.NewMessageTool()
	messageTool.SetSendCallback(func(msg bus.OutboundMessage) error {
		sent = msg
		return nil
	})

	agentInstance := &AgentInstance{
		ID:             "main",
		Model:          "test-model",
		MaxIterations:  2,
		MaxTokens:      1024,
		Temperature:    0,
		Provider:       provider,
		Sessions:       session.NewSessionManager(filepath.Join(tmpDir, "sessions")),
		ContextBuilder: NewContextBuilder(tmpDir),
		Tools:          tools.NewToolRegistry(),
	}
	agentInstance.Tools.Register(messageTool)

	al := &AgentLoop{bus: bus.NewMessageBus()}
	_, err := al.runAgentLoop(context.Background(), agentInstance, processOptions{
		SessionKey:      "agent:main:pipeline:task-2026-02-26-0037",
		Channel:         plannerTaskChannel,
		ChatID:          "task-2026-02-26-0037",
		ToolChannel:     "telegram",
		ToolChatID:      "861999008",
		UserMessage:     "send screenshots",
		DefaultResponse: "empty",
		EnableSummary:   false,
		SendResponse:    false,
		NoHistory:       true,
	})
	require.NoError(t, err)
	require.Equal(t, "telegram", sent.Channel)
	require.Equal(t, "861999008", sent.ChatID)
	require.Len(t, sent.Attachments, 1)
}

// TestRunAgentLoop_Behavior_RemapsPipelineTaskIDForTelegramButtons verifies end-to-end planner behavior
// where model emits Telegram buttons with leaked task chat ID.
// It ensures outbound delivery targets user chat and keeps all button payloads.
func TestRunAgentLoop_Behavior_RemapsPipelineTaskIDForTelegramButtons(t *testing.T) {
	tmpDir := t.TempDir()
	provider := &fixedToolCallThenDoneProvider{toolCall: providers.ToolCall{
		ID:   "tool-button-1",
		Type: "function",
		Name: "message",
		Arguments: map[string]any{
			"content": "Here are some demo buttons you can press:",
			"channel": "telegram",
			"chat_id": "task-2026-02-26-0038",
			"buttons": []any{
				map[string]any{"text": "Button 1", "callback_data": "demo_btn_1", "row": float64(0)},
				map[string]any{"text": "Button 2", "callback_data": "demo_btn_2", "row": float64(0)},
				map[string]any{"text": "Button 3", "callback_data": "demo_btn_3", "row": float64(1)},
			},
		},
	}}

	var sent bus.OutboundMessage
	messageTool := tools.NewMessageTool()
	messageTool.SetSendCallback(func(msg bus.OutboundMessage) error {
		sent = msg
		return nil
	})

	agentInstance := &AgentInstance{
		ID:             "main",
		Model:          "test-model",
		MaxIterations:  2,
		MaxTokens:      1024,
		Temperature:    0,
		Provider:       provider,
		Sessions:       session.NewSessionManager(filepath.Join(tmpDir, "sessions")),
		ContextBuilder: NewContextBuilder(tmpDir),
		Tools:          tools.NewToolRegistry(),
	}
	agentInstance.Tools.Register(messageTool)

	al := &AgentLoop{bus: bus.NewMessageBus()}
	_, err := al.runAgentLoop(context.Background(), agentInstance, processOptions{
		SessionKey:      "agent:main:pipeline:task-2026-02-26-0038",
		Channel:         plannerTaskChannel,
		ChatID:          "task-2026-02-26-0038",
		ToolChannel:     "telegram",
		ToolChatID:      "861999008",
		UserMessage:     "send buttons",
		DefaultResponse: "empty",
		EnableSummary:   false,
		SendResponse:    false,
		NoHistory:       true,
	})
	require.NoError(t, err)
	require.Equal(t, "telegram", sent.Channel)
	require.Equal(t, "861999008", sent.ChatID)
	require.Len(t, sent.Buttons, 3)
}

// TestRunAgentLoop_SanitizesSyntheticMediaMarkerWithoutMessageSend verifies fake media markers are not forwarded to users.
// It runs a direct-answer loop where no message tool send occurs and asserts outbound text is sanitized.
// It returns no value and fails when placeholder marker leaks to outbound channel.
func TestRunAgentLoop_SanitizesSyntheticMediaMarkerWithoutMessageSend(t *testing.T) {
	tmpDir := t.TempDir()
	provider := &simpleMockProvider{response: "[Sending image]"}
	msgBus := bus.NewMessageBus()

	agentInstance := &AgentInstance{
		ID:             "main",
		Model:          "test-model",
		MaxIterations:  1,
		MaxTokens:      1024,
		Temperature:    0,
		Provider:       provider,
		Sessions:       session.NewSessionManager(filepath.Join(tmpDir, "sessions")),
		ContextBuilder: NewContextBuilder(tmpDir),
		Tools:          tools.NewToolRegistry(),
	}
	agentInstance.Tools.Register(tools.NewMessageTool())

	al := &AgentLoop{bus: msgBus}
	_, err := al.runAgentLoop(context.Background(), agentInstance, processOptions{
		SessionKey:      "agent:main:test:marker",
		Channel:         "telegram",
		ChatID:          "861999008",
		UserMessage:     "take screenshot",
		DefaultResponse: "empty",
		EnableSummary:   false,
		SendResponse:    true,
		NoHistory:       true,
	})
	require.NoError(t, err)

	readCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	outbound, ok := msgBus.SubscribeOutbound(readCtx)
	require.True(t, ok)
	require.Equal(t, "telegram", outbound.Channel)
	require.NotContains(t, outbound.Content, "[Sending image]")
	require.Contains(t, outbound.Content, "could not send an attachment")
}

// TestToolResult_SilentToolDoesNotSendUserMessage verifies silent tools don't trigger outbound
func TestToolResult_SilentToolDoesNotSendUserMessage(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "agent-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := &config.Config{
		Agents: config.AgentsConfig{
			Defaults: config.AgentDefaults{
				Workspace:         tmpDir,
				Model:             "test-model",
				MaxTokens:         4096,
				MaxToolIterations: 10,
			},
		},
	}

	msgBus := bus.NewMessageBus()
	provider := &simpleMockProvider{response: "File operation complete"}
	al := NewAgentLoop(cfg, msgBus, provider)
	helper := testHelper{al: al}

	// ReadFileTool returns SilentResult, which should not send user message
	ctx := context.Background()
	msg := bus.InboundMessage{
		Channel:    "test",
		SenderID:   "user1",
		ChatID:     "chat1",
		Content:    "read test.txt",
		SessionKey: "test-session",
	}

	response := helper.executeAndGetResponse(t, ctx, msg)

	// Silent tool should return the LLM's response directly
	if response != "File operation complete" {
		t.Errorf("Expected 'File operation complete', got: %s", response)
	}
}

// TestToolResult_UserFacingToolDoesSendMessage verifies user-facing tools trigger outbound
func TestToolResult_UserFacingToolDoesSendMessage(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "agent-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := &config.Config{
		Agents: config.AgentsConfig{
			Defaults: config.AgentDefaults{
				Workspace:         tmpDir,
				Model:             "test-model",
				MaxTokens:         4096,
				MaxToolIterations: 10,
			},
		},
	}

	msgBus := bus.NewMessageBus()
	provider := &simpleMockProvider{response: "Command output: hello world"}
	al := NewAgentLoop(cfg, msgBus, provider)
	helper := testHelper{al: al}

	// ExecTool returns UserResult, which should send user message
	ctx := context.Background()
	msg := bus.InboundMessage{
		Channel:    "test",
		SenderID:   "user1",
		ChatID:     "chat1",
		Content:    "run hello",
		SessionKey: "test-session",
	}

	response := helper.executeAndGetResponse(t, ctx, msg)

	// User-facing tool should include the output in final response
	if response != "Command output: hello world" {
		t.Errorf("Expected 'Command output: hello world', got: %s", response)
	}
}

// failFirstMockProvider fails on the first N calls with a specific error
type failFirstMockProvider struct {
	failures    int
	currentCall int
	failError   error
	successResp string
}

func (m *failFirstMockProvider) Chat(
	ctx context.Context,
	messages []providers.Message,
	tools []providers.ToolDefinition,
	model string,
	opts map[string]any,
) (*providers.LLMResponse, error) {
	m.currentCall++
	if m.currentCall <= m.failures {
		return nil, m.failError
	}
	return &providers.LLMResponse{
		Content:   m.successResp,
		ToolCalls: []providers.ToolCall{},
	}, nil
}

func (m *failFirstMockProvider) GetDefaultModel() string {
	return "mock-fail-model"
}

// overflowRecoveryMockProvider simulates context overflow on the first main call.
// It also handles emergency compression prompts and then succeeds on retry.
type overflowRecoveryMockProvider struct {
	mainCalls          int
	compressionCalls   int
	compressionSummary string
	compressed         bool
}

// Chat returns context overflow for the first main call, handles compression prompt,
// and then returns a successful direct answer on retry.
// The messages parameter is inspected by marker text and tools is used to detect main calls.
func (m *overflowRecoveryMockProvider) Chat(
	_ context.Context,
	messages []providers.Message,
	tools []providers.ToolDefinition,
	_ string,
	_ map[string]any,
) (*providers.LLMResponse, error) {
	if len(tools) == 0 && len(messages) >= 2 && strings.Contains(messages[1].Content, "EMERGENCY_CONTEXT_COMPRESSION") {
		m.compressionCalls++
		m.compressed = true
		return &providers.LLMResponse{Content: m.compressionSummary}, nil
	}

	m.mainCalls++
	if !m.compressed {
		return nil, fmt.Errorf("context_length_exceeded: Please reduce the length of the messages or completion")
	}

	return &providers.LLMResponse{Content: "Recovered after emergency compression"}, nil
}

// GetDefaultModel returns deterministic mock model name.
// The method takes no parameters and always returns a static string.
func (m *overflowRecoveryMockProvider) GetDefaultModel() string {
	return "mock-overflow-model"
}

// TestAgentLoop_ContextExhaustionRetry verify that the agent retries on context errors
func TestAgentLoop_ContextExhaustionRetry(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "agent-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := &config.Config{
		Agents: config.AgentsConfig{
			Defaults: config.AgentDefaults{
				Workspace:         tmpDir,
				Model:             "test-model",
				MaxTokens:         4096,
				MaxToolIterations: 10,
			},
		},
	}

	msgBus := bus.NewMessageBus()

	// Create a provider that fails once with a context error
	contextErr := fmt.Errorf("InvalidParameter: Total tokens of image and text exceed max message tokens")
	provider := &failFirstMockProvider{
		failures:    1,
		failError:   contextErr,
		successResp: "Recovered from context error",
	}

	al := NewAgentLoop(cfg, msgBus, provider)

	// Inject some history to simulate a full context
	sessionKey := "test-session-context"
	// Create dummy history
	history := []providers.Message{
		{Role: "system", Content: "System prompt"},
		{Role: "user", Content: "Old message 1"},
		{Role: "assistant", Content: "Old response 1"},
		{Role: "user", Content: "Old message 2"},
		{Role: "assistant", Content: "Old response 2"},
		{Role: "user", Content: "Trigger message"},
	}
	defaultAgent := al.registry.GetDefaultAgent()
	if defaultAgent == nil {
		t.Fatal("No default agent found")
	}
	defaultAgent.Sessions.SetHistory(sessionKey, history)

	// Call ProcessDirectWithChannel
	// Note: ProcessDirectWithChannel calls processMessage which will execute runLLMIteration
	response, err := al.ProcessDirectWithChannel(
		context.Background(),
		"Trigger message",
		sessionKey,
		"test",
		"test-chat",
	)
	if err != nil {
		t.Fatalf("Expected success after retry, got error: %v", err)
	}

	if response != "Recovered from context error" {
		t.Errorf("Expected 'Recovered from context error', got '%s'", response)
	}

	// We expect 2 calls: 1st failed, 2nd succeeded
	if provider.currentCall != 2 {
		t.Errorf("Expected 2 calls (1 fail + 1 success), got %d", provider.currentCall)
	}

	// Check final history length
	finalHistory := defaultAgent.Sessions.GetHistory(sessionKey)
	// We verify that the history has been modified (compressed)
	// Original length: 6
	// Expected behavior: compression drops ~50% of history (mid slice)
	// We can assert that the length is NOT what it would be without compression.
	// Without compression: 6 + 1 (new user msg) + 1 (assistant msg) = 8
	if len(finalHistory) >= 8 {
		t.Errorf("Expected history to be compressed (len < 8), got %d", len(finalHistory))
	}
}

// TestAgentLoop_ContextExhaustionRetry_UsesEmergencyLLMCompression verifies overflow
// handling can recover by requesting a bounded memory summary and trimming oversized history.
// The test creates very large tool output in history and asserts recovery path behavior.
func TestAgentLoop_ContextExhaustionRetry_UsesEmergencyLLMCompression(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "agent-test-overflow-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	cfg := &config.Config{
		Agents: config.AgentsConfig{
			Defaults: config.AgentDefaults{
				Workspace:         tmpDir,
				Model:             "test-model",
				MaxTokens:         4096,
				MaxToolIterations: 10,
			},
		},
	}

	msgBus := bus.NewMessageBus()
	provider := &overflowRecoveryMockProvider{compressionSummary: "compressed memory summary with key goals and pending steps"}
	al := NewAgentLoop(cfg, msgBus, provider)

	defaultAgent := al.registry.GetDefaultAgent()
	require.NotNil(t, defaultAgent)

	largeToolOutput := strings.Repeat("<html>very large tool output block</html>", 900)
	sessionKey := "agent:main:main"
	defaultAgent.Sessions.SetHistory(sessionKey, []providers.Message{
		{Role: "user", Content: "Need article analysis"},
		{Role: "assistant", Content: "Fetching page..."},
		{Role: "tool", Content: largeToolOutput},
		{Role: "assistant", Content: "Fetched a lot of content"},
		{Role: "tool", Content: largeToolOutput},
	})

	response, runErr := al.ProcessDirectWithChannel(
		context.Background(),
		"Please summarize the article and keep key points only.",
		"test-overflow-recovery",
		"test",
		"chat-overflow",
	)
	require.NoError(t, runErr)
	require.Equal(t, "Recovered after emergency compression", response)
	require.GreaterOrEqual(t, provider.mainCalls, 2)
	require.GreaterOrEqual(t, provider.compressionCalls, 1)

	summary := defaultAgent.Sessions.GetSummary(sessionKey)
	require.Contains(t, summary, "compressed memory summary")

	history := defaultAgent.Sessions.GetHistory(sessionKey)
	require.LessOrEqual(t, len(history), 5)
	for _, msg := range history {
		require.LessOrEqual(t, utf8.RuneCountInString(msg.Content), 1300)
	}
}

// blockingMockProvider blocks Chat calls to detect unwanted foreground LLM usage.
// The wait parameter defines simulated delay and callCount records invocation count.
// It returns a static response after waiting unless context is canceled.
type blockingMockProvider struct {
	wait      time.Duration
	callCount atomic.Int32
}

// Chat blocks for a fixed duration to emulate a slow upstream model call.
// The ctx parameter may cancel waiting and remaining parameters are ignored in this mock.
// It returns either context error or a static mock response.
func (m *blockingMockProvider) Chat(
	ctx context.Context,
	_ []providers.Message,
	_ []providers.ToolDefinition,
	_ string,
	_ map[string]any,
) (*providers.LLMResponse, error) {
	m.callCount.Add(1)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(m.wait):
		return &providers.LLMResponse{Content: "blocked-call-finished"}, nil
	}
}

// GetDefaultModel returns a static model identifier for tests.
// It accepts no parameters and returns mock model name.
func (m *blockingMockProvider) GetDefaultModel() string {
	return "blocking-mock-model"
}

// Calls returns number of Chat invocations observed by this provider.
// It accepts no parameters and returns call counter value.
func (m *blockingMockProvider) Calls() int32 {
	return m.callCount.Load()
}

// TestProcessMessage_ExternalChannelsDelegateWithoutForegroundLLM verifies chat channels enqueue directly to planner pipeline.
// The test parameter exercises multiple external channels.
// It returns no value and fails when delegation blocks on provider Chat.
func TestProcessMessage_ExternalChannelsDelegateWithoutForegroundLLM(t *testing.T) {
	t.Parallel()

	channels := []string{"telegram", "slack", "discord"}
	for _, channelName := range channels {
		t.Run(channelName, func(t *testing.T) {
			tmpDir, err := os.MkdirTemp("", "agent-delegate-fast-*")
			require.NoError(t, err)
			defer os.RemoveAll(tmpDir)

			cfg := &config.Config{
				Agents: config.AgentsConfig{
					Defaults: config.AgentDefaults{
						Workspace:         tmpDir,
						Model:             "test-model",
						MaxTokens:         4096,
						MaxToolIterations: 10,
					},
				},
			}

			msgBus := bus.NewMessageBus()
			provider := &blockingMockProvider{wait: 2 * time.Second}
			al := NewAgentLoop(cfg, msgBus, provider)

			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()

			response, runErr := al.processMessage(ctx, bus.InboundMessage{
				Channel:  channelName,
				SenderID: "user-1",
				ChatID:   "chat-1",
				Content:  "Please investigate this issue thoroughly and execute a full solution plan.",
			})
			require.NoError(t, runErr)
			require.Contains(t, response, "accepted. I started it in the background")
			require.EqualValues(t, 1, provider.Calls(), "task creation should call provider once for task brief generation")
			require.True(t, strings.Contains(strings.ToLower(response), "task "))
			require.Regexp(t, regexp.MustCompile(`task-\d{4}-\d{2}-\d{2}-\d{4}\([^)]+\)`), response)
		})
	}
}
