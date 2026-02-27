package tools

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestRemoteMCPToolBootstrapConfiguredServers verifies config-driven remote servers are persisted.
// The t parameter controls test lifecycle.
// It returns no value and fails the test on assertion errors.
func TestRemoteMCPToolBootstrapConfiguredServers(t *testing.T) {
	workspace := t.TempDir()
	tool := NewRemoteMCPTool(workspace)

	err := tool.BootstrapConfiguredServers([]ConfiguredRemoteMCPServer{
		{
			Name: "laisky",
			Type: "http",
			URL:  "https://mcp.laisky.com",
			Headers: map[string]string{
				"Authorization": "Bearer abc",
			},
		},
	})
	require.NoError(t, err)

	listResult := tool.Execute(context.Background(), map[string]any{
		"operation": "list_servers",
	})
	require.False(t, listResult.IsError)
	require.Contains(t, listResult.ForUser, "laisky")
	require.Contains(t, listResult.ForUser, "https://mcp.laisky.com")
}

// TestRemoteMCPToolBootstrapConfiguredServersRejectsUnsupportedType verifies only HTTP transport is accepted.
// The t parameter controls test lifecycle.
// It returns no value and fails the test on assertion errors.
func TestRemoteMCPToolBootstrapConfiguredServersRejectsUnsupportedType(t *testing.T) {
	tool := NewRemoteMCPTool(t.TempDir())
	err := tool.BootstrapConfiguredServers([]ConfiguredRemoteMCPServer{
		{
			Name: "bad",
			Type: "stdio",
			URL:  "https://example.com/mcp",
		},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "only \"http\" is supported")
}

// TestRemoteMCPToolRegistryLifecycle verifies add/list/remove server operations persist correctly.
// The t parameter controls test lifecycle.
// It returns no value and fails the test on assertion errors.
func TestRemoteMCPToolRegistryLifecycle(t *testing.T) {
	workspace := t.TempDir()
	tool := NewRemoteMCPTool(workspace)

	addResult := tool.Execute(context.Background(), map[string]any{
		"operation": "add_server",
		"name":      "demo",
		"url":       "https://example.com/mcp",
	})
	require.False(t, addResult.IsError)
	require.Contains(t, addResult.ForUser, "added")

	listResult := tool.Execute(context.Background(), map[string]any{
		"operation": "list_servers",
	})
	require.False(t, listResult.IsError)
	require.Contains(t, listResult.ForUser, "demo")
	require.Contains(t, listResult.ForUser, "https://example.com/mcp")

	registryPath := filepath.Join(workspace, "memory", "mcp_servers.json")
	raw, err := os.ReadFile(registryPath)
	require.NoError(t, err)
	require.Contains(t, string(raw), "demo")

	removeResult := tool.Execute(context.Background(), map[string]any{
		"operation": "remove_server",
		"name":      "demo",
	})
	require.False(t, removeResult.IsError)
	require.Contains(t, removeResult.ForUser, "Removed")

	afterRemove := tool.Execute(context.Background(), map[string]any{
		"operation": "list_servers",
	})
	require.False(t, afterRemove.IsError)
	require.Contains(t, afterRemove.ForUser, "No remote MCP servers")
}

// TestRemoteMCPToolListAndCall verifies list_tools and call_tool against a mock MCP endpoint.
// The t parameter controls test lifecycle.
// It returns no value and fails the test on assertion errors.
func TestRemoteMCPToolListAndCall(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		var req map[string]any
		err := json.NewDecoder(r.Body).Decode(&req)
		require.NoError(t, err)

		method, _ := req["method"].(string)
		switch method {
		case "initialize":
			w.Header().Set("Mcp-Session-Id", "session-1")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"protocolVersion": "2024-11-05",
				},
			})
		case "tools/list":
			require.Equal(t, "session-1", r.Header.Get("Mcp-Session-Id"))
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"tools": []map[string]any{
						{"name": "echo"},
					},
				},
			})
		case "tools/call":
			require.Equal(t, "session-1", r.Header.Get("Mcp-Session-Id"))
			params, _ := req["params"].(map[string]any)
			require.Equal(t, "echo", params["name"])
			callArgs, _ := params["arguments"].(map[string]any)
			require.Equal(t, "hello", callArgs["message"])
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"content": []map[string]any{{"type": "text", "text": "ok"}},
				},
			})
		default:
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"error": map[string]any{
					"code":    -32601,
					"message": "method not found",
				},
			})
		}
	}))
	defer server.Close()

	tool := NewRemoteMCPTool(t.TempDir())

	listToolsResult := tool.Execute(context.Background(), map[string]any{
		"operation": "list_tools",
		"url":       server.URL,
		"name":      "test-server",
	})
	require.False(t, listToolsResult.IsError)
	require.Contains(t, listToolsResult.ForUser, "echo")

	callResult := tool.Execute(context.Background(), map[string]any{
		"operation": "call_tool",
		"url":       server.URL,
		"name":      "test-server",
		"tool_name": "echo",
		"arguments": map[string]any{"message": "hello"},
	})
	require.False(t, callResult.IsError)
	require.Contains(t, callResult.ForUser, "ok")
}

// TestRemoteMCPToolDiscoverConfiguredTools verifies discovery output from configured servers.
// The t parameter controls test lifecycle.
// It returns no value and fails the test on assertion errors.
func TestRemoteMCPToolDiscoverConfiguredTools(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		var req map[string]any
		err := json.NewDecoder(r.Body).Decode(&req)
		require.NoError(t, err)

		method, _ := req["method"].(string)
		switch method {
		case "initialize":
			w.Header().Set("Mcp-Session-Id", "session-discovery")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result":  map[string]any{"protocolVersion": "2024-11-05"},
			})
		case "tools/list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"tools": []map[string]any{{
						"name":        "file_write",
						"description": "Write file",
						"inputSchema": map[string]any{"type": "object"},
					}},
				},
			})
		default:
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()

	tool := NewRemoteMCPTool(t.TempDir())
	bootErr := tool.BootstrapConfiguredServers([]ConfiguredRemoteMCPServer{{
		Name: "laisky",
		Type: "http",
		URL:  server.URL,
	}})
	require.NoError(t, bootErr)

	discovered, discoverErr := tool.DiscoverConfiguredTools(context.Background())
	require.NoError(t, discoverErr)
	require.Len(t, discovered, 1)
	require.Equal(t, "laisky", discovered[0].ServerName)
	require.Equal(t, "file_write", discovered[0].Name)
}

// TestRemoteMCPProxyToolExecute verifies proxy tool forwards calls to configured remote server.
// The t parameter controls test lifecycle.
// It returns no value and fails the test on assertion errors.
func TestRemoteMCPProxyToolExecute(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		var req map[string]any
		err := json.NewDecoder(r.Body).Decode(&req)
		require.NoError(t, err)

		method, _ := req["method"].(string)
		switch method {
		case "initialize":
			w.Header().Set("Mcp-Session-Id", "session-proxy")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result":  map[string]any{"protocolVersion": "2024-11-05"},
			})
		case "tools/call":
			params, _ := req["params"].(map[string]any)
			callArgs, _ := params["arguments"].(map[string]any)
			require.Equal(t, "default", callArgs["project"])
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result":  map[string]any{"ok": true},
			})
		default:
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()

	remote := NewRemoteMCPTool(t.TempDir())
	bootErr := remote.BootstrapConfiguredServers([]ConfiguredRemoteMCPServer{{
		Name: "laisky",
		Type: "http",
		URL:  server.URL,
	}})
	require.NoError(t, bootErr)

	proxy := NewRemoteMCPProxyTool("file_write", RemoteMCPDiscoveredTool{
		ServerName:  "laisky",
		Name:        "file_write",
		Description: "Write file",
		InputSchema: map[string]any{"type": "object"},
	}, remote)

	result := proxy.Execute(context.Background(), map[string]any{"project": "default"})
	require.False(t, result.IsError)
	require.Contains(t, result.ForUser, "\"ok\": true")
}
