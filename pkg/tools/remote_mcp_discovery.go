package tools

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/sipeed/picoclaw/pkg/logger"
)

// DiscoverConfiguredTools enumerates all tools from configured remote MCP servers.
// The ctx parameter controls outbound network call cancellation.
// It returns discovered tool metadata or an error when any server query fails.
func (t *RemoteMCPTool) DiscoverConfiguredTools(ctx context.Context) ([]RemoteMCPDiscoveredTool, error) {
	t.mu.Lock()
	registry, err := t.loadRegistry()
	t.mu.Unlock()
	if err != nil {
		return nil, fmt.Errorf("load remote MCP registry: %w", err)
	}

	if len(registry.Servers) == 0 {
		return nil, nil
	}

	servers := append([]remoteMCPServer(nil), registry.Servers...)
	sort.Slice(servers, func(i, j int) bool {
		return servers[i].Name < servers[j].Name
	})

	discovered := make([]RemoteMCPDiscoveredTool, 0, len(servers)*4)
	for _, server := range servers {
		initSessionID, initErr := t.initializeServer(ctx, server)
		if initErr != nil {
			return nil, fmt.Errorf("initialize %q failed: %w", server.Name, initErr)
		}

		result, listErr := t.callJSONRPC(ctx, server.URL, server.Headers, initSessionID, "tools/list", map[string]any{})
		if listErr != nil {
			return nil, fmt.Errorf("tools/list %q failed: %w", server.Name, listErr)
		}

		toolsForServer, parseErr := parseDiscoveredToolsResult(server.Name, result)
		if parseErr != nil {
			return nil, fmt.Errorf("parse tools/list %q failed: %w", server.Name, parseErr)
		}

		discovered = append(discovered, toolsForServer...)
	}

	return discovered, nil
}

// CallConfiguredTool invokes one tool on a configured remote MCP server.
// The ctx parameter controls cancellation, serverName and toolName identify target tool,
// and arguments carries remote tool input values.
// It returns raw JSON-RPC result payload or an error.
func (t *RemoteMCPTool) CallConfiguredTool(
	ctx context.Context,
	serverName, toolName string,
	arguments map[string]any,
) (any, error) {
	resolvedArgs := map[string]any{"name": serverName}
	server, err := t.resolveServer(resolvedArgs)
	if err != nil {
		return nil, err
	}

	initSessionID, err := t.initializeServer(ctx, server)
	if err != nil {
		return nil, fmt.Errorf("MCP initialize failed for %s: %w", server.Name, err)
	}

	if arguments == nil {
		arguments = map[string]any{}
	}
	arguments = normalizeLaiskyToolArguments(server, arguments)

	result, err := t.callJSONRPC(ctx, server.URL, server.Headers, initSessionID, "tools/call", map[string]any{
		"name":      strings.TrimSpace(toolName),
		"arguments": arguments,
	})
	if err != nil {
		return nil, fmt.Errorf("MCP tools/call failed for %s: %w", server.Name, err)
	}

	return result, nil
}

// parseDiscoveredToolsResult extracts MCP tool metadata from tools/list response payload.
// The serverName parameter labels output records and result is the raw JSON-RPC result body.
// It returns parsed tool list or an error when payload format is invalid.
func parseDiscoveredToolsResult(serverName string, result any) ([]RemoteMCPDiscoveredTool, error) {
	root, ok := result.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("tools/list result is not an object")
	}

	rawTools, ok := root["tools"]
	if !ok {
		return nil, nil
	}

	items, ok := rawTools.([]any)
	if !ok {
		return nil, fmt.Errorf("tools field is not an array")
	}

	toolsList := make([]RemoteMCPDiscoveredTool, 0, len(items))
	for _, item := range items {
		entry, ok := item.(map[string]any)
		if !ok {
			continue
		}

		name := strings.TrimSpace(fmt.Sprintf("%v", entry["name"]))
		if name == "" {
			continue
		}

		description := strings.TrimSpace(fmt.Sprintf("%v", entry["description"]))
		inputSchema := map[string]any{"type": "object"}
		if rawSchema, ok := entry["inputSchema"].(map[string]any); ok && rawSchema != nil {
			normalized, changed := normalizeRemoteToolSchema(rawSchema)
			inputSchema = normalized
			if changed {
				logger.DebugCF("tool", "Normalized remote MCP tool schema for provider compatibility", map[string]any{
					"server": serverName,
					"tool":   name,
				})
			}
		}

		toolsList = append(toolsList, RemoteMCPDiscoveredTool{
			ServerName:  serverName,
			Name:        name,
			Description: description,
			InputSchema: inputSchema,
		})
	}

	return toolsList, nil
}

// normalizeRemoteToolSchema normalizes remote JSON schema to avoid provider validation failures.
// The schema parameter is the discovered inputSchema and it returns normalized schema plus whether it changed.
func normalizeRemoteToolSchema(schema map[string]any) (map[string]any, bool) {
	if schema == nil {
		return map[string]any{"type": "object"}, true
	}

	cloned, changed := normalizeRemoteToolSchemaValue(schema)
	result, ok := cloned.(map[string]any)
	if !ok || result == nil {
		return map[string]any{"type": "object"}, true
	}

	return result, changed
}

// normalizeRemoteToolSchemaValue recursively normalizes one schema node.
// The value parameter accepts arbitrary decoded JSON and returns normalized value plus changed flag.
func normalizeRemoteToolSchemaValue(value any) (any, bool) {
	switch node := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(node)+1)
		changed := false

		for key, raw := range node {
			norm, childChanged := normalizeRemoteToolSchemaValue(raw)
			out[key] = norm
			if childChanged {
				changed = true
			}
		}

		typeValue, _ := out["type"].(string)
		if strings.EqualFold(strings.TrimSpace(typeValue), "array") {
			if _, ok := out["items"]; !ok {
				out["items"] = map[string]any{}
				changed = true
			}
		}

		return out, changed
	case []any:
		out := make([]any, len(node))
		changed := false
		for idx, item := range node {
			norm, childChanged := normalizeRemoteToolSchemaValue(item)
			out[idx] = norm
			if childChanged {
				changed = true
			}
		}
		return out, changed
	default:
		return value, false
	}
}
