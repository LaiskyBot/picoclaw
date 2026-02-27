package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

// RemoteMCPProxyTool exposes one remote MCP tool as a normal local tool definition.
// Name and schema are presented directly to the model while execution is proxied
// to the configured remote MCP endpoint.
type RemoteMCPProxyTool struct {
	name          string
	description   string
	parameters    map[string]any
	serverName    string
	remoteTool    string
	remoteMCPTool *RemoteMCPTool
}

// NewRemoteMCPProxyTool creates a proxy tool for one discovered remote MCP tool.
// The proxyName parameter is the public tool name, discovered carries source metadata,
// and remoteMCPTool executes remote JSON-RPC calls.
// It returns a Tool implementation compatible with the local tool registry.
func NewRemoteMCPProxyTool(
	proxyName string,
	discovered RemoteMCPDiscoveredTool,
	remoteMCPTool *RemoteMCPTool,
) *RemoteMCPProxyTool {
	description := strings.TrimSpace(discovered.Description)
	if description == "" {
		description = fmt.Sprintf("Execute tool %q.", discovered.Name)
	}

	params := map[string]any{"type": "object"}
	if discovered.InputSchema != nil {
		params = discovered.InputSchema
	}

	return &RemoteMCPProxyTool{
		name:          strings.TrimSpace(proxyName),
		description:   description,
		parameters:    params,
		serverName:    strings.TrimSpace(discovered.ServerName),
		remoteTool:    strings.TrimSpace(discovered.Name),
		remoteMCPTool: remoteMCPTool,
	}
}

// Name returns the externally visible tool name.
// It takes no parameters and returns stable proxy name.
func (t *RemoteMCPProxyTool) Name() string {
	return t.name
}

// Description returns the remote tool description used in prompt injection.
// It takes no parameters and returns user-facing function description.
func (t *RemoteMCPProxyTool) Description() string {
	return t.description
}

// Parameters returns the remote tool JSON schema as provider parameters.
// It takes no parameters and returns the input schema object.
func (t *RemoteMCPProxyTool) Parameters() map[string]any {
	return t.parameters
}

// Execute forwards the tool call to remote MCP and returns rendered JSON payload.
// The ctx parameter controls remote request lifecycle and args are forwarded as-is.
// It returns an error ToolResult on call failure, otherwise a user-visible JSON response.
func (t *RemoteMCPProxyTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	if t.remoteMCPTool == nil {
		return ErrorResult("remote MCP proxy is not initialized")
	}

	result, err := t.remoteMCPTool.CallConfiguredTool(ctx, t.serverName, t.remoteTool, args)
	if err != nil {
		return ErrorResult(err.Error())
	}

	pretty, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return ErrorResult(fmt.Sprintf("failed to render remote tool result: %v", err))
	}

	return UserResult(string(pretty))
}
