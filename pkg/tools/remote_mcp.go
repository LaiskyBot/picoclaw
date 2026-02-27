package tools

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sipeed/picoclaw/pkg/logger"
)

const (
	remoteMCPOpListServers  = "list_servers"
	remoteMCPOpAddServer    = "add_server"
	remoteMCPOpRemoveServer = "remove_server"
	remoteMCPOpListTools    = "list_tools"
	remoteMCPOpCallTool     = "call_tool"

	remoteMCPRequestTimeout = 45 * time.Second
)

var remoteMCPRequestID atomic.Uint64

// RemoteMCPTool manages remote MCP servers and calls tools exposed by those servers.
// It stores server definitions in workspace memory and performs MCP JSON-RPC calls over HTTP.
type RemoteMCPTool struct {
	workspace string
	client    *http.Client
	mu        sync.Mutex
}

// remoteMCPRegistry persists configured remote MCP servers.
type remoteMCPRegistry struct {
	Servers []remoteMCPServer `json:"servers"`
}

// remoteMCPServer represents one remote MCP endpoint entry.
type remoteMCPServer struct {
	Name    string            `json:"name"`
	URL     string            `json:"url"`
	Headers map[string]string `json:"headers,omitempty"`
}

// ConfiguredRemoteMCPServer represents one remote MCP server defined from static configuration.
type ConfiguredRemoteMCPServer struct {
	Name    string
	Type    string
	URL     string
	Headers map[string]string
}

// RemoteMCPDiscoveredTool represents one tool discovered from configured remote MCP servers.
// ServerName identifies source server, Name is remote tool name, Description is optional,
// and InputSchema carries JSON schema for tool parameters.
type RemoteMCPDiscoveredTool struct {
	ServerName  string
	Name        string
	Description string
	InputSchema map[string]any
}

// remoteMCPRequest represents a JSON-RPC request to an MCP endpoint.
type remoteMCPRequest struct {
	JSONRPC string `json:"jsonrpc"`
	ID      string `json:"id"`
	Method  string `json:"method"`
	Params  any    `json:"params,omitempty"`
}

// remoteMCPResponse represents a JSON-RPC response from an MCP endpoint.
type remoteMCPResponse struct {
	JSONRPC string `json:"jsonrpc"`
	ID      any    `json:"id"`
	Result  any    `json:"result,omitempty"`
	Error   *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

// NewRemoteMCPTool creates a remote MCP tool bound to the given workspace.
// The workspace parameter is used to persist MCP server definitions.
// It returns a configured tool instance.
func NewRemoteMCPTool(workspace string) *RemoteMCPTool {
	return &RemoteMCPTool{
		workspace: workspace,
		client: &http.Client{
			Timeout: remoteMCPRequestTimeout,
		},
	}
}

// Name returns the tool name.
// It takes no parameters.
// It returns the canonical tool identifier used by the LLM.
func (t *RemoteMCPTool) Name() string {
	return "remote_mcp"
}

// Description returns a concise tool description.
// It takes no parameters.
// It returns guidance for supported MCP operations.
func (t *RemoteMCPTool) Description() string {
	return "Manage and use remote MCP servers: list/add/remove servers, list remote tools, and call a remote MCP tool."
}

// Parameters returns the JSON schema for supported tool arguments.
// It takes no parameters.
// It returns an object schema describing operation-specific fields.
func (t *RemoteMCPTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"operation": map[string]any{
				"type":        "string",
				"description": "Operation to run: list_servers, add_server, remove_server, list_tools, call_tool",
				"enum": []string{
					remoteMCPOpListServers,
					remoteMCPOpAddServer,
					remoteMCPOpRemoveServer,
					remoteMCPOpListTools,
					remoteMCPOpCallTool,
				},
			},
			"name": map[string]any{
				"type":        "string",
				"description": "Server name (required for add_server/remove_server and usually for list_tools/call_tool)",
			},
			"url": map[string]any{
				"type":        "string",
				"description": "Remote MCP endpoint URL (required for add_server; optional override for list_tools/call_tool)",
			},
			"headers": map[string]any{
				"type":        "object",
				"description": "Optional HTTP headers for remote server auth, e.g. {\"Authorization\": \"Bearer ...\"}",
				"additionalProperties": map[string]any{
					"type": "string",
				},
			},
			"tool_name": map[string]any{
				"type":        "string",
				"description": "Remote tool name for call_tool",
			},
			"arguments": map[string]any{
				"type":                 "object",
				"description":          "Arguments object for call_tool",
				"additionalProperties": true,
			},
		},
		"required": []string{"operation"},
	}
}

// Execute runs one remote MCP operation.
// The ctx parameter controls cancellation for outgoing HTTP calls.
// It returns a ToolResult containing operation output or error details.
func (t *RemoteMCPTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	op, _ := args["operation"].(string)
	op = strings.TrimSpace(strings.ToLower(op))
	if op == "" {
		return ErrorResult("missing required argument: operation")
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	switch op {
	case remoteMCPOpListServers:
		return t.executeListServers()
	case remoteMCPOpAddServer:
		return t.executeAddServer(args)
	case remoteMCPOpRemoveServer:
		return t.executeRemoveServer(args)
	case remoteMCPOpListTools:
		return t.executeListTools(ctx, args)
	case remoteMCPOpCallTool:
		return t.executeCallTool(ctx, args)
	default:
		return ErrorResult(fmt.Sprintf("unsupported operation %q", op))
	}
}

// executeListServers lists configured remote MCP servers.
// It takes no parameters.
// It returns a user-visible list in ToolResult format.
func (t *RemoteMCPTool) executeListServers() *ToolResult {
	registry, err := t.loadRegistry()
	if err != nil {
		return ErrorResult(fmt.Sprintf("failed to load remote MCP registry: %v", err))
	}

	if len(registry.Servers) == 0 {
		return UserResult("No remote MCP servers are configured.")
	}

	sort.Slice(registry.Servers, func(i, j int) bool {
		return registry.Servers[i].Name < registry.Servers[j].Name
	})

	var sb strings.Builder
	sb.WriteString("Configured remote MCP servers:\n")
	for _, server := range registry.Servers {
		sb.WriteString("- ")
		sb.WriteString(server.Name)
		sb.WriteString(": ")
		sb.WriteString(server.URL)
		if len(server.Headers) > 0 {
			sb.WriteString(" (headers configured)")
		}
		sb.WriteString("\n")
	}

	return UserResult(strings.TrimSpace(sb.String()))
}

// executeAddServer adds or updates one remote MCP server entry.
// The args parameter must contain name and url, with optional headers.
// It returns a ToolResult describing the persisted configuration.
func (t *RemoteMCPTool) executeAddServer(args map[string]any) *ToolResult {
	name, _ := args["name"].(string)
	name = strings.TrimSpace(name)
	if name == "" {
		return ErrorResult("missing required argument: name")
	}

	rawURL, _ := args["url"].(string)
	rawURL = strings.TrimSpace(rawURL)
	if rawURL == "" {
		return ErrorResult("missing required argument: url")
	}

	parsedURL, err := validateRemoteMCPURL(rawURL)
	if err != nil {
		return ErrorResult(fmt.Sprintf("invalid url %q: %v", rawURL, err))
	}

	headers := normalizeHeaders(args["headers"])

	registry, err := t.loadRegistry()
	if err != nil {
		return ErrorResult(fmt.Sprintf("failed to load remote MCP registry: %v", err))
	}

	updated := false
	for i := range registry.Servers {
		if registry.Servers[i].Name == name {
			registry.Servers[i].URL = parsedURL
			registry.Servers[i].Headers = headers
			updated = true
			break
		}
	}
	if !updated {
		registry.Servers = append(registry.Servers, remoteMCPServer{
			Name:    name,
			URL:     parsedURL,
			Headers: headers,
		})
	}

	if err := t.saveRegistry(registry); err != nil {
		return ErrorResult(fmt.Sprintf("failed to save remote MCP registry: %v", err))
	}

	verb := "added"
	if updated {
		verb = "updated"
	}

	return UserResult(fmt.Sprintf("Remote MCP server %q %s: %s", name, verb, parsedURL))
}

// executeRemoveServer removes one remote MCP server by name.
// The args parameter must contain name.
// It returns a ToolResult describing removal status.
func (t *RemoteMCPTool) executeRemoveServer(args map[string]any) *ToolResult {
	name, _ := args["name"].(string)
	name = strings.TrimSpace(name)
	if name == "" {
		return ErrorResult("missing required argument: name")
	}

	registry, err := t.loadRegistry()
	if err != nil {
		return ErrorResult(fmt.Sprintf("failed to load remote MCP registry: %v", err))
	}

	filtered := make([]remoteMCPServer, 0, len(registry.Servers))
	removed := false
	for _, server := range registry.Servers {
		if server.Name == name {
			removed = true
			continue
		}
		filtered = append(filtered, server)
	}

	if !removed {
		return ErrorResult(fmt.Sprintf("remote MCP server %q not found", name))
	}

	registry.Servers = filtered
	if err := t.saveRegistry(registry); err != nil {
		return ErrorResult(fmt.Sprintf("failed to save remote MCP registry: %v", err))
	}

	return UserResult(fmt.Sprintf("Removed remote MCP server %q", name))
}

// executeListTools lists tools from a remote MCP server.
// The args parameter accepts name or url with optional headers.
// It returns a ToolResult with remote tool metadata.
func (t *RemoteMCPTool) executeListTools(ctx context.Context, args map[string]any) *ToolResult {
	server, err := t.resolveServer(args)
	if err != nil {
		return ErrorResult(err.Error())
	}

	initSessionID, err := t.initializeServer(ctx, server)
	if err != nil {
		return ErrorResult(fmt.Sprintf("MCP initialize failed for %s: %v", server.Name, err))
	}

	result, err := t.callJSONRPC(ctx, server.URL, server.Headers, initSessionID, "tools/list", map[string]any{})
	if err != nil {
		return ErrorResult(fmt.Sprintf("MCP tools/list failed for %s: %v", server.Name, err))
	}

	pretty, prettyErr := json.MarshalIndent(result, "", "  ")
	if prettyErr != nil {
		return ErrorResult(fmt.Sprintf("failed to render tools/list result: %v", prettyErr))
	}

	return UserResult(fmt.Sprintf("Remote MCP tools from %q:\n%s", server.Name, string(pretty)))
}

// executeCallTool calls one tool on a remote MCP server.
// The args parameter accepts server selector, tool_name, and arguments.
// It returns a ToolResult containing remote tool call output.
func (t *RemoteMCPTool) executeCallTool(ctx context.Context, args map[string]any) *ToolResult {
	server, err := t.resolveServer(args)
	if err != nil {
		return ErrorResult(err.Error())
	}

	toolName, _ := args["tool_name"].(string)
	toolName = strings.TrimSpace(toolName)
	if toolName == "" {
		return ErrorResult("missing required argument: tool_name")
	}

	arguments := map[string]any{}
	if rawArgs, ok := args["arguments"].(map[string]any); ok && rawArgs != nil {
		arguments = rawArgs
	}
	arguments = normalizeLaiskyToolArguments(server, arguments)

	logger.DebugCF("tool", "remote_mcp call_tool prepared arguments", map[string]any{
		"server":        server.Name,
		"tool_name":     toolName,
		"argument_keys": len(arguments),
	})

	initSessionID, err := t.initializeServer(ctx, server)
	if err != nil {
		return ErrorResult(fmt.Sprintf("MCP initialize failed for %s: %v", server.Name, err))
	}

	result, err := t.callJSONRPC(ctx, server.URL, server.Headers, initSessionID, "tools/call", map[string]any{
		"name":      toolName,
		"arguments": arguments,
	})
	if err != nil {
		return ErrorResult(fmt.Sprintf("MCP tools/call failed for %s: %v", server.Name, err))
	}

	pretty, prettyErr := json.MarshalIndent(result, "", "  ")
	if prettyErr != nil {
		return ErrorResult(fmt.Sprintf("failed to render tools/call result: %v", prettyErr))
	}

	return UserResult(fmt.Sprintf("Remote MCP tool %q on %q returned:\n%s", toolName, server.Name, string(pretty)))
}

// resolveServer resolves server settings from args using name or direct url override.
// The args parameter may contain name/url/headers.
// It returns a fully populated remote server definition.
func (t *RemoteMCPTool) resolveServer(args map[string]any) (remoteMCPServer, error) {
	if rawURL, ok := args["url"].(string); ok && strings.TrimSpace(rawURL) != "" {
		resolvedURL, err := validateRemoteMCPURL(strings.TrimSpace(rawURL))
		if err != nil {
			return remoteMCPServer{}, fmt.Errorf("invalid url %q: %w", rawURL, err)
		}
		name, _ := args["name"].(string)
		name = strings.TrimSpace(name)
		if name == "" {
			name = "adhoc"
		}
		return remoteMCPServer{
			Name:    name,
			URL:     resolvedURL,
			Headers: normalizeHeaders(args["headers"]),
		}, nil
	}

	name, _ := args["name"].(string)
	name = strings.TrimSpace(name)
	if name == "" {
		return remoteMCPServer{}, fmt.Errorf("missing server selector: provide name or url")
	}

	registry, err := t.loadRegistry()
	if err != nil {
		return remoteMCPServer{}, fmt.Errorf("failed to load remote MCP registry: %w", err)
	}
	for _, server := range registry.Servers {
		if server.Name == name {
			return server, nil
		}
	}

	return remoteMCPServer{}, fmt.Errorf("remote MCP server %q not found", name)
}

// initializeServer performs the MCP initialize request for a server.
// The ctx parameter controls network cancellation and timeout.
// It returns the negotiated session ID if the server provides one.
func (t *RemoteMCPTool) initializeServer(ctx context.Context, server remoteMCPServer) (string, error) {
	result, sessionID, err := t.callJSONRPCWithSession(ctx, server.URL, server.Headers, "", "initialize", map[string]any{
		"protocolVersion": "2024-11-05",
		"capabilities":    map[string]any{},
		"clientInfo": map[string]any{
			"name":    "picoclaw",
			"version": "1.0.0",
		},
	})
	if err != nil {
		return "", err
	}

	if result == nil {
		return sessionID, nil
	}

	return sessionID, nil
}

// callJSONRPC performs one JSON-RPC request and returns response result.
// The sessionID parameter is optional and attached when available.
// It returns the result payload or an error.
func (t *RemoteMCPTool) callJSONRPC(
	ctx context.Context,
	endpoint string,
	headers map[string]string,
	sessionID string,
	method string,
	params any,
) (any, error) {
	result, _, err := t.callJSONRPCWithSession(ctx, endpoint, headers, sessionID, method, params)
	return result, err
}

// callJSONRPCWithSession performs one JSON-RPC request and captures session header.
// The endpoint, headers, method, and params define outbound request details.
// It returns result payload, discovered session ID, and an error when applicable.
func (t *RemoteMCPTool) callJSONRPCWithSession(
	ctx context.Context,
	endpoint string,
	headers map[string]string,
	sessionID string,
	method string,
	params any,
) (any, string, error) {
	requestID := fmt.Sprintf("%d", remoteMCPRequestID.Add(1))
	payload := remoteMCPRequest{
		JSONRPC: "2.0",
		ID:      requestID,
		Method:  method,
		Params:  params,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return nil, "", fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, "", fmt.Errorf("build request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if sessionID != "" {
		req.Header.Set("Mcp-Session-Id", sessionID)
	}
	for key, value := range headers {
		if strings.TrimSpace(key) == "" {
			continue
		}
		req.Header.Set(key, value)
	}

	resp, err := t.client.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("http request failed: %w", err)
	}
	defer resp.Body.Close()

	var rpcResp remoteMCPResponse
	if err := json.NewDecoder(resp.Body).Decode(&rpcResp); err != nil {
		return nil, "", fmt.Errorf("decode response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, "", fmt.Errorf("http status %d", resp.StatusCode)
	}

	if rpcResp.Error != nil {
		return nil, "", fmt.Errorf("rpc error %d: %s", rpcResp.Error.Code, rpcResp.Error.Message)
	}

	newSessionID := strings.TrimSpace(resp.Header.Get("Mcp-Session-Id"))
	if newSessionID == "" {
		newSessionID = strings.TrimSpace(resp.Header.Get("mcp-session-id"))
	}

	return rpcResp.Result, newSessionID, nil
}

// loadRegistry reads the persisted MCP server registry.
// It takes no parameters.
// It returns an empty registry when no file exists.
func (t *RemoteMCPTool) loadRegistry() (*remoteMCPRegistry, error) {
	path := t.registryPath()
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return &remoteMCPRegistry{Servers: []remoteMCPServer{}}, nil
		}
		return nil, err
	}

	var registry remoteMCPRegistry
	if err := json.Unmarshal(data, &registry); err != nil {
		return nil, err
	}
	if registry.Servers == nil {
		registry.Servers = []remoteMCPServer{}
	}

	return &registry, nil
}

// saveRegistry persists the MCP server registry in workspace memory.
// The registry parameter is serialized as JSON.
// It returns an error when writing fails.
func (t *RemoteMCPTool) saveRegistry(registry *remoteMCPRegistry) error {
	if registry == nil {
		registry = &remoteMCPRegistry{Servers: []remoteMCPServer{}}
	}

	path := t.registryPath()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}

	data, err := json.MarshalIndent(registry, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(path, data, 0o600)
}

// registryPath returns the persisted registry file path.
// It takes no parameters.
// It returns an absolute path within workspace memory.
func (t *RemoteMCPTool) registryPath() string {
	return filepath.Join(t.workspace, "memory", "mcp_servers.json")
}

// BootstrapConfiguredServers merges configured remote servers into persisted registry.
// The servers parameter should contain HTTP remote MCP server definitions from config.
// It returns an error when validation or persistence fails.
func (t *RemoteMCPTool) BootstrapConfiguredServers(servers []ConfiguredRemoteMCPServer) error {
	if len(servers) == 0 {
		return nil
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	registry, err := t.loadRegistry()
	if err != nil {
		return fmt.Errorf("load remote MCP registry: %w", err)
	}

	changed := false
	for _, candidate := range servers {
		name := strings.TrimSpace(candidate.Name)
		if name == "" {
			return fmt.Errorf("configured remote MCP server name is required")
		}

		transport := strings.ToLower(strings.TrimSpace(candidate.Type))
		if transport == "" {
			transport = "http"
		}
		if transport != "http" {
			return fmt.Errorf("configured remote MCP server %q type %q is not supported; only \"http\" is supported", name, transport)
		}

		resolvedURL, err := validateRemoteMCPURL(strings.TrimSpace(candidate.URL))
		if err != nil {
			return fmt.Errorf("configured remote MCP server %q has invalid url %q: %w", name, candidate.URL, err)
		}

		normalizedHeaders := normalizeStringHeaders(candidate.Headers)

		updated := false
		for i := range registry.Servers {
			if registry.Servers[i].Name != name {
				continue
			}
			if registry.Servers[i].URL != resolvedURL || !stringMapEqual(registry.Servers[i].Headers, normalizedHeaders) {
				registry.Servers[i].URL = resolvedURL
				registry.Servers[i].Headers = normalizedHeaders
				changed = true
			}
			updated = true
			break
		}
		if updated {
			continue
		}

		registry.Servers = append(registry.Servers, remoteMCPServer{
			Name:    name,
			URL:     resolvedURL,
			Headers: normalizedHeaders,
		})
		changed = true
	}

	if !changed {
		return nil
	}

	if err := t.saveRegistry(registry); err != nil {
		return fmt.Errorf("save remote MCP registry: %w", err)
	}

	return nil
}

// validateRemoteMCPURL validates and normalizes a remote MCP endpoint URL.
// The raw parameter is a user-provided URL string.
// It returns a normalized URL or an error.
func validateRemoteMCPURL(raw string) (string, error) {
	parsed, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "", fmt.Errorf("scheme must be http or https")
	}
	if strings.TrimSpace(parsed.Host) == "" {
		return "", fmt.Errorf("host is required")
	}
	return parsed.String(), nil
}

// normalizeHeaders converts arbitrary header input into a string map.
// The value parameter should be a map from JSON tool arguments.
// It returns sanitized header key/value pairs.
func normalizeHeaders(value any) map[string]string {
	headers := map[string]string{}
	rawMap, ok := value.(map[string]any)
	if !ok || rawMap == nil {
		return headers
	}

	for key, raw := range rawMap {
		headerName := strings.TrimSpace(key)
		if headerName == "" {
			continue
		}
		headers[headerName] = strings.TrimSpace(fmt.Sprintf("%v", raw))
	}

	return headers
}

// normalizeStringHeaders sanitizes string headers by trimming blank keys and values.
// The headers parameter is a user or config supplied map.
// It returns a canonicalized header map safe for persistence.
func normalizeStringHeaders(headers map[string]string) map[string]string {
	out := map[string]string{}
	for key, value := range headers {
		headerName := strings.TrimSpace(key)
		if headerName == "" {
			continue
		}
		out[headerName] = strings.TrimSpace(value)
	}
	return out
}

// stringMapEqual compares two string maps for exact key/value equality.
// The a and b parameters are treated as empty maps when nil.
// It returns true when both maps contain the same entries.
func stringMapEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for key, value := range a {
		if b[key] != value {
			return false
		}
	}
	return true
}

// normalizeLaiskyToolArguments enforces required project/task identifiers for LAISKY MCP integrations.
// The server parameter identifies target MCP endpoint and arguments carries tool parameters.
// It returns a copied arguments map with required identifier fields coerced to "bot" when applicable.
func normalizeLaiskyToolArguments(server remoteMCPServer, arguments map[string]any) map[string]any {
	if len(arguments) == 0 {
		return map[string]any{}
	}

	normalized := make(map[string]any, len(arguments))
	for key, value := range arguments {
		normalized[key] = value
	}

	if !shouldForceBotScopedIdentifiers(server) {
		return normalized
	}

	for _, key := range []string{"task_id", "project", "project_id"} {
		value, exists := normalized[key]
		if !exists {
			continue
		}

		if strings.TrimSpace(fmt.Sprintf("%v", value)) == "bot" {
			continue
		}

		logger.DebugCF("tool", "Normalized LAISKY MCP scoped identifier", map[string]any{
			"server":     server.Name,
			"server_url": server.URL,
			"field":      key,
		})
		normalized[key] = "bot"
	}

	return normalized
}

// shouldForceBotScopedIdentifiers checks whether bot-scoped MCP identifiers are required.
// The server parameter contains configured MCP server metadata and return value is true for LAISKY MCP endpoints.
func shouldForceBotScopedIdentifiers(server remoteMCPServer) bool {
	if strings.HasPrefix(strings.ToLower(strings.TrimSpace(server.Name)), "laisky") {
		return true
	}

	parsed, err := url.Parse(strings.TrimSpace(server.URL))
	if err != nil {
		return false
	}

	return strings.EqualFold(parsed.Hostname(), "mcp.laisky.com")
}
