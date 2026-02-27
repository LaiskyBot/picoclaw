package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/logger"
)

const (
	memoryMCPHost            = "mcp.laisky.com"
	memoryMCPProjectID       = "bot"
	memoryBeforeToolName     = "memory_before_turn"
	memoryAfterToolName      = "memory_after_turn"
	memoryMCPRequestTimeout  = 20 * time.Second
	memoryMCPMaxInputTokens  = 120000
	memoryMCPRetryStep1Delay = 200 * time.Millisecond
	memoryMCPRetryStep2Delay = 500 * time.Millisecond
	memoryMCPRetryStep3Delay = 1 * time.Second
	memoryMCPRetryStep4Delay = 2 * time.Second
)

var memoryTurnCounter atomic.Uint64

// mcpTurnMemoryClient manages turn-level memory calls against a remote MCP endpoint.
// The endpoint field stores the remote MCP base URL, headers stores auth headers,
// and sessionID caches the MCP negotiated session value across calls.
type mcpTurnMemoryClient struct {
	endpoint string
	headers  map[string]string
	client   *http.Client

	mu        sync.Mutex
	sessionID string
}

// memoryTurnContext stores per-turn memory request state used by memory_after_turn.
// The turnID parameter identifies one logical model turn and inputItems captures
// the exact items used in memory_before_turn response.
type memoryTurnContext struct {
	turnID     string
	inputItems []memoryTurnItem
}

// memoryTurnItem represents one Responses-style message item used by MCP memory tools.
// The type and role fields describe the item, and content holds text blocks.
type memoryTurnItem struct {
	Type    string                  `json:"type"`
	Role    string                  `json:"role,omitempty"`
	Content []memoryTurnItemContent `json:"content,omitempty"`
}

// memoryTurnItemContent represents one text block in a Responses-style item.
// The type field indicates input/output text and text carries block contents.
type memoryTurnItemContent struct {
	Type string `json:"type"`
	Text string `json:"text,omitempty"`
}

// memoryBeforeTurnResult defines parsed response payload from memory_before_turn.
// The inputItems field carries model input items with recalled memory context.
type memoryBeforeTurnResult struct {
	InputItems []memoryTurnItem `json:"input_items"`
}

// memoryRPCEnvelope represents JSON-RPC response payload from the MCP server.
// The result field carries method output while error carries RPC-level failures.
type memoryRPCEnvelope struct {
	Result any `json:"result"`
	Error  *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

// memoryToolCallResult represents the canonical payload returned by tools/call.
// The content field may include text blocks containing serialized JSON results.
type memoryToolCallResult struct {
	StructuredContent map[string]any   `json:"structuredContent"`
	Content           []map[string]any `json:"content"`
	IsError           bool             `json:"isError"`
	Error             *memoryToolError `json:"error,omitempty"`
}

// memoryToolError stores structured tool-side error details for retry decisions.
// The retryable field controls whether caller should backoff and retry.
type memoryToolError struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
}

// hasLaiskyMemoryMCPRemote checks whether config contains a remote MCP entry targeting mcp.laisky.com.
// The remote parameter is the configured tools.mcp.remote map and return value is true when enabled.
func hasLaiskyMemoryMCPRemote(remote map[string]config.RemoteMCPServerConfig) bool {
	_, _, ok := pickLaiskyMemoryMCPRemote(remote)
	return ok
}

// pickLaiskyMemoryMCPRemote selects a deterministic mcp.laisky.com remote MCP config if present.
// The remote parameter is the configured tools.mcp.remote map and return values are chosen config and found flag.
func pickLaiskyMemoryMCPRemote(remote map[string]config.RemoteMCPServerConfig) (string, config.RemoteMCPServerConfig, bool) {
	if len(remote) == 0 {
		return "", config.RemoteMCPServerConfig{}, false
	}

	names := make([]string, 0, len(remote))
	for name := range remote {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		entry := remote[name]
		parsed, err := url.Parse(strings.TrimSpace(entry.URL))
		if err != nil {
			continue
		}
		if !strings.EqualFold(parsed.Hostname(), memoryMCPHost) {
			continue
		}
		return name, entry, true
	}

	return "", config.RemoteMCPServerConfig{}, false
}

// newMCPMemoryClientFromRemote constructs a turn-memory client from remote MCP config.
// The remote parameter is tools.mcp.remote and return value is nil when laisky memory MCP is not configured.
func newMCPMemoryClientFromRemote(remote map[string]config.RemoteMCPServerConfig) *mcpTurnMemoryClient {
	name, entry, ok := pickLaiskyMemoryMCPRemote(remote)
	if !ok {
		logger.DebugCF("agent", "MCP memory client disabled: no mcp.laisky.com remote configured", map[string]any{
			"remote_entries": len(remote),
			"required_host":  memoryMCPHost,
		})
		return nil
	}

	headers := cloneStringMap(entry.Headers)
	for key, value := range headers {
		headerKey := strings.TrimSpace(key)
		if headerKey == "" {
			delete(headers, key)
			continue
		}
		if headerKey != key {
			delete(headers, key)
		}
		headers[headerKey] = strings.TrimSpace(value)
	}

	logger.DebugCF("agent", "Initialized MCP memory client from remote config", map[string]any{
		"remote_name":        name,
		"endpoint":           strings.TrimSpace(entry.URL),
		"header_count":       len(headers),
		"has_authorization":  hasAuthorizationHeader(headers),
		"memory_project":     memoryMCPProjectID,
		"memory_before_tool": memoryBeforeToolName,
		"memory_after_tool":  memoryAfterToolName,
	})

	return &mcpTurnMemoryClient{
		endpoint: strings.TrimSpace(entry.URL),
		headers:  headers,
		client: &http.Client{
			Timeout: memoryMCPRequestTimeout,
		},
	}
}

// beforeTurn executes memory_before_turn and returns parsed context plus turn metadata.
// The sessionID identifies chat scope, userID labels sender, and userMessage is current request text.
func (c *mcpTurnMemoryClient) beforeTurn(ctx context.Context, sessionID, userID, userMessage string) (*memoryTurnContext, string, error) {
	if c == nil {
		return nil, "", nil
	}

	turnID := newMemoryTurnID()
	currentInput := []memoryTurnItem{newUserInputItem(userMessage)}
	args := map[string]any{
		"project":       memoryMCPProjectID,
		"session_id":    sessionID,
		"user_id":       userID,
		"turn_id":       turnID,
		"current_input": currentInput,
		"max_input_tok": memoryMCPMaxInputTokens,
	}

	var result memoryBeforeTurnResult
	if err := c.callToolWithRetry(ctx, memoryBeforeToolName, args, &result); err != nil {
		return nil, "", err
	}

	if len(result.InputItems) == 0 {
		result.InputItems = currentInput
	}

	recallText := extractRecallText(result.InputItems, userMessage)
	return &memoryTurnContext{turnID: turnID, inputItems: result.InputItems}, recallText, nil
}

// afterTurn executes memory_after_turn to persist this turn's memory signals.
// The sessionID and userID match before_turn call and assistantOutput is final assistant text.
func (c *mcpTurnMemoryClient) afterTurn(ctx context.Context, sessionID, userID, assistantOutput string, turn *memoryTurnContext) error {
	if c == nil || turn == nil {
		return nil
	}

	args := map[string]any{
		"project":      memoryMCPProjectID,
		"session_id":   sessionID,
		"user_id":      userID,
		"turn_id":      turn.turnID,
		"input_items":  turn.inputItems,
		"output_items": []memoryTurnItem{newAssistantOutputItem(assistantOutput)},
	}

	var ack map[string]any
	return c.callToolWithRetry(ctx, memoryAfterToolName, args, &ack)
}

// callToolWithRetry invokes a memory tool and retries on retryable busy/transient errors.
// The toolName identifies memory tool, args contains tool arguments, and out receives decoded payload.
func (c *mcpTurnMemoryClient) callToolWithRetry(ctx context.Context, toolName string, args map[string]any, out any) error {
	delays := []time.Duration{
		0,
		memoryMCPRetryStep1Delay,
		memoryMCPRetryStep2Delay,
		memoryMCPRetryStep3Delay,
		memoryMCPRetryStep4Delay,
	}

	var lastErr error
	for idx, delay := range delays {
		if delay > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
		}

		lastErr = c.callTool(ctx, toolName, args, out)
		if lastErr == nil {
			return nil
		}

		if !isRetryableMemoryError(lastErr) {
			return lastErr
		}

		logger.DebugCF("agent", "retryable MCP memory tool error", map[string]any{
			"tool":    toolName,
			"attempt": idx + 1,
			"error":   lastErr.Error(),
		})
	}

	return lastErr
}

// callTool invokes one MCP tools/call for memory operations and decodes tool payload.
// The toolName names remote tool, args contains request arguments, and out receives result body.
func (c *mcpTurnMemoryClient) callTool(ctx context.Context, toolName string, args map[string]any, out any) error {
	sessionID, err := c.ensureSession(ctx)
	if err != nil {
		return err
	}

	rpcResult, err := c.callJSONRPC(ctx, sessionID, "tools/call", map[string]any{
		"name":      toolName,
		"arguments": args,
	})
	if err != nil {
		return err
	}

	payload, err := decodeMemoryToolPayload(rpcResult)
	if err != nil {
		return err
	}

	if payload.IsError {
		code, message, retryable := extractMemoryToolErrorFields(payload)
		logger.DebugCF("agent", "MCP memory tool returned error payload", map[string]any{
			"tool":       toolName,
			"error_code": code,
			"message":    message,
			"retryable":  retryable,
		})
		if payload.Error != nil {
			return fmt.Errorf("memory tool %q failed: %s (%s), retryable=%v", toolName, payload.Error.Message, payload.Error.Code, payload.Error.Retryable)
		}
		return fmt.Errorf("memory tool %q returned error result", toolName)
	}

	decoded, err := extractStructuredPayload(payload)
	if err != nil {
		return err
	}

	if out == nil {
		return nil
	}
	body, err := json.Marshal(decoded)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(body, out); err != nil {
		return err
	}

	return nil
}

// ensureSession initializes MCP session once and returns cached session ID.
// The ctx parameter controls request cancellation and return value is session header value.
func (c *mcpTurnMemoryClient) ensureSession(ctx context.Context) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.sessionID != "" {
		return c.sessionID, nil
	}

	_, sessionID, err := c.callJSONRPCWithSession(ctx, "", "initialize", map[string]any{
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

	c.sessionID = sessionID
	return sessionID, nil
}

// callJSONRPC wraps callJSONRPCWithSession and returns only result payload.
// The sessionID, method, and params define one outbound JSON-RPC request.
func (c *mcpTurnMemoryClient) callJSONRPC(ctx context.Context, sessionID, method string, params any) (any, error) {
	result, _, err := c.callJSONRPCWithSession(ctx, sessionID, method, params)
	return result, err
}

// callJSONRPCWithSession sends JSON-RPC to remote endpoint and captures returned session header.
// The sessionID, method, and params define request details and returns result plus next session ID.
func (c *mcpTurnMemoryClient) callJSONRPCWithSession(ctx context.Context, sessionID, method string, params any) (any, string, error) {
	requestID := fmt.Sprintf("mcp-memory-%d", memoryTurnCounter.Add(1))
	body, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      requestID,
		"method":  method,
		"params":  params,
	})
	if err != nil {
		return nil, "", fmt.Errorf("marshal json-rpc request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, "", fmt.Errorf("build json-rpc request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if sessionID != "" {
		req.Header.Set("Mcp-Session-Id", sessionID)
	}
	for key, value := range c.headers {
		headerKey := strings.TrimSpace(key)
		if headerKey == "" {
			continue
		}
		req.Header.Set(headerKey, strings.TrimSpace(value))
	}

	logger.DebugCF("agent", "Sending MCP memory JSON-RPC request", map[string]any{
		"method":             method,
		"endpoint":           c.endpoint,
		"request_id":         requestID,
		"has_session_id":     strings.TrimSpace(sessionID) != "",
		"header_count":       len(c.headers),
		"has_authorization":  hasAuthorizationHeader(c.headers),
		"memory_before_tool": memoryBeforeToolName,
		"memory_after_tool":  memoryAfterToolName,
	})

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("send json-rpc request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		bodyPreview := strings.TrimSpace(string(bodyBytes))
		if len(bodyPreview) > 512 {
			bodyPreview = bodyPreview[:512]
		}
		logger.DebugCF("agent", "MCP memory HTTP error response", map[string]any{
			"method":            method,
			"endpoint":          c.endpoint,
			"status_code":       resp.StatusCode,
			"has_authorization": hasAuthorizationHeader(c.headers),
			"response_preview":  bodyPreview,
		})
		return nil, "", fmt.Errorf("mcp http status %d: %s", resp.StatusCode, bodyPreview)
	}

	var envelope memoryRPCEnvelope
	if err := json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
		return nil, "", fmt.Errorf("decode json-rpc response: %w", err)
	}
	if envelope.Error != nil {
		logger.DebugCF("agent", "MCP memory JSON-RPC error response", map[string]any{
			"method":            method,
			"endpoint":          c.endpoint,
			"rpc_code":          envelope.Error.Code,
			"rpc_message":       envelope.Error.Message,
			"has_authorization": hasAuthorizationHeader(c.headers),
		})
		return nil, "", fmt.Errorf("mcp rpc error %d: %s", envelope.Error.Code, envelope.Error.Message)
	}

	nextSessionID := strings.TrimSpace(resp.Header.Get("Mcp-Session-Id"))
	if nextSessionID == "" {
		nextSessionID = strings.TrimSpace(resp.Header.Get("mcp-session-id"))
	}
	if nextSessionID == "" {
		nextSessionID = sessionID
	}

	return envelope.Result, nextSessionID, nil
}

// decodeMemoryToolPayload converts generic tools/call result into memoryToolCallResult structure.
// The result parameter is raw json-rpc result and return value includes structured content and errors.
func decodeMemoryToolPayload(result any) (*memoryToolCallResult, error) {
	if result == nil {
		return nil, fmt.Errorf("empty MCP tools/call result")
	}

	body, err := json.Marshal(result)
	if err != nil {
		return nil, fmt.Errorf("marshal tools/call result: %w", err)
	}

	var payload memoryToolCallResult
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, fmt.Errorf("decode tools/call result: %w", err)
	}

	return &payload, nil
}

// extractStructuredPayload returns decoded JSON payload from structuredContent or text blocks.
// The payload parameter is parsed tools/call output and return value is a map-like decoded object.
func extractStructuredPayload(payload *memoryToolCallResult) (any, error) {
	if payload == nil {
		return nil, fmt.Errorf("nil memory tool payload")
	}

	if payload.StructuredContent != nil {
		return payload.StructuredContent, nil
	}

	for _, block := range payload.Content {
		blockType := strings.TrimSpace(fmt.Sprintf("%v", block["type"]))
		if blockType != "text" {
			continue
		}
		text := strings.TrimSpace(fmt.Sprintf("%v", block["text"]))
		if text == "" {
			continue
		}
		var decoded any
		if err := json.Unmarshal([]byte(text), &decoded); err == nil {
			return decoded, nil
		}
		return map[string]any{"message": text}, nil
	}

	return map[string]any{}, nil
}

// extractRecallText extracts non-user textual context from memory_before_turn items.
// The items parameter contains returned input_items and userMessage is used to skip duplicates.
func extractRecallText(items []memoryTurnItem, userMessage string) string {
	if len(items) == 0 {
		return ""
	}

	trimmedUser := strings.TrimSpace(userMessage)
	parts := make([]string, 0, len(items))
	for _, item := range items {
		if item.Type != "message" {
			continue
		}

		text := strings.TrimSpace(joinItemText(item.Content))
		if text == "" {
			continue
		}

		if item.Role == "user" && text == trimmedUser {
			continue
		}
		parts = append(parts, text)
	}

	return strings.TrimSpace(strings.Join(parts, "\n\n"))
}

// joinItemText concatenates all textual blocks from one Responses-style content list.
// The content parameter contains text blocks and return value is newline-joined text.
func joinItemText(content []memoryTurnItemContent) string {
	if len(content) == 0 {
		return ""
	}
	parts := make([]string, 0, len(content))
	for _, block := range content {
		text := strings.TrimSpace(block.Text)
		if text == "" {
			continue
		}
		parts = append(parts, text)
	}
	return strings.Join(parts, "\n")
}

// newMemoryTurnID creates one stable turn identifier used across before/after calls.
// It takes no parameters and returns a unique ID string for one logical turn.
func newMemoryTurnID() string {
	counter := memoryTurnCounter.Add(1)
	return fmt.Sprintf("turn-%d-%06x", time.Now().UnixMilli(), counter&0xffffff)
}

// newUserInputItem builds one user message item with input_text content for memory_before_turn.
// The message parameter is current user text and return value is a Responses-style item.
func newUserInputItem(message string) memoryTurnItem {
	return memoryTurnItem{
		Type: "message",
		Role: "user",
		Content: []memoryTurnItemContent{{
			Type: "input_text",
			Text: message,
		}},
	}
}

// newAssistantOutputItem builds one assistant message item with output_text content for memory_after_turn.
// The message parameter is final assistant output and return value is a Responses-style item.
func newAssistantOutputItem(message string) memoryTurnItem {
	return memoryTurnItem{
		Type: "message",
		Role: "assistant",
		Content: []memoryTurnItemContent{{
			Type: "output_text",
			Text: message,
		}},
	}
}

// isRetryableMemoryError checks whether an error indicates retryable busy/transient memory tool failure.
// The err parameter is the call error and return value is true when retry with backoff is recommended.
func isRetryableMemoryError(err error) bool {
	if err == nil {
		return false
	}

	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "retryable=true") {
		return true
	}
	if strings.Contains(msg, "resource_busy") || strings.Contains(msg, "internal_error") {
		return true
	}
	if strings.Contains(msg, "timeout") || strings.Contains(msg, "temporar") {
		return true
	}
	return false
}

// cloneStringMap copies string map input and returns an independent map instance.
// The in parameter may be nil and return value is always a non-nil map.
func cloneStringMap(in map[string]string) map[string]string {
	out := map[string]string{}
	for key, value := range in {
		out[key] = value
	}
	return out
}

// hasAuthorizationHeader checks whether headers include a non-empty Authorization value.
// The headers parameter is an HTTP header map and return value is true when auth is configured.
func hasAuthorizationHeader(headers map[string]string) bool {
	for key, value := range headers {
		if strings.EqualFold(strings.TrimSpace(key), "authorization") && strings.TrimSpace(value) != "" {
			return true
		}
	}

	return false
}

// extractMemoryToolErrorFields extracts normalized error fields from MCP tool payload.
// The payload parameter is decoded tools/call output and return values are code, message, and retryable.
func extractMemoryToolErrorFields(payload *memoryToolCallResult) (string, string, bool) {
	if payload == nil {
		return "", "", false
	}

	if payload.Error != nil {
		return strings.TrimSpace(payload.Error.Code), strings.TrimSpace(payload.Error.Message), payload.Error.Retryable
	}

	if payload.StructuredContent == nil {
		return "", "", false
	}

	code := strings.TrimSpace(fmt.Sprintf("%v", payload.StructuredContent["code"]))
	message := strings.TrimSpace(fmt.Sprintf("%v", payload.StructuredContent["message"]))
	retryable := false
	if value, ok := payload.StructuredContent["retryable"].(bool); ok {
		retryable = value
	}

	return code, message, retryable
}
