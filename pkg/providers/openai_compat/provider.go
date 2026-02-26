package openai_compat

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/sipeed/picoclaw/pkg/logger"
	"github.com/sipeed/picoclaw/pkg/providers/protocoltypes"
)

type (
	ToolCall               = protocoltypes.ToolCall
	FunctionCall           = protocoltypes.FunctionCall
	LLMResponse            = protocoltypes.LLMResponse
	UsageInfo              = protocoltypes.UsageInfo
	Message                = protocoltypes.Message
	ToolDefinition         = protocoltypes.ToolDefinition
	ToolFunctionDefinition = protocoltypes.ToolFunctionDefinition
	ExtraContent           = protocoltypes.ExtraContent
	GoogleExtra            = protocoltypes.GoogleExtra
)

type Provider struct {
	apiKey         string
	apiBase        string
	maxTokensField string // Field name for max tokens (e.g., "max_completion_tokens" for o1/glm models)
	httpClient     *http.Client
}

func NewProvider(apiKey, apiBase, proxy string) *Provider {
	return NewProviderWithMaxTokensField(apiKey, apiBase, proxy, "")
}

func NewProviderWithMaxTokensField(apiKey, apiBase, proxy, maxTokensField string) *Provider {
	client := &http.Client{
		Timeout: 120 * time.Second,
	}

	if proxy != "" {
		parsed, err := url.Parse(proxy)
		if err == nil {
			client.Transport = &http.Transport{
				Proxy: http.ProxyURL(parsed),
			}
		} else {
			log.Printf("openai_compat: invalid proxy URL %q: %v", proxy, err)
		}
	}

	return &Provider{
		apiKey:         apiKey,
		apiBase:        strings.TrimRight(apiBase, "/"),
		maxTokensField: maxTokensField,
		httpClient:     client,
	}
}

func (p *Provider) Chat(
	ctx context.Context,
	messages []Message,
	tools []ToolDefinition,
	model string,
	options map[string]any,
) (*LLMResponse, error) {
	if p.apiBase == "" {
		return nil, fmt.Errorf("API base not configured")
	}

	model = normalizeModel(model, p.apiBase)

	requestBody := map[string]any{
		"model": model,
		"input": buildResponseInput(messages),
	}

	if len(tools) > 0 {
		requestBody["tools"] = toResponseTools(tools)
		requestBody["tool_choice"] = "auto"
	}

	if maxTokens, ok := asInt(options["max_tokens"]); ok {
		requestBody["max_output_tokens"] = maxTokens
	}

	if temperature, ok := asFloat(options["temperature"]); ok {
		lowerModel := strings.ToLower(model)
		// Kimi k2 models only support temperature=1.
		if strings.Contains(lowerModel, "kimi") && strings.Contains(lowerModel, "k2") {
			requestBody["temperature"] = 1.0
		} else {
			requestBody["temperature"] = temperature
		}
	}

	// Prompt caching: pass a stable cache key so OpenAI can bucket requests
	// with the same key and reuse prefix KV cache across calls.
	// The key is typically the agent ID — stable per agent, shared across requests.
	// See: https://platform.openai.com/docs/guides/prompt-caching
	if cacheKey, ok := options["prompt_cache_key"].(string); ok && cacheKey != "" {
		requestBody["prompt_cache_key"] = cacheKey
	}

	jsonData, err := json.Marshal(requestBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", p.apiBase+"/responses", bytes.NewReader(jsonData))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	if p.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+p.apiKey)
	}

	requestURL := sanitizedURL(req.URL)
	logger.DebugCF("openai_compat", "Sending responses request", map[string]any{
		"url":           requestURL,
		"model":         model,
		"messages":      len(messages),
		"tools":         len(tools),
		"request_bytes": len(jsonData),
	})

	start := time.Now()

	resp, err := p.httpClient.Do(req)
	if err != nil {
		logger.DebugCF("openai_compat", "Responses request failed before response", map[string]any{
			"url":        requestURL,
			"model":      model,
			"latency_ms": time.Since(start).Milliseconds(),
			"error":      err.Error(),
		})
		return nil, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		requestID := firstNonEmpty(
			resp.Header.Get("x-request-id"),
			resp.Header.Get("request-id"),
			resp.Header.Get("cf-ray"),
		)
		bodyPreview := summarizeBody(body, 2000)
		endpointError := summarizeEndpointError(body, 500)

		logger.DebugCF("openai_compat", "Responses non-200 response", map[string]any{
			"url":            requestURL,
			"model":          model,
			"status_code":    resp.StatusCode,
			"latency_ms":     time.Since(start).Milliseconds(),
			"response_bytes": len(body),
			"request_id":     requestID,
			"server":         resp.Header.Get("server"),
			"body_preview":   summarizeBody(body, 300),
			"endpoint_error": endpointError,
		})

		errorText := fmt.Sprintf("API request failed:\n  Status: %d\n  URL:    %s\n  Model:  %s\n  Body:   %s", resp.StatusCode, requestURL, model, bodyPreview)
		if endpointError != "" {
			errorText += fmt.Sprintf("\n  EndpointError: %s", endpointError)
		}
		if requestID != "" {
			errorText += fmt.Sprintf("\n  RequestID: %s", requestID)
		}
		return nil, fmt.Errorf("%s", errorText)
	}

	return parseResponse(body)
}

func parseResponse(body []byte) (*LLMResponse, error) {
	var apiResponse struct {
		Status string `json:"status"`
		Output []struct {
			Type      string `json:"type"`
			Name      string `json:"name"`
			Arguments string `json:"arguments"`
			CallID    string `json:"call_id"`
			Content   []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"content"`
			Summary []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"summary"`
		} `json:"output"`
		Usage struct {
			PromptTokens     int `json:"prompt_tokens"`
			CompletionTokens int `json:"completion_tokens"`
			InputTokens      int `json:"input_tokens"`
			OutputTokens     int `json:"output_tokens"`
			TotalTokens      int `json:"total_tokens"`
		} `json:"usage"`
	}

	if err := json.Unmarshal(body, &apiResponse); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	var contentBuilder strings.Builder
	var reasoningBuilder strings.Builder
	toolCalls := make([]ToolCall, 0)

	for _, item := range apiResponse.Output {
		switch item.Type {
		case "message":
			for _, content := range item.Content {
				switch content.Type {
				case "output_text", "text":
					contentBuilder.WriteString(content.Text)
				case "reasoning":
					reasoningBuilder.WriteString(content.Text)
				}
			}
		case "reasoning":
			for _, summary := range item.Summary {
				if summary.Type == "summary_text" || summary.Type == "text" {
					reasoningBuilder.WriteString(summary.Text)
				}
			}
		case "function_call":
			arguments := make(map[string]any)
			if item.Arguments != "" {
				if err := json.Unmarshal([]byte(item.Arguments), &arguments); err != nil {
					log.Printf("openai_compat: failed to decode tool call arguments for %q: %v", item.Name, err)
					arguments["raw"] = item.Arguments
				}
			}

			toolCalls = append(toolCalls, ToolCall{
				ID:        item.CallID,
				Name:      item.Name,
				Arguments: arguments,
			})
		}
	}

	finishReason := "stop"
	if len(toolCalls) > 0 {
		finishReason = "tool_calls"
	} else if apiResponse.Status == "incomplete" {
		finishReason = "length"
	}

	var usage *UsageInfo
	totalTokens := apiResponse.Usage.TotalTokens
	if totalTokens == 0 {
		totalTokens = apiResponse.Usage.InputTokens + apiResponse.Usage.OutputTokens
	}
	if totalTokens > 0 {
		promptTokens := apiResponse.Usage.PromptTokens
		if promptTokens == 0 {
			promptTokens = apiResponse.Usage.InputTokens
		}
		completionTokens := apiResponse.Usage.CompletionTokens
		if completionTokens == 0 {
			completionTokens = apiResponse.Usage.OutputTokens
		}
		usage = &UsageInfo{
			PromptTokens:     promptTokens,
			CompletionTokens: completionTokens,
			TotalTokens:      totalTokens,
		}
	}

	return &LLMResponse{
		Content:          contentBuilder.String(),
		ReasoningContent: reasoningBuilder.String(),
		ToolCalls:        toolCalls,
		FinishReason:     finishReason,
		Usage:            usage,
	}, nil
}

// buildResponseInput converts internal messages to Responses API input items.
// It maps text turns to `message`, assistant tool invocations to `function_call`,
// and tool outputs to `function_call_output`.
func buildResponseInput(messages []Message) []map[string]any {
	input := make([]map[string]any, 0, len(messages))
	for _, msg := range messages {
		if isToolOutputMessage(msg) {
			input = append(input, map[string]any{
				"type":    "function_call_output",
				"call_id": msg.ToolCallID,
				"output":  msg.Content,
			})
			continue
		}

		if msg.Role == "assistant" && len(msg.ToolCalls) > 0 {
			if msg.Content != "" {
				input = append(input, responseMessageItem(msg.Role, msg.Content))
			}
			for _, toolCall := range msg.ToolCalls {
				name, arguments, ok := resolveToolCallForResponseInput(toolCall)
				if !ok {
					continue
				}
				input = append(input, map[string]any{
					"type":      "function_call",
					"call_id":   toolCall.ID,
					"name":      name,
					"arguments": arguments,
				})
			}
			continue
		}

		if msg.Content == "" {
			continue
		}
		input = append(input, responseMessageItem(msg.Role, msg.Content))
	}
	return input
}

// toResponseTools converts internal tool definitions to Responses API tools.
func toResponseTools(tools []ToolDefinition) []map[string]any {
	result := make([]map[string]any, 0, len(tools))
	for _, tool := range tools {
		if tool.Type != "function" {
			continue
		}
		entry := map[string]any{
			"type":       "function",
			"name":       tool.Function.Name,
			"parameters": tool.Function.Parameters,
		}
		if tool.Function.Description != "" {
			entry["description"] = tool.Function.Description
		}
		result = append(result, entry)
	}
	return result
}

// responseMessageItem builds a Responses API message input item.
func responseMessageItem(role, content string) map[string]any {
	if role == "system" {
		role = "developer"
	}
	return map[string]any{
		"type": "message",
		"role": role,
		"content": []map[string]any{
			{
				"type": "input_text",
				"text": content,
			},
		},
	}
}

// isToolOutputMessage reports whether a message is a tool result.
func isToolOutputMessage(msg Message) bool {
	return msg.ToolCallID != "" && (msg.Role == "tool" || msg.Role == "user")
}

// resolveToolCallForResponseInput extracts name and arguments for a function call item.
func resolveToolCallForResponseInput(toolCall ToolCall) (string, string, bool) {
	name := toolCall.Name
	if name == "" && toolCall.Function != nil {
		name = toolCall.Function.Name
	}
	if name == "" {
		return "", "", false
	}

	if len(toolCall.Arguments) > 0 {
		encoded, err := json.Marshal(toolCall.Arguments)
		if err != nil {
			return "", "", false
		}
		return name, string(encoded), true
	}

	if toolCall.Function != nil && toolCall.Function.Arguments != "" {
		return name, toolCall.Function.Arguments, true
	}

	return name, "{}", true
}

func normalizeModel(model, apiBase string) string {
	idx := strings.Index(model, "/")
	if idx == -1 {
		return model
	}

	if strings.Contains(strings.ToLower(apiBase), "openrouter.ai") {
		return model
	}

	prefix := strings.ToLower(model[:idx])
	switch prefix {
	case "moonshot", "nvidia", "groq", "ollama", "deepseek", "google", "openrouter", "zhipu", "mistral":
		return model[idx+1:]
	default:
		return model
	}
}

func asInt(v any) (int, bool) {
	switch val := v.(type) {
	case int:
		return val, true
	case int64:
		return int(val), true
	case float64:
		return int(val), true
	case float32:
		return int(val), true
	default:
		return 0, false
	}
}

func asFloat(v any) (float64, bool) {
	switch val := v.(type) {
	case float64:
		return val, true
	case float32:
		return float64(val), true
	case int:
		return float64(val), true
	case int64:
		return float64(val), true
	default:
		return 0, false
	}
}

// sanitizedURL returns a URL string safe for logs by removing user info, query, and fragment.
func sanitizedURL(u *url.URL) string {
	if u == nil {
		return ""
	}
	copyURL := *u
	copyURL.User = nil
	copyURL.RawQuery = ""
	copyURL.Fragment = ""
	return copyURL.String()
}

// summarizeBody returns a compact single-line body preview with a max byte length.
func summarizeBody(body []byte, max int) string {
	if max <= 0 {
		return ""
	}
	text := strings.TrimSpace(string(body))
	text = strings.ReplaceAll(text, "\n", "\\n")
	text = strings.ReplaceAll(text, "\r", "")
	if len(text) <= max {
		return text
	}
	return text[:max] + "...(truncated)"
}

// summarizeEndpointError extracts common JSON error fields from endpoint responses.
// It returns a compact single-line string suitable for logs and surfaced errors.
func summarizeEndpointError(body []byte, max int) string {
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		return ""
	}

	parts := make([]string, 0, 6)
	if v, ok := lookupJSONPath(payload, "error", "message"); ok {
		parts = append(parts, fmt.Sprintf("message=%q", summarizeBody([]byte(stringifyJSONValue(v)), max)))
	} else if v, ok := lookupJSONPath(payload, "message"); ok {
		parts = append(parts, fmt.Sprintf("message=%q", summarizeBody([]byte(stringifyJSONValue(v)), max)))
	}

	if v, ok := lookupJSONPath(payload, "error", "type"); ok {
		parts = append(parts, fmt.Sprintf("type=%s", stringifyJSONValue(v)))
	}

	if v, ok := lookupJSONPath(payload, "error", "code"); ok {
		parts = append(parts, fmt.Sprintf("code=%s", stringifyJSONValue(v)))
	} else if v, ok := lookupJSONPath(payload, "code"); ok {
		parts = append(parts, fmt.Sprintf("code=%s", stringifyJSONValue(v)))
	} else if v, ok := lookupJSONPath(payload, "error_code"); ok {
		parts = append(parts, fmt.Sprintf("code=%s", stringifyJSONValue(v)))
	}

	if v, ok := lookupJSONPath(payload, "error", "param"); ok {
		parts = append(parts, fmt.Sprintf("param=%s", stringifyJSONValue(v)))
	}

	if v, ok := lookupJSONPath(payload, "error", "request_id"); ok {
		parts = append(parts, fmt.Sprintf("request_id=%s", stringifyJSONValue(v)))
	} else if v, ok := lookupJSONPath(payload, "request_id"); ok {
		parts = append(parts, fmt.Sprintf("request_id=%s", stringifyJSONValue(v)))
	}

	if len(parts) == 0 {
		return ""
	}

	joined := strings.Join(parts, ", ")
	if max <= 0 || len(joined) <= max {
		return joined
	}
	return joined[:max] + "...(truncated)"
}

// lookupJSONPath returns a nested value from a JSON map by path.
func lookupJSONPath(root map[string]any, path ...string) (any, bool) {
	var current any = root
	for _, key := range path {
		asMap, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		next, ok := asMap[key]
		if !ok {
			return nil, false
		}
		current = next
	}
	return current, true
}

// stringifyJSONValue converts a JSON value into a compact single-line string.
func stringifyJSONValue(value any) string {
	switch typed := value.(type) {
	case string:
		v := strings.TrimSpace(typed)
		v = strings.ReplaceAll(v, "\n", "\\n")
		v = strings.ReplaceAll(v, "\r", "")
		return v
	default:
		encoded, err := json.Marshal(typed)
		if err != nil {
			return fmt.Sprintf("%v", typed)
		}
		return string(encoded)
	}
}

// firstNonEmpty returns the first non-empty trimmed string.
func firstNonEmpty(values ...string) string {
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed != "" {
			return trimmed
		}
	}
	return ""
}
