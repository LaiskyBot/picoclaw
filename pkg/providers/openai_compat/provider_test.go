package openai_compat

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProviderChat_UsesMaxOutputTokens(t *testing.T) {
	var requestBody map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/responses" {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		if err := json.NewDecoder(r.Body).Decode(&requestBody); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		resp := map[string]any{
			"status": "completed",
			"output": []map[string]any{
				{
					"type": "message",
					"content": []map[string]any{
						{"type": "output_text", "text": "ok"},
					},
				},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	p := NewProvider("key", server.URL, "")
	_, err := p.Chat(
		t.Context(),
		[]Message{{Role: "user", Content: "hi"}},
		nil,
		"gpt-5-mini",
		map[string]any{"max_tokens": 1234},
	)
	if err != nil {
		t.Fatalf("Chat() error = %v", err)
	}

	if _, ok := requestBody["max_output_tokens"]; !ok {
		t.Fatalf("expected max_output_tokens in request body")
	}
	if _, ok := requestBody["max_tokens"]; ok {
		t.Fatalf("did not expect max_tokens key in responses request")
	}
}

func TestProviderChat_ParsesToolCalls(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := map[string]any{
			"status": "completed",
			"output": []map[string]any{
				{
					"type":      "function_call",
					"call_id":   "call_1",
					"name":      "get_weather",
					"arguments": "{\"city\":\"SF\"}",
				},
			},
			"usage": map[string]any{
				"input_tokens":  10,
				"output_tokens": 5,
				"total_tokens":  15,
			},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	p := NewProvider("key", server.URL, "")
	out, err := p.Chat(t.Context(), []Message{{Role: "user", Content: "hi"}}, nil, "gpt-4o", nil)
	if err != nil {
		t.Fatalf("Chat() error = %v", err)
	}
	if len(out.ToolCalls) != 1 {
		t.Fatalf("len(ToolCalls) = %d, want 1", len(out.ToolCalls))
	}
	if out.ToolCalls[0].Name != "get_weather" {
		t.Fatalf("ToolCalls[0].Name = %q, want %q", out.ToolCalls[0].Name, "get_weather")
	}
	if out.ToolCalls[0].Arguments["city"] != "SF" {
		t.Fatalf("ToolCalls[0].Arguments[city] = %v, want SF", out.ToolCalls[0].Arguments["city"])
	}
}

func TestProviderChat_ParsesReasoningContent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := map[string]any{
			"status": "completed",
			"output": []map[string]any{
				{
					"type": "message",
					"content": []map[string]any{
						{"type": "output_text", "text": "The answer is 2"},
					},
				},
				{
					"type": "reasoning",
					"summary": []map[string]any{
						{"type": "summary_text", "text": "Let me think step by step... 1+1=2"},
					},
				},
				{
					"type":      "function_call",
					"call_id":   "call_1",
					"name":      "calculator",
					"arguments": "{\"expr\":\"1+1\"}",
				},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	p := NewProvider("key", server.URL, "")
	out, err := p.Chat(t.Context(), []Message{{Role: "user", Content: "1+1=?"}}, nil, "kimi-k2.5", nil)
	if err != nil {
		t.Fatalf("Chat() error = %v", err)
	}
	if out.ReasoningContent != "Let me think step by step... 1+1=2" {
		t.Fatalf("ReasoningContent = %q, want %q", out.ReasoningContent, "Let me think step by step... 1+1=2")
	}
	if out.Content != "The answer is 2" {
		t.Fatalf("Content = %q, want %q", out.Content, "The answer is 2")
	}
	if len(out.ToolCalls) != 1 {
		t.Fatalf("len(ToolCalls) = %d, want 1", len(out.ToolCalls))
	}
}

func TestProviderChat_HTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "bad request", http.StatusBadRequest)
	}))
	defer server.Close()

	p := NewProvider("key", server.URL, "")
	_, err := p.Chat(t.Context(), []Message{{Role: "user", Content: "hi"}}, nil, "gpt-4o", nil)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestProviderChat_StripsMoonshotPrefixAndNormalizesKimiTemperature(t *testing.T) {
	var requestBody map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&requestBody); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		resp := map[string]any{
			"status": "completed",
			"output": []map[string]any{
				{
					"type": "message",
					"content": []map[string]any{
						{"type": "output_text", "text": "ok"},
					},
				},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	p := NewProvider("key", server.URL, "")
	_, err := p.Chat(
		t.Context(),
		[]Message{{Role: "user", Content: "hi"}},
		nil,
		"moonshot/kimi-k2.5",
		map[string]any{"temperature": 0.3},
	)
	if err != nil {
		t.Fatalf("Chat() error = %v", err)
	}

	if requestBody["model"] != "kimi-k2.5" {
		t.Fatalf("model = %v, want kimi-k2.5", requestBody["model"])
	}
	if requestBody["temperature"] != 1.0 {
		t.Fatalf("temperature = %v, want 1.0", requestBody["temperature"])
	}
}

func TestProviderChat_StripsGroqAndOllamaPrefixes(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantModel string
	}{
		{
			name:      "strips groq prefix and keeps nested model",
			input:     "groq/openai/gpt-oss-120b",
			wantModel: "openai/gpt-oss-120b",
		},
		{
			name:      "strips ollama prefix",
			input:     "ollama/qwen2.5:14b",
			wantModel: "qwen2.5:14b",
		},
		{
			name:      "strips deepseek prefix",
			input:     "deepseek/deepseek-chat",
			wantModel: "deepseek-chat",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var requestBody map[string]any

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if err := json.NewDecoder(r.Body).Decode(&requestBody); err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
				resp := map[string]any{
						"status": "completed",
						"output": []map[string]any{
						{
								"type": "message",
								"content": []map[string]any{
									{"type": "output_text", "text": "ok"},
								},
						},
					},
				}
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(resp)
			}))
			defer server.Close()

			p := NewProvider("key", server.URL, "")
			_, err := p.Chat(t.Context(), []Message{{Role: "user", Content: "hi"}}, nil, tt.input, nil)
			if err != nil {
				t.Fatalf("Chat() error = %v", err)
			}

			if requestBody["model"] != tt.wantModel {
				t.Fatalf("model = %v, want %s", requestBody["model"], tt.wantModel)
			}
		})
	}
}

func TestProvider_ProxyConfigured(t *testing.T) {
	proxyURL := "http://127.0.0.1:8080"
	p := NewProvider("key", "https://example.com", proxyURL)

	transport, ok := p.httpClient.Transport.(*http.Transport)
	if !ok || transport == nil {
		t.Fatalf("expected http transport with proxy, got %T", p.httpClient.Transport)
	}

	req := &http.Request{URL: &url.URL{Scheme: "https", Host: "api.example.com"}}
	gotProxy, err := transport.Proxy(req)
	if err != nil {
		t.Fatalf("proxy function returned error: %v", err)
	}
	if gotProxy == nil || gotProxy.String() != proxyURL {
		t.Fatalf("proxy = %v, want %s", gotProxy, proxyURL)
	}
}

func TestProviderChat_AcceptsNumericOptionTypes(t *testing.T) {
	var requestBody map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&requestBody); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		resp := map[string]any{
			"status": "completed",
			"output": []map[string]any{
				{
					"type": "message",
					"content": []map[string]any{
						{"type": "output_text", "text": "ok"},
					},
				},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	p := NewProvider("key", server.URL, "")
	_, err := p.Chat(
		t.Context(),
		[]Message{{Role: "user", Content: "hi"}},
		nil,
		"gpt-4o",
		map[string]any{"max_tokens": float64(512), "temperature": 1},
	)
	if err != nil {
		t.Fatalf("Chat() error = %v", err)
	}

	if requestBody["max_output_tokens"] != float64(512) {
		t.Fatalf("max_output_tokens = %v, want 512", requestBody["max_output_tokens"])
	}
	if requestBody["temperature"] != float64(1) {
		t.Fatalf("temperature = %v, want 1", requestBody["temperature"])
	}
}

func TestProviderChat_BuildsResponsesInputAndTools(t *testing.T) {
	var requestBody map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/responses", r.URL.Path)
		require.NoError(t, json.NewDecoder(r.Body).Decode(&requestBody))

		resp := map[string]any{
			"status": "completed",
			"output": []map[string]any{
				{
					"type": "message",
					"content": []map[string]any{
						{"type": "output_text", "text": "ok"},
					},
				},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	p := NewProvider("key", server.URL, "")
	_, err := p.Chat(
		t.Context(),
		[]Message{
			{Role: "system", Content: "sys rule"},
			{Role: "user", Content: "what time is it"},
			{
				Role:    "assistant",
				Content: "calling tool",
				ToolCalls: []ToolCall{
					{ID: "call_1", Name: "get_time", Arguments: map[string]any{"tz": "UTC"}},
				},
			},
			{Role: "tool", ToolCallID: "call_1", Content: "2026-02-25T00:00:00Z"},
		},
		[]ToolDefinition{
			{
				Type: "function",
				Function: ToolFunctionDefinition{
					Name:        "get_time",
					Description: "Get time by timezone",
					Parameters: map[string]any{
						"type": "object",
					},
				},
			},
		},
		"gpt-5-mini",
		map[string]any{"max_tokens": 64},
	)
	require.NoError(t, err)

	require.Equal(t, "gpt-5-mini", requestBody["model"])
	require.Equal(t, float64(64), requestBody["max_output_tokens"])
	require.Equal(t, "auto", requestBody["tool_choice"])
	require.Equal(t, true, requestBody["parallel_tool_calls"])

	inputItems, ok := requestBody["input"].([]any)
	require.True(t, ok)
	require.Len(t, inputItems, 5)

	systemItem, ok := inputItems[0].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "message", systemItem["type"])
	require.Equal(t, "developer", systemItem["role"])

	toolCallItem, ok := inputItems[3].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "function_call", toolCallItem["type"])
	require.Equal(t, "call_1", toolCallItem["call_id"])
	require.Equal(t, "get_time", toolCallItem["name"])
	require.Equal(t, `{"tz":"UTC"}`, toolCallItem["arguments"])

	toolOutputItem, ok := inputItems[4].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "function_call_output", toolOutputItem["type"])
	require.Equal(t, "call_1", toolOutputItem["call_id"])
	require.Equal(t, "2026-02-25T00:00:00Z", toolOutputItem["output"])

	tools, ok := requestBody["tools"].([]any)
	require.True(t, ok)
	require.Len(t, tools, 1)
	toolDef, ok := tools[0].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "function", toolDef["type"])
	require.Equal(t, "get_time", toolDef["name"])
	require.Equal(t, "Get time by timezone", toolDef["description"])
}

func TestProviderChat_DisablesParallelToolCallsWhenRequested(t *testing.T) {
	var requestBody map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/responses", r.URL.Path)
		require.NoError(t, json.NewDecoder(r.Body).Decode(&requestBody))

		resp := map[string]any{
			"status": "completed",
			"output": []map[string]any{
				{
					"type": "message",
					"content": []map[string]any{
						{"type": "output_text", "text": "ok"},
					},
				},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	p := NewProvider("key", server.URL, "")
	_, err := p.Chat(
		t.Context(),
		[]Message{{Role: "user", Content: "hi"}},
		[]ToolDefinition{{
			Type: "function",
			Function: ToolFunctionDefinition{
				Name:        "get_time",
				Description: "Get time by timezone",
				Parameters: map[string]any{
					"type": "object",
				},
			},
		}},
		"gpt-5-mini",
		map[string]any{"parallel_tool_calls": false},
	)
	require.NoError(t, err)
	require.Equal(t, false, requestBody["parallel_tool_calls"])
}

func TestProviderChat_RetriesWithoutParallelToolCallsWhenUnsupported(t *testing.T) {
	requestBodies := make([]map[string]any, 0, 2)
	requestCount := 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		var body map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		requestBodies = append(requestBodies, body)

		if requestCount == 1 {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":{"message":"unknown field: parallel_tool_calls"}}`))
			return
		}

		resp := map[string]any{
			"status": "completed",
			"output": []map[string]any{
				{
					"type": "message",
					"content": []map[string]any{{"type": "output_text", "text": "ok"}},
				},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	p := NewProvider("key", server.URL, "")
	_, err := p.Chat(
		t.Context(),
		[]Message{{Role: "user", Content: "hi"}},
		[]ToolDefinition{{
			Type: "function",
			Function: ToolFunctionDefinition{
				Name:        "get_time",
				Description: "Get time by timezone",
				Parameters: map[string]any{
					"type": "object",
				},
			},
		}},
		"gpt-5-mini",
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, 2, requestCount)
	require.Equal(t, true, requestBodies[0]["parallel_tool_calls"])
	_, exists := requestBodies[1]["parallel_tool_calls"]
	require.False(t, exists)
}

func TestProviderChat_ParsesMessageOutputAndUsage(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := map[string]any{
			"status": "completed",
			"output": []map[string]any{
				{
					"type": "message",
					"content": []map[string]any{
						{"type": "output_text", "text": "hello"},
						{"type": "text", "text": " world"},
					},
				},
			},
			"usage": map[string]any{
				"input_tokens":  12,
				"output_tokens": 7,
				"total_tokens":  19,
			},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	p := NewProvider("key", server.URL, "")
	out, err := p.Chat(t.Context(), []Message{{Role: "user", Content: "hi"}}, nil, "gpt-4.1", nil)
	require.NoError(t, err)
	require.Equal(t, "hello world", out.Content)
	require.Equal(t, "stop", out.FinishReason)
	require.NotNil(t, out.Usage)
	require.Equal(t, 12, out.Usage.PromptTokens)
	require.Equal(t, 7, out.Usage.CompletionTokens)
	require.Equal(t, 19, out.Usage.TotalTokens)
}

func TestNormalizeModel_UsesAPIBase(t *testing.T) {
	if got := normalizeModel("deepseek/deepseek-chat", "https://api.deepseek.com/v1"); got != "deepseek-chat" {
		t.Fatalf("normalizeModel(deepseek) = %q, want %q", got, "deepseek-chat")
	}
	if got := normalizeModel("openrouter/auto", "https://openrouter.ai/api/v1"); got != "openrouter/auto" {
		t.Fatalf("normalizeModel(openrouter) = %q, want %q", got, "openrouter/auto")
	}
}

func TestProviderChat_HTTPErrorIncludesURLModelAndRequestID(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("x-request-id", "req-123")
		w.WriteHeader(522)
		_, _ = w.Write([]byte("error code: 522"))
	}))
	defer server.Close()

	p := NewProvider("key", server.URL, "")
	_, err := p.Chat(t.Context(), []Message{{Role: "user", Content: "hi"}}, nil, "gpt-4o", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "API request failed:")
	require.Contains(t, err.Error(), "Status: 522")
	require.Contains(t, err.Error(), "URL:    "+server.URL+"/responses")
	require.Contains(t, err.Error(), "Model:  gpt-4o")
	require.Contains(t, err.Error(), "Body:   error code: 522")
	require.Contains(t, err.Error(), "RequestID: req-123")
}

func TestProviderChat_HTTPErrorIncludesStructuredEndpointError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("request-id", "req-structured-456")
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(`{"error":{"message":"upstream model overloaded","type":"server_error","code":"bad_gateway","param":"model"}}`))
	}))
	defer server.Close()

	p := NewProvider("key", server.URL, "")
	_, err := p.Chat(t.Context(), []Message{{Role: "user", Content: "hi"}}, nil, "gpt-4o", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Status: 502")
	require.Contains(t, err.Error(), "EndpointError: message=\"upstream model overloaded\", type=server_error, code=bad_gateway, param=model")
	require.Contains(t, err.Error(), "RequestID: req-structured-456")
}

func TestSanitizedURL_RemovesSensitiveParts(t *testing.T) {
	u, err := url.Parse("https://user:pass@example.com/v1/responses?api_key=secret#frag")
	require.NoError(t, err)
	require.Equal(t, "https://example.com/v1/responses", sanitizedURL(u))
}

func TestSummarizeBody_SingleLineAndTruncate(t *testing.T) {
	body := []byte("line1\nline2\r\nline3")
	require.Equal(t, "line1\\nline2\\nline3", summarizeBody(body, 100))
	require.Equal(t, "line1...(truncated)", summarizeBody(body, 5))
}

func TestSummarizeEndpointError_ParsesCommonErrorFields(t *testing.T) {
	body := []byte(`{"error":{"message":"quota exceeded","type":"invalid_request_error","code":429,"param":"messages[0]"},"request_id":"abc-1"}`)
	summary := summarizeEndpointError(body, 500)
	require.Contains(t, summary, `message="quota exceeded"`)
	require.Contains(t, summary, "type=invalid_request_error")
	require.Contains(t, summary, "code=429")
	require.Contains(t, summary, "param=messages[0]")
	require.Contains(t, summary, "request_id=abc-1")
}

func TestSummarizeEndpointError_NonJSONReturnsEmpty(t *testing.T) {
	require.Equal(t, "", summarizeEndpointError([]byte("error code: 502"), 300))
}
