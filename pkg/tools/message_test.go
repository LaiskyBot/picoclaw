package tools

import (
	"context"
	"errors"
	"testing"

	"github.com/sipeed/picoclaw/pkg/bus"
)

func TestMessageTool_Execute_Success(t *testing.T) {
	tool := NewMessageTool()
	tool.SetContext("test-channel", "test-chat-id")

	var sent bus.OutboundMessage
	tool.SetSendCallback(func(msg bus.OutboundMessage) error {
		sent = msg
		return nil
	})

	ctx := context.Background()
	args := map[string]any{
		"content": "Hello, world!",
	}

	result := tool.Execute(ctx, args)

	// Verify message was sent with correct parameters
	if sent.Channel != "test-channel" {
		t.Errorf("Expected channel 'test-channel', got '%s'", sent.Channel)
	}
	if sent.ChatID != "test-chat-id" {
		t.Errorf("Expected chatID 'test-chat-id', got '%s'", sent.ChatID)
	}
	if sent.Content != "Hello, world!" {
		t.Errorf("Expected content 'Hello, world!', got '%s'", sent.Content)
	}

	// Verify ToolResult meets US-011 criteria:
	// - Send success returns SilentResult (Silent=true)
	if !result.Silent {
		t.Error("Expected Silent=true for successful send")
	}

	// - ForLLM contains send status description
	if result.ForLLM != "Message sent to test-channel:test-chat-id" {
		t.Errorf("Expected ForLLM 'Message sent to test-channel:test-chat-id', got '%s'", result.ForLLM)
	}

	// - ForUser is empty (user already received message directly)
	if result.ForUser != "" {
		t.Errorf("Expected ForUser to be empty, got '%s'", result.ForUser)
	}

	// - IsError should be false
	if result.IsError {
		t.Error("Expected IsError=false for successful send")
	}
}

func TestMessageTool_Execute_WithCustomChannel(t *testing.T) {
	tool := NewMessageTool()
	tool.SetContext("default-channel", "default-chat-id")

	var sent bus.OutboundMessage
	tool.SetSendCallback(func(msg bus.OutboundMessage) error {
		sent = msg
		return nil
	})

	ctx := context.Background()
	args := map[string]any{
		"content": "Test message",
		"channel": "custom-channel",
		"chat_id": "custom-chat-id",
	}

	result := tool.Execute(ctx, args)

	// Verify custom channel/chatID were used instead of defaults
	if sent.Channel != "custom-channel" {
		t.Errorf("Expected channel 'custom-channel', got '%s'", sent.Channel)
	}
	if sent.ChatID != "custom-chat-id" {
		t.Errorf("Expected chatID 'custom-chat-id', got '%s'", sent.ChatID)
	}

	if !result.Silent {
		t.Error("Expected Silent=true")
	}
	if result.ForLLM != "Message sent to custom-channel:custom-chat-id" {
		t.Errorf("Expected ForLLM 'Message sent to custom-channel:custom-chat-id', got '%s'", result.ForLLM)
	}
}

func TestMessageTool_Execute_SendFailure(t *testing.T) {
	tool := NewMessageTool()
	tool.SetContext("test-channel", "test-chat-id")

	sendErr := errors.New("network error")
	tool.SetSendCallback(func(msg bus.OutboundMessage) error {
		return sendErr
	})

	ctx := context.Background()
	args := map[string]any{
		"content": "Test message",
	}

	result := tool.Execute(ctx, args)

	// Verify ToolResult for send failure:
	// - Send failure returns ErrorResult (IsError=true)
	if !result.IsError {
		t.Error("Expected IsError=true for failed send")
	}

	// - ForLLM contains error description
	expectedErrMsg := "sending message: network error"
	if result.ForLLM != expectedErrMsg {
		t.Errorf("Expected ForLLM '%s', got '%s'", expectedErrMsg, result.ForLLM)
	}

	// - Err field should contain original error
	if result.Err == nil {
		t.Error("Expected Err to be set")
	}
	if result.Err != sendErr {
		t.Errorf("Expected Err to be sendErr, got %v", result.Err)
	}
}

func TestMessageTool_Execute_MissingContent(t *testing.T) {
	tool := NewMessageTool()
	tool.SetContext("test-channel", "test-chat-id")

	ctx := context.Background()
	args := map[string]any{} // content missing

	result := tool.Execute(ctx, args)

	// Verify error result for missing content
	if !result.IsError {
		t.Error("Expected IsError=true for missing content")
	}
	if result.ForLLM != "content is required" {
		t.Errorf("Expected ForLLM 'content is required', got '%s'", result.ForLLM)
	}
}

func TestMessageTool_Execute_NoTargetChannel(t *testing.T) {
	tool := NewMessageTool()
	// No SetContext called, so defaultChannel and defaultChatID are empty

	tool.SetSendCallback(func(msg bus.OutboundMessage) error {
		return nil
	})

	ctx := context.Background()
	args := map[string]any{
		"content": "Test message",
	}

	result := tool.Execute(ctx, args)

	// Verify error when no target channel specified
	if !result.IsError {
		t.Error("Expected IsError=true when no target channel")
	}
	if result.ForLLM != "No target channel/chat specified" {
		t.Errorf("Expected ForLLM 'No target channel/chat specified', got '%s'", result.ForLLM)
	}
}

func TestMessageTool_Execute_NotConfigured(t *testing.T) {
	tool := NewMessageTool()
	tool.SetContext("test-channel", "test-chat-id")
	// No SetSendCallback called

	ctx := context.Background()
	args := map[string]any{
		"content": "Test message",
	}

	result := tool.Execute(ctx, args)

	// Verify error when send callback not configured
	if !result.IsError {
		t.Error("Expected IsError=true when send callback not configured")
	}
	if result.ForLLM != "Message sending not configured" {
		t.Errorf("Expected ForLLM 'Message sending not configured', got '%s'", result.ForLLM)
	}
}

func TestMessageTool_Name(t *testing.T) {
	tool := NewMessageTool()
	if tool.Name() != "message" {
		t.Errorf("Expected name 'message', got '%s'", tool.Name())
	}
}

func TestMessageTool_Description(t *testing.T) {
	tool := NewMessageTool()
	desc := tool.Description()
	if desc == "" {
		t.Error("Description should not be empty")
	}
}

func TestMessageTool_Parameters(t *testing.T) {
	tool := NewMessageTool()
	params := tool.Parameters()

	// Verify parameters structure
	typ, ok := params["type"].(string)
	if !ok || typ != "object" {
		t.Error("Expected type 'object'")
	}

	props, ok := params["properties"].(map[string]any)
	if !ok {
		t.Fatal("Expected properties to be a map")
	}

	// Check required properties
	required, ok := params["required"].([]string)
	if !ok || len(required) != 1 || required[0] != "content" {
		t.Error("Expected 'content' to be required")
	}

	// Check content property
	contentProp, ok := props["content"].(map[string]any)
	if !ok {
		t.Error("Expected 'content' property")
	}
	if contentProp["type"] != "string" {
		t.Error("Expected content type to be 'string'")
	}

	// Check channel property (optional)
	channelProp, ok := props["channel"].(map[string]any)
	if !ok {
		t.Error("Expected 'channel' property")
	}
	if channelProp["type"] != "string" {
		t.Error("Expected channel type to be 'string'")
	}

	// Check chat_id property (optional)
	chatIDProp, ok := props["chat_id"].(map[string]any)
	if !ok {
		t.Error("Expected 'chat_id' property")
	}
	if chatIDProp["type"] != "string" {
		t.Error("Expected chat_id type to be 'string'")
	}
}

func TestMessageTool_Execute_WithAttachmentsAndButtons(t *testing.T) {
	tool := NewMessageTool()
	tool.SetContext("telegram", "123")

	var sent bus.OutboundMessage
	tool.SetSendCallback(func(msg bus.OutboundMessage) error {
		sent = msg
		return nil
	})

	result := tool.Execute(context.Background(), map[string]any{
		"content": "Rich message",
		"attachments": []any{
			map[string]any{
				"type": "image",
				"url":  "https://example.com/a.png",
			},
			map[string]any{
				"type":    "file",
				"file_id": "abc123",
			},
		},
		"buttons": []any{
			map[string]any{
				"text": "Open",
				"url":  "https://example.com",
			},
			map[string]any{
				"text":          "Run",
				"callback_data": "run:1",
				"row":           float64(1),
			},
		},
	})

	if result.IsError {
		t.Fatalf("expected success, got error: %s", result.ForLLM)
	}
	if len(sent.Attachments) != 2 {
		t.Fatalf("expected 2 attachments, got %d", len(sent.Attachments))
	}
	if sent.Attachments[0].Type != "photo" {
		t.Fatalf("expected first attachment normalized type photo, got %s", sent.Attachments[0].Type)
	}
	if len(sent.Buttons) != 2 {
		t.Fatalf("expected 2 buttons, got %d", len(sent.Buttons))
	}
	if sent.Buttons[1].Row != 1 {
		t.Fatalf("expected second button row=1, got %d", sent.Buttons[1].Row)
	}
}

func TestMessageTool_Execute_InvalidButtons(t *testing.T) {
	tool := NewMessageTool()
	tool.SetContext("telegram", "123")
	tool.SetSendCallback(func(msg bus.OutboundMessage) error { return nil })

	result := tool.Execute(context.Background(), map[string]any{
		"content": "test",
		"buttons": []any{
			map[string]any{
				"text": "Bad",
			},
		},
	})

	if !result.IsError {
		t.Fatal("expected error for invalid button action")
	}
}

func TestMessageTool_Execute_RemapPipelineTaskIDToContextTarget(t *testing.T) {
	tool := NewMessageTool()
	tool.SetContext("telegram", "861999008")

	var sent bus.OutboundMessage
	tool.SetSendCallback(func(msg bus.OutboundMessage) error {
		sent = msg
		return nil
	})

	result := tool.Execute(context.Background(), map[string]any{
		"content": "Here is your screenshot",
		"chat_id": "task-2026-02-26-0014",
		"attachments": []any{
			map[string]any{
				"type": "photo",
				"path": "/tmp/laisky_blog.png",
			},
		},
	})

	if result.IsError {
		t.Fatalf("expected success, got error: %s", result.ForLLM)
	}
	if sent.Channel != "telegram" {
		t.Fatalf("expected channel telegram, got %s", sent.Channel)
	}
	if sent.ChatID != "861999008" {
		t.Fatalf("expected chat id remapped to contextual target, got %s", sent.ChatID)
	}
	if len(sent.Attachments) != 1 {
		t.Fatalf("expected 1 attachment, got %d", len(sent.Attachments))
	}
}

func TestMessageTool_Execute_KeepExplicitExternalTarget(t *testing.T) {
	tool := NewMessageTool()
	tool.SetContext("telegram", "861999008")

	var sent bus.OutboundMessage
	tool.SetSendCallback(func(msg bus.OutboundMessage) error {
		sent = msg
		return nil
	})

	result := tool.Execute(context.Background(), map[string]any{
		"content": "Forward to another chat",
		"channel": "telegram",
		"chat_id": "861999009",
	})

	if result.IsError {
		t.Fatalf("expected success, got error: %s", result.ForLLM)
	}
	if sent.Channel != "telegram" {
		t.Fatalf("expected channel telegram, got %s", sent.Channel)
	}
	if sent.ChatID != "861999009" {
		t.Fatalf("expected explicit chat id to be preserved, got %s", sent.ChatID)
	}
}
