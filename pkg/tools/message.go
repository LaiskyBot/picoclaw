package tools

import (
	"context"
	"fmt"
	"strings"

	"github.com/sipeed/picoclaw/pkg/bus"
)

type SendCallback func(msg bus.OutboundMessage) error

type MessageTool struct {
	sendCallback   SendCallback
	defaultChannel string
	defaultChatID  string
	sentInRound    bool // Tracks whether a message was sent in the current processing round
}

func NewMessageTool() *MessageTool {
	return &MessageTool{}
}

func (t *MessageTool) Name() string {
	return "message"
}

func (t *MessageTool) Description() string {
	return "Send a message to user on a chat channel. Supports text, attachments (photo/document), and interactive buttons."
}

func (t *MessageTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"content": map[string]any{
				"type":        "string",
				"description": "The message content to send",
			},
			"channel": map[string]any{
				"type":        "string",
				"description": "Optional: target channel (telegram, whatsapp, etc.)",
			},
			"chat_id": map[string]any{
				"type":        "string",
				"description": "Optional: target chat/user ID",
			},
			"attachments": map[string]any{
				"type": "array",
				"description": "Optional: attachments to send (photo/image/document/file)",
				"items": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"type": map[string]any{
							"type":        "string",
							"description": "Attachment type: photo/image/document/file",
						},
						"url": map[string]any{
							"type":        "string",
							"description": "HTTP URL of file to send",
						},
						"file_id": map[string]any{
							"type":        "string",
							"description": "Telegram file_id to resend",
						},
						"path": map[string]any{
							"type":        "string",
							"description": "Local file path to upload",
						},
						"caption": map[string]any{
							"type":        "string",
							"description": "Optional caption for the attachment",
						},
					},
					"required": []string{"type"},
				},
			},
			"buttons": map[string]any{
				"type": "array",
				"description": "Optional interactive buttons",
				"items": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"text": map[string]any{
							"type":        "string",
							"description": "Button label",
						},
						"url": map[string]any{
							"type":        "string",
							"description": "Open this URL when clicked",
						},
						"callback_data": map[string]any{
							"type":        "string",
							"description": "Bot callback payload when clicked",
						},
						"row": map[string]any{
							"type":        "integer",
							"description": "Optional row index (0-based)",
						},
					},
					"required": []string{"text"},
				},
			},
		},
		"required": []string{"content"},
	}
}

func (t *MessageTool) SetContext(channel, chatID string) {
	t.defaultChannel = channel
	t.defaultChatID = chatID
	t.sentInRound = false // Reset send tracking for new processing round
}

// HasSentInRound returns true if the message tool sent a message during the current round.
func (t *MessageTool) HasSentInRound() bool {
	return t.sentInRound
}

func (t *MessageTool) SetSendCallback(callback SendCallback) {
	t.sendCallback = callback
}

func (t *MessageTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	_ = ctx

	content, ok := args["content"].(string)
	if !ok {
		return &ToolResult{ForLLM: "content is required", IsError: true}
	}

	channel, _ := args["channel"].(string)
	chatID, _ := args["chat_id"].(string)

	if channel == "" {
		channel = t.defaultChannel
	}
	if chatID == "" {
		chatID = t.defaultChatID
	}

	if channel == "" || chatID == "" {
		return &ToolResult{ForLLM: "No target channel/chat specified", IsError: true}
	}

	if t.sendCallback == nil {
		return &ToolResult{ForLLM: "Message sending not configured", IsError: true}
	}

	attachments, err := parseMessageAttachments(args["attachments"])
	if err != nil {
		return &ToolResult{ForLLM: fmt.Sprintf("invalid attachments: %v", err), IsError: true, Err: err}
	}

	buttons, err := parseMessageButtons(args["buttons"])
	if err != nil {
		return &ToolResult{ForLLM: fmt.Sprintf("invalid buttons: %v", err), IsError: true, Err: err}
	}

	outbound := bus.OutboundMessage{
		Channel:     channel,
		ChatID:      chatID,
		Content:     content,
		Attachments: attachments,
		Buttons:     buttons,
	}

	if err := t.sendCallback(outbound); err != nil {
		return &ToolResult{
			ForLLM:  fmt.Sprintf("sending message: %v", err),
			IsError: true,
			Err:     err,
		}
	}

	t.sentInRound = true
	// Silent: user already received the message directly
	return &ToolResult{
		ForLLM: fmt.Sprintf("Message sent to %s:%s", channel, chatID),
		Silent: true,
	}
}

// parseMessageAttachments converts tool argument attachments into typed outbound attachments.
// It validates attachment type and ensures at least one source (url/file_id/path) is provided.
func parseMessageAttachments(raw any) ([]bus.OutboundAttachment, error) {
	if raw == nil {
		return nil, nil
	}

	items, ok := raw.([]any)
	if !ok {
		return nil, fmt.Errorf("attachments must be an array")
	}

	attachments := make([]bus.OutboundAttachment, 0, len(items))
	for i, item := range items {
		obj, ok := item.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("attachments[%d] must be an object", i)
		}

		attachmentType, _ := obj["type"].(string)
		attachmentType = strings.ToLower(strings.TrimSpace(attachmentType))
		if attachmentType == "" {
			return nil, fmt.Errorf("attachments[%d].type is required", i)
		}

		switch attachmentType {
		case "image":
			attachmentType = "photo"
		case "photo", "document", "file":
		default:
			return nil, fmt.Errorf("attachments[%d].type must be one of photo/image/document/file", i)
		}

		attachment := bus.OutboundAttachment{
			Type:    attachmentType,
			URL:     strings.TrimSpace(stringArg(obj, "url")),
			FileID:  strings.TrimSpace(stringArg(obj, "file_id")),
			Path:    strings.TrimSpace(stringArg(obj, "path")),
			Caption: stringArg(obj, "caption"),
		}

		if attachment.URL == "" && attachment.FileID == "" && attachment.Path == "" {
			return nil, fmt.Errorf("attachments[%d] requires one of url/file_id/path", i)
		}

		attachments = append(attachments, attachment)
	}

	return attachments, nil
}

// parseMessageButtons converts tool argument buttons into typed outbound buttons.
// It validates that each button has text and exactly one action target (url or callback_data).
func parseMessageButtons(raw any) ([]bus.OutboundButton, error) {
	if raw == nil {
		return nil, nil
	}

	items, ok := raw.([]any)
	if !ok {
		return nil, fmt.Errorf("buttons must be an array")
	}

	buttons := make([]bus.OutboundButton, 0, len(items))
	for i, item := range items {
		obj, ok := item.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("buttons[%d] must be an object", i)
		}

		btn := bus.OutboundButton{
			Text:         stringArg(obj, "text"),
			URL:          strings.TrimSpace(stringArg(obj, "url")),
			CallbackData: strings.TrimSpace(stringArg(obj, "callback_data")),
			Row:          intArg(obj, "row"),
		}

		if strings.TrimSpace(btn.Text) == "" {
			return nil, fmt.Errorf("buttons[%d].text is required", i)
		}

		hasURL := btn.URL != ""
		hasCallback := btn.CallbackData != ""
		if hasURL == hasCallback {
			return nil, fmt.Errorf("buttons[%d] requires exactly one of url/callback_data", i)
		}

		if btn.Row < 0 {
			btn.Row = 0
		}

		buttons = append(buttons, btn)
	}

	return buttons, nil
}

// stringArg returns a string value from a map key, or empty string when missing/invalid.
func stringArg(obj map[string]any, key string) string {
	v, _ := obj[key].(string)
	return v
}

// intArg returns an integer value from a map key, supporting common numeric JSON decode types.
func intArg(obj map[string]any, key string) int {
	switch val := obj[key].(type) {
	case int:
		return val
	case int32:
		return int(val)
	case int64:
		return int(val)
	case float64:
		return int(val)
	default:
		return 0
	}
}
