package bus

type InboundMessage struct {
	Channel    string            `json:"channel"`
	SenderID   string            `json:"sender_id"`
	ChatID     string            `json:"chat_id"`
	Content    string            `json:"content"`
	Media      []string          `json:"media,omitempty"`
	SessionKey string            `json:"session_key"`
	Metadata   map[string]string `json:"metadata,omitempty"`
}

type OutboundMessage struct {
	Channel     string               `json:"channel"`
	ChatID      string               `json:"chat_id"`
	Content     string               `json:"content"`
	Attachments []OutboundAttachment `json:"attachments,omitempty"`
	Buttons     []OutboundButton     `json:"buttons,omitempty"`
}

// OutboundAttachment represents a rich media/file item to send with an outbound message.
type OutboundAttachment struct {
	Type    string `json:"type"`
	URL     string `json:"url,omitempty"`
	FileID  string `json:"file_id,omitempty"`
	Path    string `json:"path,omitempty"`
	Caption string `json:"caption,omitempty"`
}

// OutboundButton represents one interactive button attached to an outbound message.
type OutboundButton struct {
	Text         string `json:"text"`
	URL          string `json:"url,omitempty"`
	CallbackData string `json:"callback_data,omitempty"`
	Row          int    `json:"row,omitempty"`
}

type MessageHandler func(InboundMessage) error
