package agent

import (
	"context"
	"testing"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/tools"
	"github.com/stretchr/testify/require"
)

// TestHasSyntheticMediaMarker verifies marker detection across common assistant placeholder formats.
// The t parameter controls test lifecycle and assertions.
// It returns no value and fails when detection behavior is incorrect.
func TestHasSyntheticMediaMarker(t *testing.T) {
	require.True(t, hasSyntheticMediaMarker("[Sending image]"))
	require.True(t, hasSyntheticMediaMarker("Result ready. [Sending photo]"))
	require.True(t, hasSyntheticMediaMarker("[sending attachment]"))
	require.False(t, hasSyntheticMediaMarker("Screenshot saved to /tmp/a.png"))
	require.False(t, hasSyntheticMediaMarker(""))
}

// TestSanitizeSyntheticMediaStatus_StripsPlaceholder verifies fake marker text is removed when no media send occurred.
// The t parameter controls test lifecycle and assertions.
// It returns no value and fails when sanitization is incomplete.
func TestSanitizeSyntheticMediaStatus_StripsPlaceholder(t *testing.T) {
	input := "Here is the screenshot:\n[Sending image]"
	cleaned := sanitizeSyntheticMediaStatus(input, false)
	require.Equal(t, "Here is the screenshot:", cleaned)
	require.NotContains(t, cleaned, "[Sending image]")
}

// TestSanitizeSyntheticMediaStatus_FallbackMessage verifies empty marker-only responses become explicit failure text.
// The t parameter controls test lifecycle and assertions.
// It returns no value and fails when fallback text is not provided.
func TestSanitizeSyntheticMediaStatus_FallbackMessage(t *testing.T) {
	cleaned := sanitizeSyntheticMediaStatus("[Sending image]", false)
	require.Contains(t, cleaned, "could not send an attachment")
}

// TestSanitizeSyntheticMediaStatus_KeepWhenAlreadySent verifies marker text is preserved for compatibility when media was truly sent.
// The t parameter controls test lifecycle and assertions.
// It returns no value and fails when content is unexpectedly altered.
func TestSanitizeSyntheticMediaStatus_KeepWhenAlreadySent(t *testing.T) {
	input := "Summary:\n[Sending image]"
	require.Equal(t, input, sanitizeSyntheticMediaStatus(input, true))
}

// TestHasMessageToolSentInRound verifies send-state probing for registered message tool instances.
// The t parameter controls test lifecycle and assertions.
// It returns no value and fails when detection does not match message tool state.
func TestHasMessageToolSentInRound(t *testing.T) {
	agent := &AgentInstance{Tools: tools.NewToolRegistry()}
	messageTool := tools.NewMessageTool()
	agent.Tools.Register(messageTool)

	require.False(t, hasMessageToolSentInRound(agent))

	messageTool.SetContext("telegram", "chat-1")
	messageTool.SetSendCallback(func(msg bus.OutboundMessage) error { return nil })
	result := messageTool.Execute(context.Background(), map[string]any{"content": "hello"})
	require.False(t, result.IsError)
	require.True(t, hasMessageToolSentInRound(agent))
}
