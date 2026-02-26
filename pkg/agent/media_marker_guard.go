package agent

import (
	"regexp"
	"strings"

	"github.com/sipeed/picoclaw/pkg/tools"
)

var syntheticMediaMarkerAnyPattern = regexp.MustCompile(`(?i)\[(sending\s+(an?\s+)?(image|photo|file|attachment))\]`)

var syntheticMediaMarkerLinePattern = regexp.MustCompile(`(?im)^\s*\[(sending\s+(an?\s+)?(image|photo|file|attachment))\]\s*$`)

// messageSendTracker reports whether message tool already delivered content in current round.
// It exposes a single HasSentInRound method and returns true when at least one outbound send happened.
type messageSendTracker interface {
	HasSentInRound() bool
}

// hasSyntheticMediaMarker reports whether assistant content contains synthetic media placeholders.
// The content parameter is final assistant text and the return value indicates marker presence.
func hasSyntheticMediaMarker(content string) bool {
	if strings.TrimSpace(content) == "" {
		return false
	}
	return syntheticMediaMarkerAnyPattern.MatchString(content)
}

// sanitizeSyntheticMediaStatus removes synthetic media markers when no attachment was sent.
// The content parameter is assistant text, and messageSentInRound indicates real message tool delivery.
// It returns cleaned text and keeps original content when markers are absent or media was already sent.
func sanitizeSyntheticMediaStatus(content string, messageSentInRound bool) string {
	if messageSentInRound || !hasSyntheticMediaMarker(content) {
		return content
	}

	cleaned := syntheticMediaMarkerLinePattern.ReplaceAllString(content, "")
	cleaned = syntheticMediaMarkerAnyPattern.ReplaceAllString(cleaned, "")
	cleaned = strings.TrimSpace(cleaned)
	if cleaned == "" {
		return "I could not send an attachment in this turn. Please ask me to send the file path explicitly."
	}

	return cleaned
}

// hasMessageToolSentInRound checks message tool delivery state for current loop round.
// The agent parameter provides access to registered tools and return value is true on successful send.
func hasMessageToolSentInRound(agent *AgentInstance) bool {
	if agent == nil || agent.Tools == nil {
		return false
	}

	tool, ok := agent.Tools.Get("message")
	if !ok {
		return false
	}

	if tracker, ok := tool.(messageSendTracker); ok {
		return tracker.HasSentInRound()
	}

	if messageTool, ok := tool.(*tools.MessageTool); ok {
		return messageTool.HasSentInRound()
	}

	return false
}
