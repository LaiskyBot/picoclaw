package tools

import (
	"context"
	"fmt"
	"strings"
)

type SpawnTool struct {
	manager        *SubagentManager
	originChannel  string
	originChatID   string
	taskReference  string
	allowlistCheck func(targetAgentID string) bool
	callback       AsyncCallback // For async completion notification
}

func NewSpawnTool(manager *SubagentManager) *SpawnTool {
	return &SpawnTool{
		manager:       manager,
		originChannel: "cli",
		originChatID:  "direct",
	}
}

// SetCallback implements AsyncTool interface for async completion notification
func (t *SpawnTool) SetCallback(cb AsyncCallback) {
	t.callback = cb
}

func (t *SpawnTool) Name() string {
	return "spawn"
}

func (t *SpawnTool) Description() string {
	return "Spawn a subagent to handle a task in the background. Use this for complex or time-consuming tasks that can run independently. The subagent will complete the task and report back when done."
}

func (t *SpawnTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"task": map[string]any{
				"type":        "string",
				"description": "The task for subagent to complete",
			},
			"label": map[string]any{
				"type":        "string",
				"description": "Optional short label for the task (for display)",
			},
			"agent_id": map[string]any{
				"type":        "string",
				"description": "Optional target agent ID to delegate the task to",
			},
		},
		"required": []string{"task"},
	}
}

func (t *SpawnTool) SetContext(channel, chatID string) {
	t.originChannel = channel
	t.originChatID = chatID
}

// SetTaskReference stores delegation context for the next spawned worker task.
// The taskReference parameter should include relevant memory and recent history.
// It returns no value.
func (t *SpawnTool) SetTaskReference(taskReference string) {
	t.taskReference = taskReference
}

// SetRuntimeContext sets the lifecycle context used by spawned background tasks.
// The runtimeCtx parameter should usually be the process/runtime context instead of
// a per-request context.
// It returns no value.
func (t *SpawnTool) SetRuntimeContext(runtimeCtx context.Context) {
	if t.manager == nil {
		return
	}
	t.manager.SetRuntimeContext(runtimeCtx)
}

func (t *SpawnTool) SetAllowlistChecker(check func(targetAgentID string) bool) {
	t.allowlistCheck = check
}

// ResumeUnfinished resumes persisted unfinished subagent tasks after restart.
// The ctx parameter controls resumed task lifecycles.
// It returns how many tasks were resumed.
func (t *SpawnTool) ResumeUnfinished(ctx context.Context) int {
	if t.manager == nil {
		return 0
	}
	return t.manager.ResumeUnfinished(ctx, t.callback)
}

func (t *SpawnTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	task, ok := args["task"].(string)
	if !ok || strings.TrimSpace(task) == "" {
		return ErrorResult("task is required and must be a non-empty string")
	}

	label, _ := args["label"].(string)
	agentID, _ := args["agent_id"].(string)

	// Check allowlist if targeting a specific agent
	if agentID != "" && t.allowlistCheck != nil {
		if !t.allowlistCheck(agentID) {
			return ErrorResult(fmt.Sprintf("not allowed to spawn agent '%s'", agentID))
		}
	}

	if t.manager == nil {
		return ErrorResult("Subagent manager not configured")
	}

	// Pass callback to manager for async completion notification
	result, err := t.manager.Spawn(ctx, task, label, agentID, t.taskReference, t.originChannel, t.originChatID, t.callback)
	if err != nil {
		return ErrorResult(fmt.Sprintf("failed to spawn subagent: %v", err))
	}

	// Return AsyncResult since the task runs in background
	return AsyncResult(result)
}
