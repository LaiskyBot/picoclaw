package agent

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/sipeed/picoclaw/pkg/bus"
)

// buildPlannerDelegationPrompt wraps a user request for planner execution.
// The task parameter carries user request and tracking identifiers.
// It returns a strict planner instruction string.
func buildPlannerDelegationPrompt(task *PipelineTask) string {
	if task == nil {
		return ""
	}

	var sb strings.Builder
	sb.WriteString("You are picoclaw, handling a background task for the user.\n")
	sb.WriteString("Task tracking id: ")
	sb.WriteString(task.ID)
	sb.WriteString("\n")
	if strings.TrimSpace(task.Summary) != "" {
		sb.WriteString("Task short description: ")
		sb.WriteString(task.Summary)
		sb.WriteString("\n")
	}
	if strings.TrimSpace(task.DelegationContext) != "" {
		sb.WriteString("Delegation reference (memory and interaction history):\n")
		sb.WriteString(task.DelegationContext)
		sb.WriteString("\n")
	}
	sb.WriteString("Use tools to complete the task end-to-end. Delegate worker subagents only when the task is long-running or parallelizable.\n")
	sb.WriteString("When multiple independent data-gathering actions are needed, issue parallel tool calls in the same turn to reduce round trips.\n")
	sb.WriteString("For Skills workflows, prefer find_skills then install_skill, then read the installed SKILL.md and execute steps.\n")
	sb.WriteString("For MCP workflows, distinguish local vs remote MCP. Local MCP is configured under tools.mcp.local, and remote MCP is configured/managed under tools.mcp.remote and via remote_mcp operations.\n")
	sb.WriteString("When generating images/files, deliver them via the message tool using attachments (photo/document with path/url/file_id).\n")
	sb.WriteString("Never claim media was sent unless the message tool call already succeeded in this run.\n")
	sb.WriteString("Do not output placeholders such as [Sending image] without a successful attachment send.\n")
	sb.WriteString("For user-facing text, sound like a real human assistant and keep only user-relevant content. Do not include internal task IDs, system status templates, debug notes, or execution-duration lines unless the user explicitly asks for them.\n")
	sb.WriteString("When spawning workers, include enough context, explicit objective, acceptance criteria, and a concise label.\n")
	sb.WriteString("Always produce a concise final summary with outcome and any next action.\n\n")
	sb.WriteString(promptLengthControlHint)
	sb.WriteString("\n\n")
	sb.WriteString("User request:\n")
	sb.WriteString(task.Request)

	return sb.String()
}

// parseRetryTaskID extracts task ID from a retry command.
// The raw parameter is a user message such as "retry task-3".
// It returns task ID and whether parsing succeeded.
func parseRetryTaskID(raw string) (string, bool) {
	parts := strings.Fields(strings.TrimSpace(raw))
	if len(parts) != 2 || !strings.EqualFold(parts[0], "retry") {
		return "", false
	}
	id := strings.TrimSpace(parts[1])
	if id == "" {
		return "", false
	}
	if !strings.HasPrefix(strings.ToLower(id), "task-") {
		return "", false
	}
	return id, true
}

// parseParallelTaskID extracts task ID from a forced parallel command.
// The raw parameter is a user message such as "parallel task-2".
// It returns task ID and whether parsing succeeded.
func parseParallelTaskID(raw string) (string, bool) {
	parts := strings.Fields(strings.TrimSpace(raw))
	if len(parts) != 2 {
		return "", false
	}
	if !strings.EqualFold(parts[0], "parallel") && parts[0] != "并行" {
		return "", false
	}
	id := strings.TrimSpace(parts[1])
	if id == "" || !strings.HasPrefix(strings.ToLower(id), "task-") {
		return "", false
	}
	return id, true
}

// maybeHandleTaskControlCommand handles retry commands for orphaned tasks.
// The ctx parameter controls optional task-brief generation and msg carries user command text.
// It returns response text and true when command is consumed.
func (al *AgentLoop) maybeHandleTaskControlCommand(ctx context.Context, msg bus.InboundMessage) (string, bool) {
	if al.taskPipeline == nil {
		return "", false
	}

	if id, ok := parseParallelTaskID(msg.Content); ok {
		task, exists := al.taskPipeline.GetTaskByID(id)
		if !exists {
			return fmt.Sprintf("Task %s not found.", id), true
		}
		if task.Channel != msg.Channel || task.ChatID != msg.ChatID {
			return "You can only control tasks from the current conversation.", true
		}
		updated, dispatched, err := al.taskPipeline.ForceParallelDispatch(id)
		if err != nil {
			return fmt.Sprintf("Failed to switch %s to parallel: %s", id, err.Error()), true
		}
		if updated.Status != TaskStatusQueued {
			return fmt.Sprintf("Task %s is already %s and cannot be switched to queued parallel mode.", taskLabel(updated), updated.Status), true
		}
		if dispatched {
			return fmt.Sprintf("Task %s switched to parallel mode and scheduled now.", taskLabel(updated)), true
		}
		return fmt.Sprintf("Task %s is already queued for execution in parallel mode.", taskLabel(updated)), true
	}

	id, ok := parseRetryTaskID(msg.Content)
	if !ok {
		return "", false
	}
	task, exists := al.taskPipeline.GetTaskByID(id)
	if !exists {
		return fmt.Sprintf("Task %s not found.", id), true
	}
	if task.Channel != msg.Channel || task.ChatID != msg.ChatID {
		return "You can only retry tasks from the current conversation.", true
	}

	taskSummary := task.Summary
	if strings.TrimSpace(taskSummary) == "" || taskSummary == fallbackTaskSummaryFromRequest(task.Request) {
		summaryCtx, summaryCancel := context.WithTimeout(ctx, 12*time.Second)
		taskSummary = al.generateTaskSummary(summaryCtx, task.Request)
		summaryCancel()
		if taskSummary == "" {
			taskSummary = fallbackTaskSummaryFromRequest(task.Request)
		}
	}

	newTask, err := al.taskPipeline.EnqueueTask(task.Channel, task.ChatID, task.SenderID, task.Request, taskSummary)
	if err != nil {
		return fmt.Sprintf("Failed to retry %s: %s", id, err.Error()), true
	}
	return fmt.Sprintf("Retried %s as %s.", id, taskLabel(newTask)), true
}
