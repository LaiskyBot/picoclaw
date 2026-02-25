package agent

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/utils"
)

const plannerTaskChannel = "planner_task"

// startTaskPipelineRuntime starts background loops for task processing.
// The ctx parameter controls lifecycle for queue worker and timeout scanner.
// It returns no value and is a no-op when task pipeline is not initialized.
func (al *AgentLoop) startTaskPipelineRuntime(ctx context.Context) {
	if al.taskPipeline == nil {
		return
	}

	orphaned := al.taskPipeline.MarkOrphanedOnRestart()
	for _, task := range orphaned {
		al.bus.PublishOutbound(bus.OutboundMessage{
			Channel: task.Channel,
			ChatID:  task.ChatID,
			Content: fmt.Sprintf(
				"Task %s became orphaned after restart. Please confirm whether I should retry it or mark it closed.",
				task.ID,
			),
		})
	}

	go al.runPlannerDispatchLoop(ctx)
	go al.runTaskTimeoutLoop(ctx)
}

// runPlannerDispatchLoop continuously dequeues and executes planner tasks.
// The ctx parameter cancels the loop gracefully.
// It returns when context is done.
func (al *AgentLoop) runPlannerDispatchLoop(ctx context.Context) {
	for {
		task, ok := al.taskPipeline.Dequeue(ctx)
		if !ok {
			return
		}
		al.executePlannerTask(ctx, task)
	}
}

// runTaskTimeoutLoop periodically marks stale tasks as timeout.
// The ctx parameter controls ticker lifecycle.
// It returns when context is canceled.
func (al *AgentLoop) runTaskTimeoutLoop(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			timedOut := al.taskPipeline.MarkTimeouts(now)
			for _, task := range timedOut {
				al.bus.PublishOutbound(bus.OutboundMessage{
					Channel: task.Channel,
					ChatID:  task.ChatID,
					Content: fmt.Sprintf(
						"Task %s timed out and may be orphaned. Reply with 'retry %s' or send a new instruction.",
						task.ID,
						task.ID,
					),
				})
			}
		}
	}
}

// executePlannerTask runs a delegated task through the planner agent.
// The parentCtx parameter defines execution lifecycle and task supplies request data.
// It returns no value and publishes user-visible updates via message bus.
func (al *AgentLoop) executePlannerTask(parentCtx context.Context, task *PipelineTask) {
	planner := al.getPlannerAgent()
	if planner == nil {
		al.taskPipeline.MarkFailed(task.ID, "Planner agent is not configured")
		al.bus.PublishOutbound(bus.OutboundMessage{
			Channel: task.Channel,
			ChatID:  task.ChatID,
			Content: fmt.Sprintf("Task %s failed: planner agent is not configured.", task.ID),
		})
		return
	}

	al.taskPipeline.MarkRunning(task.ID, planner.ID)
	al.bus.PublishOutbound(bus.OutboundMessage{
		Channel: task.Channel,
		ChatID:  task.ChatID,
		Content: fmt.Sprintf("Task %s is now running with planner '%s'.", task.ID, planner.ID),
	})

	runCtx, cancel := context.WithTimeout(parentCtx, 40*time.Minute)
	defer cancel()

	plannerPrompt := buildPlannerDelegationPrompt(task)
	sessionKey := fmt.Sprintf("agent:%s:pipeline:%s", planner.ID, task.ID)

	result, err := al.runAgentLoop(runCtx, planner, processOptions{
		SessionKey:      sessionKey,
		Channel:         plannerTaskChannel,
		ChatID:          task.ID,
		UserMessage:     plannerPrompt,
		DefaultResponse: "Planner completed with no textual output.",
		EnableSummary:   false,
		SendResponse:    false,
		NoHistory:       true,
	})
	if err != nil {
		al.taskPipeline.MarkFailed(task.ID, err.Error())
		al.bus.PublishOutbound(bus.OutboundMessage{
			Channel: task.Channel,
			ChatID:  task.ChatID,
			Content: fmt.Sprintf("Task %s failed: %s", task.ID, err.Error()),
		})
		return
	}

	al.taskPipeline.MarkPlannerCompleted(task.ID, result)
	al.bus.PublishOutbound(bus.OutboundMessage{
		Channel: task.Channel,
		ChatID:  task.ChatID,
		Content: fmt.Sprintf("Task %s completed.\n\n%s", task.ID, utils.Truncate(result, 3000)),
	})
}

// getPlannerAgent resolves the planner role from configured agents.
// It returns agent with ID "planner" when available, otherwise default agent.
// It returns nil when registry has no agents.
func (al *AgentLoop) getPlannerAgent() *AgentInstance {
	if planner, ok := al.registry.GetAgent("planner"); ok {
		return planner
	}
	return al.registry.GetDefaultAgent()
}

// buildPlannerDelegationPrompt wraps a user request for planner execution.
// The task parameter carries user request and tracking identifiers.
// It returns a strict planner instruction string.
func buildPlannerDelegationPrompt(task *PipelineTask) string {
	if task == nil {
		return ""
	}

	var sb strings.Builder
	sb.WriteString("You are the planner agent in a plan-worker pipeline.\n")
	sb.WriteString("Task tracking id: ")
	sb.WriteString(task.ID)
	sb.WriteString("\n")
	sb.WriteString("Use tools to classify work, break it down, and delegate worker steps when needed.\n")
	sb.WriteString("When spawning workers, include explicit labels so progress is traceable.\n")
	sb.WriteString("Always produce a final status summary with success/failure and next actions.\n\n")
	sb.WriteString("User request:\n")
	sb.WriteString(task.Request)

	return sb.String()
}

// parseRetryTaskID extracts task ID from a retry command.
// The raw parameter is a user message such as "retry task-3".
// It returns task ID and whether parsing succeeded.
func parseRetryTaskID(raw string) (string, bool) {
	parts := strings.Fields(strings.TrimSpace(strings.ToLower(raw)))
	if len(parts) != 2 || parts[0] != "retry" {
		return "", false
	}
	id := strings.TrimSpace(parts[1])
	if id == "" {
		return "", false
	}
	if !strings.HasPrefix(id, "task-") {
		return "", false
	}
	return id, true
}

// maybeHandleTaskControlCommand handles retry commands for orphaned tasks.
// The msg parameter is inspected against local pipeline state.
// It returns response text and true when command is consumed.
func (al *AgentLoop) maybeHandleTaskControlCommand(msg bus.InboundMessage) (string, bool) {
	if al.taskPipeline == nil {
		return "", false
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

	newTask, err := al.taskPipeline.EnqueueTask(task.Channel, task.ChatID, task.SenderID, task.Request)
	if err != nil {
		return fmt.Sprintf("Failed to retry %s: %s", id, err.Error()), true
	}
	return fmt.Sprintf("Retried %s as %s.", id, newTask.ID), true
}
