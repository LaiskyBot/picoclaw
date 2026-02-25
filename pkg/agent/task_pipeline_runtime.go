package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/providers"
	"github.com/sipeed/picoclaw/pkg/utils"
)

const plannerTaskChannel = "planner_task"

const plannerEmptySummaryText = "Planner completed with no textual output."

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

	workers := al.taskMaxParallel
	if workers <= 0 {
		workers = 1
	}
	for i := 0; i < workers; i++ {
		go al.runPlannerDispatchLoop(ctx)
	}
	al.maybePromoteQueuedTasks()
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
			al.maybePromoteQueuedTasks()
		}
	}
}

// executePlannerTask runs a delegated task through the planner agent.
// The parentCtx parameter defines execution lifecycle and task supplies request data.
// It returns no value and publishes user-visible updates via message bus.
func (al *AgentLoop) executePlannerTask(parentCtx context.Context, task *PipelineTask) {
	defer al.maybePromoteQueuedTasks()

	planner := al.getPlannerAgent()
	if planner == nil {
		al.taskPipeline.MarkFailed(task.ID, "Planner agent is not configured")
		al.publishTaskFinalReport(task.ID, "", fmt.Errorf("planner agent is not configured"))
		return
	}

	al.taskPipeline.MarkRunning(task.ID, planner.ID)
	al.bus.PublishOutbound(bus.OutboundMessage{
		Channel: task.Channel,
		ChatID:  task.ChatID,
		Content: fmt.Sprintf("Task %s is now running with planner '%s'.", taskLabel(task), planner.ID),
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
		DefaultResponse: plannerEmptySummaryText,
		EnableSummary:   false,
		SendResponse:    false,
		NoHistory:       true,
	})
	if err != nil {
		al.taskPipeline.MarkFailed(task.ID, err.Error())
		al.publishTaskFinalReport(task.ID, "", err)
		return
	}

	al.taskPipeline.MarkPlannerCompleted(task.ID, result)
	al.publishTaskFinalReport(task.ID, result, nil)
}

// publishTaskFinalReport sends a structured completion report to the chat.
// The taskID parameter identifies persisted lifecycle data and plannerSummary holds textual output.
// The runErr parameter should be non-nil for failures and nil for successful completion.
func (al *AgentLoop) publishTaskFinalReport(taskID, plannerSummary string, runErr error) {
	if al.taskPipeline == nil {
		return
	}

	task, ok := al.taskPipeline.GetTaskByID(taskID)
	if !ok {
		return
	}

	summary := strings.TrimSpace(plannerSummary)
	if summary == "" || summary == plannerEmptySummaryText {
		summary = strings.TrimSpace(task.PlannerResult)
	}
	if summary == "" || summary == plannerEmptySummaryText {
		summary = "No textual planner summary was provided."
	}

	finalStatus := task.Status
	if runErr != nil {
		finalStatus = TaskStatusFailed
		errText := strings.TrimSpace(task.Error)
		if errText == "" {
			errText = strings.TrimSpace(runErr.Error())
		}
		summary = fmt.Sprintf("Task failed: %s", errText)
	}

	report := formatTaskFinalReport(task, finalStatus, summary)
	al.bus.PublishOutbound(bus.OutboundMessage{
		Channel: task.Channel,
		ChatID:  task.ChatID,
		Content: report,
	})
}

// formatTaskFinalReport renders a concise task lifecycle summary for users.
// The task parameter must include status and lifecycle timestamps.
// It returns a user-facing multi-line report.
func formatTaskFinalReport(task *PipelineTask, finalStatus, summary string) string {
	if task == nil {
		return "Task finished, but details are unavailable."
	}

	started := formatUnixMilliUTC(task.StartedAtUTC)
	finished := formatUnixMilliUTC(task.FinishedAtUTC)
	duration := "unknown"
	if task.StartedAtUTC > 0 && task.FinishedAtUTC > 0 && task.FinishedAtUTC >= task.StartedAtUTC {
		duration = (time.Duration(task.FinishedAtUTC-task.StartedAtUTC) * time.Millisecond).String()
	}

	textSummary := strings.TrimSpace(summary)
	if textSummary == "" {
		textSummary = "No summary available."
	}

	return fmt.Sprintf(
		"Task %s finished.\n- status: %s\n- planner: %s\n- task_summary: %s\n- started_at_utc: %s\n- finished_at_utc: %s\n- duration: %s\n- worker_updates: %d\n- summary: %s",
		task.ID,
		finalStatus,
		valueOrNA(task.PlannerAgent),
		valueOrNA(task.Summary),
		started,
		finished,
		duration,
		len(task.WorkerEvents),
		utils.Truncate(textSummary, 2800),
	)
}

// formatUnixMilliUTC formats unix milliseconds as RFC3339 in UTC.
// The ts parameter is milliseconds since unix epoch.
// It returns "n/a" when timestamp is not set.
func formatUnixMilliUTC(ts int64) string {
	if ts <= 0 {
		return "n/a"
	}
	return time.UnixMilli(ts).UTC().Format(time.RFC3339)
}

// valueOrNA returns fallback text when value is empty.
// The value parameter is user-facing string content.
// It returns "n/a" for empty input.
func valueOrNA(value string) string {
	if strings.TrimSpace(value) == "" {
		return "n/a"
	}
	return value
}

// taskScheduleDecision contains LLM output for task scheduling.
// It indicates whether a task should run in parallel, wait, or ask user.
// It includes optional dependency IDs and a question for ambiguity.
type taskScheduleDecision struct {
	Decision  string   `json:"decision"`
	Reason    string   `json:"reason"`
	DependsOn []string `json:"depends_on"`
	Question  string   `json:"question"`
}

// maybePromoteQueuedTasks promotes runnable waiting tasks according to free worker slots.
// It inspects current running count and dispatches newly-runnable queued tasks.
// It returns no value and publishes notifications for promoted tasks.
func (al *AgentLoop) maybePromoteQueuedTasks() {
	if al.taskPipeline == nil {
		return
	}

	running := al.taskPipeline.CountRunning()
	maxParallel := al.taskMaxParallel
	if maxParallel <= 0 {
		maxParallel = 1
	}
	available := maxParallel - running
	if available <= 0 {
		return
	}

	promoted := al.taskPipeline.PromoteRunnableQueuedTasks(available)
	for _, task := range promoted {
		al.bus.PublishOutbound(bus.OutboundMessage{
			Channel: task.Channel,
			ChatID:  task.ChatID,
			Content: fmt.Sprintf("Task %s is now ready and scheduled to run.", taskLabel(task)),
		})
	}
}

// generateTaskSummary asks planner LLM for a concise one-line task summary.
// The ctx parameter controls LLM call lifecycle and request carries original user input.
// It returns a normalized short summary with fallback when model call fails.
func (al *AgentLoop) generateTaskSummary(ctx context.Context, request string) string {
	fallback := fallbackTaskSummary(request)
	planner := al.getPlannerAgent()
	if planner == nil || planner.Provider == nil {
		return fallback
	}

	resp, err := planner.Provider.Chat(ctx, []providers.Message{
		{Role: "system", Content: "You generate concise task labels for tracking. Return one short line only."},
		{Role: "user", Content: "Summarize this task in 6-14 words, preserve user language, no markdown, no quotes:\n" + request},
	}, nil, planner.Model, map[string]any{
		"max_tokens":  80,
		"temperature": 0,
	})
	if err != nil || resp == nil {
		return fallback
	}

	label := normalizeTaskSummary(resp.Content)
	if label == "" {
		return fallback
	}
	return label
}

// fallbackTaskSummary builds a deterministic short summary from raw request text.
// The request parameter may contain multiline user content.
// It returns a compact single-line fallback summary.
func fallbackTaskSummary(request string) string {
	normalized := normalizeTaskSummary(request)
	if normalized == "" {
		return "task"
	}
	return normalized
}

// taskLabel formats task identifier with optional summary.
// The task parameter is a persisted pipeline task snapshot.
// It returns human-readable ID text for user-facing messages.
func taskLabel(task *PipelineTask) string {
	if task == nil {
		return "n/a"
	}
	if task.Summary == "" {
		return task.ID
	}
	return fmt.Sprintf("%s [%s]", task.ID, task.Summary)
}

// decideTaskSchedule asks LLM whether the new task should run in parallel or wait.
// The msg parameter is the incoming request and activeTasks provide queue context.
// It returns normalized scheduling decision or an error when classification fails.
func (al *AgentLoop) decideTaskSchedule(
	ctx context.Context,
	msg bus.InboundMessage,
	activeTasks []*PipelineTask,
) (taskScheduleDecision, error) {
	if len(activeTasks) == 0 {
		return taskScheduleDecision{Decision: TaskExecutionModeParallel, Reason: "No active tasks in current conversation."}, nil
	}

	planner := al.getPlannerAgent()
	if planner == nil {
		return taskScheduleDecision{}, fmt.Errorf("planner agent is not configured")
	}

	statusSummary := al.taskPipeline.BuildConversationStatusSummary(msg.Channel, msg.ChatID, msg.SenderID)

	var prompt strings.Builder
	prompt.WriteString("Decide scheduling mode for the new task in an existing task pipeline.\n")
	prompt.WriteString("Return only strict JSON with keys: decision, reason, depends_on, question.\n")
	prompt.WriteString("Allowed decision values: parallel, wait, ask_user.\n")
	prompt.WriteString("Use ask_user only when dependency cannot be inferred from text.\n\n")
	prompt.WriteString("Current task status:\n")
	prompt.WriteString(statusSummary)
	prompt.WriteString("\n\nNew user task:\n")
	prompt.WriteString(msg.Content)

	resp, err := planner.Provider.Chat(ctx, []providers.Message{
		{Role: "system", Content: "You classify pipeline scheduling decisions. Output JSON only."},
		{Role: "user", Content: prompt.String()},
	}, nil, planner.Model, map[string]any{
		"max_tokens":  300,
		"temperature": 0,
	})
	if err != nil {
		return taskScheduleDecision{}, err
	}

	decision, err := parseTaskScheduleDecision(resp.Content)
	if err != nil {
		return taskScheduleDecision{}, err
	}

	activeIDs := make(map[string]struct{}, len(activeTasks))
	for _, task := range activeTasks {
		activeIDs[task.ID] = struct{}{}
	}

	if strings.EqualFold(strings.TrimSpace(decision.Decision), "ask_user") {
		decision.Decision = "ask_user"
	} else {
		decision.Decision = normalizeExecutionMode(decision.Decision)
	}
	deps := make([]string, 0, len(decision.DependsOn))
	for _, depID := range uniqueTaskIDs(decision.DependsOn) {
		if _, ok := activeIDs[depID]; ok {
			deps = append(deps, depID)
		}
	}
	decision.DependsOn = deps

	if decision.Decision == TaskExecutionModeWait && len(decision.DependsOn) == 0 {
		for _, task := range activeTasks {
			if task.Status == TaskStatusQueued || task.Status == TaskStatusRunning {
				decision.DependsOn = append(decision.DependsOn, task.ID)
			}
		}
		decision.DependsOn = uniqueTaskIDs(decision.DependsOn)
	}

	return decision, nil
}

// parseTaskScheduleDecision parses classifier JSON response.
// The raw parameter may contain plain JSON or fenced markdown blocks.
// It returns normalized decision structure when parsing succeeds.
func parseTaskScheduleDecision(raw string) (taskScheduleDecision, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return taskScheduleDecision{}, fmt.Errorf("empty scheduling decision response")
	}

	jsonText := trimmed
	if strings.HasPrefix(jsonText, "```") {
		jsonText = strings.TrimPrefix(jsonText, "```json")
		jsonText = strings.TrimPrefix(jsonText, "```")
		jsonText = strings.TrimSuffix(strings.TrimSpace(jsonText), "```")
		jsonText = strings.TrimSpace(jsonText)
	}

	if first := strings.Index(jsonText, "{"); first >= 0 {
		if last := strings.LastIndex(jsonText, "}"); last > first {
			jsonText = jsonText[first : last+1]
		}
	}

	var decision taskScheduleDecision
	if err := json.Unmarshal([]byte(jsonText), &decision); err != nil {
		return taskScheduleDecision{}, err
	}

	decision.Decision = strings.TrimSpace(strings.ToLower(decision.Decision))
	decision.Reason = strings.TrimSpace(decision.Reason)
	decision.Question = strings.TrimSpace(decision.Question)
	switch decision.Decision {
	case TaskExecutionModeParallel, TaskExecutionModeWait, "ask_user":
	default:
		return taskScheduleDecision{}, fmt.Errorf("unsupported scheduling decision: %q", decision.Decision)
	}

	decision.DependsOn = uniqueTaskIDs(decision.DependsOn)
	return decision, nil
}

// collectTaskIDs returns task IDs in stable input order.
// The tasks parameter is expected to be pre-sorted by caller.
// It returns task ID list for dependency and prompt metadata.
func collectTaskIDs(tasks []*PipelineTask) []string {
	if len(tasks) == 0 {
		return nil
	}
	ids := make([]string, 0, len(tasks))
	for _, task := range tasks {
		if task == nil || strings.TrimSpace(task.ID) == "" {
			continue
		}
		ids = append(ids, task.ID)
	}
	return ids
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
	if strings.TrimSpace(task.Summary) != "" {
		sb.WriteString("Task short description: ")
		sb.WriteString(task.Summary)
		sb.WriteString("\n")
	}
	sb.WriteString("Use tools to classify work, break it down, and delegate worker steps when needed.\n")
	sb.WriteString("For Skills workflows, prefer find_skills then install_skill, then read the installed SKILL.md and execute steps.\n")
	sb.WriteString("For MCP workflows (especially remote MCP), use remote_mcp to add/list servers, list remote tools, and call the required remote tool.\n")
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
// The msg parameter is inspected against local pipeline state.
// It returns response text and true when command is consumed.
func (al *AgentLoop) maybeHandleTaskControlCommand(msg bus.InboundMessage) (string, bool) {
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

	newTask, err := al.taskPipeline.EnqueueTask(task.Channel, task.ChatID, task.SenderID, task.Request, task.Summary)
	if err != nil {
		return fmt.Sprintf("Failed to retry %s: %s", id, err.Error()), true
	}
	return fmt.Sprintf("Retried %s as %s.", id, taskLabel(newTask)), true
}
