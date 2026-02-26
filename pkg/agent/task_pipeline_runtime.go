package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/logger"
	"github.com/sipeed/picoclaw/pkg/providers"
	"github.com/sipeed/picoclaw/pkg/utils"
)

const plannerTaskChannel = "planner_task"

const plannerEmptySummaryText = "Planner completed with no textual output."

const taskBriefMaxRunes = 24

var sensitiveTaskSummaryPatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)https?://`),
	regexp.MustCompile(`(?i)\b[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}\b`),
	regexp.MustCompile(`(?i)\b(sk|xoxb|xapp|ghp|gho|ghu|ghs|eyj)[-_a-z0-9]{8,}\b`),
	regexp.MustCompile(`\d{6,}`),
}

// startTaskPipelineRuntime starts background loops for task processing.
// The ctx parameter controls lifecycle for queue worker and timeout scanner.
// It returns no value and is a no-op when task pipeline is not initialized.
func (al *AgentLoop) startTaskPipelineRuntime(ctx context.Context) {
	if al.taskPipeline == nil {
		return
	}

	recovered := al.taskPipeline.RecoverUnfinishedOnRestart()
	for _, task := range recovered {
		al.bus.PublishOutbound(bus.OutboundMessage{
			Channel: task.Channel,
			ChatID:  task.ChatID,
			Content: fmt.Sprintf(
				"Task %s resumed automatically after restart.",
				taskLabel(task),
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
						taskLabel(task),
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

	runner := al.getPlannerAgent()
	if runner == nil {
		al.taskPipeline.MarkFailed(task.ID, "assistant agent is not configured")
		al.publishTaskFinalReport(task.ID, "", fmt.Errorf("assistant agent is not configured"))
		return
	}

	al.taskPipeline.MarkRunning(task.ID, runner.ID)
	al.taskPipeline.CheckpointTask(task.ID, "planner run started")

	runCtx, cancel := context.WithTimeout(parentCtx, 40*time.Minute)
	defer cancel()

	heartbeatDone := make(chan struct{})
	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-runCtx.Done():
				return
			case <-heartbeatDone:
				return
			case <-ticker.C:
				al.taskPipeline.CheckpointTask(task.ID, "planner run heartbeat")
			}
		}
	}()
	defer close(heartbeatDone)

	plannerPrompt := buildPlannerDelegationPrompt(task)
	sessionKey := fmt.Sprintf("agent:%s:pipeline:%s", runner.ID, task.ID)

	result, err := al.runAgentLoop(runCtx, runner, processOptions{
		SessionKey:      sessionKey,
		Channel:         plannerTaskChannel,
		ChatID:          task.ID,
		ToolChannel:     task.Channel,
		ToolChatID:      task.ChatID,
		UserMessage:     plannerPrompt,
		DefaultResponse: plannerEmptySummaryText,
		EnableSummary:   false,
		SendResponse:    false,
		NoHistory:       true,
	})
	if err != nil {
		al.taskPipeline.MarkFailed(task.ID, err.Error())
		al.taskPipeline.CheckpointTask(task.ID, "planner run failed")
		al.publishTaskFinalReport(task.ID, "", err)
		return
	}

	al.taskPipeline.MarkPlannerCompleted(task.ID, result)
	al.taskPipeline.CheckpointTask(task.ID, "planner run completed")
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
		summary = "No textual summary was produced."
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
	al.recordDelegatedTaskResult(task, report)
}

// recordDelegatedTaskResult appends delegated task final report into the routed conversation session.
// The task parameter carries routed conversation metadata and report is the user-visible final summary.
// It returns no value.
func (al *AgentLoop) recordDelegatedTaskResult(task *PipelineTask, report string) {
	if task == nil || al.registry == nil {
		return
	}

	agentID := strings.TrimSpace(task.ConversationAgentID)
	sessionKey := strings.TrimSpace(task.ConversationSessionKey)
	if agentID == "" || sessionKey == "" {
		logger.DebugCF("agent", "Skip delegated task session write: missing conversation context", map[string]any{
			"task_id":      task.ID,
			"has_agent_id": agentID != "",
			"has_session":  sessionKey != "",
		})
		return
	}

	agent, ok := al.registry.GetAgent(agentID)
	if !ok {
		logger.DebugCF("agent", "Skip delegated task session write: routed agent not found", map[string]any{
			"task_id":     task.ID,
			"agent_id":    agentID,
			"session_key": sessionKey,
		})
		return
	}

	agent.Sessions.AddMessage(sessionKey, "assistant", report)
	if err := agent.Sessions.Save(sessionKey); err != nil {
		logger.DebugCF("agent", "Failed to persist delegated task final report into session", map[string]any{
			"task_id":     task.ID,
			"agent_id":    agentID,
			"session_key": sessionKey,
			"error":       err.Error(),
		})
		return
	}

	logger.DebugCF("agent", "Persisted delegated task final report into routed session", map[string]any{
		"task_id":      task.ID,
		"agent_id":     agentID,
		"session_key":  sessionKey,
		"report_chars": len(report),
	})
}

// formatTaskFinalReport renders a concise task lifecycle summary for users.
// The task parameter must include status and lifecycle timestamps.
// It returns a user-facing multi-line report.
func formatTaskFinalReport(task *PipelineTask, finalStatus, summary string) string {
	if task == nil {
		return "Task finished, but details are unavailable."
	}

	duration := "unknown"
	if task.StartedAtUTC > 0 && task.FinishedAtUTC > 0 && task.FinishedAtUTC >= task.StartedAtUTC {
		duration = (time.Duration(task.FinishedAtUTC-task.StartedAtUTC) * time.Millisecond).String()
	}

	textSummary := strings.TrimSpace(summary)
	if textSummary == "" {
		textSummary = "No summary available."
	}

	return fmt.Sprintf(
		"Task %s finished (%s).\nSummary: %s\nDuration: %s",
		taskLabel(task),
		finalStatus,
		utils.Truncate(textSummary, 2800),
		duration,
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

// generateTaskSummary asks planner LLM for a concise one-line task brief.
// The ctx parameter controls LLM call lifecycle and request carries original user input.
// It returns a language-preserving brief, with fallback derived from user request.
func (al *AgentLoop) generateTaskSummary(ctx context.Context, request string) string {
	fallback := fallbackTaskSummaryFromRequest(request)
	planner := al.getPlannerAgent()
	if planner == nil || planner.Provider == nil {
		return fallback
	}

	resp, err := planner.Provider.Chat(ctx, []providers.Message{
		{Role: "system", Content: "You generate concise task briefs for tracking. Keep the same language as the user request. Output must be plain text only, no markdown, no quotes, no secrets, no URLs, no emails, no tokens, and no long numbers. " + promptLengthControlHint},
		{Role: "user", Content: "Write a very short task brief for this request. Keep the user's language and summarize the goal in <= 24 characters:\n" + request + "\n" + promptLengthControlHint},
	}, nil, planner.Model, map[string]any{
		"max_tokens":  64,
		"temperature": 0,
	})
	if err != nil || resp == nil {
		return fallback
	}

	label := sanitizeTaskSummary(resp.Content)
	if !isSafeTaskBrief(label) {
		return fallback
	}
	return label
}

// fallbackTaskSummaryFromRequest derives a compact brief directly from user request text.
// The request parameter contains original user input and may include multiple lines.
// It returns a concise, language-preserving fallback summary for display.
func fallbackTaskSummaryFromRequest(request string) string {
	clean := sanitizeTaskSummary(request)
	if clean == "" {
		return "request"
	}
	if containsSensitiveTaskSummaryText(clean) {
		return "request"
	}
	return truncateRunes(clean, taskBriefMaxRunes)
}

// sanitizeTaskSummary normalizes and cleans model-generated task brief text.
// The summary parameter is raw model output and may include wrappers or unsupported punctuation.
// It returns a cleaned single-line brief suitable for safety validation.
func sanitizeTaskSummary(summary string) string {
	clean := strings.TrimSpace(summary)
	clean = strings.Trim(clean, "`\"'“”‘’")
	clean = strings.Join(strings.Fields(clean), " ")
	if clean == "" {
		return ""
	}

	var sb strings.Builder
	for _, r := range clean {
		if isAllowedTaskBriefRune(r) {
			sb.WriteRune(r)
			continue
		}
		sb.WriteRune(' ')
	}

	return normalizeTaskSummary(sb.String())
}

// isAllowedTaskBriefRune reports whether a rune is safe and readable in task brief text.
// The r parameter is a single Unicode code point from model output.
// It returns true for letters, numbers, spaces, and selected punctuation.
func isAllowedTaskBriefRune(r rune) bool {
	if unicode.IsLetter(r) || unicode.IsNumber(r) || unicode.IsSpace(r) {
		return true
	}
	switch r {
	case '/', '+', '&', ',', '_', '(', ')', '.', '-', ':', '，', '。', '、', '：', '；', '！', '？':
		return true
	default:
		return false
	}
}

// truncateRunes truncates a string by rune count and preserves UTF-8 validity.
// The text parameter is the original string and maxRunes is the inclusive limit.
// It returns the original text when within limit, otherwise a rune-truncated prefix.
func truncateRunes(text string, maxRunes int) string {
	if maxRunes <= 0 {
		return ""
	}
	if utf8.RuneCountInString(text) <= maxRunes {
		return text
	}
	runes := []rune(text)
	return string(runes[:maxRunes])
}

// isSafeTaskBrief validates generated task brief for display safety and readability.
// The summary parameter is normalized one-line text from model output.
// It returns true when the brief is English-like, short, and free of sensitive patterns.
func isSafeTaskBrief(summary string) bool {
	raw := normalizeTaskSummary(summary)
	if raw != "" && containsSensitiveTaskSummaryText(raw) {
		return false
	}

	summary = sanitizeTaskSummary(summary)
	if summary == "" || utf8.RuneCountInString(summary) > taskBriefMaxRunes {
		return false
	}
	if containsSensitiveTaskSummaryText(summary) {
		return false
	}
	return true
}

// containsSensitiveTaskSummaryText checks whether a generated label appears to expose sensitive data.
// The summary parameter is normalized one-line text from model output.
// It returns true when sensitive-like patterns are detected.
func containsSensitiveTaskSummaryText(summary string) bool {
	for _, pattern := range sensitiveTaskSummaryPatterns {
		if pattern.MatchString(summary) {
			return true
		}
	}
	return false
}

// taskLabel formats task identifier with optional summary.
// The task parameter is a persisted pipeline task snapshot.
// It returns human-readable ID text for user-facing messages.
func taskLabel(task *PipelineTask) string {
	if task == nil {
		return "n/a"
	}
	if task.Summary == "" {
		return fmt.Sprintf("%s(%s)", task.ID, fallbackTaskSummaryFromRequest(task.Request))
	}
	return fmt.Sprintf("%s(%s)", task.ID, task.Summary)
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
	prompt.WriteString("Keep reason/question concise and avoid long quotations from user text.\n")
	prompt.WriteString(promptLengthControlHint)
	prompt.WriteString("\n")
	prompt.WriteString("Use ask_user only when dependency cannot be inferred from text.\n\n")
	prompt.WriteString("Current task status:\n")
	prompt.WriteString(statusSummary)
	prompt.WriteString("\n\nNew user task:\n")
	prompt.WriteString(msg.Content)

	resp, err := planner.Provider.Chat(ctx, []providers.Message{
		{Role: "system", Content: "You classify pipeline scheduling decisions. Output JSON only. " + promptLengthControlHint},
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

// getPlannerAgent resolves the runtime assistant for delegated tasks.
// It returns the default user-facing agent and intentionally ignores dedicated planner roles.
// It returns nil when registry has no agents.
func (al *AgentLoop) getPlannerAgent() *AgentInstance {
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
	sb.WriteString("For Skills workflows, prefer find_skills then install_skill, then read the installed SKILL.md and execute steps.\n")
	sb.WriteString("For MCP workflows, distinguish local vs remote MCP. Local MCP is configured under tools.mcp.local, and remote MCP is configured/managed under tools.mcp.remote and via remote_mcp operations.\n")
	sb.WriteString("When generating images/files, deliver them via the message tool using attachments (photo/document with path/url/file_id).\n")
	sb.WriteString("Never claim media was sent unless the message tool call already succeeded in this run.\n")
	sb.WriteString("Do not output placeholders such as [Sending image] without a successful attachment send.\n")
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
