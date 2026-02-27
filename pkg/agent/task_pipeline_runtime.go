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
		err := fmt.Errorf("planner agent is not configured")
		logger.ErrorCF("agent", "Planner task cannot start: default/planner agent missing", map[string]any{
			"task_id": task.ID,
			"channel": task.Channel,
			"chat_id": task.ChatID,
		})
		al.taskPipeline.MarkFailed(task.ID, err.Error())
		al.publishTaskFinalReport(task.ID, "", err)
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
	recentHistory := al.loadTaskConversationRecentHistory(task)

	result, err := al.runAgentLoop(runCtx, runner, processOptions{
		SessionKey:      sessionKey,
		Channel:         plannerTaskChannel,
		ChatID:          task.ID,
		ToolChannel:     task.Channel,
		ToolChatID:      task.ChatID,
		RecentHistory:   recentHistory,
		UserMessage:     plannerPrompt,
		DefaultResponse: plannerEmptySummaryText,
		EnableSummary:   false,
		SendResponse:    false,
		NoHistory:       true,
	})
	if err != nil {
		logger.ErrorCF("agent", "Planner run failed", map[string]any{
			"task_id":     task.ID,
			"agent_id":    runner.ID,
			"session_key": sessionKey,
			"error":       err.Error(),
		})
		al.taskPipeline.MarkFailed(task.ID, err.Error())
		al.taskPipeline.CheckpointTask(task.ID, "planner run failed")
		al.publishTaskFinalReport(task.ID, "", err)
		return
	}

	if isPlannerSummaryEmpty(result) {
		emptySummaryErr := fmt.Errorf("planner finished without textual summary")
		logger.WarnCF("agent", "Planner task finished with empty textual output", map[string]any{
			"task_id":        task.ID,
			"agent_id":       runner.ID,
			"session_key":    sessionKey,
			"result_preview": utils.Truncate(strings.TrimSpace(result), 120),
			"max_iterations": runner.MaxIterations,
		})
		al.taskPipeline.MarkFailed(task.ID, emptySummaryErr.Error())
		al.taskPipeline.CheckpointTask(task.ID, "planner run failed: empty textual output")
		al.publishTaskFinalReport(task.ID, "", emptySummaryErr)
		return
	}

	al.taskPipeline.MarkPlannerCompleted(task.ID, result)
	al.taskPipeline.CheckpointTask(task.ID, "planner run completed")
	al.publishTaskFinalReport(task.ID, result, nil)
}

// loadTaskConversationRecentHistory returns routed conversation history for delegated planner requests.
// The task parameter carries routed agent/session metadata and return value is nil when context is unavailable.
func (al *AgentLoop) loadTaskConversationRecentHistory(task *PipelineTask) []providers.Message {
	if al == nil || al.registry == nil || task == nil {
		return nil
	}

	agentID := strings.TrimSpace(task.ConversationAgentID)
	sessionKey := strings.TrimSpace(task.ConversationSessionKey)
	if agentID == "" || sessionKey == "" {
		return nil
	}

	agent, ok := al.registry.GetAgent(agentID)
	if !ok || agent == nil || agent.Sessions == nil {
		return nil
	}

	return agent.Sessions.GetHistory(sessionKey)
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

	logger.DebugCF("agent", "Preparing delegated task final report", map[string]any{
		"task_id":            task.ID,
		"status":             task.Status,
		"planner_summary_len": len(strings.TrimSpace(plannerSummary)),
		"stored_summary_len":  len(strings.TrimSpace(task.PlannerResult)),
		"run_error":          runErr != nil,
	})

	finalStatus := task.Status
	if runErr != nil {
		finalStatus = TaskStatusFailed
		errText := strings.TrimSpace(task.Error)
		if errText == "" {
			errText = strings.TrimSpace(runErr.Error())
		}
		summary = errText
	}

	report := al.buildUserTaskFinalReport(task, finalStatus, summary)
	logReport := formatTaskFinalLogReport(task, finalStatus, summary)
	logger.DebugCF("agent", "Publishing delegated task final report to channel", map[string]any{
		"task_id":      task.ID,
		"channel":      task.Channel,
		"chat_id":      task.ChatID,
		"final_status": finalStatus,
		"report_chars": len(report),
	})
	al.bus.PublishOutbound(bus.OutboundMessage{
		Channel: task.Channel,
		ChatID:  task.ChatID,
		Content: report,
	})

	logger.InfoCF("agent", "Delegated task finished", map[string]any{
		"task_id":      task.ID,
		"status":       finalStatus,
		"duration":     taskDuration(task),
		"user_report":  report,
		"detail_report": logReport,
	})
	al.recordDelegatedTaskResult(task, report)
}

// buildUserTaskFinalReport renders delegated task completion text for end users.
// The task parameter provides request context, finalStatus is terminal lifecycle state, and summary is raw runtime output.
// It returns an LLM-refined user reply when possible, or deterministic fallback text when rewrite fails.
func (al *AgentLoop) buildUserTaskFinalReport(task *PipelineTask, finalStatus, summary string) string {
	fallback := formatTaskFinalReport(task, finalStatus, summary)
	if finalStatus == TaskStatusFailed {
		return fallback
	}
	if !shouldRewriteTaskSummary(summary) {
		return fallback
	}

	planner := al.getPlannerAgent()
	if planner == nil || planner.Provider == nil {
		return fallback
	}

	rewriteCtx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	requestText := ""
	if task != nil {
		requestText = strings.TrimSpace(task.Request)
	}

	resp, err := planner.Provider.Chat(rewriteCtx, []providers.Message{
		{Role: "system", Content: "Rewrite delegated task completion output for end users. Keep the same language as user request and summary. Output plain text only. Keep only user-relevant outcome and next action. Never include internal task IDs, status templates, execution durations, debug traces, or log metadata unless explicitly requested by the user. " + promptLengthControlHint},
		{Role: "user", Content: fmt.Sprintf("Task status: %s\nOriginal user request:\n%s\n\nRaw task result:\n%s\n\nRewrite this into a concise user-facing reply.", finalStatus, requestText, summary)},
	}, nil, planner.Model, map[string]any{
		"max_tokens":  320,
		"temperature": 0.1,
	})
	if err != nil || resp == nil {
		return fallback
	}

	rewritten := normalizeTaskUserSummary(resp.Content)
	if rewritten == "" {
		return fallback
	}

	return utils.Truncate(rewritten, 2800)
}

// isPlannerSummaryEmpty reports whether planner output has no user-meaningful text.
// The summary parameter is raw planner final content from runAgentLoop.
// It returns true when output is empty or equals the internal empty-summary sentinel.
func isPlannerSummaryEmpty(summary string) bool {
	trimmed := strings.TrimSpace(summary)
	return trimmed == "" || trimmed == plannerEmptySummaryText
}

// shouldRewriteTaskSummary reports whether raw task output needs LLM rewrite before user delivery.
// The summary parameter is raw planner/runtime output text.
// It returns true when operational wrapper patterns are present.
func shouldRewriteTaskSummary(summary string) bool {
	text := strings.TrimSpace(summary)
	if text == "" || text == plannerEmptySummaryText {
		return false
	}

	if strings.Contains(text, "\nSummary:") || strings.HasPrefix(text, "Summary:") {
		return true
	}
	if strings.Contains(text, "\nDuration:") || strings.HasPrefix(text, "Duration:") {
		return true
	}
	firstLine, _, _ := strings.Cut(text, "\n")
	firstLine = strings.TrimSpace(firstLine)
	if strings.HasPrefix(firstLine, "Task ") && strings.Contains(firstLine, " finished (") {
		return true
	}

	return false
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

// formatTaskFinalReport renders a concise, user-facing completion message.
// The task parameter carries optional status context and summary is planner output text.
// It returns friendly chat text without operational metadata.
func formatTaskFinalReport(task *PipelineTask, finalStatus, summary string) string {
	if task == nil {
		if finalStatus == TaskStatusFailed {
			return "❌ Task failed.\nError: unknown error."
		}
		return "Task completed."
	}

	textSummary := normalizeTaskUserSummary(summary)
	if finalStatus == TaskStatusFailed {
		if textSummary == "" {
			textSummary = "unknown error"
		}
		return utils.Truncate(fmt.Sprintf("❌ Task failed.\nError: %s", textSummary), 2800)
	}

	if textSummary == "" {
		return "Task completed."
	}

	return utils.Truncate(textSummary, 2800)
}

// normalizeTaskUserSummary strips operational wrapper lines and keeps user-relevant text.
// The summary parameter is raw planner/runtime summary that may include template-like fields.
// It returns concise text intended for direct user delivery.
func normalizeTaskUserSummary(summary string) string {
	text := strings.TrimSpace(summary)
	if text == "" || text == plannerEmptySummaryText {
		return ""
	}

	lines := strings.Split(text, "\n")
	clean := make([]string, 0, len(lines))
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		if strings.HasPrefix(trimmed, "Summary:") {
			trimmed = strings.TrimSpace(strings.TrimPrefix(trimmed, "Summary:"))
			if trimmed == "" {
				continue
			}
			clean = append(clean, trimmed)
			continue
		}
		if strings.HasPrefix(trimmed, "Duration:") {
			continue
		}
		if strings.HasPrefix(trimmed, "Task ") && strings.Contains(trimmed, " finished (") {
			continue
		}
		clean = append(clean, trimmed)
	}

	return strings.TrimSpace(strings.Join(clean, "\n"))
}

// formatTaskFinalLogReport renders lifecycle-rich task completion details for logs.
// The task parameter includes timestamps, finalStatus indicates terminal state, and summary is planner output.
// It returns a stable operational report string for troubleshooting.
func formatTaskFinalLogReport(task *PipelineTask, finalStatus, summary string) string {
	if task == nil {
		return "Task finished, but details are unavailable."
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
		taskDuration(task),
	)
}

// taskDuration returns elapsed runtime text for a finished task.
// The task parameter provides started and finished UTC unix milliseconds.
// It returns a human-readable duration or "unknown" when timing is incomplete.
func taskDuration(task *PipelineTask) string {
	if task == nil {
		return "unknown"
	}
	if task.StartedAtUTC > 0 && task.FinishedAtUTC > 0 && task.FinishedAtUTC >= task.StartedAtUTC {
		return (time.Duration(task.FinishedAtUTC-task.StartedAtUTC) * time.Millisecond).String()
	}
	return "unknown"
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
