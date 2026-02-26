package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/stretchr/testify/require"
)

// TestTaskPipeline_PersistenceAndStatus verifies task lifecycle persistence and status rendering.
// It creates a task, updates state, reloads from disk, and checks status output.
// It returns no value and fails test on mismatches.
func TestTaskPipeline_PersistenceAndStatus(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "task-pipeline-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	tp := NewTaskPipeline(tmpDir, 0)
	task, err := tp.EnqueueTask("telegram", "chat-1", "user-1", "build project", "build project")
	require.NoError(t, err)
	require.NotNil(t, task)

	require.True(t, tp.MarkRunning(task.ID, "planner"))
	require.True(t, tp.AppendWorkerEvent(task.ID, "worker: step 1 done"))
	require.True(t, tp.MarkPlannerCompleted(task.ID, "all done"))

	reloaded := NewTaskPipeline(tmpDir, 0)
	restored, ok := reloaded.GetTaskByID(task.ID)
	require.True(t, ok)
	require.Equal(t, TaskStatusDone, restored.Status)
	require.Equal(t, "planner", restored.PlannerAgent)
	require.Len(t, restored.WorkerEvents, 1)

	status := reloaded.BuildStatusReply("telegram", "chat-1", "user-1")
	require.Contains(t, status, task.ID)
	require.Contains(t, status, TaskStatusDone)
}

// TestTaskPipeline_RecoverUnfinishedOnRestart verifies unfinished tasks are resumed after restart.
// It simulates process restart by creating a new pipeline instance from persisted state.
// It returns no value and fails test on mismatches.
func TestTaskPipeline_RecoverUnfinishedOnRestart(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "task-pipeline-orphan-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	tp := NewTaskPipeline(tmpDir, 0)
	task, err := tp.EnqueueTask("telegram", "chat-2", "user-2", "long running", "long running")
	require.NoError(t, err)
	require.True(t, tp.MarkRunning(task.ID, "planner"))

	reloaded := NewTaskPipeline(tmpDir, 0)
	recovered := reloaded.RecoverUnfinishedOnRestart()
	require.Len(t, recovered, 1)
	require.Equal(t, task.ID, recovered[0].ID)
	require.Equal(t, TaskStatusQueued, recovered[0].Status)
	require.Equal(t, "resumed after restart", recovered[0].CheckpointNote)
	require.Zero(t, recovered[0].StartedAtUTC)
	require.False(t, recovered[0].DispatchQueued)

	restored, ok := reloaded.GetTaskByID(task.ID)
	require.True(t, ok)
	require.Equal(t, TaskStatusQueued, restored.Status)
	require.NotZero(t, restored.CheckpointAtUTC)
	require.Equal(t, "resumed after restart", restored.CheckpointNote)
}

// TestAgentLoop_ProcessMessage_DelegatesExternalTasks verifies external messages are enqueued.
// It sends a normal message and a status query to ensure chat agent remains responsive.
// It returns no value and fails test on mismatches.
func TestAgentLoop_ProcessMessage_DelegatesExternalTasks(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "agent-loop-pipeline-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	cfg := &config.Config{
		Agents: config.AgentsConfig{
			Defaults: config.AgentDefaults{
				Workspace:         tmpDir,
				Model:             "test-model",
				MaxTokens:         4096,
				MaxToolIterations: 10,
			},
		},
	}

	al := NewAgentLoop(cfg, bus.NewMessageBus(), &simpleMockProvider{response: "unused"})

	response, err := al.processMessage(context.Background(), bus.InboundMessage{
		Channel:  "telegram",
		ChatID:   "chat-42",
		SenderID: "u42",
		Content:  "Please run a complex job",
	})
	require.NoError(t, err)
	require.True(t, strings.Contains(response, "Task "))
	require.True(t, strings.Contains(response, "accepted"))

	statusResponse, err := al.processMessage(context.Background(), bus.InboundMessage{
		Channel:  "telegram",
		ChatID:   "chat-42",
		SenderID: "u42",
		Content:  "/status",
	})
	require.NoError(t, err)
	require.Contains(t, statusResponse, "Task status:")
	require.Regexp(t, regexp.MustCompile(`task-\d{4}-\d{2}-\d{2}-\d{4}\([^)]+\)`), statusResponse)
}

// TestMatchStatusQuery_UrlStatusPathDoesNotTrigger verifies URLs containing /status/ do not trigger task status intent.
// The t parameter controls test lifecycle and assertions.
// It returns no value and fails when URL-only status tokens are misclassified.
func TestMatchStatusQuery_UrlStatusPathDoesNotTrigger(t *testing.T) {
	matched, signal := MatchStatusQuery("总结分析一下这篇文章 https://x.com/1914ad/status/2026757796390449382")
	require.False(t, matched)
	require.Empty(t, signal)
}

// TestMatchStatusQuery_RecognizesCommandsAndKeywords verifies explicit status commands and keywords are still recognized.
// The t parameter controls test lifecycle and assertions.
// It returns no value and fails when expected status queries are not matched.
func TestMatchStatusQuery_RecognizesCommandsAndKeywords(t *testing.T) {
	matched, signal := MatchStatusQuery("/status")
	require.True(t, matched)
	require.Equal(t, "/status", signal)

	matched, signal = MatchStatusQuery("当前任务进度到哪了")
	require.True(t, matched)
	require.Equal(t, "进度", signal)

	matched, signal = MatchStatusQuery("what is the current task status?")
	require.True(t, matched)
	require.Equal(t, "status", signal)
}

// TestAgentLoop_ProcessMessage_StatusURLRequestIsDelegated verifies status-like URL requests are delegated instead of misrouted.
// The t parameter controls test lifecycle and assertions.
// It returns no value and fails when processMessage incorrectly returns task status listing.
func TestAgentLoop_ProcessMessage_StatusURLRequestIsDelegated(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "agent-loop-status-url-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	cfg := &config.Config{
		Agents: config.AgentsConfig{
			Defaults: config.AgentDefaults{
				Workspace:         tmpDir,
				Model:             "test-model",
				MaxTokens:         4096,
				MaxToolIterations: 10,
			},
		},
	}

	al := NewAgentLoop(cfg, bus.NewMessageBus(), &simpleMockProvider{response: "unused"})

	response, err := al.processMessage(context.Background(), bus.InboundMessage{
		Channel:  "telegram",
		ChatID:   "chat-77",
		SenderID: "u77",
		Content:  "总结分析一下这篇文章 https://x.com/1914ad/status/2026757796390449382",
	})
	require.NoError(t, err)
	require.Contains(t, response, "Task ")
	require.Contains(t, response, "accepted")
	require.NotContains(t, response, "Task status:")
}

// TestTaskPipeline_PromoteRunnableQueuedTasks verifies waiting tasks are promoted when dependencies finish.
// It enqueues a running task and a dependent waiting task, then completes dependency and promotes queued task.
// It returns no value and fails test on mismatches.
func TestTaskPipeline_PromoteRunnableQueuedTasks(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "task-pipeline-promote-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	tp := NewTaskPipeline(tmpDir, 0)

	task1, err := tp.EnqueueTask("telegram", "chat-1", "user-1", "first", "first")
	require.NoError(t, err)
	require.True(t, tp.MarkRunning(task1.ID, "planner"))

	task2, err := tp.EnqueueTaskWithScheduling(
		"telegram",
		"chat-1",
		"user-1",
		"second",
		"second",
		TaskExecutionModeWait,
		[]string{task1.ID},
		false,
	)
	require.NoError(t, err)
	require.NotNil(t, task2)

	promoted := tp.PromoteRunnableQueuedTasks(2)
	require.Len(t, promoted, 0)

	require.True(t, tp.MarkPlannerCompleted(task1.ID, "done"))
	promoted = tp.PromoteRunnableQueuedTasks(2)
	require.Len(t, promoted, 1)
	require.Equal(t, task2.ID, promoted[0].ID)
	require.True(t, promoted[0].DispatchQueued)
}

// TestTaskPipeline_ForceParallelDispatch verifies queued waiting task can be forced into parallel dispatch.
// It creates a waiting task and promotes it using ForceParallelDispatch.
// It returns no value and fails test on mismatches.
func TestTaskPipeline_ForceParallelDispatch(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "task-pipeline-force-parallel-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	tp := NewTaskPipeline(tmpDir, 0)
	task, err := tp.EnqueueTaskWithScheduling(
		"telegram",
		"chat-1",
		"user-1",
		"long task",
		"long task",
		TaskExecutionModeWait,
		[]string{"task-999"},
		false,
	)
	require.NoError(t, err)

	updated, dispatched, err := tp.ForceParallelDispatch(task.ID)
	require.NoError(t, err)
	require.True(t, dispatched)
	require.Equal(t, TaskExecutionModeParallel, updated.ExecutionMode)
	require.Empty(t, updated.WaitForTaskID)
	require.True(t, updated.DispatchQueued)
}

// TestParseTaskScheduleDecision verifies parser accepts fenced JSON classifier output.
// It feeds markdown-wrapped JSON and checks normalized scheduling fields.
// It returns no value and fails test on mismatches.
func TestParseTaskScheduleDecision(t *testing.T) {
	raw := "```json\n{\n  \"decision\": \"wait\",\n  \"reason\": \"depends on previous task\",\n  \"depends_on\": [\"task-1\", \"task-1\"],\n  \"question\": \"\"\n}\n```"

	decision, err := parseTaskScheduleDecision(raw)
	require.NoError(t, err)
	require.Equal(t, TaskExecutionModeWait, decision.Decision)
	require.Equal(t, "depends on previous task", decision.Reason)
	require.Equal(t, []string{"task-1"}, decision.DependsOn)
}

// TestParseParallelTaskID verifies the force-parallel command parser.
// It checks accepted and rejected command forms.
// It returns no value and fails test on mismatches.
func TestParseParallelTaskID(t *testing.T) {
	id, ok := parseParallelTaskID("parallel task-2")
	require.True(t, ok)
	require.Equal(t, "task-2", id)

	_, ok = parseParallelTaskID("parallel")
	require.False(t, ok)

	_, ok = parseParallelTaskID("run task-2")
	require.False(t, ok)
}

// TestBuildTaskID verifies task IDs include UTC date and sequence components.
// It formats a deterministic time and checks stable task ID output.
// It returns no value and fails test on mismatches.
func TestBuildTaskID(t *testing.T) {
	ts := time.Date(2026, time.February, 25, 16, 40, 11, 0, time.UTC)
	id := buildTaskID(ts, 6)

	require.Equal(t, "task-2026-02-25-0006", id)
	require.Regexp(t, regexp.MustCompile(`^task-\d{4}-\d{2}-\d{2}-\d{4}$`), id)
}

// TestParseRetryTaskID_PreservesCase verifies command parsing does not lowercase task IDs.
// It checks that mixed-case timestamp tokens are preserved for map lookups.
// It returns no value and fails test on mismatches.
func TestParseRetryTaskID_PreservesCase(t *testing.T) {
	id, ok := parseRetryTaskID("retry task-2026-02-25-0006")
	require.True(t, ok)
	require.Equal(t, "task-2026-02-25-0006", id)
}

// TestTaskPipeline_NextSequenceForDateLocked verifies sequence allocation increments and resets per UTC date.
// It uses explicit date keys to validate deterministic day-boundary behavior.
// It returns no value and fails test on mismatches.
func TestTaskPipeline_NextSequenceForDateLocked(t *testing.T) {
	tp := &TaskPipeline{nextSequence: 1}

	require.Equal(t, 1, tp.nextSequenceForDateLocked("2026-02-25"))
	require.Equal(t, 2, tp.nextSequenceForDateLocked("2026-02-25"))
	require.Equal(t, 1, tp.nextSequenceForDateLocked("2026-02-26"))
	require.Equal(t, 2, tp.nextSequenceForDateLocked("2026-02-26"))
}

// TestTaskPipeline_LoadLegacySnapshotRecoversCurrentDateSequence verifies sequence recovery when legacy snapshot metadata is missing.
// It writes a snapshot containing only tasks and expects the next generated ID to continue from max current-day sequence.
// It returns no value and fails test on duplicate or reset ID allocation.
func TestTaskPipeline_LoadLegacySnapshotRecoversCurrentDateSequence(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "task-pipeline-legacy-sequence-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	dateKey := time.Now().UTC().Format(time.DateOnly)
	legacy := map[string]any{
		"tasks": []map[string]any{
			{
				"id":             fmt.Sprintf("task-%s-0001", dateKey),
				"channel":        "telegram",
				"chat_id":        "chat-legacy",
				"sender_id":      "user-legacy",
				"request":        "legacy 1",
				"status":         TaskStatusDone,
				"created_at_utc": 1,
				"updated_at_utc": 1,
			},
			{
				"id":             fmt.Sprintf("task-%s-0003", dateKey),
				"channel":        "telegram",
				"chat_id":        "chat-legacy",
				"sender_id":      "user-legacy",
				"request":        "legacy 3",
				"status":         TaskStatusDone,
				"created_at_utc": 3,
				"updated_at_utc": 3,
			},
		},
	}

	stateDir := filepath.Join(tmpDir, "state")
	require.NoError(t, os.MkdirAll(stateDir, 0o755))
	data, err := json.Marshal(legacy)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(stateDir, taskPipelineFileName), data, 0o644))

	tp := NewTaskPipeline(tmpDir, 0)
	task, err := tp.EnqueueTask("telegram", "chat-legacy", "user-legacy", "new request", "new request")
	require.NoError(t, err)
	require.NotNil(t, task)
	require.Equal(t, fmt.Sprintf("task-%s-0004", dateKey), task.ID)
}

// TestTaskPipeline_EnqueueTaskAvoidsIDCollision verifies stale sequence state does not overwrite existing tasks.
// It forces sequence to collide with an existing current-day ID and expects enqueue to advance to the next available ID.
// It returns no value and fails test on duplicate ID generation.
func TestTaskPipeline_EnqueueTaskAvoidsIDCollision(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "task-pipeline-collision-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	tp := NewTaskPipeline(tmpDir, 0)
	dateKey := time.Now().UTC().Format(time.DateOnly)
	existingID := fmt.Sprintf("task-%s-0001", dateKey)

	tp.mu.Lock()
	tp.tasks[existingID] = &PipelineTask{
		ID:           existingID,
		Channel:      "telegram",
		ChatID:       "chat-collision",
		SenderID:     "user-collision",
		Request:      "existing",
		Status:       TaskStatusDone,
		CreatedAtUTC: time.Now().UTC().UnixMilli(),
		UpdatedAtUTC: time.Now().UTC().UnixMilli(),
	}
	tp.lastSeqDate = dateKey
	tp.nextSequence = 1
	require.NoError(t, tp.saveLocked())
	tp.mu.Unlock()

	task, err := tp.EnqueueTask("telegram", "chat-collision", "user-collision", "new", "new")
	require.NoError(t, err)
	require.NotNil(t, task)
	require.Equal(t, fmt.Sprintf("task-%s-0002", dateKey), task.ID)
}

// TestSanitizeTaskSummary verifies model output is normalized for task label display.
// It covers markdown wrappers, unsupported punctuation, and whitespace normalization.
// It returns no value and fails test on unexpected normalized content.
func TestSanitizeTaskSummary(t *testing.T) {
	clean := sanitizeTaskSummary("  `Investigate: telegram timeout #123`  ")
	require.Equal(t, "Investigate: telegram ti", clean)

	clean = sanitizeTaskSummary("\nfix   queue\tdeadlock\n")
	require.Equal(t, "fix queue deadlock", clean)

	clean = sanitizeTaskSummary("用户截图")
	require.Equal(t, "用户截图", clean)
}

// TestIsSafeTaskBrief verifies safety validator accepts clean labels and rejects sensitive/invalid content.
// It checks URL and long-number patterns are blocked while concise summaries pass.
// It returns no value and fails test on incorrect validation result.
func TestIsSafeTaskBrief(t *testing.T) {
	require.True(t, isSafeTaskBrief("Investigate queue deadlock"))
	require.True(t, isSafeTaskBrief("查询天气"))
	require.False(t, isSafeTaskBrief("https://example.com/reset"))
	require.False(t, isSafeTaskBrief("ticket 123456789"))
}

// TestFallbackTaskSummaryFromRequest verifies fallback summary keeps user language and intent.
// It passes multilingual requests and checks concise labels are derived from request text.
// It returns no value and fails test on unexpected fallback output.
func TestFallbackTaskSummaryFromRequest(t *testing.T) {
	brief := fallbackTaskSummaryFromRequest("帮我查询天气")
	require.Equal(t, "帮我查询天气", brief)

	brief = fallbackTaskSummaryFromRequest("Please take a screenshot of the dashboard and send it")
	require.Equal(t, "Please take a screenshot", brief)
}

// TestTaskPipeline_BuildStatusReplyIncludesTiming verifies status output includes start and elapsed/duration info.
// It checks running tasks expose started_at_utc and elapsed, and completed tasks expose duration.
// It returns no value and fails test on missing timing fields.
func TestTaskPipeline_BuildStatusReplyIncludesTiming(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "task-pipeline-status-timing-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	tp := NewTaskPipeline(tmpDir, 0)
	task, err := tp.EnqueueTask("telegram", "chat-3", "user-3", "查询天气", "查询天气")
	require.NoError(t, err)
	require.True(t, tp.MarkRunning(task.ID, "planner"))

	running := tp.BuildStatusReply("telegram", "chat-3", "user-3")
	require.Contains(t, running, "started_at_utc=")
	require.Contains(t, running, "elapsed=")

	time.Sleep(10 * time.Millisecond)
	require.True(t, tp.MarkPlannerCompleted(task.ID, "done"))
	completed := tp.BuildStatusReply("telegram", "chat-3", "user-3")
	require.Contains(t, completed, "started_at_utc=")
	require.Contains(t, completed, "duration=")
}

// TestBuildPlannerDelegationPrompt_IncludesDelegationContext verifies delegated prompt includes memory/history reference text.
// It builds a pipeline task with delegation context and checks prompt sections.
// It returns no value and fails test on missing expected content.
func TestBuildPlannerDelegationPrompt_IncludesDelegationContext(t *testing.T) {
	task := &PipelineTask{
		ID:                "task-2026-02-25-0001",
		Summary:           "Investigate intermittent restart",
		DelegationContext: "Conversation summary: user asked for root cause first",
		Request:           "Find root cause and mitigation plan",
	}

	prompt := buildPlannerDelegationPrompt(task)
	require.Contains(t, prompt, "You are picoclaw, handling a background task for the user.")
	require.Contains(t, prompt, "Task tracking id: task-2026-02-25-0001")
	require.Contains(t, prompt, "Delegation reference (memory and interaction history):")
	require.Contains(t, prompt, "root cause first")
	require.Contains(t, prompt, "deliver them via the message tool using attachments")
	require.Contains(t, prompt, "Never claim media was sent unless the message tool call already succeeded")
	require.Contains(t, prompt, "Do not output placeholders such as [Sending image]")
	require.Contains(t, prompt, "User request:")
}
