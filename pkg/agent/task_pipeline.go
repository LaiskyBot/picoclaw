package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/sipeed/picoclaw/pkg/logger"
)

const (
	taskPipelineFileName = "task_pipeline.json"
	defaultTaskTimeout   = 45 * time.Minute
)

const (
	TaskStatusQueued   = "queued"
	TaskStatusRunning  = "running"
	TaskStatusDone     = "completed"
	TaskStatusFailed   = "failed"
	TaskStatusTimeout  = "timeout"
	TaskStatusOrphaned = "orphaned"
)

const (
	TaskExecutionModeParallel = "parallel"
	TaskExecutionModeWait     = "wait"
)

var statusQueryURLPattern = regexp.MustCompile(`https?://\S+`)

// PipelineTask stores the end-to-end state of a delegated background task.
// It includes user origin metadata, planner execution status, and worker updates.
type PipelineTask struct {
	ID             string   `json:"id"`
	Summary        string   `json:"summary,omitempty"`
	DelegationContext string `json:"delegation_context,omitempty"`
	Channel        string   `json:"channel"`
	ChatID         string   `json:"chat_id"`
	SenderID       string   `json:"sender_id"`
	Request        string   `json:"request"`
	ExecutionMode  string   `json:"execution_mode,omitempty"`
	WaitForTaskID  []string `json:"wait_for_task_id,omitempty"`
	DispatchQueued bool     `json:"dispatch_queued,omitempty"`
	PlannerAgent   string   `json:"planner_agent"`
	Status         string   `json:"status"`
	CreatedAtUTC   int64    `json:"created_at_utc"`
	UpdatedAtUTC   int64    `json:"updated_at_utc"`
	CheckpointAtUTC int64   `json:"checkpoint_at_utc,omitempty"`
	CheckpointNote string   `json:"checkpoint_note,omitempty"`
	StartedAtUTC   int64    `json:"started_at_utc,omitempty"`
	FinishedAtUTC  int64    `json:"finished_at_utc,omitempty"`
	PlannerResult  string   `json:"planner_result,omitempty"`
	Error          string   `json:"error,omitempty"`
	WorkerEvents   []string `json:"worker_events,omitempty"`
}

// taskPipelineSnapshot is the on-disk format of TaskPipeline.
// It captures deterministic task ID generation and task states.
type taskPipelineSnapshot struct {
	NextID           int             `json:"next_id,omitempty"`
	NextSequence     int             `json:"next_sequence,omitempty"`
	LastSequenceDate string          `json:"last_sequence_date,omitempty"`
	Tasks            []*PipelineTask `json:"tasks"`
}

// TaskPipeline manages asynchronous background task lifecycle.
// It persists tasks to disk, supports queueing, status queries, timeout checks,
// and orphan detection after process restarts.
type TaskPipeline struct {
	mu          sync.RWMutex
	tasks       map[string]*PipelineTask
	nextSequence int
	lastSeqDate  string
	queue       chan string
	storagePath string
	timeout     time.Duration
}

// NewTaskPipeline creates a persistent task pipeline for the provided workspace.
// The workspace parameter is the root folder for state files.
// It returns an initialized pipeline with previously saved tasks loaded when available.
func NewTaskPipeline(workspace string, timeout time.Duration) *TaskPipeline {
	if timeout <= 0 {
		timeout = defaultTaskTimeout
	}

	stateDir := filepath.Join(workspace, "state")
	_ = os.MkdirAll(stateDir, 0o755)

	tp := &TaskPipeline{
		tasks:       map[string]*PipelineTask{},
		nextSequence: 1,
		queue:       make(chan string, 256),
		storagePath: filepath.Join(stateDir, taskPipelineFileName),
		timeout:     timeout,
	}
	tp.load()
	return tp
}

// EnqueueTask appends a new user task into the background queue.
// The channel/chatID/senderID identify task origin and request carries user intent.
// It returns the created task record.
func (tp *TaskPipeline) EnqueueTask(channel, chatID, senderID, request, summary string) (*PipelineTask, error) {
	return tp.EnqueueTaskWithScheduling(
		channel,
		chatID,
		senderID,
		request,
		summary,
		TaskExecutionModeParallel,
		nil,
		true,
	)
}

// EnqueueTaskWithScheduling appends a new user task with explicit scheduling controls.
// The executionMode controls preferred strategy, waitForTaskIDs tracks dependencies,
// and dispatchNow decides whether it is immediately dispatched to planner workers.
// It returns the created task record.
func (tp *TaskPipeline) EnqueueTaskWithScheduling(
	channel,
	chatID,
	senderID,
	request,
	summary,
	executionMode string,
	waitForTaskIDs []string,
	dispatchNow bool,
) (*PipelineTask, error) {
	nowUTC := time.Now().UTC().UnixMilli()
	normalizedMode := normalizeExecutionMode(executionMode)
	uniqWaitForIDs := uniqueTaskIDs(waitForTaskIDs)
	normalizedSummary := normalizeTaskSummary(summary)

	tp.mu.Lock()
	dateKey := time.UnixMilli(nowUTC).UTC().Format(time.DateOnly)
	sequence := tp.nextSequenceForDateLocked(dateKey)
	taskID := buildTaskID(time.UnixMilli(nowUTC).UTC(), sequence)
	for {
		if _, exists := tp.tasks[taskID]; !exists {
			break
		}
		logger.WarnCF("agent", "Task ID collision detected; advancing sequence", map[string]any{
			"task_id":  taskID,
			"date_key": dateKey,
		})
		sequence = tp.nextSequenceForDateLocked(dateKey)
		taskID = buildTaskID(time.UnixMilli(nowUTC).UTC(), sequence)
	}
	task := &PipelineTask{
		ID:             taskID,
		Summary:        normalizedSummary,
		Channel:        channel,
		ChatID:         chatID,
		SenderID:       senderID,
		Request:        request,
		ExecutionMode:  normalizedMode,
		WaitForTaskID:  uniqWaitForIDs,
		DispatchQueued: dispatchNow,
		Status:         TaskStatusQueued,
		CreatedAtUTC:   nowUTC,
		UpdatedAtUTC:   nowUTC,
		CheckpointAtUTC: nowUTC,
	}
	tp.tasks[taskID] = task
	saveErr := tp.saveLocked()
	tp.mu.Unlock()
	if saveErr != nil {
		return nil, saveErr
	}

	if !dispatchNow {
		return clonePipelineTask(task), nil
	}

	select {
	case tp.queue <- taskID:
	default:
		tp.mu.Lock()
		task.DispatchQueued = false
		_ = tp.saveLocked()
		tp.mu.Unlock()
		return nil, fmt.Errorf("task queue is full")
	}

	return clonePipelineTask(task), nil
}

// Dequeue blocks until a queued task is available or context is canceled.
// The ctx parameter controls waiting lifecycle.
// It returns task details and a boolean indicating whether dequeue succeeded.
func (tp *TaskPipeline) Dequeue(ctx context.Context) (*PipelineTask, bool) {
	for {
		select {
		case taskID := <-tp.queue:
			tp.mu.Lock()
			task, ok := tp.tasks[taskID]
			if !ok {
				tp.mu.Unlock()
				continue
			}
			if task.Status != TaskStatusQueued || !task.DispatchQueued {
				tp.mu.Unlock()
				continue
			}
			task.DispatchQueued = false
			task.UpdatedAtUTC = time.Now().UTC().UnixMilli()
			if err := tp.saveLocked(); err != nil {
				logger.WarnCF("agent", "Failed to persist dequeue state", map[string]any{"task_id": taskID, "error": err.Error()})
			}
			out := clonePipelineTask(task)
			tp.mu.Unlock()
			return out, true
		case <-ctx.Done():
			return nil, false
		}
	}
}

// MarkRunning marks a task as actively processed by a planner agent.
// The taskID selects task and plannerAgent names the backend planner role.
// It returns true when task status is updated.
func (tp *TaskPipeline) MarkRunning(taskID, plannerAgent string) bool {
	tp.mu.Lock()
	defer tp.mu.Unlock()

	task, ok := tp.tasks[taskID]
	if !ok {
		return false
	}
	nowUTC := time.Now().UTC().UnixMilli()
	task.Status = TaskStatusRunning
	task.DispatchQueued = false
	task.PlannerAgent = plannerAgent
	task.StartedAtUTC = nowUTC
	task.UpdatedAtUTC = nowUTC
	task.CheckpointAtUTC = nowUTC
	task.CheckpointNote = "planner running"

	if err := tp.saveLocked(); err != nil {
		logger.WarnCF("agent", "Failed to persist running task", map[string]any{"task_id": taskID, "error": err.Error()})
	}
	return true
}

// SetDelegationContext updates planner delegation context for a queued/running task.
// The taskID parameter identifies the task and context carries memory/history references.
// It returns true when the task is updated.
func (tp *TaskPipeline) SetDelegationContext(taskID, context string) bool {
	tp.mu.Lock()
	defer tp.mu.Unlock()

	task, ok := tp.tasks[taskID]
	if !ok {
		return false
	}
	nowUTC := time.Now().UTC().UnixMilli()
	task.DelegationContext = strings.TrimSpace(context)
	task.UpdatedAtUTC = nowUTC
	task.CheckpointAtUTC = nowUTC
	task.CheckpointNote = "delegation context updated"
	if err := tp.saveLocked(); err != nil {
		logger.WarnCF("agent", "Failed to persist delegation context", map[string]any{"task_id": taskID, "error": err.Error()})
	}
	return true
}

// MarkPlannerCompleted updates task as completed and records planner output.
// The taskID identifies task and plannerResult stores final planner response.
// It returns true when the task is found and updated.
func (tp *TaskPipeline) MarkPlannerCompleted(taskID, plannerResult string) bool {
	tp.mu.Lock()
	defer tp.mu.Unlock()

	task, ok := tp.tasks[taskID]
	if !ok {
		return false
	}
	nowUTC := time.Now().UTC().UnixMilli()
	task.Status = TaskStatusDone
	task.PlannerResult = plannerResult
	task.UpdatedAtUTC = nowUTC
	task.CheckpointAtUTC = nowUTC
	task.CheckpointNote = "planner completed"
	task.FinishedAtUTC = nowUTC
	task.Error = ""

	if err := tp.saveLocked(); err != nil {
		logger.WarnCF("agent", "Failed to persist completed task", map[string]any{"task_id": taskID, "error": err.Error()})
	}
	return true
}

// MarkFailed updates task as failed with a stable error message.
// The taskID identifies task and errMessage stores reason.
// It returns true when the task is found and updated.
func (tp *TaskPipeline) MarkFailed(taskID, errMessage string) bool {
	tp.mu.Lock()
	defer tp.mu.Unlock()

	task, ok := tp.tasks[taskID]
	if !ok {
		return false
	}
	nowUTC := time.Now().UTC().UnixMilli()
	task.Status = TaskStatusFailed
	task.Error = errMessage
	task.UpdatedAtUTC = nowUTC
	task.CheckpointAtUTC = nowUTC
	task.CheckpointNote = "planner failed"
	task.FinishedAtUTC = nowUTC

	if err := tp.saveLocked(); err != nil {
		logger.WarnCF("agent", "Failed to persist failed task", map[string]any{"task_id": taskID, "error": err.Error()})
	}
	return true
}

// AppendWorkerEvent appends a worker update line to a parent task.
// The taskID links to parent task and event carries worker status text.
// It returns true when task exists and event is stored.
func (tp *TaskPipeline) AppendWorkerEvent(taskID, event string) bool {
	tp.mu.Lock()
	defer tp.mu.Unlock()

	task, ok := tp.tasks[taskID]
	if !ok {
		return false
	}
	task.WorkerEvents = append(task.WorkerEvents, event)
	task.UpdatedAtUTC = time.Now().UTC().UnixMilli()
	task.CheckpointAtUTC = task.UpdatedAtUTC
	task.CheckpointNote = "worker update"

	if err := tp.saveLocked(); err != nil {
		logger.WarnCF("agent", "Failed to persist worker event", map[string]any{"task_id": taskID, "error": err.Error()})
	}
	return true
}

// UpdateSummary updates the normalized summary for an existing task.
// The taskID parameter identifies the task and summary provides new label text.
// It returns true when the task exists and summary value changes.
func (tp *TaskPipeline) UpdateSummary(taskID, summary string) bool {
	normalizedSummary := normalizeTaskSummary(summary)

	tp.mu.Lock()
	defer tp.mu.Unlock()

	task, ok := tp.tasks[taskID]
	if !ok {
		return false
	}
	if task.Summary == normalizedSummary {
		return false
	}
	task.Summary = normalizedSummary
	task.UpdatedAtUTC = time.Now().UTC().UnixMilli()
	task.CheckpointAtUTC = task.UpdatedAtUTC
	task.CheckpointNote = "summary updated"

	if err := tp.saveLocked(); err != nil {
		logger.WarnCF("agent", "Failed to persist task summary update", map[string]any{"task_id": taskID, "error": err.Error()})
	}
	return true
}

// RecoverUnfinishedOnRestart recovers unfinished tasks after process restart.
// It converts running/orphaned tasks back to queued state and resets in-memory dispatch flags.
// It returns tasks that were recovered and should be resumed automatically.
func (tp *TaskPipeline) RecoverUnfinishedOnRestart() []*PipelineTask {
	tp.mu.Lock()
	defer tp.mu.Unlock()

	recovered := make([]*PipelineTask, 0)
	nowUTC := time.Now().UTC().UnixMilli()
	for _, task := range tp.tasks {
		switch task.Status {
		case TaskStatusQueued:
			task.DispatchQueued = false
			task.UpdatedAtUTC = nowUTC
			task.CheckpointAtUTC = nowUTC
			task.CheckpointNote = "resumed after restart"
			recovered = append(recovered, clonePipelineTask(task))
		case TaskStatusRunning, TaskStatusOrphaned:
			task.Status = TaskStatusQueued
			task.DispatchQueued = false
			task.PlannerAgent = ""
			task.StartedAtUTC = 0
			task.FinishedAtUTC = 0
			task.Error = "Recovered after process restart; resuming execution."
			task.UpdatedAtUTC = nowUTC
			task.CheckpointAtUTC = nowUTC
			task.CheckpointNote = "resumed after restart"
			recovered = append(recovered, clonePipelineTask(task))
		}
	}

	if len(recovered) > 0 {
		if err := tp.saveLocked(); err != nil {
			logger.WarnCF("agent", "Failed to persist recovered tasks", map[string]any{"error": err.Error()})
		}
	}

	sort.Slice(recovered, func(i, j int) bool { return recovered[i].CreatedAtUTC < recovered[j].CreatedAtUTC })
	return recovered
}

// MarkOrphanedOnRestart marks queued/running tasks as orphaned after restart.
// The method is retained for compatibility with legacy callers.
// It returns the list of tasks transitioned to orphaned state for user notification.
func (tp *TaskPipeline) MarkOrphanedOnRestart() []*PipelineTask {
	tp.mu.Lock()
	defer tp.mu.Unlock()

	orphans := make([]*PipelineTask, 0)
	nowUTC := time.Now().UTC().UnixMilli()
	for _, task := range tp.tasks {
		if task.Status == TaskStatusQueued || task.Status == TaskStatusRunning {
			task.Status = TaskStatusOrphaned
			task.Error = "Tracking was interrupted by process restart."
			task.UpdatedAtUTC = nowUTC
			task.CheckpointAtUTC = nowUTC
			task.CheckpointNote = "orphaned after restart"
			task.FinishedAtUTC = nowUTC
			orphans = append(orphans, clonePipelineTask(task))
		}
	}

	if len(orphans) > 0 {
		if err := tp.saveLocked(); err != nil {
			logger.WarnCF("agent", "Failed to persist orphaned tasks", map[string]any{"error": err.Error()})
		}
	}

	sort.Slice(orphans, func(i, j int) bool { return orphans[i].CreatedAtUTC < orphans[j].CreatedAtUTC })
	return orphans
}

// CheckpointTask updates task heartbeat metadata for restart recovery and timeout tracking.
// The taskID parameter identifies the task and note is a concise checkpoint description.
// It returns true when task exists and checkpoint is persisted.
func (tp *TaskPipeline) CheckpointTask(taskID, note string) bool {
	tp.mu.Lock()
	defer tp.mu.Unlock()

	task, ok := tp.tasks[taskID]
	if !ok {
		return false
	}
	nowUTC := time.Now().UTC().UnixMilli()
	task.UpdatedAtUTC = nowUTC
	task.CheckpointAtUTC = nowUTC
	task.CheckpointNote = strings.TrimSpace(note)

	if err := tp.saveLocked(); err != nil {
		logger.WarnCF("agent", "Failed to persist task checkpoint", map[string]any{"task_id": taskID, "error": err.Error()})
	}
	return true
}

// MarkTimeouts marks long-running tasks as timeout state.
// The now parameter is compared with UpdatedAtUTC against configured timeout.
// It returns tasks that were newly marked as timed out.
func (tp *TaskPipeline) MarkTimeouts(now time.Time) []*PipelineTask {
	tp.mu.Lock()
	defer tp.mu.Unlock()

	timedOut := make([]*PipelineTask, 0)
	nowUTC := now.UTC().UnixMilli()
	timeoutMs := tp.timeout.Milliseconds()
	for _, task := range tp.tasks {
		if task.Status != TaskStatusQueued && task.Status != TaskStatusRunning {
			continue
		}
		if nowUTC-task.UpdatedAtUTC < timeoutMs {
			continue
		}
		task.Status = TaskStatusTimeout
		task.Error = fmt.Sprintf("Task exceeded timeout of %s.", tp.timeout)
		task.UpdatedAtUTC = nowUTC
		task.CheckpointAtUTC = nowUTC
		task.CheckpointNote = "timeout"
		task.FinishedAtUTC = nowUTC
		timedOut = append(timedOut, clonePipelineTask(task))
	}

	if len(timedOut) > 0 {
		if err := tp.saveLocked(); err != nil {
			logger.WarnCF("agent", "Failed to persist timed out tasks", map[string]any{"error": err.Error()})
		}
	}

	sort.Slice(timedOut, func(i, j int) bool { return timedOut[i].CreatedAtUTC < timedOut[j].CreatedAtUTC })
	return timedOut
}

// BuildStatusReply builds a compact status report for a specific chat and sender.
// The channel/chatID/senderID parameters scope visible tasks for the requester.
// It returns a formatted status string suitable for direct user response.
func (tp *TaskPipeline) BuildStatusReply(channel, chatID, senderID string) string {
	tp.mu.RLock()
	defer tp.mu.RUnlock()

	items := make([]*PipelineTask, 0)
	for _, task := range tp.tasks {
		if task.Channel != channel || task.ChatID != chatID {
			continue
		}
		if senderID != "" && task.SenderID != "" && task.SenderID != senderID {
			continue
		}
		items = append(items, task)
	}

	if len(items) == 0 {
		return "No tracked tasks in this conversation."
	}

	sort.Slice(items, func(i, j int) bool { return items[i].CreatedAtUTC > items[j].CreatedAtUTC })
	if len(items) > 5 {
		items = items[:5]
	}

	var sb strings.Builder
	sb.WriteString("Task status:\n")
	for _, task := range items {
		fmt.Fprintf(&sb, "- %s: %s", taskLabel(task), task.Status)
		if task.Status == TaskStatusQueued {
			if task.DispatchQueued {
				sb.WriteString(" (ready)")
			} else {
				sb.WriteString(" (waiting)")
			}
		}
		if task.ExecutionMode != "" {
			fmt.Fprintf(&sb, " (mode=%s)", task.ExecutionMode)
		}
		if len(task.WaitForTaskID) > 0 {
			fmt.Fprintf(&sb, " (depends_on=%s)", strings.Join(task.WaitForTaskID, ","))
		}
		if task.PlannerAgent != "" {
			fmt.Fprintf(&sb, " (planner=%s)", task.PlannerAgent)
		}
		if len(task.WorkerEvents) > 0 {
			fmt.Fprintf(&sb, " [worker_updates=%d]", len(task.WorkerEvents))
		}
		if task.Error != "" {
			fmt.Fprintf(&sb, " error=%s", task.Error)
		}
		if task.StartedAtUTC > 0 {
			fmt.Fprintf(&sb, " (started_at_utc=%s)", formatUnixMilliUTC(task.StartedAtUTC))
			if task.Status == TaskStatusRunning {
				elapsed := time.Since(time.UnixMilli(task.StartedAtUTC).UTC()).Round(time.Second)
				if elapsed < 0 {
					elapsed = 0
				}
				fmt.Fprintf(&sb, " (elapsed=%s)", elapsed.String())
			} else if task.FinishedAtUTC > 0 && task.FinishedAtUTC >= task.StartedAtUTC {
				duration := (time.Duration(task.FinishedAtUTC-task.StartedAtUTC) * time.Millisecond).Round(time.Second)
				fmt.Fprintf(&sb, " (duration=%s)", duration.String())
			}
		}
		sb.WriteString("\n")
	}

	return strings.TrimSpace(sb.String())
}

// CountRunning returns the number of currently running tasks.
// It inspects all tasks in the pipeline without conversation filtering.
// It returns a non-negative running task count.
func (tp *TaskPipeline) CountRunning() int {
	tp.mu.RLock()
	defer tp.mu.RUnlock()

	count := 0
	for _, task := range tp.tasks {
		if task.Status == TaskStatusRunning {
			count++
		}
	}

	return count
}

// ListActiveConversationTasks returns queued and running tasks in a conversation.
// The channel/chatID/senderID filters scope task visibility for requester context.
// It returns tasks ordered by created time ascending.
func (tp *TaskPipeline) ListActiveConversationTasks(channel, chatID, senderID string) []*PipelineTask {
	tp.mu.RLock()
	defer tp.mu.RUnlock()

	items := make([]*PipelineTask, 0)
	for _, task := range tp.tasks {
		if task.Channel != channel || task.ChatID != chatID {
			continue
		}
		if senderID != "" && task.SenderID != "" && task.SenderID != senderID {
			continue
		}
		if task.Status != TaskStatusQueued && task.Status != TaskStatusRunning {
			continue
		}
		items = append(items, clonePipelineTask(task))
	}

	sort.Slice(items, func(i, j int) bool { return items[i].CreatedAtUTC < items[j].CreatedAtUTC })
	return items
}

// PromoteRunnableQueuedTasks dispatches queued tasks that are now runnable.
// The maxCount parameter caps how many tasks are promoted in this call.
// It returns promoted task snapshots in created order.
func (tp *TaskPipeline) PromoteRunnableQueuedTasks(maxCount int) []*PipelineTask {
	if maxCount <= 0 {
		return nil
	}

	tp.mu.Lock()
	nowUTC := time.Now().UTC().UnixMilli()
	candidates := make([]*PipelineTask, 0)
	for _, task := range tp.tasks {
		if task.Status != TaskStatusQueued || task.DispatchQueued {
			continue
		}
		candidates = append(candidates, task)
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].CreatedAtUTC < candidates[j].CreatedAtUTC })

	promoted := make([]*PipelineTask, 0, maxCount)
	for _, task := range candidates {
		if len(promoted) >= maxCount {
			break
		}
		if !tp.isTaskRunnableLocked(task) {
			continue
		}
		task.DispatchQueued = true
		task.UpdatedAtUTC = nowUTC
		promoted = append(promoted, clonePipelineTask(task))
	}

	if len(promoted) > 0 {
		if err := tp.saveLocked(); err != nil {
			logger.WarnCF("agent", "Failed to persist promoted tasks", map[string]any{"error": err.Error()})
		}
	}
	tp.mu.Unlock()

	if len(promoted) == 0 {
		return nil
	}

	for _, task := range promoted {
		select {
		case tp.queue <- task.ID:
		default:
			logger.WarnCF("agent", "Failed to dispatch promoted task because queue is full", map[string]any{"task_id": task.ID})
			tp.mu.Lock()
			stored, ok := tp.tasks[task.ID]
			if ok && stored.Status == TaskStatusQueued {
				stored.DispatchQueued = false
				stored.UpdatedAtUTC = time.Now().UTC().UnixMilli()
				if err := tp.saveLocked(); err != nil {
					logger.WarnCF("agent", "Failed to rollback promoted task state", map[string]any{"task_id": task.ID, "error": err.Error()})
				}
			}
			tp.mu.Unlock()
		}
	}

	return promoted
}

// ForceParallelDispatch marks a queued task as parallel and schedules it immediately.
// The taskID identifies the queued task that should be promoted to dispatch queue.
// It returns the updated task snapshot and whether a change happened.
func (tp *TaskPipeline) ForceParallelDispatch(taskID string) (*PipelineTask, bool, error) {
	tp.mu.Lock()
	task, ok := tp.tasks[taskID]
	if !ok {
		tp.mu.Unlock()
		return nil, false, fmt.Errorf("task %s not found", taskID)
	}
	if task.Status != TaskStatusQueued {
		out := clonePipelineTask(task)
		tp.mu.Unlock()
		return out, false, nil
	}

	shouldDispatch := !task.DispatchQueued
	task.ExecutionMode = TaskExecutionModeParallel
	task.WaitForTaskID = nil
	task.DispatchQueued = true
	task.UpdatedAtUTC = time.Now().UTC().UnixMilli()
	task.CheckpointAtUTC = task.UpdatedAtUTC
	task.CheckpointNote = "forced parallel dispatch"
	if err := tp.saveLocked(); err != nil {
		tp.mu.Unlock()
		return nil, false, err
	}
	out := clonePipelineTask(task)
	tp.mu.Unlock()

	if shouldDispatch {
		select {
		case tp.queue <- taskID:
		default:
			tp.mu.Lock()
			stored, ok := tp.tasks[taskID]
			if ok && stored.Status == TaskStatusQueued {
				stored.DispatchQueued = false
				stored.UpdatedAtUTC = time.Now().UTC().UnixMilli()
				_ = tp.saveLocked()
			}
			tp.mu.Unlock()
			return nil, false, fmt.Errorf("task queue is full")
		}
	}

	return out, shouldDispatch, nil
}

// BuildConversationStatusSummary returns queued/running status lines for scheduler prompts.
// The channel/chatID/senderID scopes tasks to current requester conversation.
// It returns a compact multi-line summary suitable for LLM scheduling decisions.
func (tp *TaskPipeline) BuildConversationStatusSummary(channel, chatID, senderID string) string {
	tasks := tp.ListActiveConversationTasks(channel, chatID, senderID)
	if len(tasks) == 0 {
		return "(none)"
	}

	var sb strings.Builder
	for _, task := range tasks {
		fmt.Fprintf(&sb, "- %s: %s", taskLabel(task), task.Status)
		if task.Status == TaskStatusQueued {
			if task.DispatchQueued {
				sb.WriteString(" (ready)")
			} else {
				sb.WriteString(" (waiting)")
			}
		}
		if task.ExecutionMode != "" {
			fmt.Fprintf(&sb, " (mode=%s)", task.ExecutionMode)
		}
		if task.PlannerAgent != "" {
			fmt.Fprintf(&sb, " (planner=%s)", task.PlannerAgent)
		}
		if len(task.WaitForTaskID) > 0 {
			fmt.Fprintf(&sb, " (depends_on=%s)", strings.Join(task.WaitForTaskID, ","))
		}
		sb.WriteString("\n")
	}

	return strings.TrimSpace(sb.String())
}

// normalizeExecutionMode normalizes scheduling mode to supported values.
// The mode parameter is caller-provided free text.
// It returns either TaskExecutionModeParallel or TaskExecutionModeWait.
func normalizeExecutionMode(mode string) string {
	switch strings.TrimSpace(strings.ToLower(mode)) {
	case TaskExecutionModeWait:
		return TaskExecutionModeWait
	default:
		return TaskExecutionModeParallel
	}
}

// normalizeTaskSummary normalizes one-line task summary text.
// The summary parameter may contain extra spaces or line breaks.
// It returns a compact single-line summary with length cap.
func normalizeTaskSummary(summary string) string {
	normalized := strings.Join(strings.Fields(strings.TrimSpace(summary)), " ")
	if normalized == "" {
		return ""
	}
	if utf8.RuneCountInString(normalized) > taskBriefMaxRunes {
		return truncateRunes(normalized, taskBriefMaxRunes)
	}
	return normalized
}

// buildTaskID returns a stable task ID composed of UTC date and sequence.
// The nowUTC parameter provides the UTC date and sequence is a per-day increment.
// It returns an identifier in format task-YYYY-MM-DD-NNNN.
func buildTaskID(nowUTC time.Time, sequence int) string {
	if sequence <= 0 {
		sequence = 1
	}
	return fmt.Sprintf("task-%s-%04d", nowUTC.UTC().Format(time.DateOnly), sequence)
}

// nextSequenceForDateLocked returns the next sequence number for the provided UTC date.
// The dateKey parameter must use RFC3339 full-date format YYYY-MM-DD.
// It returns a positive sequence that resets when the date changes.
func (tp *TaskPipeline) nextSequenceForDateLocked(dateKey string) int {
	if tp.lastSeqDate != dateKey {
		tp.lastSeqDate = dateKey
		tp.nextSequence = 1
	}
	if tp.nextSequence <= 0 {
		tp.nextSequence = 1
	}
	seq := tp.nextSequence
	tp.nextSequence++
	return seq
}

// uniqueTaskIDs removes duplicates and empty values while preserving order.
// The ids parameter may include repeated task IDs.
// It returns a sanitized unique slice.
func uniqueTaskIDs(ids []string) []string {
	if len(ids) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(ids))
	result := make([]string, 0, len(ids))
	for _, id := range ids {
		tid := strings.TrimSpace(id)
		if tid == "" {
			continue
		}
		if _, ok := seen[tid]; ok {
			continue
		}
		seen[tid] = struct{}{}
		result = append(result, tid)
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

// isTaskRunnableLocked determines whether a queued task can be dispatched now.
// Caller must hold tp.mu lock before invoking this method.
// It returns true when task dependencies are satisfied and runnable.
func (tp *TaskPipeline) isTaskRunnableLocked(task *PipelineTask) bool {
	if task == nil {
		return false
	}
	if len(task.WaitForTaskID) == 0 {
		return true
	}

	for _, depID := range task.WaitForTaskID {
		dep, ok := tp.tasks[depID]
		if !ok {
			continue
		}
		if dep.Status == TaskStatusQueued || dep.Status == TaskStatusRunning {
			return false
		}
	}

	return true
}

// GetTaskByID returns a copy of task by ID.
// The taskID parameter identifies the task.
// It returns the copied task and whether it exists.
func (tp *TaskPipeline) GetTaskByID(taskID string) (*PipelineTask, bool) {
	tp.mu.RLock()
	defer tp.mu.RUnlock()
	task, ok := tp.tasks[taskID]
	if !ok {
		return nil, false
	}
	return clonePipelineTask(task), true
}

// load restores task pipeline snapshot from disk when file exists.
// It uses tp.storagePath and updates in-memory tasks/nextID.
// It returns no value and falls back to empty state on read errors.
func (tp *TaskPipeline) load() {
	data, err := os.ReadFile(tp.storagePath)
	if err != nil {
		if !os.IsNotExist(err) {
			logger.WarnCF("agent", "Failed to read task pipeline file", map[string]any{"error": err.Error()})
		}
		return
	}

	var snapshot taskPipelineSnapshot
	if err = json.Unmarshal(data, &snapshot); err != nil {
		logger.WarnCF("agent", "Failed to parse task pipeline file", map[string]any{"error": err.Error()})
		return
	}

	if snapshot.LastSequenceDate != "" {
		tp.lastSeqDate = snapshot.LastSequenceDate
	}
	if snapshot.NextSequence > 0 {
		tp.nextSequence = snapshot.NextSequence
	} else if snapshot.NextID > 0 {
		tp.nextSequence = snapshot.NextID
	}

	maxSeqByDate := map[string]int{}
	for _, task := range snapshot.Tasks {
		if task == nil || task.ID == "" {
			continue
		}
		tp.tasks[task.ID] = task
		dateKey, seq, ok := parseTaskIDDateAndSequence(task.ID)
		if !ok {
			continue
		}
		if seq > maxSeqByDate[dateKey] {
			maxSeqByDate[dateKey] = seq
		}
	}

	currentDateKey := time.Now().UTC().Format(time.DateOnly)
	if maxCurrentDateSeq := maxSeqByDate[currentDateKey]; maxCurrentDateSeq > 0 {
		if tp.lastSeqDate != currentDateKey {
			tp.lastSeqDate = currentDateKey
		}
		if tp.nextSequence <= maxCurrentDateSeq {
			tp.nextSequence = maxCurrentDateSeq + 1
		}
	}

	if tp.nextSequence <= 0 {
		tp.nextSequence = 1
	}

	logger.DebugCF("agent", "Loaded task pipeline state", map[string]any{
		"tasks":             len(tp.tasks),
		"next_sequence":     tp.nextSequence,
		"last_sequence_date": tp.lastSeqDate,
		"storage_path":      tp.storagePath,
	})
}

// parseTaskIDDateAndSequence extracts date and sequence from task IDs in format task-YYYY-MM-DD-NNNN.
// The taskID parameter is the persisted task identifier string.
// It returns parsed date key, numeric sequence, and whether parsing succeeded.
func parseTaskIDDateAndSequence(taskID string) (string, int, bool) {
	parts := strings.Split(strings.TrimSpace(taskID), "-")
	if len(parts) != 5 {
		return "", 0, false
	}
	if parts[0] != "task" {
		return "", 0, false
	}
	dateKey := fmt.Sprintf("%s-%s-%s", parts[1], parts[2], parts[3])
	if _, err := time.Parse(time.DateOnly, dateKey); err != nil {
		return "", 0, false
	}
	seq, err := strconv.Atoi(parts[4])
	if err != nil || seq <= 0 {
		return "", 0, false
	}

	return dateKey, seq, true
}

// saveLocked persists current pipeline state atomically.
// Caller must hold tp.mu lock when invoking this method.
// It returns an error if serialization or file operations fail.
func (tp *TaskPipeline) saveLocked() error {
	tasks := make([]*PipelineTask, 0, len(tp.tasks))
	for _, task := range tp.tasks {
		tasks = append(tasks, clonePipelineTask(task))
	}
	sort.Slice(tasks, func(i, j int) bool { return tasks[i].CreatedAtUTC < tasks[j].CreatedAtUTC })

	snapshot := taskPipelineSnapshot{
		NextID:           tp.nextSequence,
		NextSequence:     tp.nextSequence,
		LastSequenceDate: tp.lastSeqDate,
		Tasks:            tasks,
	}

	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return err
	}

	tmpPath := tp.storagePath + ".tmp"
	if err = os.WriteFile(tmpPath, data, 0o644); err != nil {
		return err
	}
	if err = os.Rename(tmpPath, tp.storagePath); err != nil {
		return err
	}

	return nil
}

// clonePipelineTask creates a deep copy for safe external usage.
// The task parameter is copied to avoid races on shared memory.
// It returns an independent PipelineTask instance.
func clonePipelineTask(task *PipelineTask) *PipelineTask {
	if task == nil {
		return nil
	}
	out := *task
	if len(task.WorkerEvents) > 0 {
		out.WorkerEvents = append([]string(nil), task.WorkerEvents...)
	}
	return &out
}

// IsStatusQuery returns whether message text asks for task progress/status.
// The text parameter is checked with URL-aware keyword matching.
// It returns true when the input should be handled as status inquiry.
func IsStatusQuery(text string) bool {
	matched, _ := MatchStatusQuery(text)
	return matched
}

// MatchStatusQuery checks whether input is a task status query and returns matched signal.
// The text parameter is raw user input that may include URLs or punctuation noise.
// It returns whether status intent is detected and the matched keyword/pattern.
func MatchStatusQuery(text string) (bool, string) {
	trimmed := strings.TrimSpace(strings.ToLower(text))
	if trimmed == "" {
		return false, ""
	}

	if strings.HasPrefix(trimmed, "/status") {
		return true, "/status"
	}
	if strings.HasPrefix(trimmed, "/tasks") {
		return true, "/tasks"
	}

	cleaned := strings.TrimSpace(statusQueryURLPattern.ReplaceAllString(trimmed, " "))
	if cleaned == "" {
		return false, ""
	}

	asciiWordKeywords := []string{"status", "progress", "task", "tasks", "update", "running", "finished"}
	for _, keyword := range asciiWordKeywords {
		if hasASCIIWord(cleaned, keyword) {
			return true, keyword
		}
	}

	cjkKeywords := []string{"状态", "进度", "任务", "完成了吗", "还在", "到哪了", "超时", "卡住"}
	for _, keyword := range cjkKeywords {
		if strings.Contains(cleaned, keyword) {
			return true, keyword
		}
	}

	return false, ""
}

// hasASCIIWord reports whether keyword appears as an ASCII word token.
// The text parameter should be pre-normalized to lowercase and keyword must be non-empty lowercase ASCII.
// It returns true only when keyword is delimited by non-alphanumeric characters or boundaries.
func hasASCIIWord(text, keyword string) bool {
	if text == "" || keyword == "" {
		return false
	}

	start := 0
	for {
		idx := strings.Index(text[start:], keyword)
		if idx < 0 {
			return false
		}
		idx += start
		leftOK := idx == 0 || !isASCIIAlnum(rune(text[idx-1]))
		rightPos := idx + len(keyword)
		rightOK := rightPos >= len(text) || !isASCIIAlnum(rune(text[rightPos]))
		if leftOK && rightOK {
			return true
		}
		start = idx + len(keyword)
		if start >= len(text) {
			return false
		}
	}
}

// isASCIIAlnum reports whether r is an ASCII letter or digit.
// The r parameter is a single rune inspected as a token boundary marker.
// It returns true for [0-9A-Za-z].
func isASCIIAlnum(r rune) bool {
	return (r >= '0' && r <= '9') || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z')
}
