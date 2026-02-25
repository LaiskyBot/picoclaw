package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

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

// PipelineTask stores the end-to-end state of a delegated background task.
// It includes user origin metadata, planner execution status, and worker updates.
type PipelineTask struct {
	ID            string   `json:"id"`
	Channel       string   `json:"channel"`
	ChatID        string   `json:"chat_id"`
	SenderID      string   `json:"sender_id"`
	Request       string   `json:"request"`
	PlannerAgent  string   `json:"planner_agent"`
	Status        string   `json:"status"`
	CreatedAtUTC  int64    `json:"created_at_utc"`
	UpdatedAtUTC  int64    `json:"updated_at_utc"`
	StartedAtUTC  int64    `json:"started_at_utc,omitempty"`
	FinishedAtUTC int64    `json:"finished_at_utc,omitempty"`
	PlannerResult string   `json:"planner_result,omitempty"`
	Error         string   `json:"error,omitempty"`
	WorkerEvents  []string `json:"worker_events,omitempty"`
}

// taskPipelineSnapshot is the on-disk format of TaskPipeline.
// It captures deterministic task ID generation and task states.
type taskPipelineSnapshot struct {
	NextID int             `json:"next_id"`
	Tasks  []*PipelineTask `json:"tasks"`
}

// TaskPipeline manages asynchronous background task lifecycle.
// It persists tasks to disk, supports queueing, status queries, timeout checks,
// and orphan detection after process restarts.
type TaskPipeline struct {
	mu          sync.RWMutex
	tasks       map[string]*PipelineTask
	nextID      int
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
		nextID:      1,
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
func (tp *TaskPipeline) EnqueueTask(channel, chatID, senderID, request string) (*PipelineTask, error) {
	nowUTC := time.Now().UTC().UnixMilli()

	tp.mu.Lock()
	taskID := fmt.Sprintf("task-%d", tp.nextID)
	tp.nextID++
	task := &PipelineTask{
		ID:           taskID,
		Channel:      channel,
		ChatID:       chatID,
		SenderID:     senderID,
		Request:      request,
		Status:       TaskStatusQueued,
		CreatedAtUTC: nowUTC,
		UpdatedAtUTC: nowUTC,
	}
	tp.tasks[taskID] = task
	saveErr := tp.saveLocked()
	tp.mu.Unlock()
	if saveErr != nil {
		return nil, saveErr
	}

	select {
	case tp.queue <- taskID:
	default:
		return nil, fmt.Errorf("task queue is full")
	}

	return clonePipelineTask(task), nil
}

// Dequeue blocks until a queued task is available or context is canceled.
// The ctx parameter controls waiting lifecycle.
// It returns task details and a boolean indicating whether dequeue succeeded.
func (tp *TaskPipeline) Dequeue(ctx context.Context) (*PipelineTask, bool) {
	select {
	case taskID := <-tp.queue:
		tp.mu.RLock()
		task, ok := tp.tasks[taskID]
		tp.mu.RUnlock()
		if !ok {
			return nil, false
		}
		return clonePipelineTask(task), true
	case <-ctx.Done():
		return nil, false
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
	task.PlannerAgent = plannerAgent
	task.StartedAtUTC = nowUTC
	task.UpdatedAtUTC = nowUTC

	if err := tp.saveLocked(); err != nil {
		logger.WarnCF("agent", "Failed to persist running task", map[string]any{"task_id": taskID, "error": err.Error()})
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

	if err := tp.saveLocked(); err != nil {
		logger.WarnCF("agent", "Failed to persist worker event", map[string]any{"task_id": taskID, "error": err.Error()})
	}
	return true
}

// MarkOrphanedOnRestart marks queued/running tasks as orphaned after restart.
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
		fmt.Fprintf(&sb, "- %s: %s", task.ID, task.Status)
		if task.PlannerAgent != "" {
			fmt.Fprintf(&sb, " (planner=%s)", task.PlannerAgent)
		}
		if len(task.WorkerEvents) > 0 {
			fmt.Fprintf(&sb, " [worker_updates=%d]", len(task.WorkerEvents))
		}
		if task.Error != "" {
			fmt.Fprintf(&sb, " error=%s", task.Error)
		}
		sb.WriteString("\n")
	}

	return strings.TrimSpace(sb.String())
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

	if snapshot.NextID > 0 {
		tp.nextID = snapshot.NextID
	}
	for _, task := range snapshot.Tasks {
		if task == nil || task.ID == "" {
			continue
		}
		tp.tasks[task.ID] = task
	}
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
		NextID: tp.nextID,
		Tasks:  tasks,
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
// The text parameter is checked with lightweight keyword matching.
// It returns true when the input should be handled as status inquiry.
func IsStatusQuery(text string) bool {
	trimmed := strings.TrimSpace(strings.ToLower(text))
	if trimmed == "" {
		return false
	}

	statusKeywords := []string{
		"/status", "/tasks", "status", "progress", "task", "update", "running", "finished",
		"状态", "进度", "任务", "完成了吗", "还在", "到哪了", "超时", "卡住",
	}
	for _, keyword := range statusKeywords {
		if strings.Contains(trimmed, keyword) {
			return true
		}
	}
	return false
}