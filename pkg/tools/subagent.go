package tools

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

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/logger"
	"github.com/sipeed/picoclaw/pkg/providers"
)

const subagentTaskFileName = "subagent_tasks.json"

const (
	SubagentStatusQueued    = "queued"
	SubagentStatusRunning   = "running"
	SubagentStatusCompleted = "completed"
	SubagentStatusFailed    = "failed"
	SubagentStatusCanceled  = "canceled"
)

type SubagentTask struct {
	ID               string `json:"id"`
	Task             string `json:"task"`
	Label            string `json:"label,omitempty"`
	AgentID          string `json:"agent_id,omitempty"`
	TaskReference    string `json:"task_reference,omitempty"`
	OriginChannel    string `json:"origin_channel"`
	OriginChatID     string `json:"origin_chat_id"`
	Status           string `json:"status"`
	Result           string `json:"result,omitempty"`
	CreatedAtUTC     int64  `json:"created_at_utc"`
	UpdatedAtUTC     int64  `json:"updated_at_utc"`
	CheckpointAtUTC  int64  `json:"checkpoint_at_utc,omitempty"`
	CheckpointDetail string `json:"checkpoint_detail,omitempty"`
}

type subagentSnapshot struct {
	NextID int             `json:"next_id"`
	Tasks  []*SubagentTask `json:"tasks"`
}

type SubagentManager struct {
	tasks          map[string]*SubagentTask
	mu             sync.RWMutex
	runtimeCtx     context.Context
	provider       providers.LLMProvider
	defaultModel   string
	bus            *bus.MessageBus
	workspace      string
	tools          *ToolRegistry
	maxIterations  int
	maxTokens      int
	temperature    float64
	hasMaxTokens   bool
	hasTemperature bool
	taskReference  string
	nextID         int
	storagePath    string
}

func NewSubagentManager(
	provider providers.LLMProvider,
	defaultModel, workspace string,
	bus *bus.MessageBus,
) *SubagentManager {
	stateDir := filepath.Join(workspace, "state")
	_ = os.MkdirAll(stateDir, 0o755)

	sm := &SubagentManager{
		tasks:         make(map[string]*SubagentTask),
		runtimeCtx:    context.Background(),
		provider:      provider,
		defaultModel:  defaultModel,
		bus:           bus,
		workspace:     workspace,
		tools:         NewToolRegistry(),
		maxIterations: 10,
		nextID:        1,
		storagePath:   filepath.Join(stateDir, subagentTaskFileName),
	}
	sm.loadLocked()
	return sm
}

// SetRuntimeContext sets the long-lived lifecycle context used by async subagent workers.
// The runtimeCtx parameter should outlive individual tool-call contexts and usually maps to
// process runtime context in gateway mode.
// It returns no value.
func (sm *SubagentManager) SetRuntimeContext(runtimeCtx context.Context) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if runtimeCtx == nil {
		runtimeCtx = context.Background()
	}
	sm.runtimeCtx = runtimeCtx
}

// getExecutionContext resolves the context used for background subagent execution.
// The callCtx parameter is the per-tool-invocation context and may be short-lived.
// It returns the manager runtime context when configured, otherwise a safe background context.
func (sm *SubagentManager) getExecutionContext(callCtx context.Context) context.Context {
	sm.mu.RLock()
	runtimeCtx := sm.runtimeCtx
	sm.mu.RUnlock()
	if runtimeCtx != nil {
		return runtimeCtx
	}
	if callCtx != nil {
		return callCtx
	}
	return context.Background()
}

// SetLLMOptions sets max tokens and temperature for subagent LLM calls.
func (sm *SubagentManager) SetLLMOptions(maxTokens int, temperature float64) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.maxTokens = maxTokens
	sm.hasMaxTokens = true
	sm.temperature = temperature
	sm.hasTemperature = true
}

// SetTools sets the tool registry for subagent execution.
// If not set, subagent will have access to the provided tools.
func (sm *SubagentManager) SetTools(tools *ToolRegistry) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.tools = tools
}

// RegisterTool registers a tool for subagent execution.
func (sm *SubagentManager) RegisterTool(tool Tool) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.tools.Register(tool)
}

// SetTaskReference sets delegation context used by synchronous subagent execution.
// The taskReference parameter should contain memory and recent interaction history.
// It returns no value.
func (sm *SubagentManager) SetTaskReference(taskReference string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.taskReference = taskReference
}

func (sm *SubagentManager) Spawn(
	ctx context.Context,
	task, label, agentID, taskReference, originChannel, originChatID string,
	callback AsyncCallback,
) (string, error) {
	sm.mu.Lock()

	taskID := fmt.Sprintf("subagent-%d", sm.nextID)
	sm.nextID++
	nowUTC := time.Now().UTC().UnixMilli()

	subagentTask := &SubagentTask{
		ID:               taskID,
		Task:             task,
		Label:            label,
		AgentID:          agentID,
		TaskReference:    taskReference,
		OriginChannel:    originChannel,
		OriginChatID:     originChatID,
		Status:           SubagentStatusQueued,
		CreatedAtUTC:     nowUTC,
		UpdatedAtUTC:     nowUTC,
		CheckpointAtUTC:  nowUTC,
		CheckpointDetail: "spawned",
	}
	sm.tasks[taskID] = subagentTask
	if err := sm.saveLocked(); err != nil {
		sm.mu.Unlock()
		return "", err
	}
	sm.mu.Unlock()

	execCtx := sm.getExecutionContext(ctx)

	logger.DebugCF("subagent", "Scheduling subagent background task", map[string]any{
		"task_id":       taskID,
		"label":         label,
		"agent_id":      agentID,
		"origin_channel": originChannel,
		"origin_chat_id": originChatID,
		"task_len":      len(strings.TrimSpace(task)),
		"ctx_err":       fmt.Sprint(ctx.Err()),
	})

	// Start task in background with context cancellation support
	go sm.runTask(execCtx, taskID, callback)

	if label != "" {
		return fmt.Sprintf("Spawned subagent '%s' (task_id=%s) for task: %s", label, taskID, task), nil
	}
	return fmt.Sprintf("Spawned subagent (task_id=%s) for task: %s", taskID, task), nil
}

// ResumeUnfinished resumes unfinished subagent tasks after a process restart.
// The ctx parameter controls resumed background execution and callback receives completion results.
// It returns how many tasks were scheduled for resume.
func (sm *SubagentManager) ResumeUnfinished(ctx context.Context, callback AsyncCallback) int {
	sm.mu.Lock()
	nowUTC := time.Now().UTC().UnixMilli()
	resumableIDs := make([]string, 0)
	for _, task := range sm.tasks {
		if task == nil {
			continue
		}
		if task.Status != SubagentStatusQueued && task.Status != SubagentStatusRunning {
			continue
		}
		task.Status = SubagentStatusQueued
		task.UpdatedAtUTC = nowUTC
		task.CheckpointAtUTC = nowUTC
		task.CheckpointDetail = "recovered after restart"
		resumableIDs = append(resumableIDs, task.ID)
	}
	if len(resumableIDs) > 0 {
		_ = sm.saveLocked()
	}
	sm.mu.Unlock()

	for _, taskID := range resumableIDs {
		go sm.runTask(ctx, taskID, callback)
	}

	return len(resumableIDs)
}

// runTask executes one persisted subagent task and stores periodic checkpoints.
// The ctx parameter controls cancellation and taskID identifies persisted task state.
// It returns no value and updates task status/result in durable storage.
func (sm *SubagentManager) runTask(ctx context.Context, taskID string, callback AsyncCallback) {
	sm.mu.Lock()
	task, ok := sm.tasks[taskID]
	if !ok || task == nil {
		sm.mu.Unlock()
		return
	}
	nowUTC := time.Now().UTC().UnixMilli()
	task.Status = SubagentStatusRunning
	task.UpdatedAtUTC = nowUTC
	task.CheckpointAtUTC = nowUTC
	task.CheckpointDetail = "running"
	if err := sm.saveLocked(); err != nil {
		sm.mu.Unlock()
		return
	}

	taskPrompt := task.Task
	taskReference := task.TaskReference
	taskLabel := task.Label
	originChannel := task.OriginChannel
	originChatID := task.OriginChatID
	sm.mu.Unlock()

	// Build system prompt for subagent
	systemPrompt := `You are a subagent. Complete the given task independently and report the result.
You have access to tools - use them as needed to complete your task.
After completing the task, provide a clear summary of what was done.`

	messages := []providers.Message{
		{
			Role:    "system",
			Content: systemPrompt,
		},
		{
			Role:    "user",
			Content: buildSubagentUserPrompt(taskPrompt, taskReference),
		},
	}

	// Check if context is already canceled before starting
	select {
	case <-ctx.Done():
		logger.DebugCF("subagent", "Subagent task canceled before execution", map[string]any{
			"task_id": taskID,
			"reason":  ctx.Err().Error(),
		})
		sm.mu.Lock()
		task.Status = SubagentStatusCanceled
		task.Result = "Task canceled before execution"
		nowUTC := time.Now().UTC().UnixMilli()
		task.UpdatedAtUTC = nowUTC
		task.CheckpointAtUTC = nowUTC
		task.CheckpointDetail = "canceled before execution"
		_ = sm.saveLocked()
		sm.mu.Unlock()
		return
	default:
	}

	heartbeatDone := make(chan struct{})
	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-heartbeatDone:
				return
			case <-ticker.C:
				sm.checkpointTask(taskID, "running heartbeat")
			}
		}
	}()
	defer close(heartbeatDone)

	// Run tool loop with access to tools
	sm.mu.RLock()
	tools := sm.tools
	maxIter := sm.maxIterations
	maxTokens := sm.maxTokens
	temperature := sm.temperature
	hasMaxTokens := sm.hasMaxTokens
	hasTemperature := sm.hasTemperature
	sm.mu.RUnlock()

	var llmOptions map[string]any
	if hasMaxTokens || hasTemperature {
		llmOptions = map[string]any{}
		if hasMaxTokens {
			llmOptions["max_tokens"] = maxTokens
		}
		if hasTemperature {
			llmOptions["temperature"] = temperature
		}
	}

	loopResult, err := RunToolLoop(ctx, ToolLoopConfig{
		Provider:      sm.provider,
		Model:         sm.defaultModel,
		Tools:         tools,
		MaxIterations: maxIter,
		LLMOptions:    llmOptions,
	}, messages, originChannel, originChatID)

	sm.mu.Lock()
	var result *ToolResult
	task, ok = sm.tasks[taskID]
	if !ok || task == nil {
		sm.mu.Unlock()
		return
	}
	defer func() {
		_ = sm.saveLocked()
		sm.mu.Unlock()
		// Call callback if provided and result is set
		if callback != nil && result != nil {
			callback(ctx, result)
		}
	}()

	if err != nil {
		task.Status = SubagentStatusFailed
		task.Result = fmt.Sprintf("Error: %v", err)
		// Check if it was canceled
		if ctx.Err() != nil {
			logger.DebugCF("subagent", "Subagent task canceled during execution", map[string]any{
				"task_id": taskID,
				"reason":  ctx.Err().Error(),
			})
			task.Status = SubagentStatusCanceled
			task.Result = "Task canceled during execution"
		}
		nowUTC := time.Now().UTC().UnixMilli()
		task.UpdatedAtUTC = nowUTC
		task.CheckpointAtUTC = nowUTC
		task.CheckpointDetail = task.Status
		result = &ToolResult{
			ForLLM:  task.Result,
			ForUser: "",
			Silent:  false,
			IsError: true,
			Async:   false,
			Err:     err,
		}
	} else {
		task.Status = SubagentStatusCompleted
		task.Result = loopResult.Content
		nowUTC := time.Now().UTC().UnixMilli()
		task.UpdatedAtUTC = nowUTC
		task.CheckpointAtUTC = nowUTC
		task.CheckpointDetail = "completed"
		result = &ToolResult{
			ForLLM: fmt.Sprintf(
				"Subagent '%s' completed (iterations: %d): %s",
				taskLabel,
				loopResult.Iterations,
				loopResult.Content,
			),
			ForUser: loopResult.Content,
			Silent:  false,
			IsError: false,
			Async:   false,
		}
	}

	// Send announce message back to main agent
	if sm.bus != nil {
		announceContent := fmt.Sprintf("Subagent task id=%s label='%s' status=%s.\n\nResult:\n%s", task.ID, taskLabel, task.Status, task.Result)
		sm.bus.PublishInbound(bus.InboundMessage{
			Channel:  "system",
			SenderID: fmt.Sprintf("subagent:%s", task.ID),
			// Format: "original_channel:original_chat_id" for routing back
			ChatID:  fmt.Sprintf("%s:%s", task.OriginChannel, task.OriginChatID),
			Content: announceContent,
		})
	}
}

func (sm *SubagentManager) GetTask(taskID string) (*SubagentTask, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	task, ok := sm.tasks[taskID]
	if !ok {
		return nil, false
	}
	return cloneSubagentTask(task), true
}

func (sm *SubagentManager) ListTasks() []*SubagentTask {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	tasks := make([]*SubagentTask, 0, len(sm.tasks))
	for _, task := range sm.tasks {
		tasks = append(tasks, cloneSubagentTask(task))
	}
	return tasks
}

// checkpointTask records a heartbeat checkpoint for one subagent task.
// The taskID parameter identifies the task and detail describes current phase.
// It returns true when checkpoint is persisted.
func (sm *SubagentManager) checkpointTask(taskID, detail string) bool {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	task, ok := sm.tasks[taskID]
	if !ok || task == nil {
		return false
	}
	nowUTC := time.Now().UTC().UnixMilli()
	task.UpdatedAtUTC = nowUTC
	task.CheckpointAtUTC = nowUTC
	task.CheckpointDetail = detail
	if err := sm.saveLocked(); err != nil {
		return false
	}
	return true
}

// loadLocked restores subagent tasks from disk into memory.
// Caller must hold sm.mu lock before invoking this method.
// It returns no value and keeps empty state on load failure.
func (sm *SubagentManager) loadLocked() {
	data, err := os.ReadFile(sm.storagePath)
	if err != nil {
		return
	}

	var snapshot subagentSnapshot
	if err = json.Unmarshal(data, &snapshot); err != nil {
		return
	}

	if snapshot.NextID > 0 {
		sm.nextID = snapshot.NextID
	}
	for _, task := range snapshot.Tasks {
		if task == nil || task.ID == "" {
			continue
		}
		sm.tasks[task.ID] = task
	}
}

// saveLocked persists subagent state atomically to disk.
// Caller must hold sm.mu lock before invoking this method.
// It returns an error when serialization or file writing fails.
func (sm *SubagentManager) saveLocked() error {
	tasks := make([]*SubagentTask, 0, len(sm.tasks))
	for _, task := range sm.tasks {
		tasks = append(tasks, cloneSubagentTask(task))
	}
	sort.Slice(tasks, func(i, j int) bool { return tasks[i].CreatedAtUTC < tasks[j].CreatedAtUTC })

	snapshot := subagentSnapshot{
		NextID: sm.nextID,
		Tasks:  tasks,
	}

	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return err
	}

	tmpPath := sm.storagePath + ".tmp"
	if err = os.WriteFile(tmpPath, data, 0o644); err != nil {
		return err
	}
	if err = os.Rename(tmpPath, sm.storagePath); err != nil {
		return err
	}

	return nil
}

// cloneSubagentTask creates a safe copy of a subagent task.
// The task parameter is copied to avoid sharing mutable references.
// It returns an independent task snapshot.
func cloneSubagentTask(task *SubagentTask) *SubagentTask {
	if task == nil {
		return nil
	}
	out := *task
	return &out
}

// SubagentTool executes a subagent task synchronously and returns the result.
// Unlike SpawnTool which runs tasks asynchronously, SubagentTool waits for completion
// and returns the result directly in the ToolResult.
type SubagentTool struct {
	manager       *SubagentManager
	originChannel string
	originChatID  string
}

func NewSubagentTool(manager *SubagentManager) *SubagentTool {
	return &SubagentTool{
		manager:       manager,
		originChannel: "cli",
		originChatID:  "direct",
	}
}

func (t *SubagentTool) Name() string {
	return "subagent"
}

func (t *SubagentTool) Description() string {
	return "Execute a subagent task synchronously and return the result. Use this for delegating specific tasks to an independent agent instance. Returns execution summary to user and full details to LLM."
}

func (t *SubagentTool) Parameters() map[string]any {
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
		},
		"required": []string{"task"},
	}
}

func (t *SubagentTool) SetContext(channel, chatID string) {
	t.originChannel = channel
	t.originChatID = chatID
}

// SetTaskReference updates delegation context for synchronous subagent runs.
// The taskReference parameter includes memory and recent interaction history.
// It returns no value.
func (t *SubagentTool) SetTaskReference(taskReference string) {
	if t.manager == nil {
		return
	}
	t.manager.SetTaskReference(taskReference)
}

func (t *SubagentTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	task, ok := args["task"].(string)
	if !ok {
		return ErrorResult("task is required").WithError(fmt.Errorf("task parameter is required"))
	}

	label, _ := args["label"].(string)

	if t.manager == nil {
		return ErrorResult("Subagent manager not configured").WithError(fmt.Errorf("manager is nil"))
	}

	// Build messages for subagent
	sm := t.manager
	sm.mu.RLock()
	taskReference := sm.taskReference
	tools := sm.tools
	maxIter := sm.maxIterations
	maxTokens := sm.maxTokens
	temperature := sm.temperature
	hasMaxTokens := sm.hasMaxTokens
	hasTemperature := sm.hasTemperature
	sm.mu.RUnlock()

	messages := []providers.Message{
		{
			Role:    "system",
			Content: "You are a subagent. Complete the given task independently and provide a clear, concise result.",
		},
		{
			Role:    "user",
			Content: buildSubagentUserPrompt(task, taskReference),
		},
	}

	// Use RunToolLoop to execute with tools (same as async SpawnTool)

	var llmOptions map[string]any
	if hasMaxTokens || hasTemperature {
		llmOptions = map[string]any{}
		if hasMaxTokens {
			llmOptions["max_tokens"] = maxTokens
		}
		if hasTemperature {
			llmOptions["temperature"] = temperature
		}
	}

	loopResult, err := RunToolLoop(ctx, ToolLoopConfig{
		Provider:      sm.provider,
		Model:         sm.defaultModel,
		Tools:         tools,
		MaxIterations: maxIter,
		LLMOptions:    llmOptions,
	}, messages, t.originChannel, t.originChatID)
	if err != nil {
		return ErrorResult(fmt.Sprintf("Subagent execution failed: %v", err)).WithError(err)
	}

	// ForUser: Brief summary for user (truncated if too long)
	userContent := loopResult.Content
	maxUserLen := 500
	if len(userContent) > maxUserLen {
		userContent = userContent[:maxUserLen] + "..."
	}

	// ForLLM: Full execution details
	labelStr := label
	if labelStr == "" {
		labelStr = "(unnamed)"
	}
	llmContent := fmt.Sprintf("Subagent task completed:\nLabel: %s\nIterations: %d\nResult: %s",
		labelStr, loopResult.Iterations, loopResult.Content)

	return &ToolResult{
		ForLLM:  llmContent,
		ForUser: userContent,
		Silent:  false,
		IsError: false,
		Async:   false,
	}
}

// buildSubagentUserPrompt builds a worker task message with optional delegation reference.
// The task parameter is the primary assignment and taskReference contains memory/history context.
// It returns a single user prompt string for worker execution.
func buildSubagentUserPrompt(task, taskReference string) string {
	reference := strings.TrimSpace(taskReference)
	if reference == "" {
		return task
	}

	var sb strings.Builder
	sb.WriteString("Primary task:\n")
	sb.WriteString(task)
	sb.WriteString("\n\n")
	sb.WriteString("Reference context from planner (memory and interaction history):\n")
	sb.WriteString(reference)
	sb.WriteString("\n\nUse the reference as supporting context. Prioritize explicit task requirements if conflicts appear.")
	return sb.String()
}
