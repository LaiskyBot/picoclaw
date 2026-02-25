package tools

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestSubagentManager_PersistsTasksAcrossRestart verifies spawned tasks are durably stored.
// It creates a task, waits for completion, then reloads manager from disk.
// It returns no value and fails when persisted task cannot be restored.
func TestSubagentManager_PersistsTasksAcrossRestart(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "subagent-persist-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	provider := &MockLLMProvider{}
	manager := NewSubagentManager(provider, "test-model", tmpDir, nil)

	_, err = manager.Spawn(context.Background(), "collect diagnostics", "diag", "", "", "cli", "direct", nil)
	require.NoError(t, err)

	task := waitForAnySubagentTask(t, manager, 2*time.Second)
	require.NotNil(t, task)

	reloaded := NewSubagentManager(provider, "test-model", tmpDir, nil)
	restored, ok := reloaded.GetTask(task.ID)
	require.True(t, ok)
	require.Equal(t, task.ID, restored.ID)
	require.NotZero(t, restored.CheckpointAtUTC)
	require.NotEmpty(t, restored.Status)
}

// TestSubagentManager_ResumeUnfinished verifies running tasks are resumed after restart.
// It writes a persisted running task snapshot, reloads manager, and resumes unfinished work.
// It returns no value and fails when resumed task does not complete.
func TestSubagentManager_ResumeUnfinished(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "subagent-resume-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	provider := &MockLLMProvider{}
	seed := NewSubagentManager(provider, "test-model", tmpDir, nil)
	seed.mu.Lock()
	seed.tasks["subagent-9"] = &SubagentTask{
		ID:               "subagent-9",
		Task:             "resume this task",
		Label:            "resume",
		OriginChannel:    "cli",
		OriginChatID:     "direct",
		Status:           SubagentStatusRunning,
		CreatedAtUTC:     time.Now().UTC().UnixMilli(),
		UpdatedAtUTC:     time.Now().UTC().UnixMilli(),
		CheckpointAtUTC:  time.Now().UTC().UnixMilli(),
		CheckpointDetail: "running",
	}
	seed.nextID = 10
	require.NoError(t, seed.saveLocked())
	seed.mu.Unlock()

	reloaded := NewSubagentManager(provider, "test-model", tmpDir, nil)
	resumed := reloaded.ResumeUnfinished(context.Background(), nil)
	require.Equal(t, 1, resumed)

	waitForSubagentStatus(t, reloaded, "subagent-9", SubagentStatusCompleted, 2*time.Second)
}

// TestSubagentManager_Spawn_DetachesFromCallContext verifies async workers do not inherit
// short-lived per-call cancellation contexts.
// It spawns with an already-canceled call context and expects task completion.
// It returns no value and fails if the task is canceled due to call context.
func TestSubagentManager_Spawn_DetachesFromCallContext(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "subagent-detach-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	provider := &MockLLMProvider{}
	manager := NewSubagentManager(provider, "test-model", tmpDir, nil)

	callCtx, cancel := context.WithCancel(context.Background())
	cancel()

	taskID, err := manager.Spawn(callCtx, "finish despite canceled caller", "detach", "", "", "cli", "direct", nil)
	require.NoError(t, err)
	require.NotEmpty(t, taskID)

	task := waitForAnySubagentTask(t, manager, 2*time.Second)
	require.NotNil(t, task)
	require.Equal(t, SubagentStatusCompleted, task.Status)
	require.Contains(t, task.Result, "Task completed")
}

// TestSubagentManager_Spawn_UsesRuntimeContextCancellation verifies manager runtime context
// remains the authoritative lifecycle control for async workers.
// It cancels runtime context before spawn and expects task cancellation.
// It returns no value and fails if task still runs.
func TestSubagentManager_Spawn_UsesRuntimeContextCancellation(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "subagent-runtime-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	provider := &MockLLMProvider{}
	manager := NewSubagentManager(provider, "test-model", tmpDir, nil)

	runtimeCtx, runtimeCancel := context.WithCancel(context.Background())
	manager.SetRuntimeContext(runtimeCtx)
	runtimeCancel()

	taskID, err := manager.Spawn(context.Background(), "should cancel by runtime context", "runtime-cancel", "", "", "cli", "direct", nil)
	require.NoError(t, err)
	require.NotEmpty(t, taskID)

	task := waitForAnySubagentTask(t, manager, 2*time.Second)
	require.NotNil(t, task)
	require.Equal(t, SubagentStatusCanceled, task.Status)
	require.Contains(t, task.Result, "Task canceled")
}

// waitForAnySubagentTask waits until at least one task exists and is terminal.
// The manager parameter is polled until timeout expires.
// It returns the first observed task snapshot.
func waitForAnySubagentTask(t *testing.T, manager *SubagentManager, timeout time.Duration) *SubagentTask {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		tasks := manager.ListTasks()
		if len(tasks) > 0 {
			for _, task := range tasks {
				if task.Status == SubagentStatusCompleted || task.Status == SubagentStatusFailed || task.Status == SubagentStatusCanceled {
					return task
				}
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for terminal subagent task")
	return nil
}

// waitForSubagentStatus waits until one task reaches the expected status.
// The manager parameter is polled until timeout and expected status must match exactly.
// It returns no value and fails the test on timeout.
func waitForSubagentStatus(t *testing.T, manager *SubagentManager, taskID, expectedStatus string, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		task, ok := manager.GetTask(taskID)
		if ok && task != nil && task.Status == expectedStatus {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for subagent task %s status %s", taskID, expectedStatus)
}
