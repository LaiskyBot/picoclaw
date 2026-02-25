package agent

import (
	"context"
	"os"
	"strings"
	"testing"

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
	task, err := tp.EnqueueTask("telegram", "chat-1", "user-1", "build project")
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

// TestTaskPipeline_MarkOrphanedOnRestart verifies running tasks become orphaned after restart.
// It simulates process restart by creating a new pipeline instance from persisted state.
// It returns no value and fails test on mismatches.
func TestTaskPipeline_MarkOrphanedOnRestart(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "task-pipeline-orphan-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	tp := NewTaskPipeline(tmpDir, 0)
	task, err := tp.EnqueueTask("telegram", "chat-2", "user-2", "long running")
	require.NoError(t, err)
	require.True(t, tp.MarkRunning(task.ID, "planner"))

	reloaded := NewTaskPipeline(tmpDir, 0)
	orphans := reloaded.MarkOrphanedOnRestart()
	require.Len(t, orphans, 1)
	require.Equal(t, task.ID, orphans[0].ID)
	require.Equal(t, TaskStatusOrphaned, orphans[0].Status)

	restored, ok := reloaded.GetTaskByID(task.ID)
	require.True(t, ok)
	require.Equal(t, TaskStatusOrphaned, restored.Status)
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
	require.True(t, strings.Contains(response, "Task task-1 accepted"))

	statusResponse, err := al.processMessage(context.Background(), bus.InboundMessage{
		Channel:  "telegram",
		ChatID:   "chat-42",
		SenderID: "u42",
		Content:  "/status",
	})
	require.NoError(t, err)
	require.Contains(t, statusResponse, "Task status:")
	require.Contains(t, statusResponse, "task-1")
}
