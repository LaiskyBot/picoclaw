package agent

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestNewMemoryStore_BootstrapsLongTermMemoryFile verifies memory initialization creates MEMORY.md when missing.
// It creates a temporary workspace, initializes memory store, and checks bootstrap file existence and content.
// It returns no value and fails test on mismatches.
func TestNewMemoryStore_BootstrapsLongTermMemoryFile(t *testing.T) {
	workspace := t.TempDir()

	store := NewMemoryStore(workspace)
	require.NotNil(t, store)

	memoryPath := filepath.Join(workspace, "memory", "MEMORY.md")
	data, err := os.ReadFile(memoryPath)
	require.NoError(t, err)
	require.Contains(t, string(data), "# Memory")
}
