package tools

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sipeed/picoclaw/pkg/skills"
)

type fakeInstallRegistry struct {
	name       string
	metaBySlug map[string]*skills.SkillMeta
}

func (r *fakeInstallRegistry) Name() string {
	if r.name == "" {
		return "clawhub"
	}
	return r.name
}

func (r *fakeInstallRegistry) Search(ctx context.Context, query string, limit int) ([]skills.SearchResult, error) {
	return nil, nil
}

func (r *fakeInstallRegistry) GetSkillMeta(ctx context.Context, slug string) (*skills.SkillMeta, error) {
	if meta, ok := r.metaBySlug[slug]; ok {
		return meta, nil
	}
	return &skills.SkillMeta{Slug: slug, DisplayName: slug}, nil
}

func (r *fakeInstallRegistry) DownloadAndInstall(
	ctx context.Context,
	slug, version, targetDir string,
) (*skills.InstallResult, error) {
	return &skills.InstallResult{Version: "1.0.0", Summary: "ok"}, nil
}

func TestInstallSkillToolName(t *testing.T) {
	tool := NewInstallSkillTool(skills.NewRegistryManager(), t.TempDir())
	assert.Equal(t, "install_skill", tool.Name())
}

func TestInstallSkillToolMissingSlug(t *testing.T) {
	tool := NewInstallSkillTool(skills.NewRegistryManager(), t.TempDir())
	result := tool.Execute(context.Background(), map[string]any{})
	assert.True(t, result.IsError)
	assert.Contains(t, result.ForLLM, "identifier is required and must be a non-empty string")
}

func TestInstallSkillToolEmptySlug(t *testing.T) {
	tool := NewInstallSkillTool(skills.NewRegistryManager(), t.TempDir())
	result := tool.Execute(context.Background(), map[string]any{
		"slug": "   ",
	})
	assert.True(t, result.IsError)
	assert.Contains(t, result.ForLLM, "identifier is required and must be a non-empty string")
}

func TestInstallSkillToolUnsafeSlug(t *testing.T) {
	tool := NewInstallSkillTool(skills.NewRegistryManager(), t.TempDir())

	cases := []string{
		"../etc/passwd",
		"path/traversal",
		"path\\traversal",
	}

	for _, slug := range cases {
		result := tool.Execute(context.Background(), map[string]any{
			"slug": slug,
		})
		assert.True(t, result.IsError, "slug %q should be rejected", slug)
		assert.Contains(t, result.ForLLM, "invalid slug")
	}
}

func TestInstallSkillToolAlreadyExists(t *testing.T) {
	workspace := t.TempDir()
	skillDir := filepath.Join(workspace, "skills", "existing-skill")
	require.NoError(t, os.MkdirAll(skillDir, 0o755))

	tool := NewInstallSkillTool(skills.NewRegistryManager(), workspace)
	result := tool.Execute(context.Background(), map[string]any{
		"slug":     "existing-skill",
		"registry": "clawhub",
	})
	assert.True(t, result.IsError)
	assert.Contains(t, result.ForLLM, "already installed")
}

func TestInstallSkillToolRegistryNotFound(t *testing.T) {
	workspace := t.TempDir()
	tool := NewInstallSkillTool(skills.NewRegistryManager(), workspace)
	result := tool.Execute(context.Background(), map[string]any{
		"slug":     "some-skill",
		"registry": "nonexistent",
	})
	assert.True(t, result.IsError)
	assert.Contains(t, result.ForLLM, "registry")
	assert.Contains(t, result.ForLLM, "not found")
}

func TestInstallSkillToolParameters(t *testing.T) {
	tool := NewInstallSkillTool(skills.NewRegistryManager(), t.TempDir())
	params := tool.Parameters()

	props, ok := params["properties"].(map[string]any)
	assert.True(t, ok)
	assert.Contains(t, props, "slug")
	assert.Contains(t, props, "version")
	assert.Contains(t, props, "registry")
	assert.Contains(t, props, "force")

	required, ok := params["required"].([]string)
	assert.True(t, ok)
	assert.Contains(t, required, "slug")
	assert.Contains(t, required, "registry")
}

func TestInstallSkillToolMissingRegistry(t *testing.T) {
	tool := NewInstallSkillTool(skills.NewRegistryManager(), t.TempDir())
	result := tool.Execute(context.Background(), map[string]any{
		"slug": "some-skill",
	})
	assert.True(t, result.IsError)
	assert.Contains(t, result.ForLLM, "invalid registry")
}

func TestInstallSkillToolRejectsEquivalentInstalledSkill(t *testing.T) {
	workspace := t.TempDir()
	existingDir := filepath.Join(workspace, "skills", "agent-browser")
	require.NoError(t, os.MkdirAll(existingDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(existingDir, "SKILL.md"), []byte(`---
name: Agent Browser
description: Browser automation
---
# Agent Browser`), 0o644))

	rm := skills.NewRegistryManager()
	rm.AddRegistry(&fakeInstallRegistry{metaBySlug: map[string]*skills.SkillMeta{
		"agent-browser-2": {
			Slug:         "agent-browser-2",
			DisplayName:  "agent-browser",
			RegistryName: "clawhub",
		},
	}})

	tool := NewInstallSkillTool(rm, workspace)
	result := tool.Execute(context.Background(), map[string]any{
		"slug":     "agent-browser-2",
		"registry": "clawhub",
	})

	assert.True(t, result.IsError)
	assert.Contains(t, result.ForLLM, "already available")
	assert.Contains(t, result.ForLLM, "agent-browser")
}

func TestInstallSkillToolRejectsDuplicateByOriginMeta(t *testing.T) {
	workspace := t.TempDir()
	existingDir := filepath.Join(workspace, "skills", "custom-browser")
	require.NoError(t, os.MkdirAll(existingDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(existingDir, "SKILL.md"), []byte("# Existing"), 0o644))

	origin := originMeta{
		Version:          1,
		Registry:         "clawhub",
		Slug:             "agent-browser",
		InstalledVersion: "1.2.3",
	}
	data, err := json.Marshal(origin)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(existingDir, ".skill-origin.json"), data, 0o644))

	rm := skills.NewRegistryManager()
	rm.AddRegistry(&fakeInstallRegistry{metaBySlug: map[string]*skills.SkillMeta{
		"agent-browser": {
			Slug:         "agent-browser",
			DisplayName:  "agent-browser",
			RegistryName: "clawhub",
		},
	}})

	tool := NewInstallSkillTool(rm, workspace)
	result := tool.Execute(context.Background(), map[string]any{
		"slug":     "agent-browser",
		"registry": "clawhub",
	})

	assert.True(t, result.IsError)
	assert.Contains(t, result.ForLLM, "already available")
	assert.Contains(t, result.ForLLM, "custom-browser")
}
