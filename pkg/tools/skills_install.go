package tools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/sipeed/picoclaw/pkg/logger"
	"github.com/sipeed/picoclaw/pkg/skills"
	"github.com/sipeed/picoclaw/pkg/utils"
)

// InstallSkillTool allows the LLM agent to install skills from registries.
// It shares the same RegistryManager that FindSkillsTool uses,
// so all registries configured in config are available for installation.
type InstallSkillTool struct {
	registryMgr *skills.RegistryManager
	workspace   string
	mu          sync.Mutex
}

// NewInstallSkillTool creates a new InstallSkillTool.
// registryMgr is the shared registry manager (same instance as FindSkillsTool).
// workspace is the root workspace directory; skills install to {workspace}/skills/{slug}/.
func NewInstallSkillTool(registryMgr *skills.RegistryManager, workspace string) *InstallSkillTool {
	return &InstallSkillTool{
		registryMgr: registryMgr,
		workspace:   workspace,
		mu:          sync.Mutex{},
	}
}

func (t *InstallSkillTool) Name() string {
	return "install_skill"
}

func (t *InstallSkillTool) Description() string {
	return "Install a skill from a registry by slug. Downloads and extracts the skill into the workspace. Use find_skills first to discover available skills."
}

func (t *InstallSkillTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"slug": map[string]any{
				"type":        "string",
				"description": "The unique slug of the skill to install (e.g., 'github', 'docker-compose')",
			},
			"version": map[string]any{
				"type":        "string",
				"description": "Specific version to install (optional, defaults to latest)",
			},
			"registry": map[string]any{
				"type":        "string",
				"description": "Registry to install from (required, e.g., 'clawhub')",
			},
			"force": map[string]any{
				"type":        "boolean",
				"description": "Force reinstall if skill already exists (default false)",
			},
		},
		"required": []string{"slug", "registry"},
	}
}

func (t *InstallSkillTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	// Install lock to prevent concurrent directory operations.
	// Ideally this should be done at a `slug` level, currently, its at a `workspace` level.
	t.mu.Lock()
	defer t.mu.Unlock()

	// Validate slug
	slug, _ := args["slug"].(string)
	if err := utils.ValidateSkillIdentifier(slug); err != nil {
		return ErrorResult(fmt.Sprintf("invalid slug %q: error: %s", slug, err.Error()))
	}

	// Validate registry
	registryName, _ := args["registry"].(string)
	if err := utils.ValidateSkillIdentifier(registryName); err != nil {
		return ErrorResult(fmt.Sprintf("invalid registry %q: error: %s", registryName, err.Error()))
	}

	version, _ := args["version"].(string)
	force, _ := args["force"].(bool)

	// Check if already installed.
	skillsDir := filepath.Join(t.workspace, "skills")
	targetDir := filepath.Join(skillsDir, slug)

	if !force {
		if _, err := os.Stat(targetDir); err == nil {
			return ErrorResult(
				fmt.Sprintf("skill %q already installed at %s. Use force=true to reinstall.", slug, targetDir),
			)
		}
	} else {
		// Force: remove existing if present.
		os.RemoveAll(targetDir)
	}

	// Resolve which registry to use.
	registry := t.registryMgr.GetRegistry(registryName)
	if registry == nil {
		return ErrorResult(fmt.Sprintf("registry %q not found", registryName))
	}

	if !force {
		if existingPath, existingName, found := t.findEquivalentInstalledSkill(ctx, registry, skillsDir, slug); found {
			logger.DebugCF("tool", "Equivalent skill already installed",
				map[string]any{
					"tool":            "install_skill",
					"requested_slug":  slug,
					"registry":        registry.Name(),
					"existing_name":   existingName,
					"existing_path":   existingPath,
					"requested_target": targetDir,
				})
			return ErrorResult(
				fmt.Sprintf("skill %q is already available as %q at %s. Reuse the installed skill or set force=true to install anyway.",
					slug, existingName, existingPath),
			)
		}
	}

	// Ensure skills directory exists.
	if err := os.MkdirAll(skillsDir, 0o755); err != nil {
		return ErrorResult(fmt.Sprintf("failed to create skills directory: %v", err))
	}

	// Download and install (handles metadata, version resolution, extraction).
	result, err := registry.DownloadAndInstall(ctx, slug, version, targetDir)
	if err != nil {
		// Clean up partial install.
		rmErr := os.RemoveAll(targetDir)
		if rmErr != nil {
			logger.ErrorCF("tool", "Failed to remove partial install",
				map[string]any{
					"tool":       "install_skill",
					"target_dir": targetDir,
					"error":      rmErr.Error(),
				})
		}
		return ErrorResult(fmt.Sprintf("failed to install %q: %v", slug, err))
	}

	// Moderation: block malware.
	if result.IsMalwareBlocked {
		rmErr := os.RemoveAll(targetDir)
		if rmErr != nil {
			logger.ErrorCF("tool", "Failed to remove partial install",
				map[string]any{
					"tool":       "install_skill",
					"target_dir": targetDir,
					"error":      rmErr.Error(),
				})
		}
		return ErrorResult(fmt.Sprintf("skill %q is flagged as malicious and cannot be installed", slug))
	}

	// Write origin metadata.
	if err := writeOriginMeta(targetDir, registry.Name(), slug, result.Version); err != nil {
		logger.ErrorCF("tool", "Failed to write origin metadata",
			map[string]any{
				"tool":     "install_skill",
				"error":    err.Error(),
				"target":   targetDir,
				"registry": registry.Name(),
				"slug":     slug,
				"version":  result.Version,
			})
		_ = err
	}

	// Build result with moderation warning if suspicious.
	var output string
	if result.IsSuspicious {
		output = fmt.Sprintf("⚠️ Warning: skill %q is flagged as suspicious (may contain risky patterns).\n\n", slug)
	}
	output += fmt.Sprintf("Successfully installed skill %q v%s from %s registry.\nLocation: %s\n",
		slug, result.Version, registry.Name(), targetDir)

	if result.Summary != "" {
		output += fmt.Sprintf("Description: %s\n", result.Summary)
	}
	output += "\nThe skill is now available and can be loaded in the current session."

	return SilentResult(output)
}

// findEquivalentInstalledSkill checks whether a semantically equivalent skill is already installed.
// It returns the existing SKILL.md path and canonical name when a match is found.
func (t *InstallSkillTool) findEquivalentInstalledSkill(
	ctx context.Context,
	registry skills.SkillRegistry,
	skillsDir, requestedSlug string,
) (string, string, bool) {
	if skillsDir == "" {
		return "", "", false
	}

	if path, name, found := findInstalledByOriginMeta(skillsDir, registry.Name(), requestedSlug); found {
		return path, name, true
	}

	matchNames := map[string]struct{}{
		normalizeSkillMatchName(requestedSlug): {},
	}

	meta, err := registry.GetSkillMeta(ctx, requestedSlug)
	if err != nil {
		logger.DebugCF("tool", "Failed to resolve skill metadata for dedupe",
			map[string]any{
				"tool":     "install_skill",
				"slug":     requestedSlug,
				"registry": registry.Name(),
				"error":    err.Error(),
			})
	} else {
		if n := normalizeSkillMatchName(meta.Slug); n != "" {
			matchNames[n] = struct{}{}
		}
		if n := normalizeSkillMatchName(meta.DisplayName); n != "" {
			matchNames[n] = struct{}{}
		}
	}

	loader := skills.NewSkillsLoader(t.workspace, "", "")
	for _, installed := range loader.ListSkills() {
		installedDir := filepath.Base(filepath.Dir(installed.Path))
		if strings.EqualFold(installedDir, requestedSlug) {
			continue
		}

		if _, ok := matchNames[normalizeSkillMatchName(installed.Name)]; ok {
			return installed.Path, installed.Name, true
		}
		if installed.DisplayName != "" {
			if _, ok := matchNames[normalizeSkillMatchName(installed.DisplayName)]; ok {
				return installed.Path, installed.Name, true
			}
		}
	}

	return "", "", false
}

// findInstalledByOriginMeta finds an installed skill by origin metadata.
// It returns the SKILL.md path and canonical name when registry+slug match.
func findInstalledByOriginMeta(skillsDir, registryName, slug string) (string, string, bool) {
	entries, err := os.ReadDir(skillsDir)
	if err != nil {
		if !errorsIsNotExist(err) {
			logger.DebugCF("tool", "Failed reading skills directory for dedupe",
				map[string]any{"tool": "install_skill", "skills_dir": skillsDir, "error": err.Error()})
		}
		return "", "", false
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		dirName := entry.Name()
		metaPath := filepath.Join(skillsDir, dirName, ".skill-origin.json")
		data, err := os.ReadFile(metaPath)
		if err != nil {
			continue
		}
		var meta originMeta
		if err := json.Unmarshal(data, &meta); err != nil {
			continue
		}
		if strings.EqualFold(meta.Registry, registryName) && strings.EqualFold(meta.Slug, slug) {
			skillPath := filepath.Join(skillsDir, dirName, "SKILL.md")
			if _, statErr := os.Stat(skillPath); statErr == nil {
				return skillPath, dirName, true
			}
		}
	}

	return "", "", false
}

// normalizeSkillMatchName normalizes names for fuzzy duplicate matching.
func normalizeSkillMatchName(name string) string {
	name = strings.TrimSpace(strings.ToLower(name))
	name = strings.ReplaceAll(name, "_", "-")
	name = strings.ReplaceAll(name, " ", "-")
	for strings.Contains(name, "--") {
		name = strings.ReplaceAll(name, "--", "-")
	}
	name = strings.Trim(name, "-")
	return name
}

// errorsIsNotExist returns true if err is fs.ErrNotExist-compatible.
func errorsIsNotExist(err error) bool {
	return err != nil && (os.IsNotExist(err) || errors.Is(err, fs.ErrNotExist))
}

// originMeta tracks which registry a skill was installed from.
type originMeta struct {
	Version          int    `json:"version"`
	Registry         string `json:"registry"`
	Slug             string `json:"slug"`
	InstalledVersion string `json:"installed_version"`
	InstalledAt      int64  `json:"installed_at"`
}

func writeOriginMeta(targetDir, registryName, slug, version string) error {
	meta := originMeta{
		Version:          1,
		Registry:         registryName,
		Slug:             slug,
		InstalledVersion: version,
		InstalledAt:      time.Now().UnixMilli(),
	}

	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(filepath.Join(targetDir, ".skill-origin.json"), data, 0o644)
}
