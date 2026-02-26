package skills

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/sipeed/picoclaw/pkg/logger"
)

var namePattern = regexp.MustCompile(`^[a-zA-Z0-9]+(-[a-zA-Z0-9]+)*$`)

const (
	MaxNameLength        = 64
	MaxDescriptionLength = 1024
)

type SkillMetadata struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	ReadWhen    []string
}

type SkillInfo struct {
	Name        string `json:"name"`
	DisplayName string `json:"display_name,omitempty"`
	Path        string `json:"path"`
	Source      string `json:"source"`
	Description string `json:"description"`
	ReadWhen    []string `json:"read_when,omitempty"`
	ScriptHints []string `json:"script_hints,omitempty"`
}

func (info SkillInfo) validate() error {
	var errs error
	if info.Name == "" {
		errs = errors.Join(errs, errors.New("name is required"))
	} else {
		if len(info.Name) > MaxNameLength {
			errs = errors.Join(errs, fmt.Errorf("name exceeds %d characters", MaxNameLength))
		}
		if !namePattern.MatchString(info.Name) {
			errs = errors.Join(errs, errors.New("name must be alphanumeric with hyphens"))
		}
	}

	if info.Description == "" {
		errs = errors.Join(errs, errors.New("description is required"))
	} else if len(info.Description) > MaxDescriptionLength {
		errs = errors.Join(errs, fmt.Errorf("description exceeds %d character", MaxDescriptionLength))
	}
	return errs
}

type SkillsLoader struct {
	workspace       string
	workspaceSkills string // workspace skills (project-level)
	globalSkills    string // global skills (~/.picoclaw/skills)
	builtinSkills   string // builtin skills
}

func NewSkillsLoader(workspace string, globalSkills string, builtinSkills string) *SkillsLoader {
	return &SkillsLoader{
		workspace:       workspace,
		workspaceSkills: filepath.Join(workspace, "skills"),
		globalSkills:    globalSkills, // ~/.picoclaw/skills
		builtinSkills:   builtinSkills,
	}
}

func (sl *SkillsLoader) ListSkills() []SkillInfo {
	skills := make([]SkillInfo, 0)
	seen := make(map[string]bool)

	addSkills := func(dir, source string) {
		if dir == "" {
			return
		}
		dirs, err := os.ReadDir(dir)
		if err != nil {
			return
		}
		for _, d := range dirs {
			if !d.IsDir() {
				continue
			}
			dirName := d.Name()
			skillDir := filepath.Join(dir, d.Name())
			skillFile := filepath.Join(dir, d.Name(), "SKILL.md")
			if _, err := os.Stat(skillFile); err != nil {
				continue
			}
			info := SkillInfo{
				Name:   dirName,
				Path:   skillFile,
				Source: source,
			}
			metadata := sl.getSkillMetadata(skillFile)
			if metadata != nil {
				info.Description = metadata.Description
				info.ReadWhen = append([]string(nil), metadata.ReadWhen...)
				if metadata.Name != "" {
					info.DisplayName = metadata.Name
					if namePattern.MatchString(metadata.Name) {
						info.Name = metadata.Name
					} else {
						slog.Debug("skill metadata name is not identifier, fallback to directory name",
							"dir", dirName,
							"metadata_name", metadata.Name,
							"source", source,
						)
					}
				}
			}
			if err := info.validate(); err != nil {
				slog.Warn("invalid skill from "+source, "name", info.Name, "error", err)
				continue
			}
			if seen[info.Name] {
				continue
			}
			info.ScriptHints = sl.listSkillScripts(skillDir)
			seen[info.Name] = true
			skills = append(skills, info)
		}
	}

	// Priority: workspace > global > builtin
	addSkills(sl.workspaceSkills, "workspace")
	addSkills(sl.globalSkills, "global")
	addSkills(sl.builtinSkills, "builtin")

	return skills
}

func (sl *SkillsLoader) LoadSkill(name string) (string, bool) {
	// 1. load from workspace skills first (project-level)
	if sl.workspaceSkills != "" {
		skillFile := filepath.Join(sl.workspaceSkills, name, "SKILL.md")
		if content, err := os.ReadFile(skillFile); err == nil {
			return sl.stripFrontmatter(string(content)), true
		}
	}

	// 2. then load from global skills (~/.picoclaw/skills)
	if sl.globalSkills != "" {
		skillFile := filepath.Join(sl.globalSkills, name, "SKILL.md")
		if content, err := os.ReadFile(skillFile); err == nil {
			return sl.stripFrontmatter(string(content)), true
		}
	}

	// 3. finally load from builtin skills
	if sl.builtinSkills != "" {
		skillFile := filepath.Join(sl.builtinSkills, name, "SKILL.md")
		if content, err := os.ReadFile(skillFile); err == nil {
			return sl.stripFrontmatter(string(content)), true
		}
	}

	return "", false
}

func (sl *SkillsLoader) LoadSkillsForContext(skillNames []string) string {
	if len(skillNames) == 0 {
		return ""
	}

	var parts []string
	for _, name := range skillNames {
		content, ok := sl.LoadSkill(name)
		if ok {
			parts = append(parts, fmt.Sprintf("### Skill: %s\n\n%s", name, content))
		}
	}

	return strings.Join(parts, "\n\n---\n\n")
}

func (sl *SkillsLoader) BuildSkillsSummary() string {
	return sl.BuildSkillsSummaryWithFilter(nil)
}

// BuildSkillsSummaryWithFilter builds an XML-like summary for all discoverable skills.
// If filterNames is non-empty, only matching canonical skill names are included.
func (sl *SkillsLoader) BuildSkillsSummaryWithFilter(filterNames []string) string {
	allSkills := sl.ListSkills()
	allSkills = filterSkillsByName(allSkills, filterNames)
	if len(allSkills) == 0 {
		return ""
	}

	var lines []string
	lines = append(lines, "<skills>")
	for _, s := range allSkills {
		escapedName := escapeXML(s.Name)
		escapedDisplayName := escapeXML(s.DisplayName)
		escapedDesc := escapeXML(s.Description)
		escapedPath := escapeXML(s.Path)

		lines = append(lines, fmt.Sprintf("  <skill>"))
		lines = append(lines, fmt.Sprintf("    <name>%s</name>", escapedName))
		if escapedDisplayName != "" && escapedDisplayName != escapedName {
			lines = append(lines, fmt.Sprintf("    <display_name>%s</display_name>", escapedDisplayName))
		}
		lines = append(lines, fmt.Sprintf("    <description>%s</description>", escapedDesc))
		if len(s.ReadWhen) > 0 {
			lines = append(lines, "    <read_when>")
			for _, item := range s.ReadWhen {
				lines = append(lines, fmt.Sprintf("      <hint>%s</hint>", escapeXML(item)))
			}
			lines = append(lines, "    </read_when>")
		}
		if len(s.ScriptHints) > 0 {
			lines = append(lines, "    <scripts>")
			for _, script := range s.ScriptHints {
				lines = append(lines, fmt.Sprintf("      <entry>%s</entry>", escapeXML(script)))
			}
			lines = append(lines, "    </scripts>")
		}
		lines = append(lines, fmt.Sprintf("    <location>%s</location>", escapedPath))
		lines = append(lines, fmt.Sprintf("    <source>%s</source>", s.Source))
		lines = append(lines, "  </skill>")
	}
	lines = append(lines, "</skills>")

	return strings.Join(lines, "\n")
}

// filterSkillsByName filters skills by canonical names.
// It accepts both exact matches and normalized user-provided names.
func filterSkillsByName(skills []SkillInfo, filterNames []string) []SkillInfo {
	if len(filterNames) == 0 {
		return skills
	}

	wanted := make(map[string]struct{}, len(filterNames))
	for _, raw := range filterNames {
		name := normalizeSkillIdentifier(raw)
		if name == "" {
			continue
		}
		wanted[name] = struct{}{}
	}
	if len(wanted) == 0 {
		return skills
	}

	filtered := make([]SkillInfo, 0, len(skills))
	for _, skill := range skills {
		if _, ok := wanted[normalizeSkillIdentifier(skill.Name)]; ok {
			filtered = append(filtered, skill)
		}
	}
	return filtered
}

// normalizeSkillIdentifier normalizes skill names for matching.
func normalizeSkillIdentifier(name string) string {
	name = strings.TrimSpace(strings.ToLower(name))
	name = strings.ReplaceAll(name, "_", "-")
	name = strings.ReplaceAll(name, " ", "-")
	for strings.Contains(name, "--") {
		name = strings.ReplaceAll(name, "--", "-")
	}
	name = strings.Trim(name, "-")
	return name
}

// listSkillScripts returns script entry paths under {skillDir}/scripts.
// It only lists relative file paths and never reads script content.
func (sl *SkillsLoader) listSkillScripts(skillDir string) []string {
	if skillDir == "" {
		return nil
	}
	scriptsDir := filepath.Join(skillDir, "scripts")
	if _, err := os.Stat(scriptsDir); err != nil {
		return nil
	}

	entries := make([]string, 0)
	_ = filepath.WalkDir(scriptsDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		rel, relErr := filepath.Rel(skillDir, path)
		if relErr != nil {
			return nil
		}
		entries = append(entries, filepath.ToSlash(rel))
		return nil
	})

	sort.Strings(entries)
	if len(entries) > 12 {
		entries = entries[:12]
	}
	return entries
}

func (sl *SkillsLoader) getSkillMetadata(skillPath string) *SkillMetadata {
	content, err := os.ReadFile(skillPath)
	if err != nil {
		logger.WarnCF("skills", "Failed to read skill metadata",
			map[string]any{
				"skill_path": skillPath,
				"error":      err.Error(),
			})
		return nil
	}

	frontmatter := sl.extractFrontmatter(string(content))
	if frontmatter == "" {
		return &SkillMetadata{
			Name: filepath.Base(filepath.Dir(skillPath)),
		}
	}

	// Try JSON first (for backward compatibility)
	var jsonMeta struct {
		Name        string `json:"name"`
		Description string `json:"description"`
		ReadWhen    []string `json:"read_when"`
	}
	if err := json.Unmarshal([]byte(frontmatter), &jsonMeta); err == nil {
		return &SkillMetadata{
			Name:        jsonMeta.Name,
			Description: jsonMeta.Description,
			ReadWhen:    jsonMeta.ReadWhen,
		}
	}

	// Fall back to simple YAML parsing
	yamlMeta := sl.parseSimpleYAML(frontmatter)
	return &SkillMetadata{
		Name:        yamlMeta["name"],
		Description: yamlMeta["description"],
		ReadWhen:    sl.parseSimpleYAMLList(frontmatter, "read_when"),
	}
}

// parseSimpleYAML parses simple key: value YAML format
// Example: name: github\n description: "..."
// Normalizes line endings to handle \n (Unix), \r\n (Windows), and \r (classic Mac)
func (sl *SkillsLoader) parseSimpleYAML(content string) map[string]string {
	result := make(map[string]string)

	// Normalize line endings: convert \r\n and \r to \n
	normalized := strings.ReplaceAll(content, "\r\n", "\n")
	normalized = strings.ReplaceAll(normalized, "\r", "\n")

	for _, line := range strings.Split(normalized, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		parts := strings.SplitN(line, ":", 2)
		if len(parts) == 2 {
			key := strings.TrimSpace(parts[0])
			value := strings.TrimSpace(parts[1])
			// Remove quotes if present
			value = strings.Trim(value, "\"'")
			result[key] = value
		}
	}

	return result
}

// parseSimpleYAMLList parses a simple YAML list for a given key.
// Example:
// read_when:
//   - First condition
//   - Second condition
func (sl *SkillsLoader) parseSimpleYAMLList(content, key string) []string {
	result := make([]string, 0)

	// Normalize line endings: convert \r\n and \r to \n
	normalized := strings.ReplaceAll(content, "\r\n", "\n")
	normalized = strings.ReplaceAll(normalized, "\r", "\n")
	lines := strings.Split(normalized, "\n")

	inList := false
	listIndent := -1
	for _, raw := range lines {
		trimmed := strings.TrimSpace(raw)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}

		if !inList {
			if strings.TrimSuffix(trimmed, " ") == key+":" {
				inList = true
				listIndent = leadingWhitespaceCount(raw)
			}
			continue
		}

		if leadingWhitespaceCount(raw) <= listIndent && !strings.HasPrefix(trimmed, "-") {
			break
		}

		if strings.HasPrefix(trimmed, "- ") {
			item := strings.TrimSpace(strings.TrimPrefix(trimmed, "- "))
			item = strings.Trim(item, "\"'")
			if item != "" {
				result = append(result, item)
			}
		}
	}

	return result
}

// leadingWhitespaceCount returns the number of leading spaces or tabs.
func leadingWhitespaceCount(s string) int {
	count := 0
	for _, ch := range s {
		if ch == ' ' || ch == '\t' {
			count++
			continue
		}
		break
	}
	return count
}

func (sl *SkillsLoader) extractFrontmatter(content string) string {
	// Support \n (Unix), \r\n (Windows), and \r (classic Mac) line endings for frontmatter blocks
	// (?s) enables DOTALL so . matches newlines;
	// ^--- at start, then ... --- at start of line, honoring all three line ending types
	re := regexp.MustCompile(`(?s)^---(?:\r\n|\n|\r)(.*?)(?:\r\n|\n|\r)---`)
	match := re.FindStringSubmatch(content)
	if len(match) > 1 {
		return match[1]
	}
	return ""
}

func (sl *SkillsLoader) stripFrontmatter(content string) string {
	// Support \n (Unix), \r\n (Windows), and \r (classic Mac) line endings for frontmatter blocks
	// (?s) enables DOTALL so . matches newlines;
	// ^--- at start, then ... --- at start of line, honoring all three line ending types
	// Match zero or more trailing line endings after closing --- (handles both with and without blank lines)
	re := regexp.MustCompile(`(?s)^---(?:\r\n|\n|\r)(.*?)(?:\r\n|\n|\r)---(?:\r\n|\n|\r)*`)
	return re.ReplaceAllString(content, "")
}

func escapeXML(s string) string {
	s = strings.ReplaceAll(s, "&", "&amp;")
	s = strings.ReplaceAll(s, "<", "&lt;")
	s = strings.ReplaceAll(s, ">", "&gt;")
	return s
}
