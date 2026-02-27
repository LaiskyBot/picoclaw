// PicoClaw - Ultra-lightweight personal AI agent
// Inspired by and based on nanobot: https://github.com/HKUDS/nanobot
// License: MIT
//
// Copyright (c) 2026 PicoClaw contributors

package agent

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/channels"
	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/constants"
	"github.com/sipeed/picoclaw/pkg/logger"
	"github.com/sipeed/picoclaw/pkg/providers"
	"github.com/sipeed/picoclaw/pkg/routing"
	"github.com/sipeed/picoclaw/pkg/skills"
	"github.com/sipeed/picoclaw/pkg/state"
	"github.com/sipeed/picoclaw/pkg/tools"
	"github.com/sipeed/picoclaw/pkg/utils"
)

type AgentLoop struct {
	bus                *bus.MessageBus
	cfg                *config.Config
	registry           *AgentRegistry
	state              *state.Manager
	taskPipeline       *TaskPipeline
	taskMaxParallel    int
	recentHistoryLimit int
	running            atomic.Bool
	summarizing        sync.Map
	fallback           *providers.FallbackChain
	channelManager     *channels.Manager
	memoryMCP          *mcpTurnMemoryClient
}

const promptLengthControlHint = "Length control: keep output concise, prefer summaries over raw long logs/HTML, and avoid unnecessary long excerpts."

const nonEmptyDefaultResponse = "I've completed processing but have no response to give."

// processOptions configures how a message is processed
type processOptions struct {
	SessionKey      string              // Session identifier for history/context
	Channel         string              // Target channel for tool execution
	ChatID          string              // Target chat ID for tool execution
	ToolChannel     string              // Optional channel override used only for tool context injection
	ToolChatID      string              // Optional chat ID override used only for tool context injection
	RecentHistory   []providers.Message // Optional preloaded history for LLM request composition
	UserMessage     string              // User message content (may include prefix)
	DefaultResponse string              // Response when LLM returns empty
	EnableSummary   bool                // Whether to trigger summarization
	SendResponse    bool                // Whether to send response via bus
	NoHistory       bool                // If true, don't load session history (for heartbeat)
}

func NewAgentLoop(cfg *config.Config, msgBus *bus.MessageBus, provider providers.LLMProvider) *AgentLoop {
	registry := NewAgentRegistry(cfg, provider)

	// Register shared tools to all agents
	registerSharedTools(cfg, msgBus, registry, provider)

	// Set up shared fallback chain
	cooldown := providers.NewCooldownTracker()
	fallbackChain := providers.NewFallbackChain(cooldown)

	// Create state manager using default agent's workspace for channel recording
	defaultAgent := registry.GetDefaultAgent()
	var stateManager *state.Manager
	var taskPipeline *TaskPipeline
	if defaultAgent != nil {
		stateManager = state.NewManager(defaultAgent.Workspace)
		taskPipeline = NewTaskPipeline(defaultAgent.Workspace, 0)
	}

	return &AgentLoop{
		bus:                msgBus,
		cfg:                cfg,
		registry:           registry,
		state:              stateManager,
		taskPipeline:       taskPipeline,
		taskMaxParallel:    cfg.Agents.Defaults.TaskMaxParallel,
		recentHistoryLimit: cfg.Agents.Defaults.RecentHistoryLimit,
		summarizing:        sync.Map{},
		fallback:           fallbackChain,
		memoryMCP:          newMCPMemoryClientFromRemote(cfg.Tools.MCP.Remote),
	}
}

// registerSharedTools registers tools that are shared across all agents (web, message, spawn).
func registerSharedTools(
	cfg *config.Config,
	msgBus *bus.MessageBus,
	registry *AgentRegistry,
	provider providers.LLMProvider,
) {
	for _, agentID := range registry.ListAgentIDs() {
		agent, ok := registry.GetAgent(agentID)
		if !ok {
			continue
		}

		// Web tools
		if searchTool := tools.NewWebSearchTool(tools.WebSearchToolOptions{
			BraveAPIKey:          cfg.Tools.Web.Brave.APIKey,
			BraveMaxResults:      cfg.Tools.Web.Brave.MaxResults,
			BraveEnabled:         cfg.Tools.Web.Brave.Enabled,
			TavilyAPIKey:         cfg.Tools.Web.Tavily.APIKey,
			TavilyBaseURL:        cfg.Tools.Web.Tavily.BaseURL,
			TavilyMaxResults:     cfg.Tools.Web.Tavily.MaxResults,
			TavilyEnabled:        cfg.Tools.Web.Tavily.Enabled,
			DuckDuckGoMaxResults: cfg.Tools.Web.DuckDuckGo.MaxResults,
			DuckDuckGoEnabled:    cfg.Tools.Web.DuckDuckGo.Enabled,
			PerplexityAPIKey:     cfg.Tools.Web.Perplexity.APIKey,
			PerplexityMaxResults: cfg.Tools.Web.Perplexity.MaxResults,
			PerplexityEnabled:    cfg.Tools.Web.Perplexity.Enabled,
			Proxy:                cfg.Tools.Web.Proxy,
		}); searchTool != nil {
			agent.Tools.Register(searchTool)
		}
		agent.Tools.Register(tools.NewWebFetchToolWithProxy(50000, cfg.Tools.Web.Proxy))

		// Hardware tools (I2C, SPI) - Linux only, returns error on other platforms
		agent.Tools.Register(tools.NewI2CTool())
		agent.Tools.Register(tools.NewSPITool())

		// Message tool
		messageTool := tools.NewMessageTool()
		messageTool.SetSendCallback(func(msg bus.OutboundMessage) error {
			msgBus.PublishOutbound(msg)
			return nil
		})
		agent.Tools.Register(messageTool)

		// Skill discovery and installation tools
		registryMgr := skills.NewRegistryManagerFromConfig(skills.RegistryConfig{
			MaxConcurrentSearches: cfg.Tools.Skills.MaxConcurrentSearches,
			ClawHub:               skills.ClawHubConfig(cfg.Tools.Skills.Registries.ClawHub),
		})
		searchCache := skills.NewSearchCache(
			cfg.Tools.Skills.SearchCache.MaxSize,
			time.Duration(cfg.Tools.Skills.SearchCache.TTLSeconds)*time.Second,
		)
		agent.Tools.Register(tools.NewFindSkillsTool(registryMgr, searchCache))
		agent.Tools.Register(tools.NewInstallSkillTool(registryMgr, agent.Workspace))
		remoteMCPTool := tools.NewRemoteMCPTool(agent.Workspace)
		if err := remoteMCPTool.BootstrapConfiguredServers(buildConfiguredRemoteMCPServers(cfg.Tools.MCP.Remote)); err != nil {
			logger.WarnC("agent", fmt.Sprintf("failed to bootstrap configured remote MCP servers for agent %q: %v", agentID, err))
		}
		agent.Tools.Register(remoteMCPTool)

		// Spawn tool with allowlist checker
		subagentManager := tools.NewSubagentManager(provider, agent.Model, agent.Workspace, msgBus)
		subagentManager.SetLLMOptions(agent.MaxTokens, agent.Temperature)
		spawnTool := tools.NewSpawnTool(subagentManager)
		currentAgentID := agentID
		spawnTool.SetAllowlistChecker(func(targetAgentID string) bool {
			return registry.CanSpawnSubagent(currentAgentID, targetAgentID)
		})
		agent.Tools.Register(spawnTool)
	}
}

// buildConfiguredRemoteMCPServers converts config remote MCP map into deterministic tool bootstrap inputs.
// The remote parameter is keyed by server name.
// It returns server entries sorted by name for stable behavior.
func buildConfiguredRemoteMCPServers(remote map[string]config.RemoteMCPServerConfig) []tools.ConfiguredRemoteMCPServer {
	if len(remote) == 0 {
		return nil
	}

	names := make([]string, 0, len(remote))
	for name := range remote {
		names = append(names, name)
	}
	sort.Strings(names)

	servers := make([]tools.ConfiguredRemoteMCPServer, 0, len(names))
	for _, name := range names {
		entry := remote[name]
		servers = append(servers, tools.ConfiguredRemoteMCPServer{
			Name:    name,
			Type:    entry.Type,
			URL:     entry.URL,
			Headers: entry.Headers,
		})
	}

	return servers
}

func (al *AgentLoop) Run(ctx context.Context) error {
	al.running.Store(true)
	logger.InfoC("agent", "Agent loop started")
	al.startTaskPipelineRuntime(ctx)
	al.resumeSubagentTasks(ctx)

	for al.running.Load() {
		select {
		case <-ctx.Done():
			return nil
		default:
			msg, ok := al.bus.ConsumeInbound(ctx)
			if !ok {
				continue
			}

			response, err := al.processMessage(ctx, msg)
			if err != nil {
				response = fmt.Sprintf("Error processing message: %v", err)
			}

			if response != "" {
				// Check if the message tool already sent a response during this round.
				// If so, skip publishing to avoid duplicate messages to the user.
				// Use default agent's tools to check (message tool is shared).
				alreadySent := false
				defaultAgent := al.registry.GetDefaultAgent()
				if defaultAgent != nil {
					if tool, ok := defaultAgent.Tools.Get("message"); ok {
						if mt, ok := tool.(*tools.MessageTool); ok {
							alreadySent = mt.HasSentInRound()
						}
					}
				}

				if !alreadySent {
					al.bus.PublishOutbound(bus.OutboundMessage{
						Channel: msg.Channel,
						ChatID:  msg.ChatID,
						Content: response,
					})
				}
			}
		}
	}

	logger.InfoC("agent", "Agent loop stopped")

	return nil
}

// resumeSubagentTasks resumes persisted unfinished subagent jobs for all registered agents.
// The ctx parameter controls the lifecycle of resumed jobs.
// It returns no value and logs aggregated resume count.
func (al *AgentLoop) resumeSubagentTasks(ctx context.Context) {
	resumedTotal := 0
	for _, agentID := range al.registry.ListAgentIDs() {
		agent, ok := al.registry.GetAgent(agentID)
		if !ok || agent == nil {
			continue
		}
		tool, ok := agent.Tools.Get("spawn")
		if !ok {
			continue
		}
		spawnTool, ok := tool.(*tools.SpawnTool)
		if !ok {
			continue
		}
		spawnTool.SetRuntimeContext(ctx)
		resumedTotal += spawnTool.ResumeUnfinished(ctx)
	}

	if resumedTotal > 0 {
		logger.InfoCF("agent", "Resumed unfinished subagent tasks after restart", map[string]any{"count": resumedTotal})
	}
}

func (al *AgentLoop) Stop() {
	al.running.Store(false)
}

func (al *AgentLoop) RegisterTool(tool tools.Tool) {
	for _, agentID := range al.registry.ListAgentIDs() {
		if agent, ok := al.registry.GetAgent(agentID); ok {
			agent.Tools.Register(tool)
		}
	}
}

func (al *AgentLoop) SetChannelManager(cm *channels.Manager) {
	al.channelManager = cm
}

// RecordLastChannel records the last active channel for this workspace.
// This uses the atomic state save mechanism to prevent data loss on crash.
func (al *AgentLoop) RecordLastChannel(channel string) error {
	if al.state == nil {
		return nil
	}
	return al.state.SetLastChannel(channel)
}

// RecordLastChatID records the last active chat ID for this workspace.
// This uses the atomic state save mechanism to prevent data loss on crash.
func (al *AgentLoop) RecordLastChatID(chatID string) error {
	if al.state == nil {
		return nil
	}
	return al.state.SetLastChatID(chatID)
}

func (al *AgentLoop) ProcessDirect(ctx context.Context, content, sessionKey string) (string, error) {
	return al.ProcessDirectWithChannel(ctx, content, sessionKey, "cli", "direct")
}

func (al *AgentLoop) ProcessDirectWithChannel(
	ctx context.Context,
	content, sessionKey, channel, chatID string,
) (string, error) {
	msg := bus.InboundMessage{
		Channel:    channel,
		SenderID:   "cron",
		ChatID:     chatID,
		Content:    content,
		SessionKey: sessionKey,
	}

	return al.processMessage(ctx, msg)
}

// ProcessHeartbeat processes a heartbeat request without session history.
// Each heartbeat is independent and doesn't accumulate context.
func (al *AgentLoop) ProcessHeartbeat(ctx context.Context, content, channel, chatID string) (string, error) {
	agent := al.registry.GetDefaultAgent()
	return al.runAgentLoop(ctx, agent, processOptions{
		SessionKey:      "heartbeat",
		Channel:         channel,
		ChatID:          chatID,
		UserMessage:     content,
		DefaultResponse: "I've completed processing but have no response to give.",
		EnableSummary:   false,
		SendResponse:    false,
		NoHistory:       true, // Don't load session history for heartbeat
	})
}

// shouldDelegateToPipeline reports whether inbound channel should use async planner handoff.
// The channel parameter is inbound transport identifier.
// It returns true for user-facing chat channels and false for internal or local/testing channels.
func (al *AgentLoop) shouldDelegateToPipeline(channel string) bool {
	if constants.IsInternalChannel(channel) {
		return false
	}

	switch strings.ToLower(strings.TrimSpace(channel)) {
	case "telegram", "discord", "slack", "feishu", "line", "wecom", "onebot", "qq", "dingtalk", "maixcam", "whatsapp":
		return true
	default:
		return false
	}
}

func (al *AgentLoop) processMessage(ctx context.Context, msg bus.InboundMessage) (string, error) {
	// Add message preview to log (show full content for error messages)
	var logContent string
	if strings.Contains(msg.Content, "Error:") || strings.Contains(msg.Content, "error") {
		logContent = msg.Content // Full content for errors
	} else {
		logContent = utils.Truncate(msg.Content, 80)
	}
	logger.InfoCF("agent", fmt.Sprintf("Processing message from %s:%s: %s", msg.Channel, msg.SenderID, logContent),
		map[string]any{
			"channel":     msg.Channel,
			"chat_id":     msg.ChatID,
			"sender_id":   msg.SenderID,
			"session_key": msg.SessionKey,
		})

	// Route system messages to processSystemMessage
	if msg.Channel == "system" {
		return al.processSystemMessage(ctx, msg)
	}

	// Check for commands
	if response, handled := al.handleCommand(ctx, msg); handled {
		return response, nil
	}

	if al.shouldDelegateToPipeline(msg.Channel) && al.taskPipeline != nil {
		if response, handled := al.maybeHandleTaskControlCommand(ctx, msg); handled {
			return response, nil
		}

		if matched, signal := MatchStatusQuery(msg.Content); matched {
			logger.DebugCF("agent", "Detected task status query", map[string]any{
				"channel":        msg.Channel,
				"chat_id":        msg.ChatID,
				"sender_id":      msg.SenderID,
				"matched_signal": signal,
				"content_chars":  len(msg.Content),
			})
			return al.taskPipeline.BuildStatusReply(msg.Channel, msg.ChatID, msg.SenderID), nil
		}

		summaryCtx, summaryCancel := context.WithTimeout(ctx, 12*time.Second)
		taskSummary := al.generateTaskSummary(summaryCtx, msg.Content)
		summaryCancel()
		if taskSummary == "" {
			taskSummary = fallbackTaskSummaryFromRequest(msg.Content)
		}

		task, err := al.taskPipeline.EnqueueTaskWithScheduling(
			msg.Channel,
			msg.ChatID,
			msg.SenderID,
			msg.Content,
			taskSummary,
			TaskExecutionModeParallel,
			nil,
			true,
		)
		if err != nil {
			logger.ErrorCF("agent", "Failed to enqueue delegated task", map[string]any{
				"channel": msg.Channel,
				"chat_id": msg.ChatID,
				"error":   err.Error(),
			})
			return "Failed to enqueue your task for background planning. Please retry.", nil
		}

		route := al.registry.ResolveRoute(routing.RouteInput{
			Channel:    msg.Channel,
			AccountID:  msg.Metadata["account_id"],
			Peer:       extractPeer(msg),
			ParentPeer: extractParentPeer(msg),
			GuildID:    msg.Metadata["guild_id"],
			TeamID:     msg.Metadata["team_id"],
		})
		if !al.taskPipeline.BindConversationContext(task.ID, route.AgentID, route.SessionKey) {
			logger.DebugCF("agent", "Failed to bind delegated task conversation context",
				map[string]any{
					"task_id":     task.ID,
					"agent_id":    route.AgentID,
					"session_key": route.SessionKey,
				})
		}

		if routeAgent, ok := al.registry.GetAgent(route.AgentID); ok {
			routeAgent.Sessions.AddMessage(route.SessionKey, "user", msg.Content)
			if err := routeAgent.Sessions.Save(route.SessionKey); err != nil {
				logger.DebugCF("agent", "Failed to save delegated user message in routed session",
					map[string]any{
						"task_id":     task.ID,
						"agent_id":    route.AgentID,
						"session_key": route.SessionKey,
						"error":       err.Error(),
					})
			}

			history := routeAgent.Sessions.GetHistory(route.SessionKey)
			conversationSummary := routeAgent.Sessions.GetSummary(route.SessionKey)
			delegationContext := buildDelegationReference(routeAgent, history, conversationSummary)
			if delegationContext != "" {
				_ = al.taskPipeline.SetDelegationContext(task.ID, delegationContext)
			}
			logger.DebugCF("agent", "Delegated task context prepared",
				map[string]any{
					"task_id":                  task.ID,
					"agent_id":                 route.AgentID,
					"session_key":              route.SessionKey,
					"history_count":            len(history),
					"has_summary":              strings.TrimSpace(conversationSummary) != "",
					"delegation_context_chars": len(delegationContext),
				})
		}

		return fmt.Sprintf(
			"Task %s accepted. I started it in the background and will report back.",
			taskLabel(task),
		), nil
	}

	// Route to determine agent and session key
	route := al.registry.ResolveRoute(routing.RouteInput{
		Channel:    msg.Channel,
		AccountID:  msg.Metadata["account_id"],
		Peer:       extractPeer(msg),
		ParentPeer: extractParentPeer(msg),
		GuildID:    msg.Metadata["guild_id"],
		TeamID:     msg.Metadata["team_id"],
	})

	agent, ok := al.registry.GetAgent(route.AgentID)
	if !ok {
		agent = al.registry.GetDefaultAgent()
	}

	// Use routed session key, but honor pre-set agent-scoped keys (for ProcessDirect/cron)
	sessionKey := route.SessionKey
	if msg.SessionKey != "" && strings.HasPrefix(msg.SessionKey, "agent:") {
		sessionKey = msg.SessionKey
	}

	logger.InfoCF("agent", "Routed message",
		map[string]any{
			"agent_id":    agent.ID,
			"session_key": sessionKey,
			"matched_by":  route.MatchedBy,
		})

	return al.runAgentLoop(ctx, agent, processOptions{
		SessionKey:      sessionKey,
		Channel:         msg.Channel,
		ChatID:          msg.ChatID,
		UserMessage:     msg.Content,
		DefaultResponse: "I've completed processing but have no response to give.",
		EnableSummary:   true,
		SendResponse:    false,
	})
}

func (al *AgentLoop) processSystemMessage(ctx context.Context, msg bus.InboundMessage) (string, error) {
	if msg.Channel != "system" {
		return "", fmt.Errorf("processSystemMessage called with non-system message channel: %s", msg.Channel)
	}

	logger.InfoCF("agent", "Processing system message",
		map[string]any{
			"sender_id": msg.SenderID,
			"chat_id":   msg.ChatID,
		})

	// Parse origin channel from chat_id (format: "channel:chat_id")
	var originChannel, originChatID string
	if idx := strings.Index(msg.ChatID, ":"); idx > 0 {
		originChannel = msg.ChatID[:idx]
		originChatID = msg.ChatID[idx+1:]
	} else {
		originChannel = "cli"
		originChatID = msg.ChatID
	}

	if originChannel == plannerTaskChannel && al.taskPipeline != nil {
		event := fmt.Sprintf("%s: %s", msg.SenderID, msg.Content)
		if !al.taskPipeline.AppendWorkerEvent(originChatID, event) {
			logger.WarnCF("agent", "Worker update for unknown pipeline task", map[string]any{
				"pipeline_task_id": originChatID,
				"sender_id":        msg.SenderID,
			})
			return "", nil
		}
		return "", nil
	}

	// Extract subagent result from message content
	// Format: "Task 'label' completed.\n\nResult:\n<actual content>"
	content := msg.Content
	if idx := strings.Index(content, "Result:\n"); idx >= 0 {
		content = content[idx+8:] // Extract just the result part
	}

	// Skip internal channels - only log, don't send to user
	if constants.IsInternalChannel(originChannel) {
		logger.InfoCF("agent", "Subagent completed (internal channel)",
			map[string]any{
				"sender_id":   msg.SenderID,
				"content_len": len(content),
				"channel":     originChannel,
			})
		return "", nil
	}

	// Use default agent for system messages
	agent := al.registry.GetDefaultAgent()

	// Use the origin session for context
	sessionKey := routing.BuildAgentMainSessionKey(agent.ID)

	return al.runAgentLoop(ctx, agent, processOptions{
		SessionKey:      sessionKey,
		Channel:         originChannel,
		ChatID:          originChatID,
		UserMessage:     fmt.Sprintf("[System: %s] %s", msg.SenderID, msg.Content),
		DefaultResponse: "Background task completed.",
		EnableSummary:   false,
		SendResponse:    true,
	})
}

// runAgentLoop is the core message processing logic.
func (al *AgentLoop) runAgentLoop(ctx context.Context, agent *AgentInstance, opts processOptions) (string, error) {
	// 0. Record last channel for heartbeat notifications (skip internal channels)
	if opts.Channel != "" && opts.ChatID != "" {
		// Don't record internal channels (cli, system, subagent)
		if !constants.IsInternalChannel(opts.Channel) {
			channelKey := fmt.Sprintf("%s:%s", opts.Channel, opts.ChatID)
			if err := al.RecordLastChannel(channelKey); err != nil {
				logger.WarnCF("agent", "Failed to record last channel", map[string]any{"error": err.Error()})
			}
		}
	}

	// 1. Build messages (skip history for heartbeat)
	var sessionHistory []providers.Message
	var summary string
	if !opts.NoHistory {
		sessionHistory = agent.Sessions.GetHistory(opts.SessionKey)
		summary = agent.Sessions.GetSummary(opts.SessionKey)
	}

	historyForRequest := pickRecentHistoryForRequest(sessionHistory, opts.RecentHistory, al.recentHistoryLimit)

	delegationReference := buildDelegationReference(agent, sessionHistory, summary)

	// 2. Update tool contexts
	toolChannel := opts.Channel
	if strings.TrimSpace(opts.ToolChannel) != "" {
		toolChannel = strings.TrimSpace(opts.ToolChannel)
	}
	toolChatID := opts.ChatID
	if strings.TrimSpace(opts.ToolChatID) != "" {
		toolChatID = strings.TrimSpace(opts.ToolChatID)
	}
	if toolChannel != opts.Channel || toolChatID != opts.ChatID {
		logger.DebugCF("agent", "Applying tool context override for current run", map[string]any{
			"session_key":        opts.SessionKey,
			"origin_channel":     opts.Channel,
			"origin_chat_id":     opts.ChatID,
			"tool_channel":       toolChannel,
			"tool_chat_id":       toolChatID,
			"task_reference_len": len(delegationReference),
		})
	}
	al.updateToolContexts(agent, toolChannel, toolChatID, delegationReference)

	// 3. Build messages
	messages := agent.ContextBuilder.BuildMessages(
		historyForRequest,
		summary,
		opts.UserMessage,
		nil,
		opts.Channel,
		opts.ChatID,
	)

	logger.DebugCF("agent", "Prepared recent conversation history for LLM request", map[string]any{
		"session_key":           opts.SessionKey,
		"recent_history_limit":  al.recentHistoryLimit,
		"session_history_count": len(sessionHistory),
		"request_history_count": len(historyForRequest),
		"preloaded_history":     len(opts.RecentHistory),
	})

	var memoryTurn *memoryTurnContext
	memoryUserID := resolveMemoryUserID(opts)
	if al.memoryMCP != nil {
		logger.DebugCF("agent", "Preparing memory_before_turn call", map[string]any{
			"agent_id":       agent.ID,
			"session_key":    opts.SessionKey,
			"memory_user_id": memoryUserID,
			"has_tool_chat":  strings.TrimSpace(opts.ToolChatID) != "",
		})
		turn, recallText, memErr := al.memoryMCP.beforeTurn(ctx, opts.SessionKey, memoryUserID, opts.UserMessage)
		if memErr != nil {
			logger.WarnCF("agent", "memory_before_turn call failed", map[string]any{
				"agent_id":       agent.ID,
				"session_key":    opts.SessionKey,
				"memory_user_id": memoryUserID,
				"error":          memErr.Error(),
			})
		} else {
			memoryTurn = turn
			messages = injectMCPMemoryRecall(messages, recallText)
			logger.DebugCF("agent", "Injected memory recall near latest user message for cache-friendly prompt prefix", map[string]any{
				"session_key":  opts.SessionKey,
				"recall_chars": len(strings.TrimSpace(recallText)),
			})
		}
	}

	// 4. Save user message to session
	agent.Sessions.AddMessage(opts.SessionKey, "user", opts.UserMessage)

	// 5. Run LLM iteration loop
	finalContent, iteration, err := al.runLLMIteration(ctx, agent, messages, opts)
	if err != nil {
		return "", err
	}

	messageSentInRound := hasMessageToolSentInRound(agent)
	if hasSyntheticMediaMarker(finalContent) {
		if messageSentInRound {
			logger.DebugCF("agent", "Assistant response contains media marker after successful message tool send", map[string]any{
				"agent_id":      agent.ID,
				"session_key":   opts.SessionKey,
				"channel":       opts.Channel,
				"chat_id":       opts.ChatID,
				"content_chars": len(finalContent),
			})
		} else {
			logger.DebugCF("agent", "Assistant response contains synthetic media marker without actual attachment send", map[string]any{
				"agent_id":      agent.ID,
				"session_key":   opts.SessionKey,
				"channel":       opts.Channel,
				"chat_id":       opts.ChatID,
				"content_chars": len(finalContent),
			})
			finalContent = sanitizeSyntheticMediaStatus(finalContent, false)
		}
	}

	// If last tool had ForUser content and we already sent it, we might not need to send final response
	// This is controlled by the tool's Silent flag and ForUser content

	// 6. Handle empty response
	if strings.TrimSpace(finalContent) == "" {
		defaultResponse := strings.TrimSpace(opts.DefaultResponse)
		if defaultResponse == "" {
			logger.WarnCF("agent", "DefaultResponse is empty; using non-empty fallback", map[string]any{
				"agent_id":    agent.ID,
				"session_key": opts.SessionKey,
				"channel":     opts.Channel,
				"chat_id":     opts.ChatID,
			})
			defaultResponse = nonEmptyDefaultResponse
		}
		finalContent = defaultResponse
	}

	if strings.TrimSpace(finalContent) == "" {
		logger.ErrorCF("agent", "Final response unexpectedly empty after fallback; forcing non-empty response", map[string]any{
			"agent_id":    agent.ID,
			"session_key": opts.SessionKey,
			"channel":     opts.Channel,
			"chat_id":     opts.ChatID,
		})
		finalContent = nonEmptyDefaultResponse
	}

	// 7. Save final assistant message to session
	agent.Sessions.AddMessage(opts.SessionKey, "assistant", finalContent)
	agent.Sessions.Save(opts.SessionKey)

	if memoryTurn != nil {
		if memErr := al.memoryMCP.afterTurn(ctx, opts.SessionKey, memoryUserID, finalContent, memoryTurn); memErr != nil {
			logger.WarnCF("agent", "memory_after_turn call failed", map[string]any{
				"agent_id":       agent.ID,
				"session_key":    opts.SessionKey,
				"memory_user_id": memoryUserID,
				"error":          memErr.Error(),
			})
		}
	}

	// 8. Optional: summarization
	if opts.EnableSummary {
		al.maybeSummarize(agent, opts.SessionKey, opts.Channel, opts.ChatID)
	}

	// 9. Optional: send response via bus
	if opts.SendResponse {
		al.bus.PublishOutbound(bus.OutboundMessage{
			Channel: opts.Channel,
			ChatID:  opts.ChatID,
			Content: finalContent,
		})
	}

	// 10. Log response
	responsePreview := utils.Truncate(finalContent, 120)
	logger.InfoCF("agent", fmt.Sprintf("Response: %s", responsePreview),
		map[string]any{
			"agent_id":     agent.ID,
			"session_key":  opts.SessionKey,
			"iterations":   iteration,
			"final_length": len(finalContent),
		})

	return finalContent, nil
}

// resolveMemoryUserID returns a stable memory user scope ID for this turn.
// The opts parameter carries runtime channel/chat overrides and return value prefers tool chat scope when available.
func resolveMemoryUserID(opts processOptions) string {
	if toolChatID := strings.TrimSpace(opts.ToolChatID); toolChatID != "" {
		return toolChatID
	}
	if chatID := strings.TrimSpace(opts.ChatID); chatID != "" {
		return chatID
	}
	return "unknown"
}

// injectMCPMemoryRecall inserts memory recall near the tail of message list.
// The messages parameter is provider message list and recallText is non-empty recalled memory excerpt.
// It returns an updated slice that keeps the longest possible prompt prefix unchanged for cache reuse.
func injectMCPMemoryRecall(messages []providers.Message, recallText string) []providers.Message {
	trimmedRecall := strings.TrimSpace(recallText)
	if trimmedRecall == "" {
		return messages
	}

	recallMsg := providers.Message{
		Role:    "assistant",
		Content: "MCP Memory Recall:\n" + trimmedRecall,
	}

	if len(messages) == 0 {
		return []providers.Message{recallMsg}
	}

	insertAt := len(messages)
	if messages[len(messages)-1].Role == "user" {
		insertAt = len(messages) - 1
	}

	updated := make([]providers.Message, 0, len(messages)+1)
	updated = append(updated, messages[:insertAt]...)
	updated = append(updated, recallMsg)
	updated = append(updated, messages[insertAt:]...)

	return updated
}

// pickRecentHistoryForRequest selects recent history messages for model input.
// The sessionHistory parameter is persisted session history, preloadedHistory can override source,
// and limit controls how many tail messages to include (<=0 means include all).
func pickRecentHistoryForRequest(sessionHistory, preloadedHistory []providers.Message, limit int) []providers.Message {
	base := sessionHistory
	if len(preloadedHistory) > 0 {
		base = preloadedHistory
	}
	if len(base) == 0 {
		return nil
	}

	if limit <= 0 || len(base) <= limit {
		out := make([]providers.Message, len(base))
		copy(out, base)
		return out
	}

	start := len(base) - limit
	out := make([]providers.Message, len(base[start:]))
	copy(out, base[start:])
	return out
}

// runLLMIteration executes the LLM call loop with tool handling.
func (al *AgentLoop) runLLMIteration(
	ctx context.Context,
	agent *AgentInstance,
	messages []providers.Message,
	opts processOptions,
) (string, int, error) {
	iteration := 0
	var finalContent string
	sawToolCalls := false
	llmCall := al.buildLLMCall(agent)

	for iteration < agent.MaxIterations {
		iteration++

		logger.DebugCF("agent", "LLM iteration",
			map[string]any{
				"agent_id":  agent.ID,
				"iteration": iteration,
				"max":       agent.MaxIterations,
			})

		// Build tool definitions
		providerToolDefs := agent.Tools.ToProviderDefs()

		// Log LLM request details
		logger.DebugCF("agent", "LLM request",
			map[string]any{
				"agent_id":          agent.ID,
				"iteration":         iteration,
				"model":             agent.Model,
				"messages_count":    len(messages),
				"tools_count":       len(providerToolDefs),
				"max_tokens":        agent.MaxTokens,
				"temperature":       agent.Temperature,
				"system_prompt_len": len(messages[0].Content),
			})

		// Log full messages (detailed)
		logger.DebugCF("agent", "Full LLM request",
			map[string]any{
				"iteration":     iteration,
				"messages_json": formatMessagesForLog(messages),
				"tools_json":    formatToolsForLog(providerToolDefs),
			})

		// Call LLM with fallback chain if candidates are configured.
		var response *providers.LLMResponse
		var err error

		// Retry loop for context/token errors
		maxRetries := 2
		for retry := 0; retry <= maxRetries; retry++ {
			response, err = llmCall(ctx, messages, providerToolDefs, iteration)
			if err == nil {
				break
			}

			isContextError := isContextWindowError(err)

			if isContextError && retry < maxRetries {
				logger.WarnCF("agent", "Context window error detected, attempting compression", map[string]any{
					"error": err.Error(),
					"retry": retry,
				})
				al.logContextSizeDiagnostics(messages, opts.SessionKey, retry)

				if retry == 0 && !constants.IsInternalChannel(opts.Channel) {
					al.bus.PublishOutbound(bus.OutboundMessage{
						Channel: opts.Channel,
						ChatID:  opts.ChatID,
						Content: "Context window exceeded. Compressing history and retrying...",
					})
				}

				compressed := al.emergencyCompressHistoryWithLLM(ctx, agent, opts.SessionKey, retry)
				if !compressed {
					al.forceCompression(agent, opts.SessionKey)
				}
				newHistory := agent.Sessions.GetHistory(opts.SessionKey)
				newSummary := agent.Sessions.GetSummary(opts.SessionKey)
				messages = agent.ContextBuilder.BuildMessages(
					newHistory, newSummary, "",
					nil, opts.Channel, opts.ChatID,
				)
				logger.DebugCF("agent", "Rebuilt messages after context compression", map[string]any{
					"session_key":       opts.SessionKey,
					"retry":             retry,
					"compressed_by_llm": compressed,
					"messages_count":    len(messages),
				})
				continue
			}
			break
		}

		if err != nil {
			logger.ErrorCF("agent", "LLM call failed",
				map[string]any{
					"agent_id":  agent.ID,
					"iteration": iteration,
					"error":     err.Error(),
				})
			return "", iteration, fmt.Errorf("LLM call failed after retries: %w", err)
		}

		logger.DebugCF("agent", "LLM response metadata", map[string]any{
			"agent_id":        agent.ID,
			"iteration":       iteration,
			"content_chars":   len(strings.TrimSpace(response.Content)),
			"reasoning_chars": len(strings.TrimSpace(response.ReasoningContent)),
			"tool_calls":      len(response.ToolCalls),
		})

		// Check if no tool calls - we're done
		if len(response.ToolCalls) == 0 {
			finalContent = response.Content
			logger.InfoCF("agent", "LLM response without tool calls (direct answer)",
				map[string]any{
					"agent_id":      agent.ID,
					"iteration":     iteration,
					"content_chars": len(finalContent),
				})
			break
		}

		normalizedToolCalls := make([]providers.ToolCall, 0, len(response.ToolCalls))
		for _, tc := range response.ToolCalls {
			normalizedToolCalls = append(normalizedToolCalls, providers.NormalizeToolCall(tc))
		}

		// Log tool calls
		toolNames := make([]string, 0, len(normalizedToolCalls))
		for _, tc := range normalizedToolCalls {
			toolNames = append(toolNames, tc.Name)
		}
		logger.InfoCF("agent", "LLM requested tool calls",
			map[string]any{
				"agent_id":  agent.ID,
				"tools":     toolNames,
				"count":     len(normalizedToolCalls),
				"iteration": iteration,
			})
		sawToolCalls = true

		if iteration == agent.MaxIterations {
			recoveredContent, done := al.handleMaxIterationWithToolCalls(ctx, agent, messages, opts, iteration, response, llmCall)
			if done {
				finalContent = recoveredContent
				break
			}
		}

		messages = al.appendAssistantAndExecuteToolsForIteration(ctx, agent, messages, opts, iteration, response, normalizedToolCalls)
	}

	if finalContent == "" && iteration >= agent.MaxIterations && sawToolCalls {
		logger.WarnCF("agent", "LLM loop reached max iterations without textual final response", map[string]any{
			"agent_id":       agent.ID,
			"session_key":    opts.SessionKey,
			"channel":        opts.Channel,
			"chat_id":        opts.ChatID,
			"max_iterations": agent.MaxIterations,
		})
	}

	return finalContent, iteration, nil
}

// isContextWindowError checks whether an error indicates model context/token length overflow.
// The err parameter is any upstream provider error and may include transport wrappers.
// It returns true when the error text strongly suggests context length exhaustion.
func isContextWindowError(err error) bool {
	if err == nil {
		return false
	}
	errMsg := strings.ToLower(err.Error())
	return strings.Contains(errMsg, "token") ||
		strings.Contains(errMsg, "context") ||
		strings.Contains(errMsg, "invalidparameter") ||
		strings.Contains(errMsg, "length")
}

// updateToolContexts updates context for tools that need channel/chatID and task references.
// The agent parameter contains tool registry, channel/chatID define chat scope, and taskReference carries memory/history references.
// It returns no value.
func (al *AgentLoop) updateToolContexts(agent *AgentInstance, channel, chatID, taskReference string) {
	// Use ContextualTool interface instead of type assertions
	if tool, ok := agent.Tools.Get("message"); ok {
		if mt, ok := tool.(tools.ContextualTool); ok {
			mt.SetContext(channel, chatID)
		}
	}
	if tool, ok := agent.Tools.Get("spawn"); ok {
		if st, ok := tool.(tools.ContextualTool); ok {
			st.SetContext(channel, chatID)
		}
		if rt, ok := tool.(tools.TaskReferenceTool); ok {
			rt.SetTaskReference(taskReference)
		}
	}
	if tool, ok := agent.Tools.Get("subagent"); ok {
		if st, ok := tool.(tools.ContextualTool); ok {
			st.SetContext(channel, chatID)
		}
		if rt, ok := tool.(tools.TaskReferenceTool); ok {
			rt.SetTaskReference(taskReference)
		}
	}
}

// buildDelegationReference composes memory and recent conversation history for delegated tasks.
// The agent parameter provides memory access while history and summary capture recent interaction state.
// It returns a compact plain-text reference payload for planner/worker tools.
func buildDelegationReference(agent *AgentInstance, history []providers.Message, summary string) string {
	if agent == nil {
		return ""
	}

	trimmedSummary := strings.TrimSpace(summary)
	memoryContext := ""
	if agent.ContextBuilder != nil && agent.ContextBuilder.memory != nil {
		memoryContext = strings.TrimSpace(agent.ContextBuilder.memory.GetMemoryContext())
	}
	recent := formatRecentConversationHistory(history, 8)

	if trimmedSummary == "" && memoryContext == "" && recent == "" {
		return ""
	}

	var sb strings.Builder
	if trimmedSummary != "" {
		sb.WriteString("Conversation summary:\n")
		sb.WriteString(utils.Truncate(trimmedSummary, 1200))
		sb.WriteString("\n\n")
	}
	if recent != "" {
		sb.WriteString("Recent interaction history:\n")
		sb.WriteString(recent)
		sb.WriteString("\n\n")
	}
	if memoryContext != "" {
		sb.WriteString("Memory highlights:\n")
		sb.WriteString(utils.Truncate(memoryContext, 1200))
	}

	return strings.TrimSpace(utils.Truncate(sb.String(), 3200))
}

// formatRecentConversationHistory formats recent user/assistant turns for delegated task references.
// The history parameter is the session message list and maxMessages limits returned turns.
// It returns formatted one-line entries in chronological order.
func formatRecentConversationHistory(history []providers.Message, maxMessages int) string {
	if len(history) == 0 || maxMessages <= 0 {
		return ""
	}

	turns := make([]string, 0, maxMessages)
	for i := len(history) - 1; i >= 0 && len(turns) < maxMessages; i-- {
		msg := history[i]
		if msg.Role != "user" && msg.Role != "assistant" {
			continue
		}
		content := strings.TrimSpace(msg.Content)
		if content == "" {
			continue
		}
		turns = append(turns, fmt.Sprintf("- %s: %s", msg.Role, utils.Truncate(content, 260)))
	}
	if len(turns) == 0 {
		return ""
	}

	for i, j := 0, len(turns)-1; i < j; i, j = i+1, j-1 {
		turns[i], turns[j] = turns[j], turns[i]
	}
	return strings.Join(turns, "\n")
}

// maybeSummarize triggers summarization if the session history exceeds thresholds.
func (al *AgentLoop) maybeSummarize(agent *AgentInstance, sessionKey, channel, chatID string) {
	newHistory := agent.Sessions.GetHistory(sessionKey)
	tokenEstimate := al.estimateTokens(newHistory)
	threshold := agent.ContextWindow * 75 / 100

	if len(newHistory) > 20 || tokenEstimate > threshold {
		summarizeKey := agent.ID + ":" + sessionKey
		if _, loading := al.summarizing.LoadOrStore(summarizeKey, true); !loading {
			go func() {
				defer al.summarizing.Delete(summarizeKey)
				if !constants.IsInternalChannel(channel) {
					al.bus.PublishOutbound(bus.OutboundMessage{
						Channel: channel,
						ChatID:  chatID,
						Content: "Memory threshold reached. Optimizing conversation history...",
					})
				}
				al.summarizeSession(agent, sessionKey)
			}()
		}
	}
}

// forceCompression aggressively reduces context when the limit is hit.
// It drops the oldest 50% of messages (keeping system prompt and last user message).
func (al *AgentLoop) forceCompression(agent *AgentInstance, sessionKey string) {
	history := agent.Sessions.GetHistory(sessionKey)
	if len(history) <= 4 {
		return
	}

	// Keep system prompt (usually [0]) and the very last message (user's trigger)
	// We want to drop the oldest half of the *conversation*
	// Assuming [0] is system, [1:] is conversation
	conversation := history[1 : len(history)-1]
	if len(conversation) == 0 {
		return
	}

	// Helper to find the mid-point of the conversation
	mid := len(conversation) / 2

	// New history structure:
	// 1. System Prompt (with compression note appended)
	// 2. Second half of conversation
	// 3. Last message

	droppedCount := mid
	keptConversation := conversation[mid:]

	newHistory := make([]providers.Message, 0, 1+len(keptConversation)+1)

	// Append compression note to the original system prompt instead of adding a new system message
	// This avoids having two consecutive system messages which some APIs (like Zhipu) reject
	compressionNote := fmt.Sprintf(
		"\n\n[System Note: Emergency compression dropped %d oldest messages due to context limit]",
		droppedCount,
	)
	enhancedSystemPrompt := history[0]
	enhancedSystemPrompt.Content = enhancedSystemPrompt.Content + compressionNote
	newHistory = append(newHistory, enhancedSystemPrompt)

	newHistory = append(newHistory, keptConversation...)
	newHistory = append(newHistory, history[len(history)-1]) // Last message

	// Update session
	agent.Sessions.SetHistory(sessionKey, newHistory)
	agent.Sessions.Save(sessionKey)

	logger.WarnCF("agent", "Forced compression executed", map[string]any{
		"session_key":  sessionKey,
		"dropped_msgs": droppedCount,
		"new_count":    len(newHistory),
	})
}

// logContextSizeDiagnostics records size-only context metrics for overflow troubleshooting.
// The messages parameter is the pending prompt payload and sessionKey identifies the chat session.
// It returns no value and intentionally avoids logging message content.
func (al *AgentLoop) logContextSizeDiagnostics(messages []providers.Message, sessionKey string, retry int) {
	type messageSize struct {
		index   int
		role    string
		content int
	}

	totalChars := 0
	top := make([]messageSize, 0, 3)
	for idx, msg := range messages {
		msgLen := utf8.RuneCountInString(msg.Content)
		totalChars += msgLen
		candidate := messageSize{index: idx, role: msg.Role, content: msgLen}
		top = append(top, candidate)
		sort.Slice(top, func(i, j int) bool {
			return top[i].content > top[j].content
		})
		if len(top) > 3 {
			top = top[:3]
		}
	}

	largest := make([]map[string]any, 0, len(top))
	for _, item := range top {
		largest = append(largest, map[string]any{
			"index":         item.index,
			"role":          item.role,
			"content_chars": item.content,
		})
	}

	logger.DebugCF("agent", "Context size diagnostics", map[string]any{
		"session_key":      sessionKey,
		"retry":            retry,
		"messages_count":   len(messages),
		"total_chars":      totalChars,
		"largest_messages": largest,
	})
}

// emergencyCompressHistoryWithLLM compacts session history via a bounded summarization request.
// The ctx parameter controls the provider call lifecycle, and retry indicates current retry index.
// It returns true when summary/history are updated and false when fallback compression is required.
func (al *AgentLoop) emergencyCompressHistoryWithLLM(
	ctx context.Context,
	agent *AgentInstance,
	sessionKey string,
	retry int,
) bool {
	history := agent.Sessions.GetHistory(sessionKey)
	if len(history) == 0 {
		return false
	}

	compressionSource, sourceMessages, sourceChars, omittedMessages := buildEmergencyCompressionSource(history)
	if compressionSource == "" {
		return false
	}

	existingSummary := strings.TrimSpace(agent.Sessions.GetSummary(sessionKey))
	var prompt strings.Builder
	prompt.WriteString("EMERGENCY_CONTEXT_COMPRESSION\n")
	prompt.WriteString("The previous request exceeded the provider context window. Compress memory aggressively.\n")
	prompt.WriteString("Return plain text only, maximum 1200 characters.\n")
	prompt.WriteString(promptLengthControlHint)
	prompt.WriteString("\n")
	prompt.WriteString("Keep only: user goals, constraints, important facts, completed actions, pending actions.\n")
	prompt.WriteString("Drop verbose logs, HTML, duplicated details, and low-value tool output.\n")
	if existingSummary != "" {
		prompt.WriteString("\nExisting summary:\n")
		prompt.WriteString(existingSummary)
		prompt.WriteString("\n")
	}
	prompt.WriteString("\nConversation snippets:\n")
	prompt.WriteString(compressionSource)

	compressCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	response, err := agent.Provider.Chat(
		compressCtx,
		[]providers.Message{
			{Role: "system", Content: "You compress overflowing chat context into a compact memory note."},
			{Role: "user", Content: prompt.String()},
		},
		nil,
		agent.Model,
		map[string]any{
			"max_tokens":       640,
			"temperature":      0.2,
			"prompt_cache_key": agent.ID,
		},
	)
	if err != nil || response == nil {
		logger.DebugCF("agent", "Emergency LLM compression failed", map[string]any{
			"session_key":      sessionKey,
			"retry":            retry,
			"source_messages":  sourceMessages,
			"source_chars":     sourceChars,
			"omitted_messages": omittedMessages,
			"error":            safeErrString(err),
		})
		return false
	}

	compressedSummary := strings.TrimSpace(response.Content)
	if compressedSummary == "" {
		return false
	}

	agent.Sessions.SetSummary(sessionKey, compressedSummary)
	trimmedHistory := trimHistoryForEmergencyRetry(history, 2, 1200)
	agent.Sessions.SetHistory(sessionKey, trimmedHistory)
	agent.Sessions.Save(sessionKey)

	logger.WarnCF("agent", "Emergency LLM compression executed", map[string]any{
		"session_key":         sessionKey,
		"retry":               retry,
		"source_messages":     sourceMessages,
		"source_chars":        sourceChars,
		"omitted_messages":    omittedMessages,
		"summary_chars":       utf8.RuneCountInString(compressedSummary),
		"trimmed_history_len": len(trimmedHistory),
	})

	return true
}

// safeErrString formats optional errors for structured logs.
// The err parameter may be nil.
// It returns an empty string when err is nil, otherwise the error text.
func safeErrString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// buildEmergencyCompressionSource converts history into bounded snippets for LLM compression.
// The history parameter is full session history and may contain very large tool outputs.
// It returns snippet text, included message count, included rune count, and omitted message count.
func buildEmergencyCompressionSource(history []providers.Message) (string, int, int, int) {
	if len(history) == 0 {
		return "", 0, 0, 0
	}

	maxMessages := 18
	maxPerMessageChars := 1200
	maxTotalChars := 12000

	start := 0
	if len(history) > maxMessages {
		start = len(history) - maxMessages
	}

	var sb strings.Builder
	includedMessages := 0
	includedChars := 0
	omittedMessages := 0

	for idx, msg := range history[start:] {
		if msg.Role == "system" {
			continue
		}
		content := strings.TrimSpace(msg.Content)
		if content == "" {
			continue
		}
		trimmed, truncated := truncateRunesWithNote(content, maxPerMessageChars)
		entryChars := utf8.RuneCountInString(trimmed)
		if includedChars+entryChars > maxTotalChars {
			omittedMessages++
			continue
		}
		fmt.Fprintf(&sb, "[%d] %s: %s\n", start+idx, msg.Role, trimmed)
		includedMessages++
		includedChars += entryChars
		if truncated {
			omittedMessages++
		}
	}

	if includedMessages == 0 {
		return "", 0, 0, omittedMessages
	}
	return sb.String(), includedMessages, includedChars, omittedMessages
}

// truncateRunesWithNote trims text by rune count and appends a truncation note when needed.
// The text parameter may contain multi-byte characters and maxRunes defines hard rune cap.
// It returns the possibly trimmed text and whether truncation occurred.
func truncateRunesWithNote(text string, maxRunes int) (string, bool) {
	if maxRunes <= 0 {
		return "", text != ""
	}
	if utf8.RuneCountInString(text) <= maxRunes {
		return text, false
	}
	runes := []rune(text)
	trimmed := strings.TrimSpace(string(runes[:maxRunes]))
	if trimmed == "" {
		return "[truncated]", true
	}
	return trimmed + " ... [truncated]", true
}

// trimHistoryForEmergencyRetry keeps a short tail of history and clips oversized message content.
// The history parameter is the original session history and keepLast controls tail size.
// It returns a sanitized tail suitable for immediate retry after overflow errors.
func trimHistoryForEmergencyRetry(history []providers.Message, keepLast, maxContentChars int) []providers.Message {
	if keepLast <= 0 {
		return []providers.Message{}
	}
	if len(history) == 0 {
		return []providers.Message{}
	}

	start := len(history) - keepLast
	if start < 0 {
		start = 0
	}

	trimmed := make([]providers.Message, 0, keepLast)
	for _, msg := range history[start:] {
		if msg.Role == "system" {
			continue
		}
		copied := msg
		copied.Content, _ = truncateRunesWithNote(copied.Content, maxContentChars)
		trimmed = append(trimmed, copied)
	}

	if len(trimmed) == 0 {
		for i := len(history) - 1; i >= 0; i-- {
			if history[i].Role == "system" {
				continue
			}
			copied := history[i]
			copied.Content, _ = truncateRunesWithNote(copied.Content, maxContentChars)
			return []providers.Message{copied}
		}
	}

	return trimmed
}

// GetStartupInfo returns information about loaded tools and skills for logging.
func (al *AgentLoop) GetStartupInfo() map[string]any {
	info := make(map[string]any)

	agent := al.registry.GetDefaultAgent()
	if agent == nil {
		return info
	}

	// Tools info
	toolsList := agent.Tools.List()
	info["tools"] = map[string]any{
		"count": len(toolsList),
		"names": toolsList,
	}

	// Skills info
	info["skills"] = agent.ContextBuilder.GetSkillsInfo()

	// Agents info
	info["agents"] = map[string]any{
		"count": len(al.registry.ListAgentIDs()),
		"ids":   al.registry.ListAgentIDs(),
	}

	return info
}

// formatMessagesForLog formats messages for logging
func formatMessagesForLog(messages []providers.Message) string {
	if len(messages) == 0 {
		return "[]"
	}

	var sb strings.Builder
	sb.WriteString("[\n")
	for i, msg := range messages {
		fmt.Fprintf(&sb, "  [%d] Role: %s\n", i, msg.Role)
		if len(msg.ToolCalls) > 0 {
			sb.WriteString("  ToolCalls:\n")
			for _, tc := range msg.ToolCalls {
				fmt.Fprintf(&sb, "    - ID: %s, Type: %s, Name: %s\n", tc.ID, tc.Type, tc.Name)
				if tc.Function != nil {
					fmt.Fprintf(&sb, "      Arguments: %s\n", utils.Truncate(tc.Function.Arguments, 200))
				}
			}
		}
		if msg.Content != "" {
			content := utils.Truncate(msg.Content, 200)
			fmt.Fprintf(&sb, "  Content: %s\n", content)
		}
		if msg.ToolCallID != "" {
			fmt.Fprintf(&sb, "  ToolCallID: %s\n", msg.ToolCallID)
		}
		sb.WriteString("\n")
	}
	sb.WriteString("]")
	return sb.String()
}

// formatToolsForLog formats tool definitions for logging
func formatToolsForLog(toolDefs []providers.ToolDefinition) string {
	if len(toolDefs) == 0 {
		return "[]"
	}

	var sb strings.Builder
	sb.WriteString("[\n")
	for i, tool := range toolDefs {
		fmt.Fprintf(&sb, "  [%d] Type: %s, Name: %s\n", i, tool.Type, tool.Function.Name)
		fmt.Fprintf(&sb, "      Description: %s\n", tool.Function.Description)
		if len(tool.Function.Parameters) > 0 {
			fmt.Fprintf(&sb, "      Parameters: %s\n", utils.Truncate(fmt.Sprintf("%v", tool.Function.Parameters), 200))
		}
	}
	sb.WriteString("]")
	return sb.String()
}

// summarizeSession summarizes the conversation history for a session.
func (al *AgentLoop) summarizeSession(agent *AgentInstance, sessionKey string) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	history := agent.Sessions.GetHistory(sessionKey)
	summary := agent.Sessions.GetSummary(sessionKey)

	// Keep last 4 messages for continuity
	if len(history) <= 4 {
		return
	}

	toSummarize := history[:len(history)-4]

	// Oversized Message Guard
	maxMessageTokens := agent.ContextWindow / 2
	validMessages := make([]providers.Message, 0)
	omitted := false

	for _, m := range toSummarize {
		if m.Role != "user" && m.Role != "assistant" {
			continue
		}
		msgTokens := len(m.Content) / 2
		if msgTokens > maxMessageTokens {
			omitted = true
			continue
		}
		validMessages = append(validMessages, m)
	}

	if len(validMessages) == 0 {
		return
	}

	// Multi-Part Summarization
	var finalSummary string
	if len(validMessages) > 10 {
		mid := len(validMessages) / 2
		part1 := validMessages[:mid]
		part2 := validMessages[mid:]

		s1, _ := al.summarizeBatch(ctx, agent, part1, "")
		s2, _ := al.summarizeBatch(ctx, agent, part2, "")

		mergePrompt := fmt.Sprintf(
			"Merge these two conversation summaries into one cohesive summary. Return plain text only and keep it short. %s\n\n1: %s\n\n2: %s",
			promptLengthControlHint,
			s1,
			s2,
		)
		resp, err := agent.Provider.Chat(
			ctx,
			[]providers.Message{{Role: "user", Content: mergePrompt}},
			nil,
			agent.Model,
			map[string]any{
				"max_tokens":       1024,
				"temperature":      0.3,
				"prompt_cache_key": agent.ID,
			},
		)
		if err == nil {
			finalSummary = resp.Content
		} else {
			finalSummary = s1 + " " + s2
		}
	} else {
		finalSummary, _ = al.summarizeBatch(ctx, agent, validMessages, summary)
	}

	if omitted && finalSummary != "" {
		finalSummary += "\n[Note: Some oversized messages were omitted from this summary for efficiency.]"
	}

	if finalSummary != "" {
		agent.Sessions.SetSummary(sessionKey, finalSummary)
		agent.Sessions.TruncateHistory(sessionKey, 4)
		agent.Sessions.Save(sessionKey)
	}
}

// summarizeBatch summarizes a batch of messages.
func (al *AgentLoop) summarizeBatch(
	ctx context.Context,
	agent *AgentInstance,
	batch []providers.Message,
	existingSummary string,
) (string, error) {
	var sb strings.Builder
	sb.WriteString("Provide a concise summary of this conversation segment, preserving core context and key points.\n")
	sb.WriteString(promptLengthControlHint)
	sb.WriteString("\n")
	if existingSummary != "" {
		sb.WriteString("Existing context: ")
		sb.WriteString(existingSummary)
		sb.WriteString("\n")
	}
	sb.WriteString("\nCONVERSATION:\n")
	for _, m := range batch {
		fmt.Fprintf(&sb, "%s: %s\n", m.Role, m.Content)
	}
	prompt := sb.String()

	response, err := agent.Provider.Chat(
		ctx,
		[]providers.Message{{Role: "user", Content: prompt}},
		nil,
		agent.Model,
		map[string]any{
			"max_tokens":       1024,
			"temperature":      0.3,
			"prompt_cache_key": agent.ID,
		},
	)
	if err != nil {
		return "", err
	}
	return response.Content, nil
}

// estimateTokens estimates the number of tokens in a message list.
// Uses a safe heuristic of 2.5 characters per token to account for CJK and other
// overheads better than the previous 3 chars/token.
func (al *AgentLoop) estimateTokens(messages []providers.Message) int {
	totalChars := 0
	for _, m := range messages {
		totalChars += utf8.RuneCountInString(m.Content)
	}
	// 2.5 chars per token = totalChars * 2 / 5
	return totalChars * 2 / 5
}

func (al *AgentLoop) handleCommand(ctx context.Context, msg bus.InboundMessage) (string, bool) {
	content := strings.TrimSpace(msg.Content)
	if !strings.HasPrefix(content, "/") {
		return "", false
	}

	parts := strings.Fields(content)
	if len(parts) == 0 {
		return "", false
	}

	cmd := parts[0]
	args := parts[1:]

	switch cmd {
	case "/show":
		if len(args) < 1 {
			return "Usage: /show [model|channel|agents]", true
		}
		switch args[0] {
		case "model":
			defaultAgent := al.registry.GetDefaultAgent()
			if defaultAgent == nil {
				return "No default agent configured", true
			}
			return fmt.Sprintf("Current model: %s", defaultAgent.Model), true
		case "channel":
			return fmt.Sprintf("Current channel: %s", msg.Channel), true
		case "agents":
			agentIDs := al.registry.ListAgentIDs()
			return fmt.Sprintf("Registered agents: %s", strings.Join(agentIDs, ", ")), true
		default:
			return fmt.Sprintf("Unknown show target: %s", args[0]), true
		}

	case "/list":
		if len(args) < 1 {
			return "Usage: /list [models|channels|agents]", true
		}
		switch args[0] {
		case "models":
			return "Available models: configured in config.json per agent", true
		case "channels":
			if al.channelManager == nil {
				return "Channel manager not initialized", true
			}
			channels := al.channelManager.GetEnabledChannels()
			if len(channels) == 0 {
				return "No channels enabled", true
			}
			return fmt.Sprintf("Enabled channels: %s", strings.Join(channels, ", ")), true
		case "agents":
			agentIDs := al.registry.ListAgentIDs()
			return fmt.Sprintf("Registered agents: %s", strings.Join(agentIDs, ", ")), true
		default:
			return fmt.Sprintf("Unknown list target: %s", args[0]), true
		}

	case "/switch":
		if len(args) < 3 || args[1] != "to" {
			return "Usage: /switch [model|channel] to <name>", true
		}
		target := args[0]
		value := args[2]

		switch target {
		case "model":
			defaultAgent := al.registry.GetDefaultAgent()
			if defaultAgent == nil {
				return "No default agent configured", true
			}
			oldModel := defaultAgent.Model
			defaultAgent.Model = value
			return fmt.Sprintf("Switched model from %s to %s", oldModel, value), true
		case "channel":
			if al.channelManager == nil {
				return "Channel manager not initialized", true
			}
			if _, exists := al.channelManager.GetChannel(value); !exists && value != "cli" {
				return fmt.Sprintf("Channel '%s' not found or not enabled", value), true
			}
			return fmt.Sprintf("Switched target channel to %s", value), true
		default:
			return fmt.Sprintf("Unknown switch target: %s", target), true
		}
	}

	return "", false
}

// extractPeer extracts the routing peer from inbound message metadata.
func extractPeer(msg bus.InboundMessage) *routing.RoutePeer {
	peerKind := msg.Metadata["peer_kind"]
	if peerKind == "" {
		return nil
	}
	peerID := msg.Metadata["peer_id"]
	if peerID == "" {
		if peerKind == "direct" {
			peerID = msg.SenderID
		} else {
			peerID = msg.ChatID
		}
	}
	return &routing.RoutePeer{Kind: peerKind, ID: peerID}
}

// extractParentPeer extracts the parent peer (reply-to) from inbound message metadata.
func extractParentPeer(msg bus.InboundMessage) *routing.RoutePeer {
	parentKind := msg.Metadata["parent_peer_kind"]
	parentID := msg.Metadata["parent_peer_id"]
	if parentKind == "" || parentID == "" {
		return nil
	}
	return &routing.RoutePeer{Kind: parentKind, ID: parentID}
}
