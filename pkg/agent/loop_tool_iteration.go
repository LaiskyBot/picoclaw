package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/logger"
	"github.com/sipeed/picoclaw/pkg/providers"
	"github.com/sipeed/picoclaw/pkg/tools"
	"github.com/sipeed/picoclaw/pkg/utils"
)

// llmIterationCall performs a single LLM call for the current iteration.
// The context controls cancellation, messages and toolDefs are request inputs, and iteration is used for diagnostics.
// It returns the provider response and any call error.
type llmIterationCall func(context.Context, []providers.Message, []providers.ToolDefinition, int) (*providers.LLMResponse, error)

// buildLLMCall creates the per-iteration LLM caller with shared fallback and request options.
// The agent parameter provides model/provider settings and fallback candidates.
// It returns a callable function used by the main iteration loop.
func (al *AgentLoop) buildLLMCall(agent *AgentInstance) llmIterationCall {
	return func(callCtx context.Context, reqMessages []providers.Message, reqToolDefs []providers.ToolDefinition, iter int) (*providers.LLMResponse, error) {
		callOptions := map[string]any{
			"max_tokens":       agent.MaxTokens,
			"temperature":      agent.Temperature,
			"prompt_cache_key": agent.ID,
		}
		if len(reqToolDefs) > 0 {
			callOptions["parallel_tool_calls"] = true
		}

		if len(agent.Candidates) > 0 && al.fallback != nil {
			fbResult, fbErr := al.fallback.Execute(callCtx, agent.Candidates,
				func(ctx context.Context, provider, model string) (*providers.LLMResponse, error) {
					return agent.Provider.Chat(ctx, reqMessages, reqToolDefs, model, callOptions)
				},
			)
			if fbErr != nil {
				return nil, fbErr
			}
			if fbResult.Provider != "" && len(fbResult.Attempts) > 0 {
				logger.InfoCF("agent", fmt.Sprintf("Fallback: succeeded with %s/%s after %d attempts",
					fbResult.Provider, fbResult.Model, len(fbResult.Attempts)+1),
					map[string]any{"agent_id": agent.ID, "iteration": iter})
			}
			return fbResult.Response, nil
		}

		return agent.Provider.Chat(callCtx, reqMessages, reqToolDefs, agent.Model, callOptions)
	}
}

// handleMaxIterationWithToolCalls resolves the terminal case where tool calls are requested on the last allowed iteration.
// The parameters carry request context, session metadata, current response, and the LLM caller used for forced no-tool finalization.
// It returns a best-effort final content and a done flag indicating the main loop should stop.
func (al *AgentLoop) handleMaxIterationWithToolCalls(
	ctx context.Context,
	agent *AgentInstance,
	messages []providers.Message,
	opts processOptions,
	iteration int,
	response *providers.LLMResponse,
	llmCall llmIterationCall,
) (string, bool) {
	if strings.TrimSpace(response.Content) != "" {
		logger.WarnCF("agent", "Max iterations reached with tool calls; using assistant textual content without executing tools", map[string]any{
			"agent_id":       agent.ID,
			"session_key":    opts.SessionKey,
			"channel":        opts.Channel,
			"chat_id":        opts.ChatID,
			"max_iterations": agent.MaxIterations,
			"content_chars":  len(strings.TrimSpace(response.Content)),
		})
		return response.Content, true
	}

	forcedSummary, forceErr := al.forceFinalTextAfterToolLoop(ctx, agent, messages, iteration, llmCall)
	if forceErr != nil {
		logger.WarnCF("agent", "Forced final textual response after tool loop exhaustion failed", map[string]any{
			"agent_id":       agent.ID,
			"iteration":      iteration,
			"max_iterations": agent.MaxIterations,
			"error":          forceErr.Error(),
		})
	}
	if strings.TrimSpace(forcedSummary) != "" {
		logger.WarnCF("agent", "Recovered textual response after tool loop exhaustion", map[string]any{
			"agent_id":       agent.ID,
			"session_key":    opts.SessionKey,
			"channel":        opts.Channel,
			"chat_id":        opts.ChatID,
			"max_iterations": agent.MaxIterations,
			"content_chars":  len(strings.TrimSpace(forcedSummary)),
		})
		return forcedSummary, true
	}

	logger.WarnCF("agent", "Max iterations reached with tool calls and no textual content; ending loop without executing additional tools", map[string]any{
		"agent_id":       agent.ID,
		"session_key":    opts.SessionKey,
		"channel":        opts.Channel,
		"chat_id":        opts.ChatID,
		"max_iterations": agent.MaxIterations,
	})
	return "", true
}

// appendAssistantAndExecuteToolsForIteration appends assistant tool calls and executes all requested tools for the iteration.
// The parameters include runtime context, current messages, session options, and normalized tool calls from the provider response.
// It returns the updated message list including assistant and tool result messages.
func (al *AgentLoop) appendAssistantAndExecuteToolsForIteration(
	ctx context.Context,
	agent *AgentInstance,
	messages []providers.Message,
	opts processOptions,
	iteration int,
	response *providers.LLMResponse,
	normalizedToolCalls []providers.ToolCall,
) []providers.Message {
	assistantMsg := providers.Message{
		Role:             "assistant",
		Content:          response.Content,
		ReasoningContent: response.ReasoningContent,
	}
	for _, tc := range normalizedToolCalls {
		argumentsJSON, _ := json.Marshal(tc.Arguments)
		extraContent := tc.ExtraContent
		thoughtSignature := ""
		if tc.Function != nil {
			thoughtSignature = tc.Function.ThoughtSignature
		}

		assistantMsg.ToolCalls = append(assistantMsg.ToolCalls, providers.ToolCall{
			ID:   tc.ID,
			Type: "function",
			Name: tc.Name,
			Function: &providers.FunctionCall{
				Name:             tc.Name,
				Arguments:        string(argumentsJSON),
				ThoughtSignature: thoughtSignature,
			},
			ExtraContent:     extraContent,
			ThoughtSignature: thoughtSignature,
		})
	}
	messages = append(messages, assistantMsg)
	agent.Sessions.AddFullMessage(opts.SessionKey, assistantMsg)

	for _, tc := range normalizedToolCalls {
		argsJSON, _ := json.Marshal(tc.Arguments)
		argsPreview := utils.Truncate(string(argsJSON), 200)
		logger.InfoCF("agent", fmt.Sprintf("Tool call: %s(%s)", tc.Name, argsPreview),
			map[string]any{
				"agent_id":  agent.ID,
				"tool":      tc.Name,
				"iteration": iteration,
			})

		asyncCallback := func(callbackCtx context.Context, result *tools.ToolResult) {
			if !result.Silent && result.ForUser != "" {
				logger.InfoCF("agent", "Async tool completed, agent will handle notification",
					map[string]any{
						"tool":        tc.Name,
						"content_len": len(result.ForUser),
					})
			}
		}

		toolExecChannel := opts.Channel
		if strings.TrimSpace(opts.ToolChannel) != "" {
			toolExecChannel = strings.TrimSpace(opts.ToolChannel)
		}
		toolExecChatID := opts.ChatID
		if strings.TrimSpace(opts.ToolChatID) != "" {
			toolExecChatID = strings.TrimSpace(opts.ToolChatID)
		}
		if toolExecChannel != opts.Channel || toolExecChatID != opts.ChatID {
			logger.DebugCF("agent", "Executing tool with overridden target context", map[string]any{
				"agent_id":          agent.ID,
				"tool":              tc.Name,
				"iteration":         iteration,
				"origin_channel":    opts.Channel,
				"origin_chat_id":    opts.ChatID,
				"tool_exec_channel": toolExecChannel,
				"tool_exec_chat_id": toolExecChatID,
			})
		}

		toolResult := agent.Tools.ExecuteWithContext(
			ctx,
			tc.Name,
			tc.Arguments,
			toolExecChannel,
			toolExecChatID,
			asyncCallback,
		)

		if !toolResult.Silent && toolResult.ForUser != "" && opts.SendResponse {
			al.bus.PublishOutbound(bus.OutboundMessage{
				Channel: opts.Channel,
				ChatID:  opts.ChatID,
				Content: toolResult.ForUser,
			})
			logger.DebugCF("agent", "Sent tool result to user",
				map[string]any{
					"tool":        tc.Name,
					"content_len": len(toolResult.ForUser),
				})
		}

		contentForLLM := toolResult.ForLLM
		if contentForLLM == "" && toolResult.Err != nil {
			contentForLLM = toolResult.Err.Error()
		}

		logger.DebugCF("agent", "Tool execution output metadata", map[string]any{
			"agent_id":       agent.ID,
			"iteration":      iteration,
			"tool":           tc.Name,
			"for_llm_chars":  len(strings.TrimSpace(contentForLLM)),
			"for_user_chars": len(strings.TrimSpace(toolResult.ForUser)),
			"silent":         toolResult.Silent,
			"has_error":      toolResult.Err != nil,
		})

		toolResultMsg := providers.Message{
			Role:       "tool",
			Content:    contentForLLM,
			ToolCallID: tc.ID,
		}
		messages = append(messages, toolResultMsg)
		agent.Sessions.AddFullMessage(opts.SessionKey, toolResultMsg)
	}

	return messages
}

// forceFinalTextAfterToolLoop asks the model to return a textual answer when tool budget is exhausted.
// The ctx parameter controls request cancellation, agent holds model/provider settings, messages is the current conversation state,
// and iteration records the active turn for logs. The llmCall parameter performs one provider call with caller-selected tools.
// It returns the best-effort textual response and any provider execution error.
func (al *AgentLoop) forceFinalTextAfterToolLoop(
	ctx context.Context,
	agent *AgentInstance,
	messages []providers.Message,
	iteration int,
	llmCall llmIterationCall,
) (string, error) {
	forcedMessages := append([]providers.Message{}, messages...)
	forcedMessages = append(forcedMessages, providers.Message{
		Role: "system",
		Content: "Tool-call budget is exhausted. Do not call any tools. Provide a concise final textual answer based only on existing observations. " +
			"If information is insufficient, clearly state the uncertainty and list what is missing.",
	})

	response, err := llmCall(ctx, forcedMessages, nil, iteration)
	if err != nil {
		return "", err
	}
	if response == nil {
		return "", nil
	}

	logger.DebugCF("agent", "Forced final textual response metadata", map[string]any{
		"agent_id":        agent.ID,
		"iteration":       iteration,
		"content_chars":   len(strings.TrimSpace(response.Content)),
		"reasoning_chars": len(strings.TrimSpace(response.ReasoningContent)),
		"tool_calls":      len(response.ToolCalls),
	})

	return strings.TrimSpace(response.Content), nil
}
