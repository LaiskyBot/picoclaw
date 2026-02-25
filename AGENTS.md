# Agents Guidelines

**No matter what language you receive, Only using English as output for codes, comments, chat, documents and everything else.**

Local tools and debugging related sensitive information is saved in .github/instructions/laisky.instructions.md.

## This Project

PicoClaw is a Go-first, ultra-lightweight personal AI assistant designed to run on constrained Linux devices. The repository is centered around a single CLI binary (`picoclaw`) that supports onboarding, agent runtime, gateway mode, auth, cron, migration, and skills management.

### Operating Modes: Agent vs Gateway

PicoClaw has two primary runtime modes, both sharing the same `pkg/agent` core logic but serving different deployment goals:

- `picoclaw agent`: direct single-process interaction mode for local usage and debugging.
    - Supports one-shot (`-m`) and interactive CLI chat.
    - Handles input/output in-process without starting channel adapters or HTTP health endpoints.
    - Best for onboarding validation, prompt/tool debugging, and local development loops.
- `picoclaw gateway`: long-running service mode for production-like operation.
    - Starts enabled channel adapters (Telegram/Discord/Slack/Feishu/OneBot/WeCom/etc.) and routes all inbound/outbound events through `pkg/bus`.
    - Runs background capabilities (cron jobs, heartbeat tasks, optional device event service).
    - Exposes health/readiness endpoints via `pkg/health` for process supervision.

Design intent: keep `agent` simple for fast iteration, and keep `gateway` reliable for continuous multi-channel automation.

### Core Runtime Architecture

The main runtime path is:

1. channel adapters receive user messages and publish them to `pkg/bus`.
2. `pkg/agent` (`AgentLoop`) consumes inbound messages, resolves target agent/session via `pkg/routing`, and executes model + tool loops.
3. responses are published back to the bus and dispatched by `pkg/channels`.

In `agent` mode, this pipeline is used in direct form (`ProcessDirect`) without channel manager startup.
In `gateway` mode, the full bus + channels + background services stack is enabled.

Keep this message-bus-driven flow consistent when adding features: avoid coupling channel logic directly to provider or tool internals.

### Configuration Model

Configuration is JSON-based (`config/config.example.json`) with env override support in `pkg/config`.

- Prefer `model_list` + `agents.defaults.model_name` for model/provider selection.
- `providers` is still supported for compatibility but is gradually deprecated.
- Multi-agent routing is configured through `agents.list` + `bindings` + `session` fields.
- New config fields must be backward-compatible and covered by tests in `pkg/config`.

### Main Modules (Where to Change What)

- `cmd/picoclaw`: CLI commands and entrypoints.
- `pkg/agent`: core LLM loop, context/memory handling, tool orchestration.
- `pkg/providers`: provider factory, auth-backed providers, fallback/cooldown logic.
- `pkg/channels`: platform adapters (Telegram, Discord, Feishu, Slack, OneBot, WeCom, etc.).
- `pkg/routing`: agent/session routing and key generation.
- `pkg/tools` and `pkg/skills`: built-in tools, web search/fetch, and skill registry integration.

### Development Expectations for New Contributors

- Prioritize low-memory, low-dependency implementations suitable for edge hardware.
- Keep channel, routing, provider, and tool concerns separated.
- Preserve compatibility with existing config and command behavior unless migration is explicit.
- Add or update unit tests close to changed modules (`*_test.go`), especially for config, routing, providers, and channels.

Local tools and debugging related sensitive information is saved in `.github/instructions/laisky.instructions.md`.

### Deployment

Configuration file location: `~/.picoclaw/config.json`

```sh
make install
sudo systemctl restart picoclaw-gateway.service
```

## General

Every single code file should not exceed 800 lines. If a file exceeds this limit, please split it into smaller files based on functionality. Automatically generated files are exempt from this rule.

When debugging, add targeted DEBUG logs that include essential details to help developers pinpoint hard‑to‑diagnose issues. After debugging, retain any logs that could be useful for future troubleshooting, but **never** include sensitive data like API keys or passwords in those logs.

### Agents

Multiple agents might be modifying the code at the same time. If you come across changes that aren't yours, preserve them and avoid interfering with other agents' work. Only halt the task and inform me when you encounter an irreconcilable conflict.

Should use TODOs tool to track tasks and progress.

After making any code changes, always verify that the code is correct: the syntax must be valid, the project should still build successfully(via `go vet ./...`, `go test -race ./...`, and all unit tests must pass. If any test fails, investigate whether the problem lies in the implementation or the test itself, and, respecting the user's specifications, fix the issue carefully.

### Security

Always use constant time comparison for sensitive data. Follow OWASP recommendations for password hashing iterations (minimum 10,000 in this context).

Never directly use user input or any untrusted external input to build a database query or allocate memory, to prevent injection or DoS attacks. Always sanitize and validate user inputs before using them in queries.

### TimeZone

Always use UTC for time handling in servers, databases, and APIs.

### Date Range

For any date‑range query, the handling of the ending date must encompass the entire final day. That means the database query should terminate **just before** 00:00 on the next day, ensuring that all hours of the last day are included.

### Testing

Please create suitable unit tests based on the current project circumstances. Whenever a new issue arises, update the unit tests during the fix to ensure thorough coverage of the problem by the test cases. Avoid creating temporary, one-off test scripts, and focus on continuously enhancing the unit test cases.

Use `"github.com/stretchr/testify/require"` for assertions in tests.

### Comments

Every function/interface must have a comment explaining its purpose, parameters, and return values. This is crucial for maintaining code clarity and facilitating future maintenance.
The comment should start with the function/interface name and be in complete sentences.

## Golang Style

This project is developed and run using Go 1.25. Please use the newest Go syntax and features as much as possible.

Ideally, a single file should not exceed 600 lines. Please split the overly long files according to their functionality.

### Context

Whenever feasible, utilize context to manage the lifecycle of the call chain.

## CSS Style

Avoid using `!important` in CSS. If you find yourself needing to use it, consider whether the CSS can be refactored to avoid this necessity.

Avoid inline styles in HTML or JSX. Instead, use CSS classes to manage styles. This approach promotes better maintainability and separation of concerns in your codebase.

## Web

When using the web console for debugging, avoid logging objects—they’re hard to copy. Strive to log only strings, making it simple for me to copy all the output and analyze it.

## Philosophy

You are a very strong reasoner and planner. Use these critical instructions to structure your plans, thoughts, and responses.

Before taking any action (either tool calls _or_ responses to the user), you must proactively, methodically, and independently plan and reason about:

1.  **Logical dependencies and constraints:** Analyze the intended action against the following conflicts in order of importance:
    1.  Policy-based rules, mandatory prerequisites, and constraints.
    2.  Order of operations: Ensure taking an action does not prevent a subsequent necessary action.
        1.  The user may request actions in a random order, but you may need to **reorder** operations to maximize successful completion of the task.
    3.  Other prerequisites (information and/or actions needed).
    4.  Explicit user constraints or preferences.

2.  **Risk assessment:** What are the consequences of taking this action? Will it cause any future issues?
    1.  For exploratory tasks (like searches), missing _optional_ parameters is a **LOW** risk.
    2.  **Prefer calling the tool with the available information over asking the user, unless** your 'Rule 1' (Logical Dependencies) reasoning determines that optional information is required for a later step in your plan.

3.  **Abductive reasoning and hypothesis exploration:** At each step, identify the most logical and likely reason for any problem encountered.
    1.  Look beyond immediate or obvious causes. The most likely reason may be the simplest and may require deeper inference.
    2.  Hypotheses may require additional research. Each hypothesis may take multiple steps to test.
    3.  Prioritize hypotheses based on likelihood, but do not discard less likely ones prematurely. A low-probability event may still be the root cause.

4.  **Outcome evaluation and adaptability:** Does the previous observation (based on gathered info) require any changes to your plan?
    1.  If your initial hypotheses are disproven, actively generate new ones.

5.  **Information availability:** Incorporate all applicable and alternative sources of information, including:
    1.  Using available tools and their capabilities.
    2.  All policies, rules, checklists, and constraints.
    3.  Previous observations and conversation history.
    4.  Information only available by asking the user.

6.  **Precision and Grounding:** Ensure your reasoning is extremely precise and relevant to the exact ongoing situation.
    1.  Verify your claims by quoting the exact applicable information (including policies) when referring to them.

7.  **Completeness:** Ensure that all requirements, constraints, options, and preferences are exhaustively incorporated into your plan.
    1.  Resolve conflicts using the order of importance in Rule #1.
    2.  Avoid premature conclusions: There may be multiple relevant options for a given situation.
        1.  To check for whether an option is relevant, reason from Rule #5.
        2.  You may need to consult the user to even know whether something is applicable. Do not assume it is not applicable without checking.
    3.  Review applicable sources of information from Rule #5 to confirm which are relevant to the current state.

8.  **Persistence and patience:** Do not give up unless all the reasoning above is exhausted.
    1.  Don't be dissuaded by time taken or user frustration.
    2.  This persistence must be intelligent: On _transient_ errors (e.g., "please try again"), you **must** retry **unless an explicit retry limit (e.g., max x tries) has been reached**. If such a limit is hit, you _must_ stop. On _other_ errors, you must change your strategy or arguments, not repeat the same action.

9.  **Inhibit your response:** Only take an action after all the above reasoning is completed. Once you've taken an action, you cannot take it back.
