# Agents Guidelines

**No matter what language you receive, Only using English as output for codes, comments, chat, documents and everything else.**

Local tools and debugging related sensitive information is saved in .github/instructions/laisky.instructions.md.

## This Project

PicoClaw is a Go-first, ultra-lightweight personal AI assistant designed to run on constrained Linux devices. The repository is centered around a single CLI binary (`picoclaw`) that supports onboarding, agent runtime, gateway mode, auth, cron, migration, and skills management.

### Operating Modes: Agent vs Gateway

PicoClaw has two primary runtime modes, both sharing the same `pkg/agent` core logic but serving different deployment goals:

- `picoclaw agent`: direct single-process interaction mode for local usage and debugging.
- `picoclaw gateway`: long-running service mode for production-like operation.

### Deployment

Users communicate with the agent through Telegram, Discord, Slack, Feishu, WeCom, OneBot, and other channels. The agent has sudo privileges on the host machine and should follow user instructions, exercising full control over the host to accomplish user commands.

Configuration file location: `~/.picoclaw/config.json`

Agent's working directory is `~/.picoclaw/workspace/`, and you can read/write files here to store important information or accomplish tasks.

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
