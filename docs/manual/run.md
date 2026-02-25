
# PicoClaw Administrator Operation Manual (VM + systemd)

This document is intended for system administrators and operations personnel. Its goal is to enable you to install, configure, manage with systemd, verify operation, perform daily maintenance, and troubleshoot PicoClaw on a Linux VM using only this manual.

## 1. Project Purpose and Operations Perspective

PicoClaw is a lightweight AI Assistant runtime written in Go. From an operations perspective, its core values are:

- Stable, persistent operation as a single binary
- Receives Telegram messages and triggers Agent task execution
- Retains session, memory, skills, scheduled tasks, and other state
- Provides observability, recoverability, and maintainability via health check endpoints and systemd

## 2. Architecture Overview (Must Understand)

PicoClaw has two operating modes:

1. `picoclaw agent`
    - Direct interaction mode (`-m` for one-shot message or interactive terminal)
    - Suitable for debugging, model/tool validation, manual operations
    - **Not** a persistent process for Telegram external service

2. `picoclaw gateway`
    - Persistent service mode
    - Starts message bus, channel adapters (including Telegram), AgentLoop, cron, heartbeat, health checks
    - **Only this should be persistent in production**

### 2.1 Key Question: Do you need to start both gateway and agent?

**Conclusion: No. Only keep `gateway` persistent.**

Reason:

- `gateway` internally creates and runs AgentLoop on startup
- Telegram messages are processed via `gateway -> channel manager -> bus -> AgentLoop`
- The `agent` command is only for local direct debugging, not for persistent channel-side service

### 2.2 How to determine if "agent is running"?

In this project, "agent is running" is equivalent to "AgentLoop inside gateway is working properly." You can determine this by:

1. `systemctl status picoclaw-gateway` shows `active (running)`
2. `journalctl -u picoclaw-gateway -f` shows Agent initialization info (tools/skills stats) in the startup logs
3. `curl http://<gateway_host>:<gateway_port>/ready` returns ready
4. Sending a message to the bot on Telegram receives a response

## 3. Deployment Prerequisites and Constraints

### 3.1 System Prerequisites

- Linux (managed by systemd)
- Network access to:
    - Telegram API
    - Your model API (OpenAI/OpenRouter/self-hosted gateway, etc.)
    - Optional skills registry (e.g., ClawHub)
- PicoClaw binary installed or buildable

### 3.2 Configuration File Path (Very Important)

CLI reads by default:

- `~/.picoclaw/config.json`

Not the one in the repository:

- `config/config.json`

Therefore, you **must** place the runtime config at `~/.picoclaw/config.json` on the VM (see standard process below).

## 4. Installation and Initialization (Non-Docker)

Assume the current repo directory is `/home/bot/repo/bot/picoclaw`.

### 4.1 Build the Binary

```bash
cd /home/bot/repo/bot/picoclaw
make build
```

Default output:

- `build/picoclaw` (usually a symlink to the target architecture binary)

### 4.2 Prepare Runtime Configuration

1. Prepare the main config:

```bash
mkdir -p ~/.picoclaw
cp /home/bot/repo/bot/picoclaw/config/config.json ~/.picoclaw/config.json
chmod 600 ~/.picoclaw/config.json
```

2. It is recommended to check the following key items:

- `agents.defaults.model_name`
- `model_list[*]` (model name, api_key, api_base)
- `channels.telegram.enabled=true`
- `channels.telegram.token` is correct
- `channels.telegram.allow_from` only contains authorized accounts
- `gateway.host` / `gateway.port`

### 4.3 Minimal Telegram Configuration Example

```json
{
    "channels": {
        "telegram": {
            "enabled": true,
            "token": "<bot token>",
            "allow_from": ["<your_user_id_or_username>"]
        }
    }
}
```

Notes:

- If `allow_from` is an empty array, all users are allowed (not recommended for production)
- It is recommended to whitelist only your own user id/username

## 5. systemd Management Solution (Recommended)

Goal: Keep essential services running long-term, auto-restart on failure, enable on boot.

### 5.1 Service Unit File

Path: `/etc/systemd/system/picoclaw-gateway.service`

Recommended content:

```ini
[Unit]
Description=PicoClaw Gateway Service
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=bot
Group=bot
WorkingDirectory=/home/bot/repo/bot/picoclaw
Environment=HOME=/home/bot
ExecStart=/home/bot/repo/bot/picoclaw/build/picoclaw gateway
Restart=always
RestartSec=5
TimeoutStopSec=20
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=full
ReadWritePaths=/home/bot/.picoclaw

[Install]
WantedBy=multi-user.target
```

> Notes:
>
> - Only keep `gateway` persistent, do **not** keep `agent` persistent
> - `ReadWritePaths=/home/bot/.picoclaw` allows state writes to the working directory
> - If you change the run user/directory, also update `User`, `HOME`, `WorkingDirectory`, `ExecStart` accordingly

### 5.2 Start and Enable on Boot

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now picoclaw-gateway.service
```

### 5.3 Verify Running Status

```bash
systemctl status picoclaw-gateway --no-pager
journalctl -u picoclaw-gateway -n 200 --no-pager
curl -sS http://<gateway_host>:<gateway_port>/health
curl -sS http://<gateway_host>:<gateway_port>/ready
```

## 6. Daily Operations Manual

### 6.1 Common Commands

```bash
# Check status
systemctl status picoclaw-gateway --no-pager

# Restart
sudo systemctl restart picoclaw-gateway

# Stop
sudo systemctl stop picoclaw-gateway

# Enable/disable on boot
sudo systemctl enable picoclaw-gateway
sudo systemctl disable picoclaw-gateway

# Real-time logs
journalctl -u picoclaw-gateway -f
```

### 6.2 Upgrade Procedure

```bash
cd /home/bot/repo/bot/picoclaw
git pull
make build
sudo systemctl restart picoclaw-gateway
```

After upgrading, be sure to verify:

- `systemctl status` is active
- `/ready` is normal
- Telegram round-trip messaging works

### 6.3 Backup Recommendations

Key items to back up:

- `~/.picoclaw/config.json`
- `~/.picoclaw/workspace/` (sessions/memory/state/cron/skills)

Recommendations:

- Back up before each upgrade
- Perform regular incremental backups

## 7. Skills Operations (Non-Docker)

Skills management does not require stopping the service, but it is recommended to perform during off-peak hours.

### 7.1 Common Commands

```bash
/home/bot/repo/bot/picoclaw/build/picoclaw skills list
/home/bot/repo/bot/picoclaw/build/picoclaw skills search
/home/bot/repo/bot/picoclaw/build/picoclaw skills install owner/repo/skill-name
/home/bot/repo/bot/picoclaw/build/picoclaw skills show skill-name
/home/bot/repo/bot/picoclaw/build/picoclaw skills remove skill-name
```

Skills loading priority:

1. `workspace/skills`
2. `~/.picoclaw/skills`
3. Built-in skills

## 8. Troubleshooting Guide

### 8.1 Telegram No Response

Check in order:

1. `systemctl status picoclaw-gateway`
2. `journalctl -u picoclaw-gateway -n 200`
3. `channels.telegram.enabled/token/allow_from` in config
4. VM outbound connectivity (Telegram API and model API)

### 8.2 Service running but `/ready` not normal

- Check logs for provider initialization errors or channel initialization failures
- Check if `model_name` matches `model_list`
- Check if API key is expired

### 8.3 Service frequently restarts

- Check the first error in `journalctl -u picoclaw-gateway -f`
- Check for config path errors (most common: missing `~/.picoclaw/config.json`)
- Check for port conflicts and network restrictions

## 9. Final Implementation Checklist

Before going live, ensure all are checked:

- [ ] `~/.picoclaw/config.json` is in place and has correct permissions
- [ ] Telegram whitelist is restricted
- [ ] `picoclaw-gateway.service` is enabled and active
- [ ] `/health` and `/ready` are normal
- [ ] Telegram message loop is verified
- [ ] Backup strategy is in place

## 10. One-Sentence Conclusion (Answers to Your Core Questions)

- **Do you need to start both gateway and agent?** No.
- **Which should be persistent in production?** Only `gateway`.
- **How to determine if agent is running?** Check `gateway` running status + `/ready` + Telegram actual response, because the agent loop is inside the gateway process.
