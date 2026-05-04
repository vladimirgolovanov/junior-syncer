# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Build
go build -o junior-syncer .

# Run locally (requires .env file)
go run main.go

# Build for production (matches Dockerfile)
CGO_ENABLED=0 go build -ldflags="-s -w" -o app .
```

No test suite exists in this repo.

## Architecture

Single-file Go service (`main.go`) that bridges a Telegram bot and RabbitMQ. It does two things concurrently:

1. **Poller** (main goroutine): Long-polls the Telegram Bot API (`getUpdates` with `timeout=30`) and publishes incoming/edited messages as `RabbitMessage` JSON to the `RABBITMQ_TG_EVENTS_QUEUE`.

2. **Send consumer** (goroutine): Consumes `SendCommand` messages from `RABBITMQ_TG_COMMANDS_QUEUE`, calls `sendMessage` or `editMessageText` on Telegram, then publishes a `SendResponse` (mapping internal ID → Telegram message ID) to `RABBITMQ_TG_COMMANDS_RESPONSES_QUEUE`.

Queue declarations use `amqp091-go` directly on startup because `go-rabbitmq` (the higher-level wrapper used for publishing/consuming) does not declare queues on the publisher side.

## Configuration

All config is via environment variables (loaded from `.env` via `godotenv`):

| Variable | Purpose |
|---|---|
| `TELEGRAM_BOT_TOKEN` | Bot token from @BotFather |
| `RABBITMQ_URL` | AMQP connection string |
| `RABBITMQ_TG_EVENTS_QUEUE` | Queue where inbound TG messages are published |
| `RABBITMQ_TG_COMMANDS_QUEUE` | Queue where send/edit commands are consumed |
| `RABBITMQ_TG_COMMANDS_RESPONSES_QUEUE` | Queue where create-message responses are published |
| `TELEGRAM_IGNORE_USER_ID` | Optional: numeric Telegram user ID to filter out |

