# simplenote-sync learn-core

Simplenote -> Notion single-direction sync service based on Simperium realtime events.

## Features

- Realtime note update ingestion from Simplenote
- SQLite local state and event log
- Idempotent Notion upsert by Simplenote ID
- Deleted sync behavior via Notion `Deleted` checkbox
- Optional dual-write mode: preview to properties and full body to page blocks
- Retry backoff with max retry cap to avoid API storm
- Version gating to block stale events
- Optional health endpoint and periodic metrics logs

## Requirements

- Node.js 18+
- A Notion integration token with access to target database
- Simplenote Simperium app id and token

## Quick Start

1. Install dependencies:

```bash
npm install
```

2. Configure environment in `.env`:

```dotenv
SIMPERIUM_APP_ID=...
SIMPERIUM_TOKEN=...
NOTION_TOKEN=...
NOTION_DATABASE_ID=...

QUIET_WINDOW_MS=15000
DELETE_QUIET_WINDOW_MS=5000
DISPATCH_INTERVAL_MS=2000

RETRY_BASE_MS=2000
RETRY_MAX_MS=300000
MAX_RETRY_COUNT=8

USE_PAGE_BLOCKS=false

SOURCE_EVENT_INFO_EVERY=20
MAX_RAW_EVENT_BYTES=65536

HEALTH_PORT=0
METRICS_LOG_ENABLED=true
METRICS_LOG_INTERVAL_MS=60000
UNAUTHORIZED_EXIT_DELAY_MS=3000

NOTE_EVENTS_RETENTION_DAYS=30
NOTE_EVENTS_PURGE_INTERVAL_MS=3600000
```

When `USE_PAGE_BLOCKS=true`, each sync writes both:

- `Content Preview` property (for quick database filtering)
- full note body in page blocks (for full-text reading in page content)

3. Start service:

```bash
npm start
```

4. Run tests:

```bash
npm test
```

## Ubuntu PM2 One-Click Setup

Use the helper script to install dependencies, configure PM2, enable startup, and setup log rotation:

```bash
chmod +x scripts/setup-pm2.sh
./scripts/setup-pm2.sh
```

Optional environment overrides:

```bash
APP_NAME=simplenote-sync MAX_MEMORY=700M ./scripts/setup-pm2.sh
```

## Notion Database Requirements

The target Notion database (or data source) should include:

- a title property (any name)
- `Simplenote ID` (rich_text)
- `Content Preview` (rich_text)
- `Deleted` (checkbox)
- `Tags` (multi_select)
- `Created Time` (date)
- `Synced At` (date)

## Health And Metrics

- Health endpoint is enabled when `HEALTH_PORT > 0`:
  - `GET /healthz`
- Periodic metrics log includes:
  - loops, selected, skippedByLock, synced, deleted, failed, gaveUp

## Status Lifecycle

`notes.status` values:

- `DIRTY`
- `SYNCING`
- `SYNCED`
- `FAILED`
- `GAVE_UP`
- `DELETED`
