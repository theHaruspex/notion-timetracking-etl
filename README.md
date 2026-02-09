# Time Tracking ETL (v1)

Small TypeScript (Node.js) data integration service that reads from Notion and normalizes into three canonical datasets.

## What it does

- Pulls Notion database schemas + pages for:
  - `workflow_definitions`
  - `workflow_stages`
  - `timeslices`
- Captures raw records in a vendor-agnostic `RawRecord` shape.
- Persists raw pulls as JSONL under `data/raw/...`.
- Normalizes raw records into canonical objects and writes JSONL under `data/canon/...`.

## Out of scope (intentionally not implemented)

- Events and events config
- Workflow records
- Webhook ingestion
- PowerBI client/export logic
- Daily wipe/reload orchestration
- Advanced logging framework

## Setup

1. Use Node 20+
2. Install dependencies:

```bash
npm install
```

3. Copy env template and fill values:

```bash
cp .env.example .env
```

## Environment variables

Required:

- `NOTION_TOKEN`
- `NOTION_DB_WORKFLOW_DEFINITIONS`
- `NOTION_DB_WORKFLOW_STAGES`
- `NOTION_DB_TIMESLICES`

Optional:

- `DATA_DIR` (default `./data`)
- `LOG_LEVEL` (parsed only, not used for filtering yet)

## Commands

- Pull raw data from Notion:

```bash
npm run cli -- pull:notion
```

- Normalize latest raw data into canonical data:

```bash
npm run cli -- normalize
```

- Run pull + normalize:

```bash
npm run cli -- run
```

- Tooling:

```bash
npm run test
npm run lint
npm run build
npm run format
```

## Project structure

```text
src/
  cli/
  ingress/
  normalize/
  canon/
  sinks/
  clients/
  config/
  lib/
  test/
```

Role-based module naming is used (`ingress`, `normalize`, `sinks`). Vendor-specific code is isolated in `src/clients/`.
