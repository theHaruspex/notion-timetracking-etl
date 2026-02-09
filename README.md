# Time Tracking ETL (v1)

Small TypeScript (Node.js) data integration service that reads from Notion and normalizes into three canonical datasets.

## What it does

- Pulls Notion database schemas + pages for:
  - `workflow_definitions`
  - `workflow_stages`
  - `timeslices`
- Captures raw records in a vendor-agnostic `RawRecord` shape (all properties, keyed by property ID).
- Persists raw pulls as JSONL under `data/raw/...`.
- Normalizes raw records into canonical objects and writes JSONL under `data/canon/...`.
- Uses deterministic canonical extraction via configured Notion **property IDs**.

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

Required (`.env` secrets-only):

- `NOTION_TOKEN`

Database IDs are hardcoded in `src/config/env.ts`.

## Notion schema audit + property ID setup

1. Audit the three configured databases and generate schema artifacts:

```bash
npm run cli -- audit:notion-schema
```

This command will:

- print `propertyName | propertyId | type` for each database
- write machine-readable schema to `data/audit/notion-schema.json`
- generate/update `src/config/notionSchema.generated.ts`

2. Fill property ID bindings in `src/config/env.ts` under `notionConfig.propertyIds`.

Normalization fails fast with a clear error if required property IDs are missing.

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
