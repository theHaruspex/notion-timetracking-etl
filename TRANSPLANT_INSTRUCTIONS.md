# Instructions for Transplanting Power BI SDK to Another Repository

This document provides step-by-step instructions for copying the `power-bi-sdk` into a new workspace and integrating it.

## Overview

The `power-bi-sdk` is a **vendored copy** (not a Git submodule) of the Power BI Push API SDK. It's a complete, self-contained TypeScript package that can be copied directly into any project.

## Step 1: Copy the SDK Directory

### Option A: Clone from GitHub (if pushed)
```bash
# In your new workspace root
git clone https://github.com/theHaruspex/power-bi-sdk.git sdks/power-bi-sdk
cd sdks/power-bi-sdk
# Checkout the commit with the updated README
git checkout a13e1926e1df4fa8243207c1b189951204319a07
```

### Option B: Copy from This Repository
```bash
# From the source repository
cp -r sdks/power-bi-sdk /path/to/new/workspace/sdks/power-bi-sdk

# Or if you want to preserve git history:
cd /path/to/source/repo
git archive --format=tar --prefix=power-bi-sdk/ HEAD:sdks/power-bi-sdk | \
  (cd /path/to/new/workspace/sdks && tar xf -)
```

## Step 2: Install Dependencies

```bash
cd sdks/power-bi-sdk
npm install
```

**Required runtime dependencies:**
- `@azure/msal-node` (^2.16.2 or compatible)
- `undici` (^6.19.8 or compatible, Node 18+ has native fetch)

**Development dependencies** (for testing/tooling):
- TypeScript, Vitest, ESLint, Prettier (see `package.json` for versions)

## Step 3: Build the SDK

```bash
cd sdks/power-bi-sdk
npm run build
```

This compiles TypeScript to `dist/` directory.

## Step 4: Configure Environment Variables

Copy `env.example` to `.env` and fill in your Azure AD credentials:

```bash
cd sdks/power-bi-sdk
cp env.example .env
```

Required variables:
```bash
PBI_TENANT_ID=your-tenant-id-here
PBI_CLIENT_ID=your-client-id-here
PBI_CLIENT_SECRET=your-client-secret-here
PBI_GROUP_ID=your-workspace-group-id-here  # Optional, for tests/tools
```

## Step 5: Integrate into Your Project

### Option A: Import via Relative Path (Monorepo Style)

```typescript
// In your project code
import { PowerBiClient } from "./sdks/power-bi-sdk/src/index.js";
// or if built:
import { PowerBiClient } from "./sdks/power-bi-sdk/dist/index.js";
```

### Option B: Link as Local Package

```bash
# In sdks/power-bi-sdk
npm link

# In your project root
npm link @the_haruspex/powerbi-realtime
```

Then import:
```typescript
import { PowerBiClient } from "@the_haruspex/powerbi-realtime";
```

### Option C: Publish to Private Registry

If you have a private npm registry:
```bash
cd sdks/power-bi-sdk
npm publish --registry=https://your-registry.com
```

Then install normally:
```bash
npm install @the_haruspex/powerbi-realtime --registry=https://your-registry.com
```

## Step 6: Basic Usage Example

```typescript
import { PowerBiClient } from "./sdks/power-bi-sdk/src/index.js";

const client = new PowerBiClient(
  {
    tenantId: process.env.PBI_TENANT_ID!,
    clientId: process.env.PBI_CLIENT_ID!,
    clientSecret: process.env.PBI_CLIENT_SECRET!
  },
  {
    userAgent: "your-app/1.0.0",
    logger: (msg, ctx) => console.log(`[PBI] ${msg}`, ctx)
  }
);

// Push rows (automatically batched and rate-limited)
await client.postRows("workspace-id", "dataset-id", "TableName", [
  { column1: "value1", column2: 123 }
]);

// Or use the sink abstraction
const sink = client.getPushSink();
await sink.pushRows({
  groupId: "workspace-id",
  datasetId: "dataset-id",
  table: "TableName",
  rows: [{ column1: "value1", column2: 123 }]
});
```

## Step 7: Verify Installation

Run the diagnostics to verify your setup:

```bash
cd sdks/power-bi-sdk
npm run diag:auth      # Test authentication
npm run diag:workspace # Test workspace access
npm run test           # Run unit tests
```

## Troubleshooting

### TypeScript Module Resolution Issues

If you get import errors, ensure your `tsconfig.json` includes:

```json
{
  "compilerOptions": {
    "moduleResolution": "node",
    "esModuleInterop": true,
    "allowSyntheticDefaultImports": true
  },
  "include": [
    "sdks/power-bi-sdk/src/**/*"
  ]
}
```

### Node.js Version

Requires Node.js **18+** (for native `fetch` support). Check with:
```bash
node --version  # Should be v18.0.0 or higher
```

### Missing Dependencies

If you see module not found errors:
```bash
cd sdks/power-bi-sdk
rm -rf node_modules package-lock.json
npm install
```

## Key Files Reference

- **`src/index.ts`**: Main exports (PowerBiClient, types, errors)
- **`src/lib/client.ts`**: Core PowerBiClient implementation
- **`src/lib/auth.ts`**: MSAL authentication provider
- **`src/lib/http.ts`**: HTTP client with retries
- **`src/lib/rateLimiter.ts`**: Rate limiting logic
- **`src/lib/sinks/pushSink.ts`**: Push dataset sink implementation
- **`README.md`**: Full documentation
- **`env.example`**: Environment variable template

## Next Steps

1. Review `README.md` for complete API documentation
2. Check `reports/powerbi-realtime-sdk-spec.txt` for design decisions
3. Run integration tests with your Power BI workspace
4. Integrate into your ETL workflows

## Notes

- The SDK is **self-contained**—no external runtime dependencies beyond `@azure/msal-node` and `undici`
- All Power BI API rate limits are enforced automatically
- The SDK handles token refresh, retries, and batching transparently
- String length validation (4K limit) is enforced before API calls
