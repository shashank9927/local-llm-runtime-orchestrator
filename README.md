# LocalLLM Runtime Orchestrator

Local-first LLM inference orchestration with priority scheduling, isolated worker processes, live token streaming, cancellation, crash recovery, Redis queues, PostgreSQL history, and a practical CLI.

This is a backend/systems portfolio project, not a chatbot UI. It wraps slow or unreliable local inference in a small control plane that remains available when an inference worker fails.

## What it demonstrates

- Fastify API with Zod validation and consistent errors
- PostgreSQL as durable generation history through Prisma
- Redis/BullMQ queues with fair `4:2:1` priority scheduling
- Real Node.js child processes, typed IPC, heartbeats, and supervision
- SSE token streaming without buffering the generated response
- Queued and in-flight cancellation
- Retry once when a worker dies before output; fail safely after output starts
- Mock and Ollama inference providers behind one interface
- Estimated model-memory budget with inactive LRU eviction
- Terminal commands for generation, status, workers, queues, models, and history
- Strict TypeScript, ESLint, Prettier, and focused Vitest coverage

## Architecture

```mermaid
flowchart TD
  CLI[Commander CLI] -->|HTTP + SSE| API[Fastify API]
  API --> DB[(PostgreSQL)]
  API --> Redis[(Redis / BullMQ)]
  API --> Scheduler[Weighted Scheduler]
  Scheduler --> Supervisor[Worker Supervisor]
  Supervisor -->|fork + IPC| W1[Worker 1]
  Supervisor -->|fork + IPC| W2[Worker 2]
  W1 --> Provider[Mock or Ollama]
  W2 --> Provider
```

The API owns orchestration state but never runs inference. Each worker is a separate Node.js process. A provider failure or worker crash therefore cannot take down the HTTP server.

## Quick start

Prerequisites: Node.js 22+, pnpm 11+, Docker, and Docker Compose.

```bash
cp .env.example .env
pnpm install
docker compose up -d postgres redis
pnpm db:generate
pnpm db:migrate
pnpm db:seed
pnpm dev
```

On PowerShell, use `Copy-Item .env.example .env` for the first command. The default provider is `mock`, so no model download is needed.

In another terminal:

```bash
pnpm cli -- status
pnpm cli -- generate --model mock:latest --priority interactive --prompt "Explain database indexing"
```

`pnpm dev` builds all packages before starting the API so the supervisor can fork the compiled worker entry point. Use `Ctrl+C` for graceful shutdown.

## CLI

```text
pnpm cli -- generate --prompt "Explain Redis" [--model mock:latest] [--priority normal]
pnpm cli -- workers
pnpm cli -- queue
pnpm cli -- models
pnpm cli -- generations [--limit 20] [--status completed] [--priority interactive]
pnpm cli -- generation <id>
pnpm cli -- cancel <id>
pnpm cli -- status
pnpm cli -- worker kill <worker-id>
```

Set `API_BASE_URL` or pass global `--api-url` before the command to target a different API.

## Mock provider

The mock provider streams a deterministic response one token at a time and supports realistic delay, failure, timeout, and cancellation paths.

```env
INFERENCE_PROVIDER=mock
MOCK_TOKEN_DELAY_MS=75
MOCK_GENERATION_DELAY_MS=100
MOCK_FAILURE_RATE=0
```

It accepts the seeded demonstration models (`mock:latest`, `llama3.2:3b`, and `qwen2.5:3b`) without requiring model files. This makes CI and crash demos repeatable.

## Ollama provider

Run Ollama on the host so it can use the machine's GPU directly:

```bash
ollama serve
ollama pull llama3.2:3b
```

Then set:

```env
INFERENCE_PROVIDER=ollama
OLLAMA_BASE_URL=http://localhost:11434
```

Ollama's newline-delimited streaming response is forwarded chunk-by-chunk through worker IPC and SSE. PostgreSQL and Redis stay in Docker; Ollama does not need to.

## Scheduling

There are three FIFO BullMQ queues. The scheduler visits them in a repeating cycle:

```text
interactive → interactive → interactive → interactive → normal → normal → background
```

This gives interactive work more opportunities while guaranteeing that a continuously busy interactive queue cannot permanently starve background work. A small lock serializes `nextJob()` so two newly idle workers cannot claim the same job. Queue size is bounded globally and separately for interactive work; overflow returns HTTP 429 with `QUEUE_FULL`.

## Worker lifecycle and recovery

```mermaid
sequenceDiagram
  participant W as Worker
  participant S as Supervisor
  participant O as Orchestrator
  participant Q as Scheduler
  participant A as API/SSE
  W--xS: process exits
  S->>S: mark UNHEALTHY
  S->>O: crash(worker, generation, token count)
  alt no token emitted and no retry used
    O->>Q: requeue once
    O->>A: generation.queued (retry)
  else output already streamed or retry used
    O->>A: generation.failed
  end
  S->>S: exponential backoff (1s...10s)
  S->>W: fork replacement
  W->>S: worker.ready
  S->>Q: request next job
```

Workers heartbeat every two seconds by default. After six seconds without a heartbeat, the supervisor stops assigning work, kills the unhealthy process, and creates a replacement. Restart count and transient states are visible through both the API and CLI.

The retry rule deliberately avoids silently regenerating after a token reached the user: a second run could produce different output and duplicate the stream.

See [docs/failure-demo.md](docs/failure-demo.md) for the three-terminal recovery demonstration.

## Streaming and cancellation

`GET /api/v1/generations/:id/stream` is an SSE endpoint. Events include queueing, worker assignment, start, token, completion, failure, and cancellation. The server sends heartbeat comments and removes listeners when a client disconnects. A small in-process buffer closes the POST-to-SSE connection race; durable stream replay and `Last-Event-ID` are intentionally outside V1.

Queued cancellation removes the BullMQ job before marking the database row `CANCELLED`. Running cancellation sends typed IPC to the assigned child, which aborts the provider request and reports the terminal state.

## Model memory budget

The model manager tracks estimated memory, last use, load state, and active request count. Before a generation starts it:

1. checks the model estimate against `MODEL_MEMORY_BUDGET_MB`;
2. evicts least-recently-used inactive models until it fits;
3. never evicts a model with an active generation;
4. returns `MODEL_MEMORY_LIMIT` when enough memory cannot be freed.

The values are estimates used for orchestration policy; this project does not claim exact GPU allocation control.

## Prompt privacy

With `STORE_PROMPTS=false`, PostgreSQL stores only the SHA-256 prompt hash and character length. The prompt still exists temporarily in Redis and worker memory because inference requires it, and BullMQ removes the job when claimed. PostgreSQL remains the durable source of truth.

## API summary

```text
POST   /api/v1/generations
GET    /api/v1/generations
GET    /api/v1/generations/:id
GET    /api/v1/generations/:id/stream
POST   /api/v1/generations/:id/cancel
GET    /api/v1/workers
GET    /api/v1/models
GET    /api/v1/queue
GET    /api/v1/status
GET    /health/live
GET    /health/ready
POST   /api/v1/dev/workers/:id/kill
```

The kill route is never registered when `NODE_ENV=production`.

## Testing and quality

```bash
pnpm typecheck
pnpm lint
pnpm test
pnpm build
pnpm format:check
```

Tests cover state transitions, weighted fairness/FIFO/capacity/cancellation, provider streaming/failure/abort behavior, memory eviction, SSE listener cleanup, worker health/restart policy, and CLI request/error behavior. The application uses PostgreSQL and Redis in production; fast behavioral tests use controlled adapters, while the opt-in database suite exercises the Prisma repository against a real PostgreSQL URL.

To run the real database suite against an isolated disposable database, set `TEST_DATABASE_URL` to that database and run the targeted suite. In PowerShell:

```bash
$env:TEST_DATABASE_URL = 'postgresql://USER:PASSWORD@HOST:PORT/DATABASE?schema=public'
pnpm exec vitest run packages/database/src/database.integration.test.ts
```

## Project layout

```text
apps/api       Fastify routes, orchestration, event hub, supervisor
apps/worker    isolated inference child process
apps/cli       Commander terminal client and SSE parser
packages/contracts   shared domain, validation, errors, IPC and SSE types
packages/database    Prisma schema, migration, seed, repository
packages/inference   mock/Ollama providers and model memory manager
packages/scheduler   BullMQ adapter and weighted scheduler
packages/shared      configuration, logging, IDs, small utilities
docs                 tradeoffs, demo, interview and CV material
```

## Tradeoffs and limits

This is a deliberately single-machine V1. Scheduler/event-buffer state belongs to one API process, model memory is estimated, and SSE replay is not durable. It has no authentication because it is a loopback-oriented developer tool; do not expose the development API to an untrusted network. See [docs/tradeoffs.md](docs/tradeoffs.md) for the rationale and possible next steps.
