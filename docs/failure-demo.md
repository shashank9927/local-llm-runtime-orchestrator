# Worker recovery demonstration

Start PostgreSQL and Redis, migrate once, and start the runtime:

```bash
docker compose up -d postgres redis
pnpm db:migrate
pnpm db:seed
pnpm dev
```

In terminal 2, slow the mock provider if needed (`MOCK_TOKEN_DELAY_MS=300`) and start a generation:

```bash
pnpm cli -- generate --model mock:latest --priority interactive --prompt "Explain indexes in enough detail to keep the worker busy"
```

In terminal 3, inspect workers and kill the busy one:

```bash
pnpm cli -- workers
pnpm cli -- worker kill worker-1
```

The kill command calls the development-only endpoint, which sends `SIGKILL` to the real child process. Expected behavior:

```text
worker-1: BUSY → UNHEALTHY → RESTARTING → IDLE
restart count: 0 → 1
API: remains healthy
```

If no token had been emitted, the generation is requeued once. If output had already reached the CLI, it fails with an explanation instead of silently generating a different continuation. Run `pnpm cli -- generations` and `pnpm cli -- generation <id>` to inspect the durable result.

The endpoint is absent in production mode. To repeat the demo, use development mode and a worker ID returned by `pnpm cli -- workers`.
