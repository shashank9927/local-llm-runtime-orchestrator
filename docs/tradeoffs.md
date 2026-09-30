# Technical tradeoffs

## SSE instead of WebSockets

Generation output is one-directional after the HTTP request: server to client. SSE works over ordinary HTTP, reconnect behavior is familiar, events are readable during debugging, and the CLI only needs a small parser. WebSockets would add a second bidirectional protocol without solving a V1 requirement. Durable replay is left for a future version.

## Redis and PostgreSQL have different jobs

BullMQ/Redis holds temporary executable work and coordinates queue operations quickly. PostgreSQL stores durable generation history and timing/error metadata. Redis is not treated as a permanent system of record, and PostgreSQL is not polled as a queue.

## Child processes instead of worker threads

Inference adapters are isolated behind an OS process boundary. A worker exit is observable and replaceable without losing the API. Worker threads are lighter but share a process and make the failure boundary less obvious. The cost is IPC serialization and a little more memory, appropriate for a small number of heavyweight inference workers.

## BullMQ instead of a custom Redis queue

BullMQ supplies durable job storage, IDs, inspection, and safe Redis primitives. The project keeps scheduling policy in a small explicit layer and uses three BullMQ queues so weighted fairness stays understandable. Building queue persistence from raw Redis commands would add risk without teaching the orchestration concepts better.

## Weighted scheduling instead of strict priority

Strict priority gives excellent interactive latency until interactive work is continuous; then normal and background work can wait forever. A repeating 4:2:1 cycle favors latency-sensitive jobs but guarantees lower priorities execution opportunities. FIFO is preserved inside each priority.

## Ollama instead of an inference engine

The engineering target is orchestration: queueing, streaming, isolation, resource policy, and recovery. Ollama supplies a practical local model API and hardware support. Reimplementing transformer inference would overwhelm the backend lessons and be much harder to explain honestly.

## CLI instead of a browser frontend

A terminal interface matches a local developer-infrastructure tool, makes streaming and failure demonstrations fast, and keeps the repository centered on backend behavior. A monitoring page could be added later without changing the API.

## Single API process

The event hub and supervisor are process-local. This keeps ownership and cancellation clear on one developer machine. Multi-node operation would require leader election, durable event replay, globally coordinated workers, and probably a different scheduler boundary. Those are valid future features, not hidden claims of this V1.
