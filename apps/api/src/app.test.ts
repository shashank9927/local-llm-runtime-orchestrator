import { resolve } from 'node:path';
import { spawn } from 'node:child_process';
import { describe, expect, it } from 'vitest';
import {
  assertTransition,
  type Generation,
  type GenerationPriority,
  type GenerationStatus,
} from '@localllm/contracts';
import type {
  CreateGenerationRecord,
  GenerationListOptions,
  GenerationStore,
} from '@localllm/database';
import { MockInferenceProvider, ModelMemoryManager } from '@localllm/inference';
import { WeightedScheduler, type QueueBackend, type QueueJobData } from '@localllm/scheduler';
import { createLogger, loadConfig } from '@localllm/shared';
import { buildApp, type RuntimeDependencies } from './app.js';
import { GenerationEventHub } from './event-hub.js';
import { GenerationOrchestrator } from './orchestrator.js';
import { WorkerSupervisor } from './supervisor.js';

class TestQueueBackend implements QueueBackend {
  readonly queues: Record<GenerationPriority, QueueJobData[]> = {
    interactive: [],
    normal: [],
    background: [],
  };

  async add(priority: GenerationPriority, job: QueueJobData): Promise<void> {
    this.queues[priority].push(job);
  }
  async take(priority: GenerationPriority): Promise<QueueJobData | null> {
    return this.queues[priority].shift() ?? null;
  }
  async remove(id: string): Promise<boolean> {
    for (const queue of Object.values(this.queues)) {
      const index = queue.findIndex((job) => job.generationId === id);
      if (index >= 0) {
        queue.splice(index, 1);
        return true;
      }
    }
    return false;
  }
  async count(priority: GenerationPriority): Promise<number> {
    return this.queues[priority].length;
  }
  async close(): Promise<void> {}
}

class TestStore implements GenerationStore {
  readonly records = new Map<string, Generation>();

  async create(input: CreateGenerationRecord): Promise<Generation> {
    const generation: Generation = {
      id: input.id,
      model: input.model,
      priority: input.priority,
      status: 'CREATED',
      prompt: input.storePrompt ? input.prompt : null,
      promptHash: 'test-hash',
      promptLength: input.prompt.length,
      createdAt: new Date(),
      queuedAt: null,
      startedAt: null,
      completedAt: null,
      failedAt: null,
      cancelledAt: null,
      workerId: null,
      errorMessage: null,
      tokenCount: 0,
      durationMs: null,
      queueWaitMs: null,
      timeToFirstTokenMs: null,
      retryCount: 0,
    };
    this.records.set(generation.id, generation);
    return generation;
  }
  async get(id: string): Promise<Generation | null> {
    return this.records.get(id) ?? null;
  }
  async list(options: GenerationListOptions): Promise<Generation[]> {
    return [...this.records.values()]
      .filter((item) => !options.status || item.status === options.status)
      .filter((item) => !options.priority || item.priority === options.priority)
      .slice(0, options.limit);
  }
  async transition(
    id: string,
    to: GenerationStatus,
    data: Partial<Generation> = {},
  ): Promise<Generation> {
    const current = this.records.get(id);
    if (!current) throw new Error('missing test record');
    assertTransition(current.status, to);
    const next = { ...current, ...data, status: to };
    if (to === 'QUEUED') next.queuedAt = new Date();
    if (to === 'CANCELLED') next.cancelledAt = new Date();
    this.records.set(id, next);
    return next;
  }
  async incrementRetry(id: string): Promise<Generation> {
    const current = this.records.get(id);
    if (!current) throw new Error('missing test record');
    const next = { ...current, retryCount: current.retryCount + 1 };
    this.records.set(id, next);
    return next;
  }
  async ping(): Promise<boolean> {
    return true;
  }
  async disconnect(): Promise<void> {}
}

async function createTestApp(
  maxQueueSize = 10,
  workerCount = 0,
  mockTokenDelayMs = 0,
  mockGenerationDelayMs = 0,
  generationTimeoutMs = 120_000,
) {
  const config = loadConfig({ NODE_ENV: 'test', LOG_LEVEL: 'silent' });
  const store = new TestStore();
  const scheduler = new WeightedScheduler(new TestQueueBackend(), maxQueueSize, maxQueueSize);
  const logger = createLogger(config);
  const supervisor = new WorkerSupervisor(
    workerCount > 0 ? resolve(process.cwd(), 'apps/worker/dist/main.js') : 'unused-in-test',
    workerCount,
    config.WORKER_HEARTBEAT_TIMEOUT_MS,
    'mock',
    {
      ollamaBaseUrl: config.OLLAMA_BASE_URL,
      mockTokenDelayMs,
      mockGenerationDelayMs,
      mockFailureRate: 0,
      heartbeatIntervalMs: config.WORKER_HEARTBEAT_INTERVAL_MS,
      generationTimeoutMs,
    },
    logger,
  );
  const memory = new ModelMemoryManager(1000);
  memory.register('mock:latest', 128);
  const provider = new MockInferenceProvider({
    tokenDelayMs: mockTokenDelayMs,
    generationDelayMs: mockGenerationDelayMs,
    failureRate: 0,
  });
  const orchestrator = new GenerationOrchestrator(
    store,
    scheduler,
    supervisor,
    new GenerationEventHub(),
    memory,
    logger,
  );
  const dependencies: RuntimeDependencies = {
    store,
    scheduler,
    queueHealth: async () => true,
    provider,
    orchestrator,
  };
  const app = await buildApp(config, dependencies);
  await app.ready();
  return { app, store, orchestrator };
}

async function readSse(response: Response): Promise<Array<{ type: string; data: unknown }>> {
  if (!response.body) throw new Error('Expected SSE body');
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  const events: Array<{ type: string; data: unknown }> = [];
  let buffer = '';
  while (true) {
    const { value, done } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });
    const blocks = buffer.split('\n\n');
    buffer = blocks.pop() ?? '';
    for (const block of blocks) {
      const event = block.split('\n').find((line) => line.startsWith('event: '));
      const data = block.split('\n').find((line) => line.startsWith('data: '));
      if (event && data) events.push({ type: event.slice(7), data: JSON.parse(data.slice(6)) });
    }
  }
  return events;
}

async function waitFor(predicate: () => boolean, timeoutMs = 5_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error('Timed out waiting for expected state.');
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}

async function runCli(
  origin: string,
  ...args: string[]
): Promise<{ code: number | null; output: string }> {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, ['apps/cli/dist/main.js', '--api-url', origin, ...args], {
      cwd: process.cwd(),
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    let output = '';
    child.stdout.on('data', (chunk: Buffer) => {
      output += chunk.toString();
    });
    child.stderr.on('data', (chunk: Buffer) => {
      output += chunk.toString();
    });
    child.once('error', reject);
    child.once('close', (code) => resolve({ code, output }));
  });
}

describe('generation API', () => {
  it('creates, gets, lists, and cancels a queued generation', async () => {
    const { app } = await createTestApp();
    const created = await app.inject({
      method: 'POST',
      url: '/api/v1/generations',
      payload: { model: 'mock:latest', prompt: 'hello', priority: 'interactive' },
    });
    expect(created.statusCode).toBe(202);
    const id = created.json<{ id: string }>().id;
    expect((await app.inject(`/api/v1/generations/${id}`)).json()).toMatchObject({
      id,
      status: 'QUEUED',
    });
    expect((await app.inject('/api/v1/generations?limit=10')).json<Generation[]>()).toHaveLength(1);
    const cancelled = await app.inject({
      method: 'POST',
      url: `/api/v1/generations/${id}/cancel`,
    });
    expect(cancelled.json()).toMatchObject({ status: 'CANCELLED' });
    await app.close();
  });

  it('rejects invalid and unknown generations with consistent errors', async () => {
    const { app } = await createTestApp();
    const invalid = await app.inject({
      method: 'POST',
      url: '/api/v1/generations',
      payload: { model: '../../bad', prompt: '' },
    });
    expect(invalid.statusCode).toBe(400);
    expect(invalid.json()).toMatchObject({ error: { code: 'VALIDATION_ERROR' } });
    const unknown = await app.inject('/api/v1/generations/gen_missing');
    expect(unknown.statusCode).toBe(404);
    expect(unknown.json()).toMatchObject({ error: { code: 'GENERATION_NOT_FOUND' } });
    const malformedJson = await app.inject({
      method: 'POST',
      url: '/api/v1/generations',
      headers: { 'content-type': 'application/json' },
      payload: '{not-json',
    });
    expect(malformedJson.statusCode).toBe(400);
    expect(malformedJson.json()).toMatchObject({ error: { code: 'VALIDATION_ERROR' } });
    const unexpectedField = await app.inject({
      method: 'POST',
      url: '/api/v1/generations',
      payload: { model: 'mock:latest', prompt: 'hello', untrusted: true },
    });
    expect(unexpectedField.statusCode).toBe(400);
    expect(unexpectedField.json()).toMatchObject({ error: { code: 'VALIDATION_ERROR' } });
    await app.close();
  });

  it('returns 429 when queue capacity is exhausted', async () => {
    const { app } = await createTestApp(1);
    const payload = { model: 'mock:latest', prompt: 'hello', priority: 'normal' };
    expect(
      (await app.inject({ method: 'POST', url: '/api/v1/generations', payload })).statusCode,
    ).toBe(202);
    const overflow = await app.inject({ method: 'POST', url: '/api/v1/generations', payload });
    expect(overflow.statusCode).toBe(429);
    expect(overflow.json()).toMatchObject({ error: { code: 'QUEUE_FULL' } });
    await app.close();
  });

  it('streams ordered SSE events through a real HTTP server and forked worker', async () => {
    const { app, orchestrator } = await createTestApp(10, 1, 15, 20);
    await waitFor(() => orchestrator.supervisor.getWorkers()[0]?.status === 'IDLE');
    const origin = await app.listen({ host: '127.0.0.1', port: 0 });
    const createdResponse = await fetch(`${origin}/api/v1/generations`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ model: 'mock:latest', prompt: 'stream this response' }),
    });
    expect(createdResponse.status).toBe(202);
    const created = (await createdResponse.json()) as { id: string; streamUrl: string };
    const streamResponse = await fetch(`${origin}${created.streamUrl}`);
    expect(streamResponse.headers.get('content-type')).toContain('text/event-stream');
    const events = await readSse(streamResponse);
    const types = events.map((event) => event.type);
    expect(types).toContain('worker.assigned');
    expect(types).toContain('generation.started');
    expect(types).toContain('generation.token');
    expect(types.filter((type) => type === 'generation.completed')).toHaveLength(1);
    expect(types.indexOf('generation.started')).toBeLessThan(types.indexOf('generation.token'));
    expect(types.indexOf('generation.token')).toBeLessThan(types.indexOf('generation.completed'));
    await app.close();
  });

  it('removes repeated disconnected SSE subscriptions from the event hub', async () => {
    const { app, orchestrator } = await createTestApp();
    const origin = await app.listen({ host: '127.0.0.1', port: 0 });
    const createdResponse = await fetch(`${origin}/api/v1/generations`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ model: 'mock:latest', prompt: 'remain queued without workers' }),
    });
    const created = (await createdResponse.json()) as { id: string; streamUrl: string };
    await Promise.all(
      Array.from({ length: 10 }, async () => {
        const controller = new AbortController();
        const response = await fetch(`${origin}${created.streamUrl}`, {
          signal: controller.signal,
        });
        expect(response.status).toBe(200);
        controller.abort();
      }),
    );
    await waitFor(() => orchestrator.events.listenerCount(created.id) === 0);
    await app.close();
  });

  it('cancels a running forked generation without allowing completion to overwrite it', async () => {
    const { app, orchestrator } = await createTestApp(10, 1, 100, 10);
    await waitFor(() => orchestrator.supervisor.getWorkers()[0]?.status === 'IDLE');
    const origin = await app.listen({ host: '127.0.0.1', port: 0 });
    const createdResponse = await fetch(`${origin}/api/v1/generations`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ model: 'mock:latest', prompt: 'emit enough tokens to cancel safely' }),
    });
    const created = (await createdResponse.json()) as { id: string; streamUrl: string };
    const streamResponse = await fetch(`${origin}${created.streamUrl}`);
    if (!streamResponse.body) throw new Error('Expected SSE body');
    const reader = streamResponse.body.getReader();
    const decoder = new TextDecoder();
    const types: string[] = [];
    let buffer = '';
    let cancellationRequested = false;
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      const blocks = buffer.split('\n\n');
      buffer = blocks.pop() ?? '';
      for (const block of blocks) {
        const event = block.split('\n').find((line) => line.startsWith('event: '));
        if (!event) continue;
        const type = event.slice(7);
        types.push(type);
        if (type === 'generation.token' && !cancellationRequested) {
          cancellationRequested = true;
          const cancellation = await fetch(`${origin}/api/v1/generations/${created.id}/cancel`, {
            method: 'POST',
          });
          expect(cancellation.status).toBe(200);
        }
      }
    }
    expect(cancellationRequested).toBe(true);
    expect(types.filter((type) => type === 'generation.cancelled')).toHaveLength(1);
    expect(types).not.toContain('generation.completed');
    const detail = await app.inject(`/api/v1/generations/${created.id}`);
    expect(detail.json()).toMatchObject({ status: 'CANCELLED' });
    await app.close();
  });

  it('requeues and completes once when a worker crashes before the first token', async () => {
    const { app, store, orchestrator } = await createTestApp(10, 1, 5, 500);
    await waitFor(() => orchestrator.supervisor.getWorkers()[0]?.status === 'IDLE');
    const origin = await app.listen({ host: '127.0.0.1', port: 0 });
    const createdResponse = await fetch(`${origin}/api/v1/generations`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ model: 'mock:latest', prompt: 'retry after a worker crash' }),
    });
    const created = (await createdResponse.json()) as { id: string };
    await waitFor(() => orchestrator.supervisor.getWorkers()[0]?.status === 'BUSY');
    expect(orchestrator.supervisor.kill('worker-1')).toBe(true);
    await waitFor(() => store.records.get(created.id)?.status === 'COMPLETED', 8_000);
    expect(store.records.get(created.id)).toMatchObject({ status: 'COMPLETED', retryCount: 1 });
    expect(orchestrator.supervisor.getWorkers()[0]).toMatchObject({
      status: 'IDLE',
      restartCount: 1,
    });
    await app.close();
  });

  it('fails rather than regenerating after a worker crashes following a token', async () => {
    const { app, store, orchestrator } = await createTestApp(10, 1, 100, 10);
    await waitFor(() => orchestrator.supervisor.getWorkers()[0]?.status === 'IDLE');
    const origin = await app.listen({ host: '127.0.0.1', port: 0 });
    const createdResponse = await fetch(`${origin}/api/v1/generations`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ model: 'mock:latest', prompt: 'fail after one visible token' }),
    });
    const created = (await createdResponse.json()) as { id: string };
    await waitFor(() =>
      orchestrator.events.getHistory(created.id).some((event) => event.type === 'generation.token'),
    );
    expect(orchestrator.supervisor.kill('worker-1')).toBe(true);
    await waitFor(() => store.records.get(created.id)?.status === 'FAILED', 5_000);
    expect(store.records.get(created.id)?.errorMessage).toContain('not retried');
    expect(store.records.get(created.id)?.retryCount).toBe(0);
    await app.close();
  });

  it('fails timed-out inference and returns worker capacity to IDLE', async () => {
    const { app, store, orchestrator } = await createTestApp(10, 1, 5, 1_000, 100);
    await waitFor(() => orchestrator.supervisor.getWorkers()[0]?.status === 'IDLE');
    const origin = await app.listen({ host: '127.0.0.1', port: 0 });
    const createdResponse = await fetch(`${origin}/api/v1/generations`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ model: 'mock:latest', prompt: 'this generation should time out' }),
    });
    const created = (await createdResponse.json()) as { id: string };
    await waitFor(() => store.records.get(created.id)?.status === 'FAILED');
    expect(store.records.get(created.id)?.errorMessage).toContain('timed out');
    await waitFor(() => orchestrator.supervisor.getWorkers()[0]?.status === 'IDLE');
    await app.close();
  });

  it('runs compiled CLI commands against the live API and returns errors non-zero', async () => {
    const { app, orchestrator } = await createTestApp(10, 1, 5, 10);
    await waitFor(() => orchestrator.supervisor.getWorkers()[0]?.status === 'IDLE');
    const origin = await app.listen({ host: '127.0.0.1', port: 0 });
    const status = await runCli(origin, 'status');
    expect(status.code).toBe(0);
    expect(status.output).toContain('LocalLLM Runtime');
    const workers = await runCli(origin, 'workers');
    expect(workers.code).toBe(0);
    expect(workers.output).toContain('worker-1');
    const queue = await runCli(origin, 'queue');
    expect(queue.code).toBe(0);
    expect(queue.output).toContain('QUEUE STATUS');
    const models = await runCli(origin, 'models');
    expect(models.code).toBe(0);
    expect(models.output).toContain('mock:latest');
    const generated = await runCli(origin, 'generate', '--prompt', 'CLI end to end');
    expect(generated.code).toBe(0);
    expect(generated.output).toContain('Completed');
    const history = await runCli(origin, 'generations');
    expect(history.code).toBe(0);
    expect(history.output).toContain('COMPLETED');
    const missing = await runCli(origin, 'generation', 'gen_missing');
    expect(missing.code).toBe(1);
    expect(missing.output).toContain('GENERATION_NOT_FOUND');
    await app.close();
  }, 20_000);
});
