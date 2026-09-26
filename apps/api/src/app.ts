import { randomUUID } from 'node:crypto';
import { resolve } from 'node:path';
import Fastify from 'fastify';
import cors from '@fastify/cors';
import { ZodError } from 'zod';
import {
  AppError,
  createGenerationSchema,
  generationListQuerySchema,
  type ApiErrorBody,
} from '@localllm/contracts';
import { PrismaGenerationStore, type GenerationStore } from '@localllm/database';
import {
  createInferenceProvider,
  ModelMemoryManager,
  type InferenceProvider,
} from '@localllm/inference';
import { BullMqQueueBackend, WeightedScheduler } from '@localllm/scheduler';
import {
  createGenerationId,
  createLogger,
  isValidRequestId,
  type AppConfig,
} from '@localllm/shared';
import { GenerationEventHub } from './event-hub.js';
import { GenerationOrchestrator } from './orchestrator.js';
import { WorkerSupervisor } from './supervisor.js';

export interface RuntimeDependencies {
  store: GenerationStore;
  scheduler: WeightedScheduler;
  queueHealth: () => Promise<boolean>;
  provider: InferenceProvider;
  orchestrator: GenerationOrchestrator;
}

export function createRuntimeDependencies(config: AppConfig): RuntimeDependencies {
  const logger = createLogger(config);
  const store = new PrismaGenerationStore();
  const backend = new BullMqQueueBackend(config.REDIS_URL);
  const scheduler = new WeightedScheduler(
    backend,
    config.MAX_QUEUE_SIZE,
    config.MAX_INTERACTIVE_QUEUE_SIZE,
  );
  const provider = createInferenceProvider(config.INFERENCE_PROVIDER, {
    ollamaBaseUrl: config.OLLAMA_BASE_URL,
    mockTokenDelayMs: config.MOCK_TOKEN_DELAY_MS,
    mockGenerationDelayMs: config.MOCK_GENERATION_DELAY_MS,
    mockFailureRate: config.MOCK_FAILURE_RATE,
  });
  const memory = new ModelMemoryManager(config.MODEL_MEMORY_BUDGET_MB);
  memory.register('mock:latest', 128);
  memory.register('llama3.2:3b', 2500);
  memory.register('qwen2.5:3b', 2800);
  const workerScript = resolve(process.cwd(), 'apps/worker/dist/main.js');
  const supervisor = new WorkerSupervisor(
    workerScript,
    config.WORKER_COUNT,
    config.WORKER_HEARTBEAT_TIMEOUT_MS,
    config.INFERENCE_PROVIDER,
    {
      ollamaBaseUrl: config.OLLAMA_BASE_URL,
      mockTokenDelayMs: config.MOCK_TOKEN_DELAY_MS,
      mockGenerationDelayMs: config.MOCK_GENERATION_DELAY_MS,
      mockFailureRate: config.MOCK_FAILURE_RATE,
      heartbeatIntervalMs: config.WORKER_HEARTBEAT_INTERVAL_MS,
      generationTimeoutMs: config.GENERATION_TIMEOUT_MS,
    },
    logger,
  );
  const orchestrator = new GenerationOrchestrator(
    store,
    scheduler,
    supervisor,
    new GenerationEventHub(),
    memory,
    logger,
  );
  return { store, scheduler, queueHealth: () => backend.ping(), provider, orchestrator };
}

export async function buildApp(config: AppConfig, dependencies?: RuntimeDependencies) {
  const logger = createLogger(config);
  const runtime = dependencies ?? createRuntimeDependencies(config);
  const app = Fastify({
    loggerInstance: logger,
    bodyLimit: Math.max(config.MAX_PROMPT_LENGTH * 2, 64 * 1024),
    genReqId: (request) =>
      isValidRequestId(request.headers['x-request-id'])
        ? request.headers['x-request-id']
        : randomUUID(),
  });
  await app.register(cors, { origin: config.CORS_ORIGIN === 'false' ? false : config.CORS_ORIGIN });

  app.setErrorHandler((error, request, reply) => {
    request.log.error({ err: error, requestId: request.id }, 'Request failed');
    if (error instanceof ZodError) {
      return reply.status(400).send({
        error: {
          code: 'VALIDATION_ERROR',
          message: 'Request validation failed.',
          details: error.issues,
        },
      } satisfies ApiErrorBody);
    }
    if (error instanceof AppError) {
      return reply.status(error.statusCode).send({
        error: { code: error.code, message: error.message, details: error.details },
      } satisfies ApiErrorBody);
    }
    const possibleStatusCode = (error as { statusCode?: unknown }).statusCode;
    const statusCode = typeof possibleStatusCode === 'number' ? possibleStatusCode : 500;
    if (statusCode >= 400 && statusCode < 500) {
      return reply.status(statusCode).send({
        error: { code: 'VALIDATION_ERROR', message: 'Request body or parameters are invalid.' },
      } satisfies ApiErrorBody);
    }
    return reply.status(500).send({
      error: { code: 'INTERNAL_ERROR', message: 'An unexpected error occurred.' },
    } satisfies ApiErrorBody);
  });

  app.get('/health/live', async () => ({ status: 'ok' }));
  app.get('/health/ready', async (_request, reply) => {
    const [postgres, redis, inference] = await Promise.all([
      runtime.store.ping(),
      runtime.queueHealth(),
      runtime.provider.healthCheck(),
    ]);
    const workers = runtime.orchestrator.supervisor.isReady();
    const ready = postgres && redis && inference && workers;
    return reply.status(ready ? 200 : 503).send({
      status: ready ? 'ready' : 'not_ready',
      checks: { postgres, redis, inference, workers },
    });
  });

  app.post('/api/v1/generations', async (request, reply) => {
    const input = createGenerationSchema.parse(request.body);
    if (input.prompt.length > config.MAX_PROMPT_LENGTH)
      throw new AppError(
        'VALIDATION_ERROR',
        `Prompt exceeds ${config.MAX_PROMPT_LENGTH} characters.`,
        400,
      );
    if (config.INFERENCE_PROVIDER === 'ollama' || !runtime.orchestrator.memory.has(input.model)) {
      const providerModels = await runtime.provider.listModels();
      for (const model of providerModels)
        runtime.orchestrator.memory.register(model.name, model.estimatedMemoryMb);
      if (!runtime.orchestrator.memory.has(input.model))
        throw new AppError('MODEL_NOT_FOUND', `Model ${input.model} is unavailable.`, 404);
    }
    const generation = await runtime.store.create({
      id: createGenerationId(),
      ...input,
      storePrompt: config.STORE_PROMPTS,
    });
    await runtime.orchestrator.enqueue(generation, input.prompt);
    return reply.status(202).send({
      id: generation.id,
      status: 'QUEUED',
      priority: generation.priority,
      model: generation.model,
      streamUrl: `/api/v1/generations/${generation.id}/stream`,
    });
  });

  app.get('/api/v1/generations', async (request) => {
    const query = generationListQuerySchema.parse(request.query);
    return runtime.store.list({
      limit: query.limit,
      ...(query.status ? { status: query.status } : {}),
      ...(query.priority ? { priority: query.priority } : {}),
    });
  });
  app.get<{ Params: { id: string } }>('/api/v1/generations/:id', async (request) => {
    const generation = await runtime.store.get(request.params.id);
    if (!generation)
      throw new AppError(
        'GENERATION_NOT_FOUND',
        `Generation ${request.params.id} was not found.`,
        404,
      );
    return generation;
  });
  app.post<{ Params: { id: string } }>('/api/v1/generations/:id/cancel', async (request) =>
    runtime.orchestrator.cancel(request.params.id),
  );

  app.get<{ Params: { id: string } }>('/api/v1/generations/:id/stream', async (request, reply) => {
    const generation = await runtime.store.get(request.params.id);
    if (!generation)
      throw new AppError(
        'GENERATION_NOT_FOUND',
        `Generation ${request.params.id} was not found.`,
        404,
      );
    reply.hijack();
    reply.raw.writeHead(200, {
      'content-type': 'text/event-stream',
      'cache-control': 'no-cache, no-transform',
      connection: 'keep-alive',
      'x-accel-buffering': 'no',
    });
    const write = (event: ReturnType<typeof runtime.orchestrator.events.getHistory>[number]) => {
      reply.raw.write(`event: ${event.type}\ndata: ${JSON.stringify(event)}\n\n`);
      if (
        ['generation.completed', 'generation.failed', 'generation.cancelled'].includes(event.type)
      )
        cleanup(true);
    };
    let unsubscribe = () => {};
    const heartbeat = setInterval(() => reply.raw.write(': ping\n\n'), 15_000);
    const cleanup = (end = false) => {
      clearInterval(heartbeat);
      unsubscribe();
      if (end && !reply.raw.destroyed) reply.raw.end();
    };
    for (const event of runtime.orchestrator.events.getHistory(generation.id)) write(event);
    if (!reply.raw.writableEnded)
      unsubscribe = runtime.orchestrator.events.subscribe(generation.id, write);
    request.raw.once('close', () => cleanup());
    reply.raw.once('error', () => cleanup());
  });

  app.get('/api/v1/workers', async () => runtime.orchestrator.supervisor.getWorkers());
  app.get('/api/v1/queue', async () => {
    const workers = runtime.orchestrator.supervisor.getWorkers();
    return runtime.scheduler.getQueueStats(
      workers.filter((worker) => worker.status === 'BUSY').length,
      workers.length,
    );
  });
  app.get('/api/v1/models', async () => {
    const providerModels = await runtime.provider.listModels();
    for (const model of providerModels)
      runtime.orchestrator.memory.register(model.name, model.estimatedMemoryMb);
    return runtime.orchestrator.memory.snapshot();
  });
  app.get('/api/v1/status', async () => {
    const [postgres, redis, inference, queue] = await Promise.all([
      runtime.store.ping(),
      runtime.queueHealth(),
      runtime.provider.healthCheck(),
      runtime.scheduler.getQueueStats(),
    ]);
    const workers = runtime.orchestrator.supervisor.getWorkers();
    return {
      api: true,
      postgres,
      redis,
      inference,
      provider: config.INFERENCE_PROVIDER,
      workers: {
        healthy: workers.filter((worker) => ['IDLE', 'BUSY'].includes(worker.status)).length,
        total: workers.length,
      },
      running: workers.filter((worker) => worker.status === 'BUSY').length,
      queued: queue.interactive + queue.normal + queue.background,
    };
  });
  if (config.NODE_ENV !== 'production') {
    app.post<{ Params: { id: string } }>('/api/v1/dev/workers/:id/kill', async (request) => {
      if (!runtime.orchestrator.supervisor.kill(request.params.id))
        throw new AppError(
          'WORKER_UNAVAILABLE',
          `Worker ${request.params.id} is unavailable.`,
          404,
        );
      return { killed: true, workerId: request.params.id };
    });
  }

  app.addHook('onReady', async () => runtime.orchestrator.supervisor.start());
  app.addHook('onClose', async () => {
    runtime.orchestrator.stopAccepting();
    await runtime.orchestrator.supervisor.shutdown(config.WORKER_SHUTDOWN_GRACE_MS);
    runtime.orchestrator.events.close();
    await runtime.scheduler.close();
    await runtime.store.disconnect();
  });
  return app;
}
