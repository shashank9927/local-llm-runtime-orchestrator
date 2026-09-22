import { z } from 'zod';

export const generationPriorities = ['interactive', 'normal', 'background'] as const;
export type GenerationPriority = (typeof generationPriorities)[number];

export const generationStatuses = [
  'CREATED',
  'QUEUED',
  'RUNNING',
  'COMPLETED',
  'FAILED',
  'CANCELLED',
] as const;
export type GenerationStatus = (typeof generationStatuses)[number];

export const workerStatuses = [
  'STARTING',
  'IDLE',
  'BUSY',
  'UNHEALTHY',
  'RESTARTING',
  'STOPPED',
] as const;
export type WorkerStatus = (typeof workerStatuses)[number];

export interface Generation {
  id: string;
  model: string;
  priority: GenerationPriority;
  status: GenerationStatus;
  prompt: string | null;
  promptHash: string | null;
  promptLength: number;
  createdAt: Date;
  queuedAt: Date | null;
  startedAt: Date | null;
  completedAt: Date | null;
  failedAt: Date | null;
  cancelledAt: Date | null;
  workerId: string | null;
  errorMessage: string | null;
  tokenCount: number;
  durationMs: number | null;
  queueWaitMs: number | null;
  timeToFirstTokenMs: number | null;
  retryCount: number;
}

export interface WorkerInfo {
  id: string;
  pid: number | null;
  status: WorkerStatus;
  currentGenerationId: string | null;
  lastHeartbeatAt: string | null;
  restartCount: number;
}

export interface ModelInfo {
  name: string;
  sizeBytes?: number;
  estimatedMemoryMb: number;
  loaded: boolean;
  lastUsedAt: string | null;
  activeRequests: number;
}

export interface QueueStats {
  interactive: number;
  normal: number;
  background: number;
  running: number;
  capacity: number;
}

export const modelIdentifierSchema = z
  .string()
  .min(1)
  .max(120)
  .regex(/^[a-zA-Z0-9][a-zA-Z0-9._/-]*(?::[a-zA-Z0-9._-]+)?$/, 'Invalid model identifier');

export const createGenerationSchema = z
  .object({
    model: modelIdentifierSchema.default('mock:latest'),
    prompt: z.string().trim().min(1),
    priority: z.enum(generationPriorities).default('normal'),
  })
  .strict();
export type CreateGenerationInput = z.infer<typeof createGenerationSchema>;

export const generationListQuerySchema = z
  .object({
    limit: z.coerce.number().int().min(1).max(100).default(20),
    status: z.enum(generationStatuses).optional(),
    priority: z.enum(generationPriorities).optional(),
  })
  .strict();

export type ErrorCode =
  | 'VALIDATION_ERROR'
  | 'GENERATION_NOT_FOUND'
  | 'MODEL_NOT_FOUND'
  | 'QUEUE_FULL'
  | 'MODEL_MEMORY_LIMIT'
  | 'WORKER_UNAVAILABLE'
  | 'PROVIDER_UNAVAILABLE'
  | 'GENERATION_FAILED'
  | 'GENERATION_CANCELLED'
  | 'INVALID_STATE_TRANSITION'
  | 'INTERNAL_ERROR';

export interface ApiErrorBody {
  error: { code: ErrorCode; message: string; details?: unknown };
}

export class AppError extends Error {
  constructor(
    public readonly code: ErrorCode,
    message: string,
    public readonly statusCode = 500,
    public readonly details?: unknown,
  ) {
    super(message);
    this.name = 'AppError';
  }
}

export type GenerationEventType =
  | 'generation.queued'
  | 'generation.started'
  | 'generation.token'
  | 'generation.completed'
  | 'generation.failed'
  | 'generation.cancelled'
  | 'worker.assigned';

export interface GenerationEvent<T = Record<string, unknown>> {
  type: GenerationEventType;
  generationId: string;
  timestamp: string;
  data: T;
}

export type SupervisorToWorkerMessage =
  | {
      type: 'generation.execute';
      generationId: string;
      model: string;
      prompt: string;
      provider: 'mock' | 'ollama';
      config: WorkerProviderConfig;
    }
  | { type: 'generation.cancel'; generationId: string }
  | { type: 'worker.shutdown' };

export interface WorkerProviderConfig {
  ollamaBaseUrl: string;
  mockTokenDelayMs: number;
  mockGenerationDelayMs: number;
  mockFailureRate: number;
  heartbeatIntervalMs: number;
  generationTimeoutMs: number;
}

export type WorkerToSupervisorMessage =
  | { type: 'worker.ready'; workerId: string; pid: number }
  | { type: 'worker.heartbeat'; workerId: string; timestamp: number }
  | { type: 'generation.started'; generationId: string }
  | { type: 'generation.token'; generationId: string; token: string }
  | { type: 'generation.completed'; generationId: string; tokenCount: number }
  | { type: 'generation.cancelled'; generationId: string }
  | { type: 'generation.failed'; generationId: string; error: string };

const workerProviderConfigSchema = z
  .object({
    ollamaBaseUrl: z.url(),
    mockTokenDelayMs: z.number().int().min(0),
    mockGenerationDelayMs: z.number().int().min(0),
    mockFailureRate: z.number().min(0).max(1),
    heartbeatIntervalMs: z.number().int().min(100),
    generationTimeoutMs: z.number().int().min(1),
  })
  .strict();

export const supervisorToWorkerMessageSchema = z.discriminatedUnion('type', [
  z
    .object({
      type: z.literal('generation.execute'),
      generationId: z.string().min(1),
      model: modelIdentifierSchema,
      prompt: z.string(),
      provider: z.enum(['mock', 'ollama']),
      config: workerProviderConfigSchema,
    })
    .strict(),
  z.object({ type: z.literal('generation.cancel'), generationId: z.string().min(1) }).strict(),
  z.object({ type: z.literal('worker.shutdown') }).strict(),
]);

export const workerToSupervisorMessageSchema = z.discriminatedUnion('type', [
  z
    .object({
      type: z.literal('worker.ready'),
      workerId: z.string().min(1),
      pid: z.number().int().positive(),
    })
    .strict(),
  z
    .object({
      type: z.literal('worker.heartbeat'),
      workerId: z.string().min(1),
      timestamp: z.number().finite(),
    })
    .strict(),
  z.object({ type: z.literal('generation.started'), generationId: z.string().min(1) }).strict(),
  z
    .object({
      type: z.literal('generation.token'),
      generationId: z.string().min(1),
      token: z.string(),
    })
    .strict(),
  z
    .object({
      type: z.literal('generation.completed'),
      generationId: z.string().min(1),
      tokenCount: z.number().int().min(0),
    })
    .strict(),
  z.object({ type: z.literal('generation.cancelled'), generationId: z.string().min(1) }).strict(),
  z
    .object({
      type: z.literal('generation.failed'),
      generationId: z.string().min(1),
      error: z.string(),
    })
    .strict(),
]);

export function parseSupervisorToWorkerMessage(value: unknown): SupervisorToWorkerMessage | null {
  const result = supervisorToWorkerMessageSchema.safeParse(value);
  return result.success ? (result.data as SupervisorToWorkerMessage) : null;
}

export function parseWorkerToSupervisorMessage(value: unknown): WorkerToSupervisorMessage | null {
  const result = workerToSupervisorMessageSchema.safeParse(value);
  return result.success ? (result.data as WorkerToSupervisorMessage) : null;
}

const allowedTransitions: Record<GenerationStatus, ReadonlySet<GenerationStatus>> = {
  CREATED: new Set(['QUEUED', 'CANCELLED', 'FAILED']),
  QUEUED: new Set(['RUNNING', 'CANCELLED', 'FAILED']),
  RUNNING: new Set(['COMPLETED', 'FAILED', 'CANCELLED', 'QUEUED']),
  COMPLETED: new Set(),
  FAILED: new Set(),
  CANCELLED: new Set(),
};

export function canTransition(from: GenerationStatus, to: GenerationStatus): boolean {
  return allowedTransitions[from].has(to);
}

export function assertTransition(from: GenerationStatus, to: GenerationStatus): void {
  if (!canTransition(from, to)) {
    throw new AppError(
      'INVALID_STATE_TRANSITION',
      `Cannot transition generation from ${from} to ${to}.`,
      409,
    );
  }
}

export function createEvent<T extends Record<string, unknown>>(
  type: GenerationEventType,
  generationId: string,
  data: T,
): GenerationEvent<T> {
  return { type, generationId, timestamp: new Date().toISOString(), data };
}
