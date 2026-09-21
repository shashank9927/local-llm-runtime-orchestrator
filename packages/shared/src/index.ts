import { randomBytes } from 'node:crypto';
import pino from 'pino';
import { z } from 'zod';

const booleanString = z.enum(['true', 'false']).transform((value) => value === 'true');

export const configSchema = z.object({
  NODE_ENV: z.enum(['development', 'test', 'production']).default('development'),
  API_HOST: z.string().default('127.0.0.1'),
  API_PORT: z.coerce.number().int().min(1).max(65535).default(3000),
  API_BASE_URL: z.url().default('http://127.0.0.1:3000'),
  DATABASE_URL: z
    .string()
    .min(1)
    .default('postgresql://localllm:localllm@localhost:5432/localllm?schema=public'),
  REDIS_URL: z.url().default('redis://localhost:6379'),
  INFERENCE_PROVIDER: z.enum(['mock', 'ollama']).default('mock'),
  OLLAMA_BASE_URL: z.url().default('http://localhost:11434'),
  WORKER_COUNT: z.coerce.number().int().min(1).max(32).default(2),
  WORKER_HEARTBEAT_INTERVAL_MS: z.coerce.number().int().min(100).default(2000),
  WORKER_HEARTBEAT_TIMEOUT_MS: z.coerce.number().int().min(500).default(6000),
  WORKER_SHUTDOWN_GRACE_MS: z.coerce.number().int().min(0).default(10000),
  MAX_QUEUE_SIZE: z.coerce.number().int().min(1).default(200),
  MAX_INTERACTIVE_QUEUE_SIZE: z.coerce.number().int().min(1).default(100),
  MAX_PROMPT_LENGTH: z.coerce.number().int().min(1).default(20000),
  GENERATION_TIMEOUT_MS: z.coerce.number().int().min(1000).default(120000),
  MODEL_MEMORY_BUDGET_MB: z.coerce.number().int().min(1).default(6000),
  STORE_PROMPTS: booleanString.default(true),
  MOCK_TOKEN_DELAY_MS: z.coerce.number().int().min(0).default(75),
  MOCK_GENERATION_DELAY_MS: z.coerce.number().int().min(0).default(100),
  MOCK_FAILURE_RATE: z.coerce.number().min(0).max(1).default(0),
  LOG_LEVEL: z.enum(['fatal', 'error', 'warn', 'info', 'debug', 'trace', 'silent']).default('info'),
  CORS_ORIGIN: z.string().default('false'),
});

export type AppConfig = z.infer<typeof configSchema>;

export function loadConfig(environment: NodeJS.ProcessEnv = process.env): AppConfig {
  const result = configSchema.safeParse(environment);
  if (!result.success) {
    throw new Error(`Invalid environment configuration: ${z.prettifyError(result.error)}`);
  }
  if (result.data.WORKER_HEARTBEAT_TIMEOUT_MS <= result.data.WORKER_HEARTBEAT_INTERVAL_MS) {
    throw new Error('WORKER_HEARTBEAT_TIMEOUT_MS must exceed WORKER_HEARTBEAT_INTERVAL_MS.');
  }
  if (result.data.MAX_INTERACTIVE_QUEUE_SIZE > result.data.MAX_QUEUE_SIZE) {
    throw new Error('MAX_INTERACTIVE_QUEUE_SIZE cannot exceed MAX_QUEUE_SIZE.');
  }
  return result.data;
}

export function createLogger(config: Pick<AppConfig, 'LOG_LEVEL' | 'NODE_ENV'>) {
  return pino({
    level: config.LOG_LEVEL,
    base: { service: 'localllm-runtime', environment: config.NODE_ENV },
  });
}

export function createGenerationId(): string {
  return `gen_${randomBytes(8).toString('hex')}`;
}

export function isValidRequestId(value: unknown): value is string {
  return typeof value === 'string' && /^[a-zA-Z0-9._:-]{1,128}$/.test(value);
}

export async function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    const timer = setTimeout(resolve, ms);
    signal?.addEventListener(
      'abort',
      () => {
        clearTimeout(timer);
        reject(signal.reason instanceof Error ? signal.reason : new Error('Operation aborted'));
      },
      { once: true },
    );
  });
}
