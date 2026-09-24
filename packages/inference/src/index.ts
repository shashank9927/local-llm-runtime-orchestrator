import { AppError, type ModelInfo } from '@localllm/contracts';
import { sleep } from '@localllm/shared';

export interface InferenceRequest {
  generationId: string;
  model: string;
  prompt: string;
  signal?: AbortSignal;
}

export interface InferenceChunk {
  token: string;
  done: boolean;
}

export interface InferenceProvider {
  healthCheck(): Promise<boolean>;
  listModels(): Promise<ModelInfo[]>;
  generateStream(request: InferenceRequest): AsyncGenerator<InferenceChunk>;
}

export interface MockInferenceOptions {
  tokenDelayMs: number;
  generationDelayMs: number;
  failureRate: number;
  responseFactory?: (prompt: string) => string;
  random?: () => number;
}

export class MockInferenceProvider implements InferenceProvider {
  constructor(private readonly options: MockInferenceOptions) {}

  async healthCheck(): Promise<boolean> {
    return true;
  }

  async listModels(): Promise<ModelInfo[]> {
    return [
      {
        name: 'mock:latest',
        estimatedMemoryMb: 128,
        loaded: true,
        lastUsedAt: null,
        activeRequests: 0,
      },
    ];
  }

  async *generateStream(request: InferenceRequest): AsyncGenerator<InferenceChunk> {
    await sleep(this.options.generationDelayMs, request.signal);
    const random = this.options.random ?? Math.random;
    if (random() < this.options.failureRate)
      throw new Error('Simulated inference provider failure.');
    const response =
      this.options.responseFactory?.(request.prompt) ??
      `This is a simulated local LLM response to: ${request.prompt}`;
    const tokens = response.match(/\S+\s*/g) ?? [];
    for (let index = 0; index < tokens.length; index += 1) {
      if (request.signal?.aborted)
        throw request.signal.reason ?? new Error('Generation cancelled.');
      await sleep(this.options.tokenDelayMs, request.signal);
      yield { token: tokens[index] ?? '', done: index === tokens.length - 1 };
    }
  }
}

interface OllamaTagsResponse {
  models?: Array<{ name: string; size?: number }>;
}

interface OllamaGenerateChunk {
  response?: string;
  done?: boolean;
  error?: string;
}

export class OllamaInferenceProvider implements InferenceProvider {
  constructor(private readonly baseUrl: string) {}

  async healthCheck(): Promise<boolean> {
    try {
      const response = await fetch(`${this.baseUrl}/api/tags`, {
        signal: AbortSignal.timeout(3000),
      });
      return response.ok;
    } catch {
      return false;
    }
  }

  async listModels(): Promise<ModelInfo[]> {
    let response: Response;
    try {
      response = await fetch(`${this.baseUrl}/api/tags`, { signal: AbortSignal.timeout(3000) });
    } catch {
      throw new AppError('PROVIDER_UNAVAILABLE', 'Unable to reach Ollama.', 503);
    }
    if (!response.ok) {
      throw new AppError('PROVIDER_UNAVAILABLE', 'Unable to list Ollama models.', 503);
    }
    let body: OllamaTagsResponse;
    try {
      body = (await response.json()) as OllamaTagsResponse;
    } catch {
      throw new AppError('PROVIDER_UNAVAILABLE', 'Ollama returned invalid model metadata.', 503);
    }
    return (body.models ?? []).map((model) => ({
      name: model.name,
      ...(model.size === undefined ? {} : { sizeBytes: model.size }),
      estimatedMemoryMb: model.size ? Math.ceil(model.size / 1024 / 1024) : 2048,
      loaded: false,
      lastUsedAt: null,
      activeRequests: 0,
    }));
  }

  async *generateStream(request: InferenceRequest): AsyncGenerator<InferenceChunk> {
    const init: RequestInit = {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ model: request.model, prompt: request.prompt, stream: true }),
      ...(request.signal ? { signal: request.signal } : {}),
    };
    const response = await fetch(`${this.baseUrl}/api/generate`, init);
    if (response.status === 404)
      throw new AppError('MODEL_NOT_FOUND', `Model ${request.model} is unavailable.`, 404);
    if (!response.ok || !response.body) {
      throw new AppError('PROVIDER_UNAVAILABLE', `Ollama returned HTTP ${response.status}.`, 503);
    }
    const decoder = new TextDecoder();
    let buffer = '';
    for await (const bytes of response.body) {
      buffer += decoder.decode(bytes, { stream: true });
      const lines = buffer.split('\n');
      buffer = lines.pop() ?? '';
      for (const line of lines) {
        if (!line.trim()) continue;
        let chunk: OllamaGenerateChunk;
        try {
          chunk = JSON.parse(line) as OllamaGenerateChunk;
        } catch {
          throw new Error('Ollama returned a malformed streaming chunk.');
        }
        if (chunk.error) throw new Error(chunk.error);
        if (chunk.response) yield { token: chunk.response, done: Boolean(chunk.done) };
      }
    }
    if (buffer.trim()) {
      let chunk: OllamaGenerateChunk;
      try {
        chunk = JSON.parse(buffer) as OllamaGenerateChunk;
      } catch {
        throw new Error('Ollama returned a malformed streaming chunk.');
      }
      if (chunk.error) throw new Error(chunk.error);
      if (chunk.response) yield { token: chunk.response, done: Boolean(chunk.done) };
    }
  }
}

interface MemoryModel {
  name: string;
  estimatedMemoryMb: number;
  loaded: boolean;
  lastUsedAt: number | null;
  activeRequests: number;
}

export class ModelMemoryManager {
  private readonly models = new Map<string, MemoryModel>();

  constructor(private readonly budgetMb: number) {}

  register(name: string, estimatedMemoryMb: number): void {
    const current = this.models.get(name);
    this.models.set(
      name,
      current
        ? { ...current, estimatedMemoryMb }
        : {
            name,
            estimatedMemoryMb,
            loaded: false,
            lastUsedAt: null,
            activeRequests: 0,
          },
    );
  }

  has(name: string): boolean {
    return this.models.has(name);
  }

  acquire(name: string, now = Date.now()): string[] {
    const model = this.models.get(name);
    if (!model) throw new AppError('MODEL_NOT_FOUND', `Model ${name} is not registered.`, 404);
    if (model.estimatedMemoryMb > this.budgetMb) {
      throw new AppError(
        'MODEL_MEMORY_LIMIT',
        `Model ${name} exceeds the ${this.budgetMb} MB memory budget.`,
        409,
      );
    }
    const evicted: string[] = [];
    if (!model.loaded) {
      const loadedMb = () =>
        [...this.models.values()]
          .filter((item) => item.loaded)
          .reduce((sum, item) => sum + item.estimatedMemoryMb, 0);
      const candidates = [...this.models.values()]
        .filter((item) => item.loaded && item.activeRequests === 0 && item.name !== name)
        .sort((a, b) => (a.lastUsedAt ?? 0) - (b.lastUsedAt ?? 0));
      while (loadedMb() + model.estimatedMemoryMb > this.budgetMb && candidates.length > 0) {
        const candidate = candidates.shift();
        if (candidate) {
          candidate.loaded = false;
          evicted.push(candidate.name);
        }
      }
      if (loadedMb() + model.estimatedMemoryMb > this.budgetMb) {
        throw new AppError(
          'MODEL_MEMORY_LIMIT',
          'Not enough inactive model memory can be freed.',
          409,
        );
      }
      model.loaded = true;
    }
    model.activeRequests += 1;
    model.lastUsedAt = now;
    return evicted;
  }

  release(name: string, now = Date.now()): void {
    const model = this.models.get(name);
    if (!model) return;
    model.activeRequests = Math.max(0, model.activeRequests - 1);
    model.lastUsedAt = now;
  }

  snapshot(): ModelInfo[] {
    return [...this.models.values()].map((model) => ({
      ...model,
      lastUsedAt: model.lastUsedAt === null ? null : new Date(model.lastUsedAt).toISOString(),
    }));
  }
}

export function createInferenceProvider(
  kind: 'mock' | 'ollama',
  options: {
    ollamaBaseUrl: string;
    mockTokenDelayMs: number;
    mockGenerationDelayMs: number;
    mockFailureRate: number;
  },
): InferenceProvider {
  if (kind === 'ollama') return new OllamaInferenceProvider(options.ollamaBaseUrl);
  return new MockInferenceProvider({
    tokenDelayMs: options.mockTokenDelayMs,
    generationDelayMs: options.mockGenerationDelayMs,
    failureRate: options.mockFailureRate,
  });
}
