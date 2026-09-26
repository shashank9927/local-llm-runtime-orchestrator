import type {
  ApiErrorBody,
  CreateGenerationInput,
  Generation,
  GenerationEvent,
  ModelInfo,
  QueueStats,
  WorkerInfo,
} from '@localllm/contracts';

export interface CreatedGeneration {
  id: string;
  status: string;
  priority: string;
  model: string;
  streamUrl: string;
}

export interface StatusResponse {
  api: boolean;
  postgres: boolean;
  redis: boolean;
  inference: boolean;
  provider: string;
  workers: { healthy: number; total: number };
  running: number;
  queued: number;
}

export class ApiClientError extends Error {
  constructor(
    public readonly status: number,
    public readonly code: string,
    message: string,
  ) {
    super(message);
  }
}

export class ApiClient {
  constructor(
    private readonly baseUrl: string,
    private readonly fetcher: typeof fetch = fetch,
  ) {}

  private async request<T>(path: string, init?: RequestInit): Promise<T> {
    const response = await this.fetcher(`${this.baseUrl}${path}`, {
      ...init,
      headers: { 'content-type': 'application/json', ...init?.headers },
    });
    if (!response.ok) {
      let body: ApiErrorBody | undefined;
      try {
        body = (await response.json()) as ApiErrorBody;
      } catch {
        /* non-JSON upstream error */
      }
      throw new ApiClientError(
        response.status,
        body?.error.code ?? 'HTTP_ERROR',
        body?.error.message ?? `HTTP ${response.status}`,
      );
    }
    return response.json() as Promise<T>;
  }

  createGeneration(input: CreateGenerationInput): Promise<CreatedGeneration> {
    return this.request('/api/v1/generations', { method: 'POST', body: JSON.stringify(input) });
  }
  listGenerations(query: {
    limit: number;
    status?: string;
    priority?: string;
  }): Promise<Generation[]> {
    const params = new URLSearchParams({ limit: String(query.limit) });
    if (query.status) params.set('status', query.status.toUpperCase());
    if (query.priority) params.set('priority', query.priority);
    return this.request(`/api/v1/generations?${params}`);
  }
  getGeneration(id: string): Promise<Generation> {
    return this.request(`/api/v1/generations/${encodeURIComponent(id)}`);
  }
  cancel(id: string): Promise<Generation> {
    return this.request(`/api/v1/generations/${encodeURIComponent(id)}/cancel`, { method: 'POST' });
  }
  workers(): Promise<WorkerInfo[]> {
    return this.request('/api/v1/workers');
  }
  queue(): Promise<QueueStats> {
    return this.request('/api/v1/queue');
  }
  models(): Promise<ModelInfo[]> {
    return this.request('/api/v1/models');
  }
  status(): Promise<StatusResponse> {
    return this.request('/api/v1/status');
  }
  killWorker(id: string): Promise<{ killed: boolean }> {
    return this.request(`/api/v1/dev/workers/${encodeURIComponent(id)}/kill`, { method: 'POST' });
  }

  async stream(path: string, onEvent: (event: GenerationEvent) => void): Promise<void> {
    const response = await this.fetcher(`${this.baseUrl}${path}`, {
      headers: { accept: 'text/event-stream' },
    });
    if (!response.ok || !response.body)
      throw new ApiClientError(
        response.status,
        'STREAM_ERROR',
        `Unable to open generation stream (HTTP ${response.status}).`,
      );
    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      const blocks = buffer.split('\n\n');
      buffer = blocks.pop() ?? '';
      for (const block of blocks) {
        const data = block.split('\n').find((line) => line.startsWith('data: '));
        if (data) onEvent(JSON.parse(data.slice(6)) as GenerationEvent);
      }
    }
  }
}
