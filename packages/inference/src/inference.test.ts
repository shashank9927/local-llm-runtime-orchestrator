import { afterEach, describe, expect, it, vi } from 'vitest';
import { AppError } from '@localllm/contracts';
import { MockInferenceProvider, ModelMemoryManager, OllamaInferenceProvider } from './index.js';

afterEach(() => {
  vi.unstubAllGlobals();
});

describe('MockInferenceProvider', () => {
  it('streams ordered tokens', async () => {
    const provider = new MockInferenceProvider({
      tokenDelayMs: 0,
      generationDelayMs: 0,
      failureRate: 0,
      responseFactory: () => 'one two three',
    });
    const chunks = [];
    for await (const chunk of provider.generateStream({
      generationId: 'g1',
      model: 'mock:latest',
      prompt: 'hello',
    }))
      chunks.push(chunk);
    expect(chunks.map((chunk) => chunk.token).join('')).toBe('one two three');
    expect(chunks.at(-1)?.done).toBe(true);
  });

  it('simulates failures', async () => {
    const provider = new MockInferenceProvider({
      tokenDelayMs: 0,
      generationDelayMs: 0,
      failureRate: 1,
      random: () => 0,
    });
    const consume = async () => {
      for await (const chunk of provider.generateStream({
        generationId: 'g1',
        model: 'mock:latest',
        prompt: 'hello',
      }))
        void chunk;
    };
    await expect(consume()).rejects.toThrow('Simulated');
  });

  it('supports cancellation', async () => {
    const controller = new AbortController();
    controller.abort(new Error('cancelled'));
    const provider = new MockInferenceProvider({
      tokenDelayMs: 1,
      generationDelayMs: 1,
      failureRate: 0,
    });
    const consume = async () => {
      for await (const chunk of provider.generateStream({
        generationId: 'g1',
        model: 'mock:latest',
        prompt: 'hello',
        signal: controller.signal,
      }))
        void chunk;
    };
    await expect(consume()).rejects.toThrow('cancelled');
  });
});

describe('ModelMemoryManager', () => {
  it('evicts the least recently used inactive model', () => {
    const manager = new ModelMemoryManager(6000);
    manager.register('a', 3000);
    manager.register('b', 2500);
    manager.register('c', 3000);
    manager.acquire('a', 1);
    manager.release('a', 2);
    manager.acquire('b', 3);
    manager.release('b', 4);
    expect(manager.acquire('c', 5)).toEqual(['a']);
    expect(manager.snapshot().find((m) => m.name === 'a')?.loaded).toBe(false);
  });

  it('never evicts an active model', () => {
    const manager = new ModelMemoryManager(5000);
    manager.register('active', 3000);
    manager.register('new', 3000);
    manager.acquire('active');
    expect(() => manager.acquire('new')).toThrow(AppError);
  });

  it('rejects a model larger than the budget', () => {
    const manager = new ModelMemoryManager(1000);
    manager.register('huge', 2000);
    expect(() => manager.acquire('huge')).toThrow(/exceeds/);
  });

  it('evicts more than one inactive model when that is needed to fit', () => {
    const manager = new ModelMemoryManager(6000);
    manager.register('a', 2000);
    manager.register('b', 2000);
    manager.register('c', 5000);
    manager.acquire('a', 1);
    manager.release('a', 2);
    manager.acquire('b', 3);
    manager.release('b', 4);
    expect(manager.acquire('c', 5)).toEqual(['a', 'b']);
    expect(manager.snapshot().find((model) => model.name === 'c')).toMatchObject({
      loaded: true,
      activeRequests: 1,
    });
  });

  it('counts concurrent acquisitions of one loaded model without double-loading it', () => {
    const manager = new ModelMemoryManager(1000);
    manager.register('shared', 500);
    expect(manager.acquire('shared')).toEqual([]);
    expect(manager.acquire('shared')).toEqual([]);
    expect(manager.snapshot().find((model) => model.name === 'shared')).toMatchObject({
      loaded: true,
      activeRequests: 2,
    });
  });
});

describe('OllamaInferenceProvider HTTP contract', () => {
  it('converts list-model connection failures into a safe provider error', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(async () => Promise.reject(new Error('connection refused'))),
    );
    const provider = new OllamaInferenceProvider('http://127.0.0.1:11434');
    await expect(provider.listModels()).rejects.toMatchObject({
      code: 'PROVIDER_UNAVAILABLE',
      statusCode: 503,
    });
  });

  it('parses fragmented newline-delimited Ollama chunks in order', async () => {
    const body = new ReadableStream<Uint8Array>({
      start(controller) {
        const encoder = new TextEncoder();
        controller.enqueue(encoder.encode('{"response":"Hel'));
        controller.enqueue(
          encoder.encode('lo ","done":false}\n{"response":"world","done":true}\n'),
        );
        controller.close();
      },
    });
    vi.stubGlobal(
      'fetch',
      vi.fn(async () => new Response(body, { status: 200 })),
    );
    const provider = new OllamaInferenceProvider('http://127.0.0.1:11434');
    const chunks = [];
    for await (const chunk of provider.generateStream({
      generationId: 'g1',
      model: 'llama3.2:3b',
      prompt: 'hello',
    })) {
      chunks.push(chunk);
    }
    expect(chunks).toEqual([
      { token: 'Hello ', done: false },
      { token: 'world', done: true },
    ]);
  });

  it('fails clearly for missing models and malformed streaming chunks', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(async () => new Response('', { status: 404 })),
    );
    const provider = new OllamaInferenceProvider('http://127.0.0.1:11434');
    const consumeMissing = async () => {
      for await (const chunk of provider.generateStream({
        generationId: 'g1',
        model: 'missing:latest',
        prompt: 'hello',
      })) {
        void chunk;
      }
    };
    await expect(consumeMissing()).rejects.toMatchObject({ code: 'MODEL_NOT_FOUND' });

    vi.stubGlobal(
      'fetch',
      vi.fn(async () => new Response('{not-json}\n', { status: 200 })),
    );
    const consumeMalformed = async () => {
      for await (const chunk of provider.generateStream({
        generationId: 'g1',
        model: 'llama3.2:3b',
        prompt: 'hello',
      })) {
        void chunk;
      }
    };
    await expect(consumeMalformed()).rejects.toThrow('malformed streaming chunk');
  });
});
