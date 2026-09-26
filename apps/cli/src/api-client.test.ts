import { describe, expect, it, vi } from 'vitest';
import { ApiClient } from './api-client.js';

describe('ApiClient', () => {
  it('constructs a generation request', async () => {
    const fetcher = vi.fn<typeof fetch>(
      async () =>
        new Response(
          JSON.stringify({
            id: 'g1',
            status: 'QUEUED',
            priority: 'interactive',
            model: 'mock:latest',
            streamUrl: '/stream',
          }),
          { status: 202, headers: { 'content-type': 'application/json' } },
        ),
    );
    const client = new ApiClient('http://test', fetcher);
    await client.createGeneration({
      model: 'mock:latest',
      prompt: 'hello',
      priority: 'interactive',
    });
    expect(fetcher).toHaveBeenCalledWith(
      'http://test/api/v1/generations',
      expect.objectContaining({
        method: 'POST',
        body: JSON.stringify({ model: 'mock:latest', prompt: 'hello', priority: 'interactive' }),
      }),
    );
  });

  it('formats API errors into a typed error', async () => {
    const fetcher = vi.fn<typeof fetch>(
      async () =>
        new Response(JSON.stringify({ error: { code: 'QUEUE_FULL', message: 'full' } }), {
          status: 429,
        }),
    );
    const client = new ApiClient('http://test', fetcher);
    await expect(client.queue()).rejects.toEqual(
      expect.objectContaining({ code: 'QUEUE_FULL', message: 'full', status: 429 }),
    );
  });
});
