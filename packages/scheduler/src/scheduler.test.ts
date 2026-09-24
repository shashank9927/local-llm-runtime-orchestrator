import { describe, expect, it } from 'vitest';
import type { GenerationPriority } from '@localllm/contracts';
import { WeightedScheduler, type QueueBackend, type QueueJobData } from './index.js';

class MemoryBackend implements QueueBackend {
  queues: Record<GenerationPriority, QueueJobData[]> = {
    interactive: [],
    normal: [],
    background: [],
  };
  async add(priority: GenerationPriority, job: QueueJobData) {
    this.queues[priority].push(job);
  }
  async take(priority: GenerationPriority) {
    return this.queues[priority].shift() ?? null;
  }
  async remove(id: string) {
    for (const queue of Object.values(this.queues)) {
      const index = queue.findIndex((job) => job.generationId === id);
      if (index >= 0) {
        queue.splice(index, 1);
        return true;
      }
    }
    return false;
  }
  async count(priority: GenerationPriority) {
    return this.queues[priority].length;
  }
  async close() {
    /* no-op */
  }
}

const job = (generationId: string, priority: GenerationPriority): QueueJobData => ({
  generationId,
  priority,
  model: 'mock:latest',
  prompt: generationId,
});

describe('WeightedScheduler', () => {
  it('uses 4:2:1 weighted turns without starving background jobs', async () => {
    const backend = new MemoryBackend();
    const scheduler = new WeightedScheduler(backend, 100, 100);
    for (let i = 0; i < 8; i += 1) await scheduler.enqueue(job(`i${i}`, 'interactive'));
    for (let i = 0; i < 4; i += 1) await scheduler.enqueue(job(`n${i}`, 'normal'));
    for (let i = 0; i < 2; i += 1) await scheduler.enqueue(job(`b${i}`, 'background'));
    const firstCycle = [];
    for (let i = 0; i < 7; i += 1) firstCycle.push((await scheduler.nextJob())?.priority);
    expect(firstCycle).toEqual([
      'interactive',
      'interactive',
      'interactive',
      'interactive',
      'normal',
      'normal',
      'background',
    ]);
  });

  it('preserves FIFO within a priority', async () => {
    const scheduler = new WeightedScheduler(new MemoryBackend(), 10, 10);
    await scheduler.enqueue(job('first', 'interactive'));
    await scheduler.enqueue(job('second', 'interactive'));
    expect((await scheduler.nextJob())?.generationId).toBe('first');
    expect((await scheduler.nextJob())?.generationId).toBe('second');
  });

  it('removes cancelled queued work and enforces capacity', async () => {
    const scheduler = new WeightedScheduler(new MemoryBackend(), 1, 1);
    await scheduler.enqueue(job('cancel-me', 'interactive'));
    await expect(scheduler.enqueue(job('overflow', 'normal'))).rejects.toMatchObject({
      code: 'QUEUE_FULL',
    });
    expect(await scheduler.cancelJob('cancel-me')).toBe(true);
    expect(await scheduler.nextJob()).toBeNull();
  });

  it('serializes concurrent capacity checks so only one job is admitted', async () => {
    const scheduler = new WeightedScheduler(new MemoryBackend(), 1, 1);
    const results = await Promise.allSettled([
      scheduler.enqueue(job('first', 'normal')),
      scheduler.enqueue(job('second', 'normal')),
    ]);
    expect(results.filter((result) => result.status === 'fulfilled')).toHaveLength(1);
    expect(results.filter((result) => result.status === 'rejected')).toHaveLength(1);
    expect((await scheduler.getQueueStats()).normal).toBe(1);
  });
});
