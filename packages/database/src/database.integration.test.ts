import { afterAll, beforeEach, describe, expect, it } from 'vitest';
import { PrismaClient } from '@prisma/client';
import type { AppError } from '@localllm/contracts';
import { PrismaGenerationStore } from './index.js';

const testDatabaseUrl = process.env.TEST_DATABASE_URL;
const databaseUrl = testDatabaseUrl ?? 'postgresql://unused:unused@127.0.0.1:1/unused';
const describeDatabase = testDatabaseUrl ? describe : describe.skip;

describeDatabase('PrismaGenerationStore integration', () => {
  const prisma = new PrismaClient({ datasources: { db: { url: databaseUrl } } });
  const store = new PrismaGenerationStore(prisma);

  beforeEach(async () => {
    await prisma.generation.deleteMany({ where: { id: { startsWith: 'audit_' } } });
  });

  afterAll(async () => {
    await prisma.generation.deleteMany({ where: { id: { startsWith: 'audit_' } } });
    await prisma.$disconnect();
  });

  it('persists privacy-safe generation data and lifecycle timestamps', async () => {
    const created = await store.create({
      id: 'audit_private',
      model: 'mock:latest',
      priority: 'interactive',
      prompt: 'do not store this complete prompt',
      storePrompt: false,
    });
    expect(created).toMatchObject({ prompt: null, promptLength: 33 });
    expect(created.promptHash).toHaveLength(64);

    const queued = await store.transition(created.id, 'QUEUED');
    const running = await store.transition(created.id, 'RUNNING', {
      workerId: 'worker-1',
      queueWaitMs: 12,
    });
    const completed = await store.transition(created.id, 'COMPLETED', {
      tokenCount: 4,
      durationMs: 50,
      timeToFirstTokenMs: 10,
    });
    expect(queued.queuedAt).toBeInstanceOf(Date);
    expect(running.startedAt).toBeInstanceOf(Date);
    expect(completed).toMatchObject({
      status: 'COMPLETED',
      workerId: 'worker-1',
      tokenCount: 4,
      durationMs: 50,
      timeToFirstTokenMs: 10,
    });
    expect(completed.completedAt).toBeInstanceOf(Date);
  });

  it('allows exactly one competing terminal update to win', async () => {
    const created = await store.create({
      id: 'audit_race',
      model: 'mock:latest',
      priority: 'normal',
      prompt: 'race test',
      storePrompt: true,
    });
    await store.transition(created.id, 'QUEUED');
    await store.transition(created.id, 'RUNNING', { workerId: 'worker-1' });

    const results = await Promise.allSettled([
      store.transition(created.id, 'COMPLETED', { tokenCount: 1 }),
      store.transition(created.id, 'CANCELLED'),
    ]);
    expect(results.filter((result) => result.status === 'fulfilled')).toHaveLength(1);
    expect(results.filter((result) => result.status === 'rejected')).toHaveLength(1);
    expect((await store.get(created.id))?.status).toMatch(/^(COMPLETED|CANCELLED)$/);
  });

  it('uses the public not-found error for a missing transition target', async () => {
    await expect(store.transition('audit_missing', 'QUEUED')).rejects.toMatchObject({
      code: 'GENERATION_NOT_FOUND',
      statusCode: 404,
    } satisfies Partial<AppError>);
  });
});
