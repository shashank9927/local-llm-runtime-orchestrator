import { createHash } from 'node:crypto';
import { PrismaClient, type Generation as PrismaGeneration, type Prisma } from '@prisma/client';
import type { Generation, GenerationPriority, GenerationStatus } from '@localllm/contracts';
import { AppError, assertTransition } from '@localllm/contracts';

export { PrismaClient } from '@prisma/client';

export interface GenerationListOptions {
  limit: number;
  status?: GenerationStatus;
  priority?: GenerationPriority;
}

export interface CreateGenerationRecord {
  id: string;
  model: string;
  priority: GenerationPriority;
  prompt: string;
  storePrompt: boolean;
}

export interface GenerationStore {
  create(input: CreateGenerationRecord): Promise<Generation>;
  get(id: string): Promise<Generation | null>;
  list(options: GenerationListOptions): Promise<Generation[]>;
  transition(id: string, to: GenerationStatus, data?: Partial<Generation>): Promise<Generation>;
  incrementRetry(id: string): Promise<Generation>;
  ping(): Promise<boolean>;
  disconnect(): Promise<void>;
}

function mapGeneration(row: PrismaGeneration): Generation {
  return { ...row } as Generation;
}

export class PrismaGenerationStore implements GenerationStore {
  constructor(public readonly prisma: PrismaClient = new PrismaClient()) {}

  async create(input: CreateGenerationRecord): Promise<Generation> {
    const promptHash = createHash('sha256').update(input.prompt).digest('hex');
    const row = await this.prisma.generation.create({
      data: {
        id: input.id,
        model: input.model,
        priority: input.priority,
        prompt: input.storePrompt ? input.prompt : null,
        promptHash,
        promptLength: input.prompt.length,
      },
    });
    return mapGeneration(row);
  }

  async get(id: string): Promise<Generation | null> {
    const row = await this.prisma.generation.findUnique({ where: { id } });
    return row ? mapGeneration(row) : null;
  }

  async list(options: GenerationListOptions): Promise<Generation[]> {
    const where: Prisma.GenerationWhereInput = {};
    if (options.status) where.status = options.status;
    if (options.priority) where.priority = options.priority;
    const rows = await this.prisma.generation.findMany({
      where,
      take: options.limit,
      orderBy: { createdAt: 'desc' },
    });
    return rows.map(mapGeneration);
  }

  async transition(
    id: string,
    to: GenerationStatus,
    data: Partial<Generation> = {},
  ): Promise<Generation> {
    return this.prisma.$transaction(async (transaction) => {
      const current = await transaction.generation.findUnique({ where: { id } });
      if (!current) {
        throw new AppError('GENERATION_NOT_FOUND', `Generation ${id} was not found.`, 404);
      }
      assertTransition(current.status, to);
      const now = new Date();
      const timestamps: Prisma.GenerationUpdateInput = {};
      if (to === 'QUEUED') timestamps.queuedAt = now;
      if (to === 'RUNNING') timestamps.startedAt = now;
      if (to === 'COMPLETED') timestamps.completedAt = now;
      if (to === 'FAILED') timestamps.failedAt = now;
      if (to === 'CANCELLED') timestamps.cancelledAt = now;
      const allowed: Prisma.GenerationUpdateInput = {};
      if (data.workerId !== undefined) allowed.workerId = data.workerId;
      if (data.errorMessage !== undefined) allowed.errorMessage = data.errorMessage;
      if (data.tokenCount !== undefined) allowed.tokenCount = data.tokenCount;
      if (data.durationMs !== undefined) allowed.durationMs = data.durationMs;
      if (data.queueWaitMs !== undefined) allowed.queueWaitMs = data.queueWaitMs;
      if (data.timeToFirstTokenMs !== undefined)
        allowed.timeToFirstTokenMs = data.timeToFirstTokenMs;
      const result = await transaction.generation.updateMany({
        where: { id, status: current.status },
        data: { status: to, ...timestamps, ...allowed },
      });
      if (result.count !== 1) {
        const latest = await transaction.generation.findUnique({ where: { id } });
        if (!latest) {
          throw new AppError('GENERATION_NOT_FOUND', `Generation ${id} was not found.`, 404);
        }
        assertTransition(latest.status, to);
        throw new Error(`Generation ${id} changed state concurrently.`);
      }
      const row = await transaction.generation.findUniqueOrThrow({ where: { id } });
      return mapGeneration(row);
    });
  }

  async incrementRetry(id: string): Promise<Generation> {
    return mapGeneration(
      await this.prisma.generation.update({
        where: { id },
        data: { retryCount: { increment: 1 } },
      }),
    );
  }

  async ping(): Promise<boolean> {
    try {
      await this.prisma.$queryRaw`SELECT 1`;
      return true;
    } catch {
      return false;
    }
  }

  async disconnect(): Promise<void> {
    await this.prisma.$disconnect();
  }
}
