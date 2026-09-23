import { Queue, type Job } from 'bullmq';
import { Redis } from 'ioredis';
import { AppError, type GenerationPriority, type QueueStats } from '@localllm/contracts';

export interface QueueJobData {
  generationId: string;
  model: string;
  prompt: string;
  priority: GenerationPriority;
}

export interface QueueBackend {
  add(priority: GenerationPriority, job: QueueJobData): Promise<void>;
  take(priority: GenerationPriority): Promise<QueueJobData | null>;
  remove(generationId: string): Promise<boolean>;
  count(priority: GenerationPriority): Promise<number>;
  close(): Promise<void>;
}

const priorities: GenerationPriority[] = ['interactive', 'normal', 'background'];

export class BullMqQueueBackend implements QueueBackend {
  private readonly redis: Redis;
  private readonly queues: Record<GenerationPriority, Queue<QueueJobData>>;

  constructor(redisUrl: string, prefix = 'localllm') {
    this.redis = new Redis(redisUrl, { maxRetriesPerRequest: null });
    this.queues = {
      interactive: new Queue(`${prefix}-interactive`, { connection: this.redis }),
      normal: new Queue(`${prefix}-normal`, { connection: this.redis }),
      background: new Queue(`${prefix}-background`, { connection: this.redis }),
    };
  }

  async add(priority: GenerationPriority, job: QueueJobData): Promise<void> {
    await this.queues[priority].add('generation', job, {
      jobId: job.generationId,
      removeOnComplete: true,
      removeOnFail: 100,
    });
  }

  async take(priority: GenerationPriority): Promise<QueueJobData | null> {
    const jobs = await this.queues[priority].getJobs(['waiting'], 0, 0, true);
    const job: Job<QueueJobData> | undefined = jobs[0];
    if (!job) return null;
    try {
      await job.remove();
      return job.data;
    } catch {
      return null;
    }
  }

  async remove(generationId: string): Promise<boolean> {
    for (const priority of priorities) {
      const job = await this.queues[priority].getJob(generationId);
      if (job) {
        try {
          await job.remove();
          return true;
        } catch {
          return false;
        }
      }
    }
    return false;
  }

  async count(priority: GenerationPriority): Promise<number> {
    return this.queues[priority].getWaitingCount();
  }

  async ping(): Promise<boolean> {
    try {
      return (await this.redis.ping()) === 'PONG';
    } catch {
      return false;
    }
  }

  async close(): Promise<void> {
    await Promise.all(Object.values(this.queues).map((queue) => queue.close()));
    await this.redis.quit();
  }
}

const weightedCycle: GenerationPriority[] = [
  'interactive',
  'interactive',
  'interactive',
  'interactive',
  'normal',
  'normal',
  'background',
];

export class WeightedScheduler {
  private cursor = 0;
  private operationLock: Promise<void> = Promise.resolve();

  constructor(
    private readonly backend: QueueBackend,
    private readonly maxQueueSize: number,
    private readonly maxInteractiveQueueSize: number,
  ) {}

  private async serialize<T>(operation: () => Promise<T>): Promise<T> {
    let release: (() => void) | undefined;
    const previous = this.operationLock;
    this.operationLock = new Promise<void>((resolve) => {
      release = resolve;
    });
    await previous;
    try {
      return await operation();
    } finally {
      release?.();
    }
  }

  async enqueue(job: QueueJobData): Promise<void> {
    await this.serialize(async () => {
      const stats = await this.getQueueStats();
      const total = stats.interactive + stats.normal + stats.background;
      if (
        total >= this.maxQueueSize ||
        (job.priority === 'interactive' && stats.interactive >= this.maxInteractiveQueueSize)
      ) {
        throw new AppError('QUEUE_FULL', 'Generation queue has reached maximum capacity.', 429);
      }
      await this.backend.add(job.priority, job);
    });
  }

  async nextJob(): Promise<QueueJobData | null> {
    return this.serialize(async () => {
      for (let attempts = 0; attempts < weightedCycle.length; attempts += 1) {
        const priority = weightedCycle[this.cursor] ?? 'background';
        this.cursor = (this.cursor + 1) % weightedCycle.length;
        const job = await this.backend.take(priority);
        if (job) return job;
      }
      return null;
    });
  }

  cancelJob(generationId: string): Promise<boolean> {
    return this.serialize(() => this.backend.remove(generationId));
  }

  async getQueueStats(running = 0, capacity = 0): Promise<QueueStats> {
    const [interactive, normal, background] = await Promise.all(
      priorities.map((p) => this.backend.count(p)),
    );
    return {
      interactive: interactive ?? 0,
      normal: normal ?? 0,
      background: background ?? 0,
      running,
      capacity,
    };
  }

  close(): Promise<void> {
    return this.backend.close();
  }
}
