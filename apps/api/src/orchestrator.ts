import type { Logger } from 'pino';
import {
  AppError,
  createEvent,
  type Generation,
  type WorkerToSupervisorMessage,
} from '@localllm/contracts';
import type { GenerationStore } from '@localllm/database';
import { ModelMemoryManager } from '@localllm/inference';
import type { WeightedScheduler } from '@localllm/scheduler';
import { GenerationEventHub } from './event-hub.js';
import { WorkerSupervisor, type WorkerCrash } from './supervisor.js';

interface RuntimeMetric {
  startedAt: number;
  firstTokenAt: number | null;
  tokenCount: number;
  model: string;
}

export class GenerationOrchestrator {
  private draining = false;
  private drainRequested = false;
  private accepting = true;
  private readonly metrics = new Map<string, RuntimeMetric>();

  constructor(
    private readonly store: GenerationStore,
    private readonly scheduler: WeightedScheduler,
    readonly supervisor: WorkerSupervisor,
    readonly events: GenerationEventHub,
    readonly memory: ModelMemoryManager,
    private readonly logger: Logger,
  ) {
    supervisor.on('available', () => void this.drain());
    supervisor.on(
      'message',
      (workerId: string, message: WorkerToSupervisorMessage) =>
        void this.handleMessage(workerId, message),
    );
    supervisor.on('crash', (crash: WorkerCrash) => void this.handleCrash(crash));
  }

  stopAccepting(): void {
    this.accepting = false;
  }

  async enqueue(generation: Generation, prompt: string): Promise<void> {
    if (!this.accepting) throw new AppError('WORKER_UNAVAILABLE', 'Runtime is shutting down.', 503);
    const queued = await this.store.transition(generation.id, 'QUEUED');
    try {
      await this.scheduler.enqueue({
        generationId: generation.id,
        model: generation.model,
        prompt,
        priority: generation.priority,
      });
    } catch (error) {
      await this.store.transition(generation.id, 'FAILED', {
        errorMessage: error instanceof Error ? error.message : 'Unable to queue generation.',
      });
      throw error;
    }
    this.events.publish(
      createEvent('generation.queued', generation.id, { priority: generation.priority }),
    );
    this.logger.info(
      {
        event: 'generation_queued',
        generationId: queued.id,
        model: queued.model,
        priority: queued.priority,
      },
      'Generation queued',
    );
    void this.drain();
  }

  private async drain(): Promise<void> {
    if (!this.accepting) return;
    if (this.draining) {
      this.drainRequested = true;
      return;
    }
    this.draining = true;
    try {
      do {
        this.drainRequested = false;
        for (const workerId of this.supervisor.getIdleWorkerIds()) {
          const job = await this.scheduler.nextJob();
          if (!job) break;
          const generation = await this.store.get(job.generationId);
          if (!generation || generation.status !== 'QUEUED') continue;
          let memoryAcquired = false;
          try {
            const evicted = this.memory.acquire(job.model);
            memoryAcquired = true;
            if (evicted.length > 0) {
              this.logger.info(
                { event: 'models_evicted', models: evicted },
                'Evicted inactive models',
              );
            }
            const queueWaitMs = generation.queuedAt
              ? Date.now() - generation.queuedAt.getTime()
              : null;
            await this.store.transition(job.generationId, 'RUNNING', { workerId, queueWaitMs });
            this.metrics.set(job.generationId, {
              startedAt: Date.now(),
              firstTokenAt: null,
              tokenCount: 0,
              model: job.model,
            });
            this.events.publish(createEvent('worker.assigned', job.generationId, { workerId }));
            if (!this.supervisor.execute(workerId, job)) {
              this.metrics.delete(job.generationId);
              this.memory.release(job.model);
              memoryAcquired = false;
              await this.store.transition(job.generationId, 'QUEUED');
              await this.scheduler.enqueue(job);
            }
          } catch (error) {
            if (memoryAcquired) this.memory.release(job.model);
            this.metrics.delete(job.generationId);
            const latest = await this.store.get(job.generationId);
            if (latest?.status === 'QUEUED') {
              try {
                await this.store.transition(job.generationId, 'FAILED', {
                  errorMessage:
                    error instanceof Error ? error.message : 'Unable to dispatch generation.',
                });
                this.events.publish(
                  createEvent('generation.failed', job.generationId, {
                    error:
                      error instanceof Error ? error.message : 'Unable to dispatch generation.',
                  }),
                );
              } catch (transitionError) {
                this.logger.error(
                  {
                    event: 'generation_dispatch_cleanup_failed',
                    generationId: job.generationId,
                    err: transitionError,
                  },
                  'Unable to persist dispatch failure',
                );
              }
            }
            this.logger.warn(
              { event: 'generation_dispatch_failed', generationId: job.generationId, err: error },
              'Unable to dispatch generation',
            );
          }
        }
      } while (this.drainRequested && this.accepting);
    } finally {
      this.draining = false;
    }
  }

  async cancel(generationId: string): Promise<Generation> {
    let generation = await this.store.get(generationId);
    if (!generation)
      throw new AppError('GENERATION_NOT_FOUND', `Generation ${generationId} was not found.`, 404);
    for (let attempt = 0; attempt < 2; attempt += 1) {
      if (generation.status === 'QUEUED') {
        await this.scheduler.cancelJob(generationId);
        try {
          const cancelled = await this.store.transition(generationId, 'CANCELLED');
          this.events.publish(
            createEvent('generation.cancelled', generationId, { reason: 'user_request' }),
          );
          this.logger.info(
            { event: 'generation_cancelled', generationId, status: cancelled.status },
            'Queued generation cancelled',
          );
          return cancelled;
        } catch (error) {
          if (!(error instanceof AppError) || error.code !== 'INVALID_STATE_TRANSITION')
            throw error;
          generation = await this.store.get(generationId);
          if (!generation) break;
          continue;
        }
      }
      if (generation.status === 'RUNNING') {
        if (this.supervisor.cancel(generationId)) return generation;
        const latest = await this.store.get(generationId);
        if (latest && latest.status !== 'RUNNING') {
          generation = latest;
          continue;
        }
        throw new AppError('WORKER_UNAVAILABLE', 'Assigned worker is unavailable.', 503);
      }
      break;
    }
    throw new AppError(
      'INVALID_STATE_TRANSITION',
      `Cannot cancel a ${generation?.status ?? 'missing'} generation.`,
      409,
    );
  }

  private async handleMessage(workerId: string, message: WorkerToSupervisorMessage): Promise<void> {
    if (message.type === 'worker.ready' || message.type === 'worker.heartbeat') return;
    const metric = this.metrics.get(message.generationId);
    if (message.type === 'generation.started') {
      this.events.publish(createEvent('generation.started', message.generationId, { workerId }));
      return;
    }
    if (message.type === 'generation.token') {
      if (metric) {
        metric.firstTokenAt ??= Date.now();
        metric.tokenCount += 1;
      }
      this.events.publish(
        createEvent('generation.token', message.generationId, { token: message.token }),
      );
      return;
    }
    try {
      const current = await this.store.get(message.generationId);
      if (!current || current.status !== 'RUNNING') return;
      const durationMs = metric ? Date.now() - metric.startedAt : null;
      const timeToFirstTokenMs = metric?.firstTokenAt
        ? metric.firstTokenAt - metric.startedAt
        : null;
      if (message.type === 'generation.completed') {
        await this.store.transition(message.generationId, 'COMPLETED', {
          tokenCount: message.tokenCount,
          durationMs,
          timeToFirstTokenMs,
        });
        this.events.publish(
          createEvent('generation.completed', message.generationId, {
            workerId,
            tokenCount: message.tokenCount,
            durationMs,
            timeToFirstTokenMs,
          }),
        );
        this.logger.info(
          {
            event: 'generation_completed',
            generationId: message.generationId,
            workerId,
            durationMs,
          },
          'Generation completed',
        );
      } else if (message.type === 'generation.cancelled') {
        await this.store.transition(message.generationId, 'CANCELLED', {
          tokenCount: metric?.tokenCount ?? 0,
          durationMs,
          timeToFirstTokenMs,
        });
        this.events.publish(
          createEvent('generation.cancelled', message.generationId, { reason: 'user_request' }),
        );
        this.logger.info(
          {
            event: 'generation_cancelled',
            generationId: message.generationId,
            workerId,
            durationMs,
          },
          'Running generation cancelled',
        );
      } else {
        await this.store.transition(message.generationId, 'FAILED', {
          errorMessage: message.error,
          tokenCount: metric?.tokenCount ?? 0,
          durationMs,
          timeToFirstTokenMs,
        });
        this.events.publish(
          createEvent('generation.failed', message.generationId, { error: message.error }),
        );
        this.logger.warn(
          {
            event: 'generation_failed',
            generationId: message.generationId,
            workerId,
            error: message.error,
          },
          'Generation failed',
        );
      }
    } catch (error) {
      this.logger.warn(
        {
          event: 'generation_terminal_update_failed',
          generationId: message.generationId,
          err: error,
        },
        'Unable to persist worker terminal message',
      );
    } finally {
      if (metric) this.memory.release(metric.model);
      this.metrics.delete(message.generationId);
    }
  }

  private async handleCrash(crash: WorkerCrash): Promise<void> {
    const active = crash.generation;
    if (!active) return;
    const current = await this.store.get(active.generationId);
    if (!current || current.status !== 'RUNNING') return;
    const metric = this.metrics.get(active.generationId);
    if (metric) this.memory.release(metric.model);
    this.metrics.delete(active.generationId);
    try {
      if (active.tokensEmitted === 0 && current.retryCount < 1) {
        await this.store.transition(active.generationId, 'QUEUED');
        await this.store.incrementRetry(active.generationId);
        await this.scheduler.enqueue({
          generationId: active.generationId,
          model: active.model,
          prompt: active.prompt,
          priority: current.priority,
        });
        this.events.publish(
          createEvent('generation.queued', active.generationId, {
            priority: current.priority,
            retry: true,
          }),
        );
        void this.drain();
        return;
      }
      const error =
        active.tokensEmitted > 0
          ? 'Worker crashed after streaming began; generation was not retried to avoid duplicate output.'
          : 'Worker crashed and the generation retry limit was reached.';
      await this.store.transition(active.generationId, 'FAILED', {
        errorMessage: error,
        tokenCount: active.tokensEmitted,
      });
      this.events.publish(createEvent('generation.failed', active.generationId, { error }));
    } catch (error) {
      this.logger.error(
        {
          event: 'generation_crash_recovery_failed',
          generationId: active.generationId,
          err: error,
        },
        'Unable to recover generation after worker crash',
      );
      const latest = await this.store.get(active.generationId);
      if (latest?.status === 'QUEUED') {
        const message = 'Worker crashed and the generation could not be requeued.';
        try {
          await this.store.transition(active.generationId, 'FAILED', { errorMessage: message });
          this.events.publish(
            createEvent('generation.failed', active.generationId, { error: message }),
          );
        } catch (transitionError) {
          this.logger.error(
            {
              event: 'generation_crash_recovery_cleanup_failed',
              generationId: active.generationId,
              err: transitionError,
            },
            'Unable to persist worker crash recovery failure',
          );
        }
      }
    }
  }
}
