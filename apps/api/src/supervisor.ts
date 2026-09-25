import { EventEmitter } from 'node:events';
import { fork, type ChildProcess } from 'node:child_process';
import type { Logger } from 'pino';
import type {
  SupervisorToWorkerMessage,
  WorkerInfo,
  WorkerProviderConfig,
  WorkerStatus,
  WorkerToSupervisorMessage,
} from '@localllm/contracts';
import { parseWorkerToSupervisorMessage } from '@localllm/contracts';

interface ActiveGeneration {
  generationId: string;
  model: string;
  prompt: string;
  tokensEmitted: number;
}

interface WorkerSlot {
  id: string;
  child: ChildProcess | null;
  status: WorkerStatus;
  current: ActiveGeneration | null;
  lastHeartbeatAt: number | null;
  restartCount: number;
  consecutiveCrashes: number;
  expectedExit: boolean;
}

export interface WorkerCrash {
  workerId: string;
  generation: ActiveGeneration | null;
}

export interface ExecuteJob {
  generationId: string;
  model: string;
  prompt: string;
}

export function restartDelayMs(consecutiveCrashes: number): number {
  return Math.min(1000 * 2 ** Math.max(0, consecutiveCrashes - 1), 10_000);
}

export function isHeartbeatExpired(
  lastHeartbeatAt: number,
  now: number,
  timeoutMs: number,
): boolean {
  return now - lastHeartbeatAt > timeoutMs;
}

export class WorkerSupervisor extends EventEmitter {
  private readonly slots = new Map<string, WorkerSlot>();
  private heartbeatTimer: NodeJS.Timeout | null = null;
  private shuttingDown = false;

  constructor(
    private readonly workerScript: string,
    private readonly workerCount: number,
    private readonly heartbeatTimeoutMs: number,
    private readonly provider: 'mock' | 'ollama',
    private readonly providerConfig: WorkerProviderConfig,
    private readonly logger: Logger,
  ) {
    super();
  }

  start(): void {
    for (let index = 1; index <= this.workerCount; index += 1) {
      const id = `worker-${index}`;
      const slot: WorkerSlot = {
        id,
        child: null,
        status: 'STARTING',
        current: null,
        lastHeartbeatAt: null,
        restartCount: 0,
        consecutiveCrashes: 0,
        expectedExit: false,
      };
      this.slots.set(id, slot);
      this.spawn(slot);
    }
    this.heartbeatTimer = setInterval(
      () => this.checkHeartbeats(),
      Math.max(250, this.heartbeatTimeoutMs / 2),
    );
    this.heartbeatTimer.unref();
  }

  private spawn(slot: WorkerSlot): void {
    if (this.shuttingDown) return;
    slot.status = slot.restartCount > 0 ? 'RESTARTING' : 'STARTING';
    slot.expectedExit = false;
    const child = fork(this.workerScript, [slot.id], {
      stdio: ['ignore', 'inherit', 'inherit', 'ipc'],
    });
    slot.child = child;
    child.on('message', (raw) => {
      const message = parseWorkerToSupervisorMessage(raw);
      if (!message) {
        this.logger.warn(
          { event: 'worker_invalid_ipc', workerId: slot.id },
          'Ignoring malformed worker IPC message',
        );
        return;
      }
      this.handleMessage(slot, message);
    });
    child.once('exit', (code, signal) => this.handleExit(slot, code, signal));
    child.once('error', (error) =>
      this.logger.error(
        { event: 'worker_process_error', workerId: slot.id, err: error },
        'Worker process error',
      ),
    );
  }

  private handleMessage(slot: WorkerSlot, message: WorkerToSupervisorMessage): void {
    if (message.type === 'worker.ready') {
      if (
        message.workerId !== slot.id ||
        (slot.child?.pid !== undefined && message.pid !== slot.child.pid)
      ) {
        this.logger.warn(
          { event: 'worker_identity_mismatch', workerId: slot.id },
          'Ignoring worker readiness message with mismatched identity',
        );
        return;
      }
      slot.status = 'IDLE';
      slot.lastHeartbeatAt = Date.now();
      const healthyPid = slot.child?.pid;
      const resetTimer = setTimeout(() => {
        if (slot.child?.pid === healthyPid && ['IDLE', 'BUSY'].includes(slot.status)) {
          slot.consecutiveCrashes = 0;
        }
      }, 30_000);
      resetTimer.unref();
      this.logger.info(
        {
          event: slot.restartCount > 0 ? 'worker_restarted' : 'worker_ready',
          workerId: slot.id,
          pid: message.pid,
          restartCount: slot.restartCount,
        },
        'Worker is ready',
      );
      this.emit('state', this.toInfo(slot));
      this.emit('available', slot.id);
      return;
    }
    if (message.type === 'worker.heartbeat') {
      if (message.workerId !== slot.id) {
        this.logger.warn(
          { event: 'worker_identity_mismatch', workerId: slot.id },
          'Ignoring worker heartbeat with mismatched identity',
        );
        return;
      }
      slot.lastHeartbeatAt = Date.now();
      return;
    }
    if (!slot.current || slot.current.generationId !== message.generationId) {
      this.logger.warn(
        {
          event: 'worker_generation_mismatch',
          workerId: slot.id,
          generationId: message.generationId,
        },
        'Ignoring worker IPC message for a generation that is not assigned to this worker',
      );
      return;
    }
    if (message.type === 'generation.token' && slot.current) slot.current.tokensEmitted += 1;
    this.emit('message', slot.id, message);
    if (
      ['generation.completed', 'generation.failed', 'generation.cancelled'].includes(message.type)
    ) {
      slot.current = null;
      slot.status = 'IDLE';
      this.emit('state', this.toInfo(slot));
      this.emit('available', slot.id);
    }
  }

  private handleExit(slot: WorkerSlot, code: number | null, signal: NodeJS.Signals | null): void {
    const crashedGeneration = slot.current;
    slot.child = null;
    slot.status = this.shuttingDown ? 'STOPPED' : 'UNHEALTHY';
    slot.current = null;
    this.emit('state', this.toInfo(slot));
    if (this.shuttingDown || slot.expectedExit) return;
    slot.restartCount += 1;
    slot.consecutiveCrashes += 1;
    this.logger.error(
      {
        event: 'worker_crashed',
        workerId: slot.id,
        generationId: crashedGeneration?.generationId,
        code,
        signal,
      },
      'Worker exited unexpectedly',
    );
    this.emit('crash', { workerId: slot.id, generation: crashedGeneration } satisfies WorkerCrash);
    const delay = restartDelayMs(slot.consecutiveCrashes);
    slot.status = 'RESTARTING';
    this.emit('state', this.toInfo(slot));
    const timer = setTimeout(() => this.spawn(slot), delay);
    timer.unref();
  }

  private checkHeartbeats(): void {
    const now = Date.now();
    for (const slot of this.slots.values()) {
      if (!slot.child || !slot.lastHeartbeatAt || ['RESTARTING', 'STOPPED'].includes(slot.status))
        continue;
      if (isHeartbeatExpired(slot.lastHeartbeatAt, now, this.heartbeatTimeoutMs)) {
        slot.status = 'UNHEALTHY';
        this.emit('state', this.toInfo(slot));
        this.logger.warn(
          { event: 'worker_heartbeat_expired', workerId: slot.id },
          'Worker heartbeat expired',
        );
        slot.child.kill('SIGKILL');
      }
    }
  }

  execute(workerId: string, job: ExecuteJob): boolean {
    const slot = this.slots.get(workerId);
    if (!slot?.child || slot.status !== 'IDLE' || !slot.child.connected) return false;
    slot.status = 'BUSY';
    slot.current = { ...job, tokensEmitted: 0 };
    const message: SupervisorToWorkerMessage = {
      type: 'generation.execute',
      generationId: job.generationId,
      model: job.model,
      prompt: job.prompt,
      provider: this.provider,
      config: this.providerConfig,
    };
    try {
      slot.child.send(message, (error) => {
        if (error)
          this.logger.warn(
            {
              event: 'worker_ipc_send_failed',
              workerId,
              generationId: job.generationId,
              err: error,
            },
            'Unable to send generation to worker',
          );
      });
    } catch (error) {
      slot.current = null;
      slot.status = 'UNHEALTHY';
      this.logger.warn(
        { event: 'worker_ipc_send_failed', workerId, generationId: job.generationId, err: error },
        'Unable to send generation to worker',
      );
      return false;
    }
    this.emit('state', this.toInfo(slot));
    this.logger.info(
      {
        event: 'generation_dispatched',
        workerId,
        generationId: job.generationId,
        model: job.model,
      },
      'Generation dispatched to worker',
    );
    return true;
  }

  cancel(generationId: string): boolean {
    const slot = [...this.slots.values()].find(
      (item) => item.current?.generationId === generationId,
    );
    if (!slot?.child?.connected) return false;
    try {
      slot.child.send({
        type: 'generation.cancel',
        generationId,
      } satisfies SupervisorToWorkerMessage);
      return true;
    } catch (error) {
      this.logger.warn(
        { event: 'worker_ipc_send_failed', workerId: slot.id, generationId, err: error },
        'Unable to send cancellation to worker',
      );
      return false;
    }
  }

  kill(workerId: string): boolean {
    const slot = this.slots.get(workerId);
    if (!slot?.child) return false;
    return slot.child.kill('SIGKILL');
  }

  getIdleWorkerIds(): string[] {
    return [...this.slots.values()].filter((slot) => slot.status === 'IDLE').map((slot) => slot.id);
  }

  getWorkers(): WorkerInfo[] {
    return [...this.slots.values()].map((slot) => this.toInfo(slot));
  }
  isReady(): boolean {
    return [...this.slots.values()].some((slot) => ['IDLE', 'BUSY'].includes(slot.status));
  }

  private toInfo(slot: WorkerSlot): WorkerInfo {
    return {
      id: slot.id,
      pid: slot.child?.pid ?? null,
      status: slot.status,
      currentGenerationId: slot.current?.generationId ?? null,
      lastHeartbeatAt: slot.lastHeartbeatAt ? new Date(slot.lastHeartbeatAt).toISOString() : null,
      restartCount: slot.restartCount,
    };
  }

  async shutdown(graceMs: number): Promise<void> {
    this.shuttingDown = true;
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    const deadline = Date.now() + graceMs;
    while (
      [...this.slots.values()].some((slot) => slot.status === 'BUSY') &&
      Date.now() < deadline
    ) {
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
    for (const slot of this.slots.values()) {
      slot.expectedExit = true;
      if (slot.child?.connected)
        slot.child.send({ type: 'worker.shutdown' } satisfies SupervisorToWorkerMessage);
    }
    await new Promise((resolve) => setTimeout(resolve, 100));
    for (const slot of this.slots.values()) {
      if (slot.child) slot.child.kill('SIGTERM');
      slot.status = 'STOPPED';
    }
  }
}
