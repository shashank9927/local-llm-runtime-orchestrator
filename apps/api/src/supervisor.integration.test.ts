import { resolve } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import type { WorkerProviderConfig, WorkerToSupervisorMessage } from '@localllm/contracts';
import { createLogger, loadConfig } from '@localllm/shared';
import { WorkerSupervisor, type WorkerCrash } from './supervisor.js';

const config = loadConfig({ NODE_ENV: 'test', LOG_LEVEL: 'silent' });
const workerScript = resolve(process.cwd(), 'apps/worker/dist/main.js');
const providerConfig: WorkerProviderConfig = {
  ollamaBaseUrl: 'http://127.0.0.1:11434',
  mockTokenDelayMs: 5,
  mockGenerationDelayMs: 5,
  mockFailureRate: 0,
  heartbeatIntervalMs: 100,
  generationTimeoutMs: 5_000,
};
const supervisors: WorkerSupervisor[] = [];

async function waitFor(predicate: () => boolean, timeoutMs = 5_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error('Timed out waiting for worker state.');
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}

function createSupervisor(
  configOverrides: Partial<WorkerProviderConfig> = {},
  script = workerScript,
  heartbeatTimeoutMs = 500,
): WorkerSupervisor {
  const supervisor = new WorkerSupervisor(
    script,
    1,
    heartbeatTimeoutMs,
    'mock',
    { ...providerConfig, ...configOverrides },
    createLogger(config),
  );
  supervisors.push(supervisor);
  return supervisor;
}

afterEach(async () => {
  await Promise.all(supervisors.splice(0).map((supervisor) => supervisor.shutdown(0)));
});

describe('WorkerSupervisor process integration', () => {
  it('forks a real worker that streams and returns to IDLE', async () => {
    const supervisor = createSupervisor();
    const messages: WorkerToSupervisorMessage[] = [];
    supervisor.on('message', (_workerId: string, message: WorkerToSupervisorMessage) => {
      messages.push(message);
    });
    supervisor.start();
    await waitFor(() => supervisor.getWorkers()[0]?.status === 'IDLE');
    const initial = supervisor.getWorkers()[0];
    expect(initial?.pid).toBeTypeOf('number');
    expect(initial?.pid).not.toBe(process.pid);
    expect(
      supervisor.execute('worker-1', {
        generationId: 'gen_worker_integration',
        model: 'mock:latest',
        prompt: 'stream a response',
      }),
    ).toBe(true);
    await waitFor(() => messages.some((message) => message.type === 'generation.completed'));
    expect(messages.map((message) => message.type)).toContain('generation.started');
    expect(messages.some((message) => message.type === 'generation.token')).toBe(true);
    await waitFor(() => supervisor.getWorkers()[0]?.status === 'IDLE');
  });

  it('replaces a deliberately killed idle child process', async () => {
    const supervisor = createSupervisor();
    supervisor.start();
    await waitFor(() => supervisor.getWorkers()[0]?.status === 'IDLE');
    const firstPid = supervisor.getWorkers()[0]?.pid;
    expect(supervisor.kill('worker-1')).toBe(true);
    await waitFor(() => {
      const worker = supervisor.getWorkers()[0];
      return worker?.status === 'IDLE' && worker.restartCount === 1 && worker.pid !== firstPid;
    }, 7_000);
  });

  it('reports a busy crash before the first token with the assigned generation', async () => {
    const supervisor = createSupervisor({ mockGenerationDelayMs: 2_000 });
    const crash = new Promise<WorkerCrash>((resolve) => supervisor.once('crash', resolve));
    supervisor.start();
    await waitFor(() => supervisor.getWorkers()[0]?.status === 'IDLE');
    expect(
      supervisor.execute('worker-1', {
        generationId: 'gen_crash_before_token',
        model: 'mock:latest',
        prompt: 'do not emit yet',
      }),
    ).toBe(true);
    await waitFor(() => supervisor.getWorkers()[0]?.status === 'BUSY');
    expect(supervisor.kill('worker-1')).toBe(true);
    await expect(crash).resolves.toMatchObject({
      workerId: 'worker-1',
      generation: { generationId: 'gen_crash_before_token', tokensEmitted: 0 },
    });
  });

  it('kills and replaces a live worker that stops heartbeating', async () => {
    const frozenWorker = resolve(process.cwd(), 'apps/api/test-fixtures/frozen-worker.mjs');
    const supervisor = createSupervisor({}, frozenWorker, 300);
    supervisor.start();
    await waitFor(() => supervisor.getWorkers()[0]?.status === 'IDLE');
    await waitFor(() => {
      const worker = supervisor.getWorkers()[0];
      return worker?.status === 'RESTARTING' && worker.restartCount === 1;
    }, 3_000);
  });
});
