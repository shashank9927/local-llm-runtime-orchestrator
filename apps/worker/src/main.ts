import {
  parseSupervisorToWorkerMessage,
  type SupervisorToWorkerMessage,
  type WorkerToSupervisorMessage,
} from '@localllm/contracts';
import { createInferenceProvider } from '@localllm/inference';

const requestedWorkerId = process.argv[2];
if (!requestedWorkerId || !process.send)
  throw new Error('Worker must be started as a forked process with a worker ID.');
const workerId: string = requestedWorkerId;

const send = (message: WorkerToSupervisorMessage): void => {
  if (!process.connected || !process.send) return;
  try {
    process.send(message, () => undefined);
  } catch {
    // The supervisor may be shutting down; IPC failures must not crash the worker.
  }
};
const controllers = new Map<string, AbortController>();
let heartbeatIntervalMs = 2000;
let heartbeatTimer: NodeJS.Timeout | undefined;

function startHeartbeats(): void {
  if (heartbeatTimer) clearInterval(heartbeatTimer);
  heartbeatTimer = setInterval(
    () => send({ type: 'worker.heartbeat', workerId, timestamp: Date.now() }),
    heartbeatIntervalMs,
  );
  heartbeatTimer.unref();
}

async function execute(
  message: Extract<SupervisorToWorkerMessage, { type: 'generation.execute' }>,
): Promise<void> {
  const controller = new AbortController();
  controllers.set(message.generationId, controller);
  heartbeatIntervalMs = message.config.heartbeatIntervalMs;
  startHeartbeats();
  const timeout = setTimeout(
    () => controller.abort(new Error('Generation timed out.')),
    message.config.generationTimeoutMs,
  );
  try {
    const provider = createInferenceProvider(message.provider, message.config);
    send({ type: 'generation.started', generationId: message.generationId });
    let tokenCount = 0;
    for await (const chunk of provider.generateStream({
      generationId: message.generationId,
      model: message.model,
      prompt: message.prompt,
      signal: controller.signal,
    })) {
      tokenCount += 1;
      send({ type: 'generation.token', generationId: message.generationId, token: chunk.token });
    }
    send({ type: 'generation.completed', generationId: message.generationId, tokenCount });
  } catch (error) {
    if (controller.signal.aborted && controller.signal.reason === 'cancelled') {
      send({ type: 'generation.cancelled', generationId: message.generationId });
    } else {
      send({
        type: 'generation.failed',
        generationId: message.generationId,
        error: error instanceof Error ? error.message : 'Unknown inference failure.',
      });
    }
  } finally {
    clearTimeout(timeout);
    controllers.delete(message.generationId);
  }
}

process.on('message', (raw: unknown) => {
  const message = parseSupervisorToWorkerMessage(raw);
  if (!message) return;
  if (message.type === 'generation.execute') void execute(message);
  if (message.type === 'generation.cancel')
    controllers.get(message.generationId)?.abort('cancelled');
  if (message.type === 'worker.shutdown') {
    for (const controller of controllers.values()) controller.abort('cancelled');
    if (heartbeatTimer) clearInterval(heartbeatTimer);
    process.disconnect();
  }
});

startHeartbeats();
send({ type: 'worker.ready', workerId, pid: process.pid });
