import { buildApp } from './app.js';
import { loadConfig } from '@localllm/shared';

const config = loadConfig();
const app = await buildApp(config);

async function shutdown(signal: string): Promise<void> {
  app.log.info({ event: 'shutdown_started', signal }, 'Graceful shutdown started');
  await app.close();
  process.exit(0);
}

process.once('SIGINT', () => void shutdown('SIGINT'));
process.once('SIGTERM', () => void shutdown('SIGTERM'));

try {
  await app.listen({ host: config.API_HOST, port: config.API_PORT });
} catch (error) {
  app.log.fatal({ err: error }, 'API failed to start');
  process.exit(1);
}
