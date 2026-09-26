import chalk from 'chalk';
import { Command } from 'commander';
import ora from 'ora';
import { ApiClient, ApiClientError } from './api-client.js';
import { duration, health, relativeTime, table } from './format.js';

function printError(error: unknown): void {
  if (error instanceof ApiClientError) console.error(chalk.red(`${error.code}: ${error.message}`));
  else console.error(chalk.red(error instanceof Error ? error.message : 'Unexpected error'));
  process.exitCode = 1;
}

export function createProgram(
  defaultBaseUrl = process.env.API_BASE_URL ?? 'http://127.0.0.1:3000',
): Command {
  const program = new Command();
  program
    .name('llm-runtime')
    .description('Operate the LocalLLM Runtime Orchestrator')
    .version('1.0.0');
  program.option('--api-url <url>', 'API base URL', defaultBaseUrl);
  const client = () => new ApiClient(program.opts<{ apiUrl: string }>().apiUrl);

  program
    .command('generate')
    .description('Queue a generation and stream its output')
    .requiredOption('-p, --prompt <text>', 'prompt text')
    .option('-m, --model <model>', 'model identifier', 'mock:latest')
    .option('--priority <priority>', 'interactive, normal, or background', 'normal')
    .action(
      async (options: {
        prompt: string;
        model: string;
        priority: 'interactive' | 'normal' | 'background';
      }) => {
        try {
          const created = await client().createGeneration(options);
          console.log(
            `Generation: ${created.id}\nPriority: ${created.priority}\nModel: ${created.model}\n`,
          );
          const spinner = ora('Queued...').start();
          let outputStarted = false;
          await client().stream(created.streamUrl, (event) => {
            if (event.type === 'worker.assigned')
              spinner.text = `Assigned to ${String(event.data.workerId)}`;
            if (event.type === 'generation.started') spinner.text = 'Generating...';
            if (event.type === 'generation.token') {
              if (!outputStarted) {
                spinner.stop();
                process.stdout.write('\n');
                outputStarted = true;
              }
              process.stdout.write(String(event.data.token));
            }
            if (event.type === 'generation.completed') {
              if (!outputStarted) spinner.stop();
              console.log(`\n\n${chalk.green('✓ Completed')}\n`);
              console.log(
                `Worker: ${String(event.data.workerId)}\nTime to first token: ${duration(event.data.timeToFirstTokenMs as number | null)}\nTotal time: ${duration(event.data.durationMs as number | null)}`,
              );
            }
            if (event.type === 'generation.failed' || event.type === 'generation.cancelled') {
              spinner.stop();
              console.error(
                chalk.red(
                  `\n${event.type === 'generation.failed' ? 'Failed' : 'Cancelled'}: ${String(event.data.error ?? event.data.reason ?? '')}`,
                ),
              );
            }
          });
        } catch (error) {
          printError(error);
        }
      },
    );

  program
    .command('workers')
    .description('List inference workers')
    .action(async () => {
      try {
        const workers = await client().workers();
        console.log('\nWORKERS\n');
        console.log(
          table(
            ['ID', 'PID', 'STATUS', 'JOB', 'HEARTBEAT', 'RESTARTS'],
            workers.map((w) => [
              w.id,
              String(w.pid ?? '-'),
              w.status,
              w.currentGenerationId ?? '-',
              relativeTime(w.lastHeartbeatAt),
              String(w.restartCount),
            ]),
          ),
        );
      } catch (error) {
        printError(error);
      }
    });

  program
    .command('queue')
    .description('Show queue depth and worker capacity')
    .action(async () => {
      try {
        const queue = await client().queue();
        const bar = (count: number) => `${'█'.repeat(Math.min(count, 30))} ${count}`;
        console.log(
          `\nQUEUE STATUS\n\nInteractive  ${bar(queue.interactive)}\nNormal       ${bar(queue.normal)}\nBackground   ${bar(queue.background)}\n\nRunning: ${queue.running}\nCapacity: ${queue.running} / ${queue.capacity} workers`,
        );
      } catch (error) {
        printError(error);
      }
    });

  program
    .command('models')
    .description('List known models and memory state')
    .action(async () => {
      try {
        const models = await client().models();
        console.log(
          table(
            ['MODEL', 'MEMORY (EST.)', 'STATUS', 'ACTIVE', 'LAST USED'],
            models.map((m) => [
              m.name,
              `${m.estimatedMemoryMb} MB`,
              m.loaded ? 'LOADED' : 'AVAILABLE',
              String(m.activeRequests),
              relativeTime(m.lastUsedAt),
            ]),
          ),
        );
      } catch (error) {
        printError(error);
      }
    });

  program
    .command('generations')
    .description('List generation history')
    .option('--limit <number>', 'maximum rows', '20')
    .option('--status <status>')
    .option('--priority <priority>')
    .action(async (options: { limit: string; status?: string; priority?: string }) => {
      try {
        const generations = await client().listGenerations({
          limit: Number(options.limit),
          ...(options.status ? { status: options.status } : {}),
          ...(options.priority ? { priority: options.priority } : {}),
        });
        console.log(
          table(
            ['ID', 'MODEL', 'PRIORITY', 'STATUS', 'WORKER'],
            generations.map((g) => [g.id, g.model, g.priority, g.status, g.workerId ?? '-']),
          ),
        );
      } catch (error) {
        printError(error);
      }
    });

  program
    .command('generation <id>')
    .description('Show generation details')
    .action(async (id: string) => {
      try {
        const g = await client().getGeneration(id);
        console.log(
          `\nGeneration ${g.id}\n\nModel: ${g.model}\nPriority: ${g.priority}\nStatus: ${g.status}\nWorker: ${g.workerId ?? '-'}\n\nCreated: ${new Date(g.createdAt).toLocaleString()}\nQueued: ${g.queuedAt ? new Date(g.queuedAt).toLocaleString() : '-'}\nStarted: ${g.startedAt ? new Date(g.startedAt).toLocaleString() : '-'}\nCompleted: ${g.completedAt ? new Date(g.completedAt).toLocaleString() : '-'}\n\nQueue wait: ${duration(g.queueWaitMs)}\nTime to first token: ${duration(g.timeToFirstTokenMs)}\nTotal duration: ${duration(g.durationMs)}\nTokens: ${g.tokenCount}`,
        );
      } catch (error) {
        printError(error);
      }
    });

  program
    .command('cancel <id>')
    .description('Cancel a queued or running generation')
    .action(async (id: string) => {
      try {
        await client().cancel(id);
        console.log(chalk.green(`✓ Cancellation requested for ${id}`));
      } catch (error) {
        printError(error);
      }
    });

  program
    .command('status')
    .description('Show runtime health')
    .action(async () => {
      try {
        const status = await client().status();
        console.log(
          `\nLocalLLM Runtime\n\nAPI          ${health(status.api)}\nPostgreSQL   ${health(status.postgres)}\nRedis        ${health(status.redis)}\nInference    ${health(status.inference)}\n\nWorkers      ${status.workers.healthy} / ${status.workers.total} healthy\nRunning      ${status.running} generations\nQueued       ${status.queued} generations\nProvider     ${status.provider}`,
        );
      } catch (error) {
        printError(error);
      }
    });

  const worker = program.command('worker').description('Development worker controls');
  worker
    .command('kill <id>')
    .description('Kill a worker to demonstrate automatic recovery')
    .action(async (id: string) => {
      try {
        const api = client();
        const spinner = ora(`Killing ${id}...`).start();
        await api.killWorker(id);
        const deadline = Date.now() + 15_000;
        let sawRestart = false;
        while (Date.now() < deadline) {
          const found = (await api.workers()).find((item) => item.id === id);
          if (found && ['UNHEALTHY', 'RESTARTING', 'STARTING'].includes(found.status))
            sawRestart = true;
          if (found?.status === 'IDLE' && found.restartCount > 0 && sawRestart) {
            spinner.succeed('Worker successfully recovered');
            return;
          }
          await new Promise((resolve) => setTimeout(resolve, 250));
        }
        spinner.fail('Worker did not recover within 15 seconds');
        process.exitCode = 1;
      } catch (error) {
        printError(error);
      }
    });
  return program;
}
