import chalk from 'chalk';
import Table from 'cli-table3';

export function duration(value: number | null | undefined): string {
  if (value === null || value === undefined) return '-';
  return value < 1000 ? `${value} ms` : `${(value / 1000).toFixed(1)} s`;
}

export function relativeTime(value: string | Date | null): string {
  if (!value) return 'never';
  const elapsed = Date.now() - new Date(value).getTime();
  if (elapsed < 1000) return '<1s ago';
  if (elapsed < 60_000) return `${Math.floor(elapsed / 1000)}s ago`;
  return `${Math.floor(elapsed / 60_000)}m ago`;
}

export function health(value: boolean): string {
  return value ? chalk.green('HEALTHY') : chalk.red('UNHEALTHY');
}

export function table(head: string[], rows: string[][]): string {
  return new Table({ head, style: { head: ['cyan'], border: [] } }).concat(rows).toString();
}
