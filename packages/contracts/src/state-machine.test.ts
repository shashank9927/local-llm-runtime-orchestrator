import { describe, expect, it } from 'vitest';
import type { GenerationStatus } from './index.js';
import {
  AppError,
  assertTransition,
  canTransition,
  parseSupervisorToWorkerMessage,
  parseWorkerToSupervisorMessage,
} from './index.js';

const statuses: GenerationStatus[] = [
  'CREATED',
  'QUEUED',
  'RUNNING',
  'COMPLETED',
  'FAILED',
  'CANCELLED',
];

const permittedTargets: Record<GenerationStatus, GenerationStatus[]> = {
  CREATED: ['QUEUED', 'CANCELLED', 'FAILED'],
  QUEUED: ['RUNNING', 'CANCELLED', 'FAILED'],
  RUNNING: ['COMPLETED', 'FAILED', 'CANCELLED', 'QUEUED'],
  COMPLETED: [],
  FAILED: [],
  CANCELLED: [],
};

describe('generation state machine', () => {
  it('allows the normal lifecycle', () => {
    expect(canTransition('CREATED', 'QUEUED')).toBe(true);
    expect(canTransition('QUEUED', 'RUNNING')).toBe(true);
    expect(canTransition('RUNNING', 'COMPLETED')).toBe(true);
  });

  it('allows cancellation and a single recovery requeue', () => {
    expect(canTransition('QUEUED', 'CANCELLED')).toBe(true);
    expect(canTransition('RUNNING', 'CANCELLED')).toBe(true);
    expect(canTransition('RUNNING', 'QUEUED')).toBe(true);
  });

  it('allows creation to fail when queue admission fails', () => {
    expect(canTransition('CREATED', 'FAILED')).toBe(true);
  });

  it('rejects terminal-state transitions', () => {
    expect(() => assertTransition('COMPLETED', 'RUNNING')).toThrow(AppError);
    expect(canTransition('FAILED', 'QUEUED')).toBe(false);
  });

  it('defines every legal and illegal transition explicitly', () => {
    for (const from of statuses) {
      for (const to of statuses) {
        const permitted = permittedTargets[from].includes(to);
        expect(canTransition(from, to)).toBe(permitted);
        if (permitted) {
          expect(() => assertTransition(from, to)).not.toThrow();
        } else {
          expect(() => assertTransition(from, to)).toThrow(AppError);
        }
      }
    }
  });

  it('rejects malformed and over-permissive worker IPC payloads', () => {
    expect(parseWorkerToSupervisorMessage(null)).toBeNull();
    expect(
      parseWorkerToSupervisorMessage({
        type: 'generation.completed',
        generationId: 'gen_1',
        tokenCount: 1,
        unexpected: true,
      }),
    ).toBeNull();
    expect(
      parseSupervisorToWorkerMessage({
        type: 'generation.cancel',
        generationId: 'gen_1',
        unexpected: true,
      }),
    ).toBeNull();
    expect(
      parseWorkerToSupervisorMessage({
        type: 'worker.ready',
        workerId: 'worker-1',
        pid: 123,
      }),
    ).toEqual({ type: 'worker.ready', workerId: 'worker-1', pid: 123 });
  });
});
