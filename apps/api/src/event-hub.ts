import { EventEmitter } from 'node:events';
import type { GenerationEvent } from '@localllm/contracts';

export class GenerationEventHub {
  private readonly emitter = new EventEmitter();
  private readonly history = new Map<string, GenerationEvent[]>();
  private readonly expiryTimers = new Map<string, NodeJS.Timeout>();
  private readonly maxEventsPerGeneration: number;

  constructor(maxEventsPerGeneration = 2000) {
    this.maxEventsPerGeneration = maxEventsPerGeneration;
    this.emitter.setMaxListeners(0);
  }

  publish(event: GenerationEvent): void {
    const events = this.history.get(event.generationId) ?? [];
    events.push(event);
    if (events.length > this.maxEventsPerGeneration) events.shift();
    this.history.set(event.generationId, events);
    this.emitter.emit(event.generationId, event);
    if (
      ['generation.completed', 'generation.failed', 'generation.cancelled'].includes(event.type)
    ) {
      const previous = this.expiryTimers.get(event.generationId);
      if (previous) clearTimeout(previous);
      const timer = setTimeout(() => {
        this.history.delete(event.generationId);
        this.expiryTimers.delete(event.generationId);
      }, 60_000);
      this.expiryTimers.set(event.generationId, timer);
      timer.unref();
    }
  }

  subscribe(generationId: string, listener: (event: GenerationEvent) => void): () => void {
    this.emitter.on(generationId, listener);
    return () => this.emitter.off(generationId, listener);
  }

  getHistory(generationId: string): GenerationEvent[] {
    return [...(this.history.get(generationId) ?? [])];
  }

  listenerCount(generationId: string): number {
    return this.emitter.listenerCount(generationId);
  }

  close(): void {
    for (const timer of this.expiryTimers.values()) clearTimeout(timer);
    this.expiryTimers.clear();
    this.history.clear();
    this.emitter.removeAllListeners();
  }
}
