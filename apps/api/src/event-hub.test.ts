import { describe, expect, it } from 'vitest';
import { createEvent } from '@localllm/contracts';
import { GenerationEventHub } from './event-hub.js';

describe('GenerationEventHub', () => {
  it('delivers ordered events and removes disconnected listeners', () => {
    const hub = new GenerationEventHub();
    const received: string[] = [];
    const unsubscribe = hub.subscribe('g1', (event) => received.push(event.type));
    hub.publish(createEvent('generation.started', 'g1', {}));
    hub.publish(createEvent('generation.token', 'g1', { token: 'hello' }));
    expect(received).toEqual(['generation.started', 'generation.token']);
    unsubscribe();
    expect(hub.listenerCount('g1')).toBe(0);
  });
});
