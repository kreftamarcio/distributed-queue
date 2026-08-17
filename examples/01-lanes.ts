/**
 * Runs against the real in-memory Queue, not the Redis API advertised in the
 * README. That README describes a target DistributedQueue; this file shows what
 * you can execute today.
 *
 *   npx tsx examples/01-lanes.ts
 */

import { Queue } from '../src/index.js';

const queue = new Queue<{
  to: string;
  template: string;
}>({
  lanes: {
    critical: { weight: 8 },
    high: { weight: 4 },
    normal: { weight: 2 },
    low: { weight: 1 },
  },
  maxStarvationMs: 60_000,
  visibility: { timeoutMs: 30_000, maxExtensions: 10 },
  deduplication: { enabled: true, windowMs: 3_600_000 },
  maxAttempts: 5,
});

const first = queue.enqueue({
  id: 'welcome-1',
  lane: 'high',
  payload: { to: 'user@example.com', template: 'welcome' },
  dedupeKey: 'welcome-email:user_882',
});

const duplicate = queue.enqueue({
  id: 'welcome-1-retry',
  lane: 'high',
  payload: { to: 'user@example.com', template: 'welcome' },
  dedupeKey: 'welcome-email:user_882',
});

console.log('first', first);
console.log('duplicate', duplicate);

queue.enqueue({
  id: 'digest-1',
  lane: 'low',
  payload: { to: 'ops@example.com', template: 'digest' },
});

const claimed = queue.claim('worker-a');

if (!claimed) {
  throw new Error('expected a claimed message');
}

console.log('claimed', { id: claimed.id, lane: claimed.lane, attempt: claimed.attempt });

const acked = queue.ack(claimed.id, 'worker-a');
console.log('acked', acked);
console.log('stats', queue.stats());
