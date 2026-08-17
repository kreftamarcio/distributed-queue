import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { PriorityQueue, PriorityMessage } from '../src/priority';

describe('PriorityQueue', () => {
  let queue: PriorityQueue<string>;

  beforeEach(() => {
    queue = new PriorityQueue<string>({
      lanes: [
        { name: 'critical', level: 0, weight: 10, maxSize: 100 },
        { name: 'high', level: 1, weight: 5, maxSize: 200 },
        { name: 'normal', level: 2, weight: 3, maxSize: 500 },
        { name: 'low', level: 3, weight: 1, maxSize: 1000 },
      ],
      starvationThresholdMs: 30000,
      promotionEnabled: false,
    });
  });

  afterEach(() => {
    queue.destroy();
  });

  it('should enqueue messages to correct lane', () => {
    const msg: PriorityMessage<string> = {
      id: '1',
      payload: 'test',
      priority: 0,
      enqueuedAt: Date.now(),
      attempts: 0,
    };

    expect(queue.enqueue(msg)).toBe(true);
    expect(queue.sizeByPriority(0)).toBe(1);
  });

  it('should reject invalid priority', () => {
    const msg: PriorityMessage<string> = {
      id: '1',
      payload: 'test',
      priority: 99,
      enqueuedAt: Date.now(),
      attempts: 0,
    };

    expect(queue.enqueue(msg)).toBe(false);
  });

  it('should dequeue higher priority first', () => {
    queue.enqueue({ id: '1', payload: 'low', priority: 3, enqueuedAt: Date.now(), attempts: 0 });
    queue.enqueue({ id: '2', payload: 'critical', priority: 0, enqueuedAt: Date.now(), attempts: 0 });
    queue.enqueue({ id: '3', payload: 'normal', priority: 2, enqueuedAt: Date.now(), attempts: 0 });

    const first = queue.dequeue();
    expect(first?.payload).toBe('critical');
  });

  it('should respect lane max size', () => {
    for (let i = 0; i < 100; i++) {
      queue.enqueue({ id: `${i}`, payload: `msg-${i}`, priority: 0, enqueuedAt: Date.now(), attempts: 0 });
    }

    const overflow = queue.enqueue({ id: 'overflow', payload: 'overflow', priority: 0, enqueuedAt: Date.now(), attempts: 0 });
    expect(overflow).toBe(false);
  });

  it('should track metrics', () => {
    queue.enqueue({ id: '1', payload: 'a', priority: 0, enqueuedAt: Date.now(), attempts: 0 });
    queue.enqueue({ id: '2', payload: 'b', priority: 1, enqueuedAt: Date.now(), attempts: 0 });
    queue.dequeue();

    const metrics = queue.getMetrics();
    expect(metrics.totalEnqueued).toBe(2);
    expect(metrics.totalDequeued).toBe(1);
  });

  it('should return null when empty', () => {
    expect(queue.dequeue()).toBeNull();
  });

  it('should peek without removing', () => {
    queue.enqueue({ id: '1', payload: 'peek-me', priority: 0, enqueuedAt: Date.now(), attempts: 0 });
    const peeked = queue.peek();
    expect(peeked?.payload).toBe('peek-me');
    expect(queue.size()).toBe(1);
  });
});
