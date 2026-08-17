import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { DeadLetterQueue, DeadLetter } from '../src/dlq';

describe('DeadLetterQueue', () => {
  let dlq: DeadLetterQueue<string>;

  beforeEach(() => {
    dlq = new DeadLetterQueue<string>({
      maxSize: 100,
      retentionMs: 86400000, // 24h
      maxRetries: 3,
      replayBatchSize: 10,
      onOverflow: 'drop-oldest',
    });
  });

  afterEach(() => {
    dlq.destroy();
  });

  function makeDeadLetter(id: string): DeadLetter<string> {
    return {
      id,
      originalMessage: `msg-${id}`,
      originalQueue: 'orders',
      failureReason: 'Processing timeout',
      failedAt: Date.now(),
      attempts: 3,
      headers: { 'x-trace-id': `trace-${id}` },
      replayCount: 0,
      expiresAt: 0,
    };
  }

  it('should accept dead letters', () => {
    expect(dlq.send(makeDeadLetter('1'))).toBe(true);
    expect(dlq.getMetrics().currentSize).toBe(1);
  });

  it('should retrieve by id', () => {
    dlq.send(makeDeadLetter('1'));
    const msg = dlq.get('1');
    expect(msg?.originalMessage).toBe('msg-1');
  });

  it('should list with filters', () => {
    dlq.send(makeDeadLetter('1'));
    dlq.send({ ...makeDeadLetter('2'), originalQueue: 'payments' });

    const ordersDLQ = dlq.list({ queue: 'orders' });
    expect(ordersDLQ).toHaveLength(1);
  });

  it('should replay messages', async () => {
    dlq.send(makeDeadLetter('1'));
    dlq.send(makeDeadLetter('2'));

    const result = await dlq.replay(['1', '2'], async () => true);
    expect(result.successful).toEqual(['1', '2']);
    expect(dlq.getMetrics().currentSize).toBe(0);
  });

  it('should handle replay failures', async () => {
    dlq.send(makeDeadLetter('1'));

    const result = await dlq.replay(['1'], async () => false);
    expect(result.failed).toEqual(['1']);
    expect(dlq.getMetrics().currentSize).toBe(1);
  });

  it('should skip unknown ids on replay', async () => {
    const result = await dlq.replay(['unknown'], async () => true);
    expect(result.skipped).toEqual(['unknown']);
  });

  it('should drop oldest on overflow', () => {
    for (let i = 0; i < 100; i++) {
      dlq.send({ ...makeDeadLetter(`${i}`), failedAt: Date.now() - (100 - i) * 1000 });
    }

    const overflow = dlq.send(makeDeadLetter('new'));
    expect(overflow).toBe(true);
    expect(dlq.getMetrics().currentSize).toBe(100);
  });

  it('should purge with filters', () => {
    dlq.send(makeDeadLetter('1'));
    dlq.send({ ...makeDeadLetter('2'), originalQueue: 'payments' });

    const purged = dlq.purge({ queue: 'orders' });
    expect(purged).toBe(1);
    expect(dlq.getMetrics().currentSize).toBe(1);
  });

  it('should delete individual messages', () => {
    dlq.send(makeDeadLetter('1'));
    expect(dlq.delete('1')).toBe(true);
    expect(dlq.get('1')).toBeNull();
  });
});
