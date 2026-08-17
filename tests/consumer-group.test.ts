import { describe, it, expect, beforeEach } from 'vitest';
import { ConsumerGroup } from '../src/consumer-group';

describe('ConsumerGroup', () => {
  let group: ConsumerGroup;

  beforeEach(() => {
    group = new ConsumerGroup({
      groupId: 'test-group',
      topics: ['orders', 'payments'],
      sessionTimeoutMs: 5000,
      rebalanceStrategy: 'round-robin',
    });
  });

  it('should assign partitions on join', async () => {
    const assignments = await group.join('member-1', 'client-1');
    expect(assignments.length).toBeGreaterThan(0);
    expect(group.getMembers()).toHaveLength(1);
  });

  it('should rebalance when second member joins', async () => {
    await group.join('member-1', 'client-1');
    await group.join('member-2', 'client-2');

    const members = group.getMembers();
    expect(members).toHaveLength(2);

    const m1Partitions = members[0].assignedPartitions.length;
    const m2Partitions = members[1].assignedPartitions.length;
    expect(m1Partitions + m2Partitions).toBe(24); // 12 per topic * 2 topics
    expect(Math.abs(m1Partitions - m2Partitions)).toBeLessThanOrEqual(1);
  });

  it('should reassign on leave', async () => {
    await group.join('member-1', 'client-1');
    await group.join('member-2', 'client-2');
    await group.leave('member-2');

    const members = group.getMembers();
    expect(members).toHaveLength(1);
    expect(members[0].assignedPartitions.length).toBe(24);
  });

  it('should elect first member as leader', async () => {
    await group.join('member-1', 'client-1');
    await group.join('member-2', 'client-2');
    expect(group.getLeader()).toBe('member-1');
  });

  it('should increment generation on rebalance', async () => {
    expect(group.getGeneration()).toBe(0);
    await group.join('member-1', 'client-1');
    expect(group.getGeneration()).toBe(1);
    await group.join('member-2', 'client-2');
    expect(group.getGeneration()).toBe(2);
  });

  it('should accept heartbeats', async () => {
    await group.join('member-1', 'client-1');
    expect(group.heartbeat('member-1')).toBe(true);
    expect(group.heartbeat('nonexistent')).toBe(false);
  });
});
