/**
 * Consumer Group Management
 * Coordinates multiple consumers processing messages from shared queues
 * with partition assignment and rebalancing.
 */

import { EventEmitter } from 'events';

export interface ConsumerGroupConfig {
  groupId: string;
  topics: string[];
  sessionTimeoutMs?: number;
  heartbeatIntervalMs?: number;
  rebalanceStrategy?: 'range' | 'round-robin' | 'sticky';
  maxPollRecords?: number;
}

export interface ConsumerMember {
  memberId: string;
  clientId: string;
  joinedAt: number;
  lastHeartbeat: number;
  assignedPartitions: PartitionAssignment[];
  status: 'active' | 'rebalancing' | 'dead';
}

export interface PartitionAssignment {
  topic: string;
  partition: number;
  offset: number;
}

export interface RebalanceEvent {
  type: 'join' | 'leave' | 'rebalance';
  memberId: string;
  timestamp: number;
  assignments: Map<string, PartitionAssignment[]>;
}

export class ConsumerGroup extends EventEmitter {
  private members: Map<string, ConsumerMember> = new Map();
  private partitionAssignments: Map<string, PartitionAssignment[]> = new Map();
  private heartbeatTimers: Map<string, NodeJS.Timeout> = new Map();
  private generation: number = 0;
  private leader: string | null = null;

  constructor(private readonly config: ConsumerGroupConfig) {
    super();
    this.config.sessionTimeoutMs ??= 30000;
    this.config.heartbeatIntervalMs ??= 10000;
    this.config.rebalanceStrategy ??= 'round-robin';
    this.config.maxPollRecords ??= 500;
  }

  async join(memberId: string, clientId: string): Promise<PartitionAssignment[]> {
    const member: ConsumerMember = {
      memberId,
      clientId,
      joinedAt: Date.now(),
      lastHeartbeat: Date.now(),
      assignedPartitions: [],
      status: 'active',
    };

    this.members.set(memberId, member);
    this.startHeartbeatMonitor(memberId);

    if (!this.leader) {
      this.leader = memberId;
    }

    await this.rebalance('join', memberId);
    return member.assignedPartitions;
  }

  async leave(memberId: string): Promise<void> {
    const member = this.members.get(memberId);
    if (!member) return;

    this.stopHeartbeatMonitor(memberId);
    this.members.delete(memberId);

    if (this.leader === memberId) {
      this.leader = this.members.size > 0
        ? this.members.keys().next().value ?? null
        : null;
    }

    await this.rebalance('leave', memberId);
  }

  heartbeat(memberId: string): boolean {
    const member = this.members.get(memberId);
    if (!member) return false;

    member.lastHeartbeat = Date.now();
    return true;
  }

  private async rebalance(type: 'join' | 'leave', triggeredBy: string): Promise<void> {
    this.generation++;

    const activeMembers = Array.from(this.members.values())
      .filter(m => m.status !== 'dead');

    // Mark all as rebalancing
    for (const member of activeMembers) {
      member.status = 'rebalancing';
    }

    const assignments = this.computeAssignments(activeMembers);

    // Apply assignments
    for (const [memberId, partitions] of assignments) {
      const member = this.members.get(memberId);
      if (member) {
        member.assignedPartitions = partitions;
        member.status = 'active';
      }
    }

    this.partitionAssignments = assignments;

    const event: RebalanceEvent = {
      type: type === 'join' || type === 'leave' ? type : 'rebalance',
      memberId: triggeredBy,
      timestamp: Date.now(),
      assignments,
    };

    this.emit('rebalance', event);
  }

  private computeAssignments(
    members: ConsumerMember[]
  ): Map<string, PartitionAssignment[]> {
    switch (this.config.rebalanceStrategy) {
      case 'round-robin':
        return this.roundRobinAssign(members);
      case 'range':
        return this.rangeAssign(members);
      case 'sticky':
        return this.stickyAssign(members);
      default:
        return this.roundRobinAssign(members);
    }
  }

  private roundRobinAssign(
    members: ConsumerMember[]
  ): Map<string, PartitionAssignment[]> {
    const assignments = new Map<string, PartitionAssignment[]>();
    const allPartitions = this.getAllPartitions();

    for (const member of members) {
      assignments.set(member.memberId, []);
    }

    let memberIndex = 0;
    for (const partition of allPartitions) {
      const member = members[memberIndex % members.length];
      assignments.get(member.memberId)!.push(partition);
      memberIndex++;
    }

    return assignments;
  }

  private rangeAssign(
    members: ConsumerMember[]
  ): Map<string, PartitionAssignment[]> {
    const assignments = new Map<string, PartitionAssignment[]>();
    const allPartitions = this.getAllPartitions();

    for (const member of members) {
      assignments.set(member.memberId, []);
    }

    const partitionsPerMember = Math.floor(allPartitions.length / members.length);
    const remainder = allPartitions.length % members.length;

    let partitionIndex = 0;
    for (let i = 0; i < members.length; i++) {
      const count = partitionsPerMember + (i < remainder ? 1 : 0);
      const memberPartitions = allPartitions.slice(partitionIndex, partitionIndex + count);
      assignments.set(members[i].memberId, memberPartitions);
      partitionIndex += count;
    }

    return assignments;
  }

  private stickyAssign(
    members: ConsumerMember[]
  ): Map<string, PartitionAssignment[]> {
    // Preserve existing assignments where possible
    const assignments = new Map<string, PartitionAssignment[]>();
    const unassigned: PartitionAssignment[] = [];
    const allPartitions = this.getAllPartitions();

    for (const member of members) {
      assignments.set(member.memberId, []);
    }

    // Keep existing assignments for members that still exist
    for (const partition of allPartitions) {
      let assigned = false;
      for (const member of members) {
        const existing = member.assignedPartitions.find(
          p => p.topic === partition.topic && p.partition === partition.partition
        );
        if (existing) {
          assignments.get(member.memberId)!.push(existing);
          assigned = true;
          break;
        }
      }
      if (!assigned) {
        unassigned.push(partition);
      }
    }

    // Distribute unassigned partitions to least-loaded members
    for (const partition of unassigned) {
      let minMember = members[0];
      let minCount = assignments.get(minMember.memberId)!.length;

      for (const member of members) {
        const count = assignments.get(member.memberId)!.length;
        if (count < minCount) {
          minMember = member;
          minCount = count;
        }
      }

      assignments.get(minMember.memberId)!.push(partition);
    }

    return assignments;
  }

  private getAllPartitions(): PartitionAssignment[] {
    const partitions: PartitionAssignment[] = [];
    for (const topic of this.config.topics) {
      // Default 12 partitions per topic
      for (let i = 0; i < 12; i++) {
        partitions.push({ topic, partition: i, offset: 0 });
      }
    }
    return partitions;
  }

  private startHeartbeatMonitor(memberId: string): void {
    const timer = setInterval(() => {
      const member = this.members.get(memberId);
      if (!member) {
        this.stopHeartbeatMonitor(memberId);
        return;
      }

      const elapsed = Date.now() - member.lastHeartbeat;
      if (elapsed > this.config.sessionTimeoutMs!) {
        member.status = 'dead';
        this.leave(memberId);
        this.emit('member-timeout', { memberId, elapsed });
      }
    }, this.config.heartbeatIntervalMs);

    this.heartbeatTimers.set(memberId, timer);
  }

  private stopHeartbeatMonitor(memberId: string): void {
    const timer = this.heartbeatTimers.get(memberId);
    if (timer) {
      clearInterval(timer);
      this.heartbeatTimers.delete(memberId);
    }
  }

  getMembers(): ConsumerMember[] {
    return Array.from(this.members.values());
  }

  getGeneration(): number {
    return this.generation;
  }

  getLeader(): string | null {
    return this.leader;
  }

  getGroupId(): string {
    return this.config.groupId;
  }
}
