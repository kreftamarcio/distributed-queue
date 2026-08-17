/**
 * Priority Lanes
 * Multi-level priority queue with starvation prevention
 * and dynamic priority promotion.
 */

export interface PriorityConfig {
  lanes: PriorityLane[];
  starvationThresholdMs: number;
  promotionEnabled: boolean;
  promotionIntervalMs?: number;
}

export interface PriorityLane {
  name: string;
  level: number; // 0 = highest
  weight: number; // For weighted fair queuing
  maxSize: number;
}

export interface PriorityMessage<T = unknown> {
  id: string;
  payload: T;
  priority: number;
  enqueuedAt: number;
  promotedFrom?: number;
  attempts: number;
  metadata?: Record<string, unknown>;
}

export interface PriorityMetrics {
  laneDepths: Map<number, number>;
  totalEnqueued: number;
  totalDequeued: number;
  promotions: number;
  starvationEvents: number;
}

export class PriorityQueue<T = unknown> {
  private lanes: Map<number, PriorityMessage<T>[]> = new Map();
  private totalEnqueued: number = 0;
  private totalDequeued: number = 0;
  private promotions: number = 0;
  private starvationEvents: number = 0;
  private promotionTimer: NodeJS.Timeout | null = null;
  private weightedIndex: number = 0;

  constructor(private readonly config: PriorityConfig) {
    for (const lane of config.lanes) {
      this.lanes.set(lane.level, []);
    }

    if (config.promotionEnabled) {
      this.startPromotionCheck();
    }
  }

  enqueue(message: PriorityMessage<T>): boolean {
    const lane = this.lanes.get(message.priority);
    if (!lane) return false;

    const laneConfig = this.config.lanes.find(l => l.level === message.priority);
    if (!laneConfig || lane.length >= laneConfig.maxSize) return false;

    lane.push(message);
    this.totalEnqueued++;
    return true;
  }

  dequeue(): PriorityMessage<T> | null {
    // Weighted fair queuing with starvation prevention
    const sortedLanes = [...this.config.lanes].sort((a, b) => a.level - b.level);

    // Check for starved messages first
    const starved = this.findStarvedMessage();
    if (starved) {
      this.starvationEvents++;
      this.totalDequeued++;
      return starved;
    }

    // Weighted selection
    const totalWeight = sortedLanes.reduce((sum, l) => {
      const queue = this.lanes.get(l.level)!;
      return sum + (queue.length > 0 ? l.weight : 0);
    }, 0);

    if (totalWeight === 0) return null;

    let accumulated = 0;
    const target = this.weightedIndex % totalWeight;
    this.weightedIndex++;

    for (const lane of sortedLanes) {
      const queue = this.lanes.get(lane.level)!;
      if (queue.length === 0) continue;

      accumulated += lane.weight;
      if (accumulated > target) {
        const message = queue.shift()!;
        this.totalDequeued++;
        return message;
      }
    }

    // Fallback: take from highest priority
    for (const lane of sortedLanes) {
      const queue = this.lanes.get(lane.level)!;
      if (queue.length > 0) {
        this.totalDequeued++;
        return queue.shift()!;
      }
    }

    return null;
  }

  peek(): PriorityMessage<T> | null {
    for (const [, queue] of [...this.lanes.entries()].sort(([a], [b]) => a - b)) {
      if (queue.length > 0) return queue[0];
    }
    return null;
  }

  size(): number {
    let total = 0;
    for (const [, queue] of this.lanes) {
      total += queue.length;
    }
    return total;
  }

  sizeByPriority(priority: number): number {
    return this.lanes.get(priority)?.length ?? 0;
  }

  getMetrics(): PriorityMetrics {
    const laneDepths = new Map<number, number>();
    for (const [level, queue] of this.lanes) {
      laneDepths.set(level, queue.length);
    }

    return {
      laneDepths,
      totalEnqueued: this.totalEnqueued,
      totalDequeued: this.totalDequeued,
      promotions: this.promotions,
      starvationEvents: this.starvationEvents,
    };
  }

  private findStarvedMessage(): PriorityMessage<T> | null {
    const now = Date.now();
    const sortedLanes = [...this.config.lanes].sort((a, b) => b.level - a.level);

    for (const lane of sortedLanes) {
      const queue = this.lanes.get(lane.level)!;
      if (queue.length === 0) continue;

      const oldest = queue[0];
      if (now - oldest.enqueuedAt > this.config.starvationThresholdMs) {
        return queue.shift()!;
      }
    }

    return null;
  }

  private startPromotionCheck(): void {
    const interval = this.config.promotionIntervalMs ?? 5000;

    this.promotionTimer = setInterval(() => {
      this.promoteStarvedMessages();
    }, interval);
  }

  private promoteStarvedMessages(): void {
    const now = Date.now();
    const sortedLanes = [...this.config.lanes].sort((a, b) => b.level - a.level);

    for (let i = 0; i < sortedLanes.length - 1; i++) {
      const currentLane = sortedLanes[i];
      const higherLane = sortedLanes[i + 1];
      const queue = this.lanes.get(currentLane.level)!;
      const targetQueue = this.lanes.get(higherLane.level)!;

      const toPromote: number[] = [];

      for (let j = 0; j < queue.length; j++) {
        const msg = queue[j];
        if (now - msg.enqueuedAt > this.config.starvationThresholdMs) {
          toPromote.push(j);
        }
      }

      // Promote in reverse order to maintain indices
      for (let j = toPromote.length - 1; j >= 0; j--) {
        const idx = toPromote[j];
        const msg = queue.splice(idx, 1)[0];
        msg.promotedFrom = msg.priority;
        msg.priority = higherLane.level;
        targetQueue.push(msg);
        this.promotions++;
      }
    }
  }

  destroy(): void {
    if (this.promotionTimer) {
      clearInterval(this.promotionTimer);
      this.promotionTimer = null;
    }
  }
}
