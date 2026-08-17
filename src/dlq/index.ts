/**
 * Dead Letter Queue (DLQ)
 * Handles messages that fail processing after exhausting retries.
 * Supports inspection, replay, and expiration.
 */

export interface DLQConfig {
  maxSize: number;
  retentionMs: number;
  maxRetries: number;
  replayBatchSize: number;
  onOverflow: 'drop-oldest' | 'reject' | 'archive';
}

export interface DeadLetter<T = unknown> {
  id: string;
  originalMessage: T;
  originalQueue: string;
  failureReason: string;
  failureStack?: string;
  failedAt: number;
  attempts: number;
  headers: Record<string, string>;
  replayCount: number;
  expiresAt: number;
}

export interface DLQMetrics {
  totalReceived: number;
  totalReplayed: number;
  totalExpired: number;
  totalArchived: number;
  currentSize: number;
  oldestMessageAge: number;
}

export interface ReplayResult {
  successful: string[];
  failed: string[];
  skipped: string[];
}

export class DeadLetterQueue<T = unknown> {
  private messages: Map<string, DeadLetter<T>> = new Map();
  private totalReceived: number = 0;
  private totalReplayed: number = 0;
  private totalExpired: number = 0;
  private totalArchived: number = 0;
  private expirationTimer: NodeJS.Timeout | null = null;
  private archiveHandler?: (messages: DeadLetter<T>[]) => Promise<void>;

  constructor(private readonly config: DLQConfig) {
    this.startExpirationCheck();
  }

  send(message: DeadLetter<T>): boolean {
    if (this.messages.size >= this.config.maxSize) {
      return this.handleOverflow(message);
    }

    message.expiresAt = Date.now() + this.config.retentionMs;
    this.messages.set(message.id, message);
    this.totalReceived++;
    return true;
  }

  get(id: string): DeadLetter<T> | null {
    return this.messages.get(id) ?? null;
  }

  list(options?: {
    queue?: string;
    since?: number;
    limit?: number;
    offset?: number;
  }): DeadLetter<T>[] {
    let results = Array.from(this.messages.values());

    if (options?.queue) {
      results = results.filter(m => m.originalQueue === options.queue);
    }

    if (options?.since) {
      results = results.filter(m => m.failedAt >= options.since!);
    }

    results.sort((a, b) => b.failedAt - a.failedAt);

    const offset = options?.offset ?? 0;
    const limit = options?.limit ?? 50;
    return results.slice(offset, offset + limit);
  }

  async replay(
    ids: string[],
    handler: (message: T) => Promise<boolean>
  ): Promise<ReplayResult> {
    const result: ReplayResult = {
      successful: [],
      failed: [],
      skipped: [],
    };

    const batch = ids.slice(0, this.config.replayBatchSize);

    for (const id of batch) {
      const deadLetter = this.messages.get(id);
      if (!deadLetter) {
        result.skipped.push(id);
        continue;
      }

      try {
        const success = await handler(deadLetter.originalMessage);
        if (success) {
          this.messages.delete(id);
          this.totalReplayed++;
          result.successful.push(id);
        } else {
          deadLetter.replayCount++;
          result.failed.push(id);
        }
      } catch {
        deadLetter.replayCount++;
        result.failed.push(id);
      }
    }

    return result;
  }

  async replayAll(
    handler: (message: T) => Promise<boolean>,
    filter?: { queue?: string; maxReplayCount?: number }
  ): Promise<ReplayResult> {
    let candidates = Array.from(this.messages.entries());

    if (filter?.queue) {
      candidates = candidates.filter(([, m]) => m.originalQueue === filter.queue);
    }

    if (filter?.maxReplayCount !== undefined) {
      candidates = candidates.filter(([, m]) => m.replayCount < filter.maxReplayCount!);
    }

    const ids = candidates.map(([id]) => id);
    return this.replay(ids, handler);
  }

  delete(id: string): boolean {
    return this.messages.delete(id);
  }

  purge(filter?: { queue?: string; olderThanMs?: number }): number {
    let count = 0;
    const now = Date.now();

    for (const [id, message] of this.messages) {
      let shouldDelete = true;

      if (filter?.queue && message.originalQueue !== filter.queue) {
        shouldDelete = false;
      }

      if (filter?.olderThanMs && now - message.failedAt < filter.olderThanMs) {
        shouldDelete = false;
      }

      if (shouldDelete) {
        this.messages.delete(id);
        count++;
      }
    }

    return count;
  }

  getMetrics(): DLQMetrics {
    let oldestAge = 0;
    const now = Date.now();

    for (const message of this.messages.values()) {
      const age = now - message.failedAt;
      if (age > oldestAge) oldestAge = age;
    }

    return {
      totalReceived: this.totalReceived,
      totalReplayed: this.totalReplayed,
      totalExpired: this.totalExpired,
      totalArchived: this.totalArchived,
      currentSize: this.messages.size,
      oldestMessageAge: oldestAge,
    };
  }

  setArchiveHandler(handler: (messages: DeadLetter<T>[]) => Promise<void>): void {
    this.archiveHandler = handler;
  }

  private handleOverflow(message: DeadLetter<T>): boolean {
    switch (this.config.onOverflow) {
      case 'drop-oldest': {
        const oldest = this.findOldest();
        if (oldest) {
          this.messages.delete(oldest.id);
          this.messages.set(message.id, message);
          this.totalReceived++;
          return true;
        }
        return false;
      }
      case 'reject':
        return false;
      case 'archive': {
        this.archiveOldest();
        this.messages.set(message.id, message);
        this.totalReceived++;
        return true;
      }
      default:
        return false;
    }
  }

  private findOldest(): DeadLetter<T> | null {
    let oldest: DeadLetter<T> | null = null;
    for (const message of this.messages.values()) {
      if (!oldest || message.failedAt < oldest.failedAt) {
        oldest = message;
      }
    }
    return oldest;
  }

  private async archiveOldest(): Promise<void> {
    const sorted = Array.from(this.messages.values())
      .sort((a, b) => a.failedAt - b.failedAt);

    const toArchive = sorted.slice(0, Math.ceil(this.config.maxSize * 0.1));

    if (this.archiveHandler && toArchive.length > 0) {
      await this.archiveHandler(toArchive);
      for (const msg of toArchive) {
        this.messages.delete(msg.id);
        this.totalArchived++;
      }
    }
  }

  private startExpirationCheck(): void {
    this.expirationTimer = setInterval(() => {
      const now = Date.now();
      for (const [id, message] of this.messages) {
        if (now >= message.expiresAt) {
          this.messages.delete(id);
          this.totalExpired++;
        }
      }
    }, 60000);
  }

  destroy(): void {
    if (this.expirationTimer) {
      clearInterval(this.expirationTimer);
      this.expirationTimer = null;
    }
  }
}
