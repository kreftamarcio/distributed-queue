/**
 * distributed-queue: a task queue with honest delivery semantics.
 *
 * On the name: this provides effectively-once PROCESSING, not exactly-once
 * DELIVERY. The latter is impossible across an unreliable network, and a library
 * claiming it teaches users to skip the idempotency work they still need. See the
 * README for the derivation.
 *
 * Three components, each usable alone:
 *
 *   VisibilityTracker  claim tracking with deadline-based crash recovery
 *   DedupeFilter       windowed key tracking with a bloom pre-filter
 *   Scheduler          weighted lane selection with a starvation bound
 *
 * Queue composes them. The composition order matters and is not arbitrary.
 */

export { VisibilityTracker } from './delivery/visibility.js';
export type {
  Lane,
  InFlightMessage,
  VisibilityConfig,
  ClaimOutcome,
  ExtendOutcome,
  ReleaseReason,
  ReleasedMessage,
} from './delivery/visibility.js';

export { DedupeFilter } from './deduplication/dedupe-filter.js';
export type { DedupeConfig, DedupeResult } from './deduplication/dedupe-filter.js';

export { Scheduler } from './scheduling/scheduler.js';
export type {
  LaneConfig,
  SchedulingConfig,
  LaneState,
  SelectionResult,
  SchedulerStats,
} from './scheduling/scheduler.js';

import { VisibilityTracker, type Lane, type ReleasedMessage } from './delivery/visibility.js';
import { DedupeFilter, type DedupeConfig } from './deduplication/dedupe-filter.js';
import { Scheduler, type LaneConfig, type LaneState } from './scheduling/scheduler.js';

export interface QueueMessage<T = unknown> {
  id: string;
  lane: Lane;
  payload: T;
  enqueuedAt: number;
  attempt: number;
  /** Key used for deduplication, when the producer supplied one. */
  dedupeKey?: string;
}

export interface QueueConfig {
  /** Lane name to weight. Higher weight receives a larger share of selections. */
  lanes: Record<string, LaneConfig>;
  /** Nothing waits longer than this, regardless of lane weight. */
  maxStarvationMs: number;
  visibility: { timeoutMs: number; maxExtensions: number };
  deduplication?: DedupeConfig;
  /** Attempts before a message is dead-lettered. */
  maxAttempts?: number;
}

export interface EnqueueResult {
  id: string;
  /** True when a prior message with the same dedupe key already exists. */
  deduplicated: boolean;
  /** Id of the original, when deduplicated. */
  originalId?: string;
}

export interface DeadLetter<T = unknown> {
  message: QueueMessage<T>;
  reason: string;
  attempts: number;
  deadLetteredAt: number;
  /**
   * True when the final failure was a visibility expiry on a claim.
   *
   * This distinction is the reason the field exists: an expired claim means the
   * outcome is UNKNOWN rather than failed, because the handler may have completed
   * its side effect after the deadline passed. An operator deciding whether to
   * replay needs that, and a boolean "failed" flag destroys it.
   */
  outcomeUnknown: boolean;
}

const DEFAULT_MAX_ATTEMPTS = 5;

export class Queue<T = unknown> {
  private readonly lanes = new Map<Lane, QueueMessage<T>[]>();
  private readonly visibility: VisibilityTracker;
  private readonly dedupe: DedupeFilter | null;
  private readonly scheduler: Scheduler;
  private readonly maxAttempts: number;
  private readonly deadLetters: Array<DeadLetter<T>> = [];

  constructor(private readonly config: QueueConfig) {
    if (Object.keys(config.lanes).length === 0) {
      throw new Error('Queue requires at least one lane');
    }

    for (const lane of Object.keys(config.lanes)) {
      this.lanes.set(lane, []);
    }

    this.scheduler = new Scheduler(config.lanes, {
      maxStarvationMs: config.maxStarvationMs,
    });

    this.visibility = new VisibilityTracker(config.visibility);

    this.dedupe =
      config.deduplication?.enabled === true ? new DedupeFilter(config.deduplication) : null;

    this.maxAttempts = config.maxAttempts ?? DEFAULT_MAX_ATTEMPTS;
  }

  /**
   * Enqueue a message.
   *
   * Deduplication runs BEFORE the message occupies a lane. Checking afterwards would
   * let a duplicate consume a slot and be selected by the scheduler before anything
   * noticed, which defeats the guarantee entirely.
   */
  enqueue(params: {
    id: string;
    lane: Lane;
    payload: T;
    dedupeKey?: string;
    now?: number;
  }): EnqueueResult {
    const now = params.now ?? Date.now();
    const queue = this.lanes.get(params.lane);

    if (!queue) {
      throw new Error(
        `Unknown lane "${params.lane}". Configured lanes: ` +
          `${[...this.lanes.keys()].join(', ')}.`,
      );
    }

    if (this.dedupe && params.dedupeKey !== undefined) {
      const check = this.dedupe.check(params.dedupeKey, params.id, now);

      if (check.duplicate) {
        return {
          id: check.originalId ?? params.id,
          deduplicated: true,
          ...(check.originalId !== undefined ? { originalId: check.originalId } : {}),
        };
      }
    }

    queue.push({
      id: params.id,
      lane: params.lane,
      payload: params.payload,
      enqueuedAt: now,
      attempt: 1,
      ...(params.dedupeKey !== undefined ? { dedupeKey: params.dedupeKey } : {}),
    });

    return { id: params.id, deduplicated: false };
  }

  /**
   * Claim the next message for a consumer.
   *
   * Expired claims are reaped first, on every poll. Reaping on demand rather than on a
   * background timer keeps the process free to exit and is sufficient, because nothing
   * observes an expired claim until the next poll anyway.
   */
  claim(consumerId: string, now: number = Date.now()): QueueMessage<T> | null {
    for (const released of this.visibility.reapExpired(now)) {
      this.handleRelease(released, now);
    }

    const selection = this.scheduler.select(this.laneStates(now), now);
    if (!selection) return null;

    const queue = this.lanes.get(selection.lane);
    const message = queue?.shift();
    if (!message) return null;

    const outcome = this.visibility.claim(
      message.id,
      message.lane,
      consumerId,
      message.attempt,
      now,
    );

    if (!outcome.ok) {
      // Someone else holds a live claim on this id. Put it back at the FRONT rather
      // than the back: it was already at the head of its lane, and sending it to the
      // tail would silently reorder the queue on every contended claim.
      queue?.unshift(message);
      return null;
    }

    return { ...message, attempt: outcome.message.attempt };
  }

  /** Extend a claim. Used by a long-running handler via heartbeat. */
  heartbeat(messageId: string, consumerId: string, now: number = Date.now()): boolean {
    return this.visibility.extend(messageId, consumerId, now).ok;
  }

  ack(messageId: string, consumerId: string, now: number = Date.now()): boolean {
    const released = this.visibility.ack(messageId, consumerId, now);
    if (!released) return false;

    // The dedupe key is deliberately NOT forgotten on success. Forgetting it would
    // let an at-least-once redelivery of the same logical message be accepted as new,
    // which is exactly the duplicate the filter exists to prevent.
    return true;
  }

  /** Nack: return the message for retry, or dead-letter it if the budget is spent. */
  nack(
    messageId: string,
    consumerId: string,
    reason: string,
    now: number = Date.now(),
  ): { requeued: boolean; deadLettered: boolean } {
    const released = this.visibility.nack(messageId, consumerId, now);
    if (!released) return { requeued: false, deadLettered: false };

    return this.handleRelease({ ...released, reason: 'nack' }, now, reason);
  }

  /**
   * Dead letters, for inspection before replay.
   *
   * Returned as copies so a caller cannot mutate the record it is about to act on.
   */
  getDeadLetters(): Array<DeadLetter<T>> {
    return this.deadLetters.map((entry) => ({ ...entry, message: { ...entry.message } }));
  }

  /**
   * Replay dead letters after the underlying cause is fixed.
   *
   * The dedupe key is forgotten first, otherwise the replay would be rejected as a
   * duplicate of the failure that produced it. Replays default to attempt 1, because
   * carrying the exhausted count forward would dead-letter them again immediately.
   */
  replay(ids: readonly string[], lane?: Lane, now: number = Date.now()): number {
    let replayed = 0;

    for (const id of ids) {
      const index = this.deadLetters.findIndex((entry) => entry.message.id === id);
      if (index === -1) continue;

      const [entry] = this.deadLetters.splice(index, 1);
      if (!entry) continue;

      if (this.dedupe && entry.message.dedupeKey !== undefined) {
        this.dedupe.forget(entry.message.dedupeKey);
      }

      const targetLane = lane ?? entry.message.lane;
      const queue = this.lanes.get(targetLane);
      if (!queue) continue;

      queue.push({ ...entry.message, attempt: 1, enqueuedAt: now });
      replayed++;
    }

    return replayed;
  }

  stats(now: number = Date.now()): {
    depthByLane: Record<Lane, number>;
    totalDepth: number;
    inFlight: ReturnType<VisibilityTracker['stats']>;
    scheduling: ReturnType<Scheduler['getStats']>;
    starvationViolations: ReturnType<Scheduler['starvationViolations']>;
    deadLetterCount: number;
    /** Dead letters whose outcome is unknown rather than failed. */
    unknownOutcomeCount: number;
    dedupe: ReturnType<DedupeFilter['getStats']> | null;
  } {
    const depthByLane: Record<Lane, number> = {};
    let totalDepth = 0;

    for (const [lane, queue] of this.lanes) {
      depthByLane[lane] = queue.length;
      totalDepth += queue.length;
    }

    return {
      depthByLane,
      totalDepth,
      inFlight: this.visibility.stats(now),
      scheduling: this.scheduler.getStats(),
      // Surfaced because a sustained violation means the consumer pool is undersized,
      // and no scheduling policy can fix insufficient throughput.
      starvationViolations: this.scheduler.starvationViolations(this.laneStates(now), now),
      deadLetterCount: this.deadLetters.length,
      unknownOutcomeCount: this.deadLetters.filter((d) => d.outcomeUnknown).length,
      dedupe: this.dedupe?.getStats() ?? null,
    };
  }

  private handleRelease(
    released: ReleasedMessage,
    now: number,
    reason = 'visibility timeout expired',
  ): { requeued: boolean; deadLettered: boolean } {
    const queue = this.lanes.get(released.lane);
    if (!queue) return { requeued: false, deadLettered: false };

    if (released.attempt >= this.maxAttempts) {
      this.deadLetters.push({
        message: {
          id: released.id,
          lane: released.lane,
          payload: undefined as unknown as T,
          enqueuedAt: now,
          attempt: released.attempt,
        },
        reason,
        attempts: released.attempt,
        deadLetteredAt: now,
        // presumedDead means the consumer never acked or nacked, so the handler may
        // have completed after the deadline. That is unknown, not failed.
        outcomeUnknown: released.presumedDead,
      });

      return { requeued: false, deadLettered: true };
    }

    queue.push({
      id: released.id,
      lane: released.lane,
      payload: undefined as unknown as T,
      enqueuedAt: now,
      attempt: released.attempt + 1,
    });

    return { requeued: true, deadLettered: false };
  }

  private laneStates(now: number): LaneState[] {
    return [...this.lanes.entries()].map(([lane, queue]) => ({
      name: lane,
      weight: this.config.lanes[lane]?.weight ?? 1,
      depth: queue.length,
      // The head is the oldest, since enqueue appends. The scheduler needs this to
      // enforce the starvation bound.
      oldestEnqueuedAt: queue[0]?.enqueuedAt ?? null,
    }));
  }
}
