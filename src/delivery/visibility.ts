/**
 * Visibility timeout.
 *
 * When a consumer receives a message it is not deleted, it is moved to an
 * in-flight set with a deadline. Three things can then happen:
 *
 *   1. The consumer acks  -> the message is removed permanently.
 *   2. The consumer nacks -> the message returns to its lane immediately.
 *   3. The deadline passes -> the consumer is presumed dead and the message
 *                            returns to its lane automatically.
 *
 * Case 3 is what makes the queue crash-safe without a separate heartbeat
 * protocol. A consumer that segfaults mid-processing does not lose the message;
 * it simply stops extending its deadline.
 *
 * Why not a heartbeat protocol: a heartbeat requires the consumer to keep proving
 * liveness on a second channel, and a network partition then looks identical to a
 * crash. Visibility timeout inverts the default: the message is safe unless a
 * consumer actively holds it. Fewer moving parts, same guarantee.
 */

export type Lane = string;

export interface InFlightMessage {
  id: string;
  lane: Lane;
  consumerId: string;
  /** Epoch ms after which the claim expires. */
  deadline: number;
  /** Attempts consumed so far, 1-based. */
  attempt: number;
  /** Heartbeat extensions used against the cap. */
  extensions: number;
  claimedAt: number;
}

export interface VisibilityConfig {
  /** Initial claim duration in ms. */
  timeoutMs: number;
  /**
   * Maximum heartbeat extensions per claim.
   *
   * Uncapped extension lets a consumer stuck in an infinite loop hold a message
   * forever, which converts a recoverable stall into permanent loss. The cap
   * turns "stuck forever" into "eventually retried elsewhere".
   */
  maxExtensions: number;
}

export type ClaimOutcome =
  | { ok: true; message: InFlightMessage }
  | { ok: false; reason: 'ALREADY_CLAIMED'; heldBy: string; expiresInMs: number };

export type ExtendOutcome =
  | { ok: true; newDeadline: number; extensionsRemaining: number }
  | { ok: false; reason: 'NOT_CLAIMED' | 'WRONG_CONSUMER' | 'EXTENSION_CAP_REACHED' | 'ALREADY_EXPIRED' };

export type ReleaseReason = 'ack' | 'nack' | 'expired';

export interface ReleasedMessage {
  id: string;
  lane: Lane;
  reason: ReleaseReason;
  attempt: number;
  /** Time the consumer held the message, in ms. */
  heldForMs: number;
  /** True when the consumer never acked or nacked. */
  presumedDead: boolean;
}

export class VisibilityTracker {
  private readonly inFlight = new Map<string, InFlightMessage>();
  private readonly config: VisibilityConfig;

  constructor(config: VisibilityConfig) {
    if (config.timeoutMs <= 0) {
      throw new Error(`timeoutMs must be positive, received ${config.timeoutMs}`);
    }
    if (config.maxExtensions < 0) {
      throw new Error(
        `maxExtensions cannot be negative, received ${config.maxExtensions}. ` +
          'Use 0 to forbid extension entirely.',
      );
    }
    this.config = config;
  }

  /**
   * Claim a message for a consumer.
   *
   * An expired claim is silently reclaimable: the point of the deadline is that
   * the previous holder forfeits. Returning ALREADY_CLAIMED for an expired claim
   * would deadlock the message behind a dead consumer.
   */
  claim(
    id: string,
    lane: Lane,
    consumerId: string,
    attempt = 1,
    now: number = Date.now(),
  ): ClaimOutcome {
    const existing = this.inFlight.get(id);

    if (existing && existing.deadline > now) {
      return {
        ok: false,
        reason: 'ALREADY_CLAIMED',
        heldBy: existing.consumerId,
        expiresInMs: existing.deadline - now,
      };
    }

    const message: InFlightMessage = {
      id,
      lane,
      consumerId,
      deadline: now + this.config.timeoutMs,
      // A reclaim after expiry is a new attempt, not a continuation of the old
      // one. Preserving the previous count is what makes retry budgets bind.
      attempt: existing ? existing.attempt + 1 : attempt,
      extensions: 0,
      claimedAt: now,
    };

    this.inFlight.set(id, message);
    return { ok: true, message };
  }

  /**
   * Extend a claim. Called by a long-running handler via `ctx.heartbeat()`.
   *
   * Preferred over a large global timeout, because a large timeout also means a
   * crashed consumer's messages sit unprocessed for that entire duration.
   */
  extend(id: string, consumerId: string, now: number = Date.now()): ExtendOutcome {
    const message = this.inFlight.get(id);

    if (!message) return { ok: false, reason: 'NOT_CLAIMED' };

    // Ownership is checked before expiry so a consumer extending someone else's
    // claim gets the accurate reason rather than a misleading ALREADY_EXPIRED.
    if (message.consumerId !== consumerId) return { ok: false, reason: 'WRONG_CONSUMER' };

    if (message.deadline <= now) return { ok: false, reason: 'ALREADY_EXPIRED' };

    if (message.extensions >= this.config.maxExtensions) {
      return { ok: false, reason: 'EXTENSION_CAP_REACHED' };
    }

    message.extensions++;
    // Extend from now, not from the old deadline. Extending from the deadline
    // would let a consumer accumulate arbitrary future time by heartbeating
    // rapidly, defeating the cap.
    message.deadline = now + this.config.timeoutMs;

    return {
      ok: true,
      newDeadline: message.deadline,
      extensionsRemaining: this.config.maxExtensions - message.extensions,
    };
  }

  /** Ack: processing succeeded, remove permanently. */
  ack(id: string, consumerId: string, now: number = Date.now()): ReleasedMessage | null {
    const message = this.inFlight.get(id);
    if (!message || message.consumerId !== consumerId) return null;

    this.inFlight.delete(id);

    return {
      id,
      lane: message.lane,
      reason: 'ack',
      attempt: message.attempt,
      heldForMs: now - message.claimedAt,
      presumedDead: false,
    };
  }

  /** Nack: processing failed, return to the lane without waiting for expiry. */
  nack(id: string, consumerId: string, now: number = Date.now()): ReleasedMessage | null {
    const message = this.inFlight.get(id);
    if (!message || message.consumerId !== consumerId) return null;

    this.inFlight.delete(id);

    return {
      id,
      lane: message.lane,
      reason: 'nack',
      attempt: message.attempt,
      heldForMs: now - message.claimedAt,
      presumedDead: false,
    };
  }

  /**
   * Reap expired claims.
   *
   * Called by the scheduler on each poll rather than by a background timer: a
   * timer keeps the process alive and complicates shutdown, and reaping on demand
   * is sufficient because nothing observes an expired claim until the next poll.
   */
  reapExpired(now: number = Date.now()): ReleasedMessage[] {
    const reaped: ReleasedMessage[] = [];

    for (const [id, message] of this.inFlight) {
      if (message.deadline > now) continue;

      this.inFlight.delete(id);
      reaped.push({
        id,
        lane: message.lane,
        reason: 'expired',
        attempt: message.attempt,
        heldForMs: now - message.claimedAt,
        // The consumer neither acked nor nacked, so it is presumed dead. This
        // distinction matters: an expired claim on a non-idempotent handler means
        // the outcome is UNKNOWN, not failed.
        presumedDead: true,
      });
    }

    return reaped;
  }

  /**
   * Claims held by one consumer. Used to release everything on graceful shutdown,
   * which returns messages immediately instead of waiting out every deadline.
   */
  claimsFor(consumerId: string): InFlightMessage[] {
    return [...this.inFlight.values()].filter((m) => m.consumerId === consumerId);
  }

  stats(now: number = Date.now()): {
    inFlight: number;
    byLane: Record<Lane, number>;
    byConsumer: Record<string, number>;
    /** Claims within 10% of their deadline. A rising count means handlers are
     *  slower than the configured timeout. */
    nearingExpiry: number;
    atExtensionCap: number;
  } {
    const byLane: Record<Lane, number> = {};
    const byConsumer: Record<string, number> = {};
    let nearingExpiry = 0;
    let atExtensionCap = 0;

    const warningBand = this.config.timeoutMs * 0.1;

    for (const message of this.inFlight.values()) {
      byLane[message.lane] = (byLane[message.lane] ?? 0) + 1;
      byConsumer[message.consumerId] = (byConsumer[message.consumerId] ?? 0) + 1;

      if (message.deadline - now <= warningBand) nearingExpiry++;
      if (message.extensions >= this.config.maxExtensions) atExtensionCap++;
    }

    return {
      inFlight: this.inFlight.size,
      byLane,
      byConsumer,
      nearingExpiry,
      atExtensionCap,
    };
  }

  /** Total wall-clock a claim can survive: initial timeout plus every extension. */
  maxHoldMs(): number {
    return this.config.timeoutMs * (this.config.maxExtensions + 1);
  }
}
