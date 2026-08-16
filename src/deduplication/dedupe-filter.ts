/**
 * Windowed deduplication.
 *
 * Exactly-once DELIVERY is impossible across an unreliable network: a consumer
 * that acknowledges cannot guarantee the acknowledgement arrives, and a producer
 * that receives no acknowledgement cannot distinguish "never delivered" from
 * "processed, ack lost". This follows from the Two Generals problem.
 *
 * What is achievable is exactly-once PROCESSING, built from at-least-once delivery
 * plus deduplication. This module is where that guarantee is earned, and it is
 * explicit: the caller supplies a dedupe key rather than the queue pretending the
 * problem does not exist.
 *
 * Structure: a bloom filter fronts an exact map.
 *
 *   - Bloom cannot produce a FALSE NEGATIVE. "Not present" is therefore
 *     authoritative and skips the map lookup entirely, which is the common case
 *     for a key that was never seen.
 *   - Bloom CAN produce a false positive. "Maybe present" is confirmed against the
 *     exact map, which is why the map still exists.
 */

export interface DedupeConfig {
  enabled: boolean;
  /** How long a key is remembered, in ms. */
  windowMs: number;
  /** Expected distinct keys per window. Sizes the bloom filter. */
  expectedKeysPerWindow?: number;
  /** Target bloom false-positive rate. Defaults to 0.01. */
  falsePositiveRate?: number;
}

export interface DedupeResult {
  duplicate: boolean;
  /** Id of the original message, when this is a duplicate. */
  originalId?: string;
  /** Age of the original entry in ms. */
  originalAgeMs?: number;
  /** True when the bloom filter said maybe and the map said no. Diagnostic only. */
  bloomFalsePositive: boolean;
}

interface Entry {
  messageId: string;
  seenAt: number;
}

const DEFAULT_EXPECTED_KEYS = 100_000;
const DEFAULT_FALSE_POSITIVE_RATE = 0.01;

/**
 * Bloom filter sized from expected cardinality and target error rate.
 *
 *   m = -(n * ln p) / (ln 2)^2      bits
 *   k = (m / n) * ln 2              hash functions
 *
 * Both are the standard optimal-size results for a bloom filter.
 */
class BloomFilter {
  private readonly bits: Uint8Array;
  private readonly bitCount: number;
  private readonly hashCount: number;
  private insertions = 0;

  constructor(expectedItems: number, falsePositiveRate: number) {
    const n = Math.max(1, expectedItems);
    const p = Math.min(0.5, Math.max(1e-6, falsePositiveRate));

    this.bitCount = Math.ceil(-(n * Math.log(p)) / Math.LN2 ** 2);
    this.hashCount = Math.max(1, Math.round((this.bitCount / n) * Math.LN2));
    this.bits = new Uint8Array(Math.ceil(this.bitCount / 8));
  }

  add(key: string): void {
    for (const index of this.indices(key)) {
      this.bits[index >>> 3]! |= 1 << (index & 7);
    }
    this.insertions++;
  }

  /** False means definitely absent. True means probably present. */
  mightContain(key: string): boolean {
    for (const index of this.indices(key)) {
      if ((this.bits[index >>> 3]! & (1 << (index & 7))) === 0) return false;
    }
    return true;
  }

  clear(): void {
    this.bits.fill(0);
    this.insertions = 0;
  }

  /**
   * Current estimated false-positive rate, given actual insertions.
   *
   *   (1 - e^(-k*n/m))^k
   *
   * Worth exposing: if actual cardinality exceeds the configured expectation, the
   * real error rate silently drifts above the target, and the only symptom is
   * legitimate messages being dropped as duplicates.
   */
  estimatedFalsePositiveRate(): number {
    if (this.insertions === 0) return 0;
    const exponent = -(this.hashCount * this.insertions) / this.bitCount;
    return (1 - Math.exp(exponent)) ** this.hashCount;
  }

  get sizeBytes(): number {
    return this.bits.length;
  }

  /**
   * Two independent 32-bit hashes combined via Kirsch-Mitzenmacher:
   *
   *   g_i(x) = h1(x) + i * h2(x)
   *
   * This yields k hashes from two, which is provably as good asymptotically as k
   * independent hashes and avoids computing k separate digests per lookup.
   */
  private indices(key: string): number[] {
    const h1 = this.fnv1a(key);
    const h2 = this.djb2(key) | 1; // odd, so it is coprime with a power-of-two span

    const result: number[] = [];
    for (let i = 0; i < this.hashCount; i++) {
      // >>> 0 keeps the value unsigned after the multiply overflows.
      result.push(((h1 + Math.imul(i, h2)) >>> 0) % this.bitCount);
    }
    return result;
  }

  private fnv1a(input: string): number {
    let hash = 0x811c9dc5;
    for (let i = 0; i < input.length; i++) {
      hash ^= input.charCodeAt(i);
      hash = Math.imul(hash, 0x01000193);
    }
    return hash >>> 0;
  }

  private djb2(input: string): number {
    let hash = 5381;
    for (let i = 0; i < input.length; i++) {
      hash = (Math.imul(hash, 33) + input.charCodeAt(i)) | 0;
    }
    return hash >>> 0;
  }
}

export class DedupeFilter {
  private readonly config: Required<DedupeConfig>;
  private readonly seen = new Map<string, Entry>();
  private bloom: BloomFilter;

  private stats = {
    checked: 0,
    duplicates: 0,
    bloomSkips: 0,
    bloomFalsePositives: 0,
    evicted: 0,
  };

  constructor(config: DedupeConfig) {
    if (config.windowMs <= 0) {
      throw new Error(`windowMs must be positive, received ${config.windowMs}`);
    }

    this.config = {
      enabled: config.enabled,
      windowMs: config.windowMs,
      expectedKeysPerWindow: config.expectedKeysPerWindow ?? DEFAULT_EXPECTED_KEYS,
      falsePositiveRate: config.falsePositiveRate ?? DEFAULT_FALSE_POSITIVE_RATE,
    };

    this.bloom = new BloomFilter(
      this.config.expectedKeysPerWindow,
      this.config.falsePositiveRate,
    );
  }

  /**
   * Check a key and record it if new.
   *
   * Check and record are deliberately one operation. Splitting them creates a race
   * where two concurrent producers both check, both see absent, and both enqueue,
   * which is exactly the duplicate this filter exists to prevent.
   */
  check(dedupeKey: string, messageId: string, now: number = Date.now()): DedupeResult {
    if (!this.config.enabled) {
      return { duplicate: false, bloomFalsePositive: false };
    }

    this.stats.checked++;
    this.evictExpired(now);

    // Bloom cannot produce a false negative, so a negative answer is final and
    // the map lookup is skipped. This is the hot path for unseen keys.
    if (!this.bloom.mightContain(dedupeKey)) {
      this.stats.bloomSkips++;
      this.record(dedupeKey, messageId, now);
      return { duplicate: false, bloomFalsePositive: false };
    }

    // Bloom said maybe. Confirm against the exact map.
    const entry = this.seen.get(dedupeKey);

    if (!entry) {
      this.stats.bloomFalsePositives++;
      this.record(dedupeKey, messageId, now);
      return { duplicate: false, bloomFalsePositive: true };
    }

    this.stats.duplicates++;
    return {
      duplicate: true,
      originalId: entry.messageId,
      originalAgeMs: now - entry.seenAt,
      bloomFalsePositive: false,
    };
  }

  /**
   * Forget a key.
   *
   * Needed when a message is dead-lettered: the operator will fix the cause and
   * replay, and the replay must not be rejected as a duplicate of the failure.
   *
   * Note the asymmetry: the exact map forgets, the bloom filter cannot. Standard
   * bloom filters do not support deletion, because clearing bits would corrupt
   * every other key sharing them. The consequence is a bloom "maybe" for a key the
   * map no longer holds, which costs one map lookup and is otherwise harmless.
   */
  forget(dedupeKey: string): boolean {
    return this.seen.delete(dedupeKey);
  }

  getStats(): {
    checked: number;
    duplicates: number;
    duplicateRate: number;
    bloomSkipRate: number;
    bloomFalsePositives: number;
    observedFalsePositiveRate: number;
    configuredFalsePositiveRate: number;
    trackedKeys: number;
    evicted: number;
    bloomSizeBytes: number;
    /** True when actual cardinality has pushed the real error rate above target. */
    bloomOversaturated: boolean;
  } {
    const estimated = this.bloom.estimatedFalsePositiveRate();
    const bloomChecks = this.stats.bloomSkips + this.stats.bloomFalsePositives;

    return {
      checked: this.stats.checked,
      duplicates: this.stats.duplicates,
      duplicateRate: this.stats.checked > 0 ? this.stats.duplicates / this.stats.checked : 0,
      bloomSkipRate: this.stats.checked > 0 ? this.stats.bloomSkips / this.stats.checked : 0,
      bloomFalsePositives: this.stats.bloomFalsePositives,
      observedFalsePositiveRate:
        bloomChecks > 0 ? this.stats.bloomFalsePositives / bloomChecks : 0,
      configuredFalsePositiveRate: this.config.falsePositiveRate,
      trackedKeys: this.seen.size,
      evicted: this.stats.evicted,
      bloomSizeBytes: this.bloom.sizeBytes,
      // Worth alerting on: an oversaturated filter drops legitimate messages as
      // duplicates, and nothing else in the system reports that.
      bloomOversaturated: estimated > this.config.falsePositiveRate * 2,
    };
  }

  private record(dedupeKey: string, messageId: string, now: number): void {
    this.seen.set(dedupeKey, { messageId, seenAt: now });
    this.bloom.add(dedupeKey);
  }

  /**
   * Evict expired keys and rebuild the bloom filter when the map has shrunk
   * substantially.
   *
   * The rebuild is necessary because bloom bits cannot be individually cleared, so
   * without it the filter monotonically saturates and its false-positive rate
   * climbs without bound. Rebuilding only when the map has shrunk by half amortises
   * the O(n) rehash instead of paying it on every eviction.
   */
  private evictExpired(now: number): void {
    const cutoff = now - this.config.windowMs;
    const sizeBefore = this.seen.size;

    for (const [key, entry] of this.seen) {
      if (entry.seenAt <= cutoff) {
        this.seen.delete(key);
        this.stats.evicted++;
      }
    }

    if (sizeBefore > 0 && this.seen.size < sizeBefore / 2) {
      this.bloom = new BloomFilter(
        this.config.expectedKeysPerWindow,
        this.config.falsePositiveRate,
      );
      for (const key of this.seen.keys()) {
        this.bloom.add(key);
      }
    }
  }
}
