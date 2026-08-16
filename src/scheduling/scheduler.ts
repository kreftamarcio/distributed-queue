/**
 * Weighted lane scheduler with starvation prevention.
 *
 * Strict priority ordering starves lower lanes indefinitely: while any critical
 * message exists, nothing else ever runs. In practice "low priority" means
 * "later", not "never", so the scheduler combines weighted selection with an
 * age-based override that bounds the maximum wait for any message.
 */

export interface LaneConfig {
  weight: number;
}

export interface SchedulingConfig {
  /**
   * Maximum time any message may wait before its lane is force-promoted,
   * regardless of weight. This is the scheduler's explicit contract.
   */
  maxStarvationMs: number;
}

export interface LaneState {
  name: string;
  weight: number;
  depth: number;
  /** Enqueue timestamp of the oldest message, or null when the lane is empty. */
  oldestEnqueuedAt: number | null;
}

export interface SelectionResult {
  lane: string;
  reason: 'weighted' | 'starvation_override' | 'only_nonempty';
  /** Age of the oldest message in the chosen lane, in ms. */
  waitedMs: number;
}

export interface SchedulerStats {
  selectionsByLane: Record<string, number>;
  starvationOverrides: number;
  totalSelections: number;
  /** Observed share of selections per lane, useful for verifying weights. */
  observedShare: Record<string, number>;
}

export class Scheduler {
  private readonly lanes: Map<string, LaneConfig>;
  private readonly config: SchedulingConfig;
  private readonly totalWeight: number;

  /**
   * Deficit counters implement smooth weighted round-robin. Without them,
   * random weighted selection produces bursty ordering: a weight-8 lane can
   * legitimately be skipped several times in a row by chance.
   */
  private readonly deficits = new Map<string, number>();

  private stats: SchedulerStats = {
    selectionsByLane: {},
    starvationOverrides: 0,
    totalSelections: 0,
    observedShare: {},
  };

  constructor(lanes: Record<string, LaneConfig>, config: SchedulingConfig) {
    this.validate(lanes, config);

    this.lanes = new Map(Object.entries(lanes));
    this.config = config;
    this.totalWeight = Array.from(this.lanes.values()).reduce((s, l) => s + l.weight, 0);

    for (const name of this.lanes.keys()) {
      this.deficits.set(name, 0);
      this.stats.selectionsByLane[name] = 0;
    }
  }

  /**
   * Choose the next lane to pull from.
   *
   * Order of evaluation matters:
   *   1. Starvation override, because it is a hard bound and must win.
   *   2. Deficit-weighted round robin among remaining non-empty lanes.
   *
   * Returns null when every lane is empty.
   */
  select(laneStates: LaneState[], now: number = Date.now()): SelectionResult | null {
    const nonEmpty = laneStates.filter(l => l.depth > 0);

    if (nonEmpty.length === 0) {
      return null;
    }

    if (nonEmpty.length === 1) {
      const only = nonEmpty[0]!;
      this.record(only.name, false);
      return {
        lane: only.name,
        reason: 'only_nonempty',
        waitedMs: this.ageOf(only, now),
      };
    }

    // Starvation override: the oldest violator wins, not merely any violator.
    // Picking the oldest keeps the bound tight when several lanes are over.
    const starving = nonEmpty
      .filter(l => this.ageOf(l, now) > this.config.maxStarvationMs)
      .sort((a, b) => this.ageOf(b, now) - this.ageOf(a, now));

    if (starving.length > 0) {
      const chosen = starving[0]!;
      this.record(chosen.name, true);
      return {
        lane: chosen.name,
        reason: 'starvation_override',
        waitedMs: this.ageOf(chosen, now),
      };
    }

    const chosen = this.selectByDeficit(nonEmpty);
    this.record(chosen.name, false);

    return {
      lane: chosen.name,
      reason: 'weighted',
      waitedMs: this.ageOf(chosen, now),
    };
  }

  /**
   * Deficit round robin.
   *
   * Each non-empty lane accumulates credit proportional to its weight. The lane
   * with the highest accumulated credit is served, and pays the total weight as
   * cost. Over time each lane receives a share of selections equal to
   * weight / totalWeight, but the interleaving is smooth rather than bursty.
   *
   * This is the same idea as deficit round robin in packet scheduling, applied
   * to message lanes instead of network flows.
   */
  private selectByDeficit(nonEmpty: LaneState[]): LaneState {
    for (const lane of nonEmpty) {
      const current = this.deficits.get(lane.name) ?? 0;
      this.deficits.set(lane.name, current + lane.weight);
    }

    let best = nonEmpty[0]!;
    let bestDeficit = this.deficits.get(best.name) ?? 0;

    for (let i = 1; i < nonEmpty.length; i++) {
      const candidate = nonEmpty[i]!;
      const deficit = this.deficits.get(candidate.name) ?? 0;

      // Tie-break on higher static weight so the ordering is deterministic
      // rather than dependent on array order.
      if (
        deficit > bestDeficit ||
        (deficit === bestDeficit && candidate.weight > best.weight)
      ) {
        best = candidate;
        bestDeficit = deficit;
      }
    }

    this.deficits.set(best.name, bestDeficit - this.totalWeight);
    return best;
  }

  /**
   * Which lanes are currently violating the starvation bound?
   * Exposed for monitoring: sustained violations mean the consumer pool is
   * undersized, and no scheduling policy can fix insufficient throughput.
   */
  starvationViolations(
    laneStates: LaneState[],
    now: number = Date.now(),
  ): Array<{ lane: string; waitedMs: number; overBy: number }> {
    return laneStates
      .filter(l => l.depth > 0)
      .map(l => ({ lane: l.name, waitedMs: this.ageOf(l, now) }))
      .filter(v => v.waitedMs > this.config.maxStarvationMs)
      .map(v => ({ ...v, overBy: v.waitedMs - this.config.maxStarvationMs }))
      .sort((a, b) => b.overBy - a.overBy);
  }

  getStats(): SchedulerStats {
    const total = this.stats.totalSelections;
    const observedShare: Record<string, number> = {};

    for (const [lane, count] of Object.entries(this.stats.selectionsByLane)) {
      observedShare[lane] = total > 0 ? count / total : 0;
    }

    return { ...this.stats, observedShare };
  }

  /**
   * Expected share of selections per lane under pure weighted scheduling.
   * Comparing this against observedShare reveals how often starvation
   * overrides are distorting the intended distribution.
   */
  expectedShare(): Record<string, number> {
    const shares: Record<string, number> = {};
    for (const [name, lane] of this.lanes) {
      shares[name] = lane.weight / this.totalWeight;
    }
    return shares;
  }

  resetStats(): void {
    this.stats = {
      selectionsByLane: Object.fromEntries([...this.lanes.keys()].map(k => [k, 0])),
      starvationOverrides: 0,
      totalSelections: 0,
      observedShare: {},
    };
  }

  private ageOf(lane: LaneState, now: number): number {
    if (lane.oldestEnqueuedAt === null) return 0;
    return Math.max(0, now - lane.oldestEnqueuedAt);
  }

  private record(lane: string, wasOverride: boolean): void {
    this.stats.selectionsByLane[lane] = (this.stats.selectionsByLane[lane] ?? 0) + 1;
    this.stats.totalSelections++;
    if (wasOverride) this.stats.starvationOverrides++;
  }

  private validate(lanes: Record<string, LaneConfig>, config: SchedulingConfig): void {
    const entries = Object.entries(lanes);

    if (entries.length === 0) {
      throw new Error('At least one lane must be configured');
    }

    for (const [name, lane] of entries) {
      if (!Number.isFinite(lane.weight) || lane.weight <= 0) {
        throw new Error(
          `Lane "${name}" has weight ${lane.weight}. Weights must be positive finite ` +
          `numbers. A zero or negative weight would mean the lane never runs, which ` +
          `is better expressed by not creating the lane.`,
        );
      }
    }

    if (!Number.isFinite(config.maxStarvationMs) || config.maxStarvationMs <= 0) {
      throw new Error(
        `maxStarvationMs must be a positive finite number, got ${config.maxStarvationMs}. ` +
        `Without a bound, low-priority lanes can be starved indefinitely.`,
      );
    }
  }
}
