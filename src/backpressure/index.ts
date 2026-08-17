/**
 * Backpressure Management
 * Prevents queue overflow by throttling producers when consumers can't keep up.
 * Implements token bucket, sliding window, and adaptive rate limiting.
 */

export interface BackpressureConfig {
  strategy: 'token-bucket' | 'sliding-window' | 'adaptive';
  maxQueueDepth: number;
  highWatermark: number;
  lowWatermark: number;
  tokenRefillRate?: number;
  windowSizeMs?: number;
  adaptiveConfig?: AdaptiveConfig;
}

export interface AdaptiveConfig {
  targetLatencyMs: number;
  minRate: number;
  maxRate: number;
  adjustmentFactor: number;
  measurementWindowMs: number;
}

export interface BackpressureMetrics {
  currentDepth: number;
  currentRate: number;
  rejectedCount: number;
  throttledCount: number;
  avgLatencyMs: number;
  state: 'flowing' | 'throttled' | 'blocked';
}

export type BackpressureState = 'flowing' | 'throttled' | 'blocked';

export class BackpressureController {
  private currentDepth: number = 0;
  private state: BackpressureState = 'flowing';
  private tokens: number;
  private lastRefill: number = Date.now();
  private windowEvents: number[] = [];
  private rejectedCount: number = 0;
  private throttledCount: number = 0;
  private latencies: number[] = [];
  private currentRate: number;
  private listeners: Map<string, Set<(state: BackpressureState) => void>> = new Map();

  constructor(private readonly config: BackpressureConfig) {
    this.tokens = config.maxQueueDepth;
    this.currentRate = config.adaptiveConfig?.maxRate ?? config.tokenRefillRate ?? 1000;
    this.listeners.set('stateChange', new Set());
  }

  canAccept(): boolean {
    switch (this.config.strategy) {
      case 'token-bucket':
        return this.tokenBucketCheck();
      case 'sliding-window':
        return this.slidingWindowCheck();
      case 'adaptive':
        return this.adaptiveCheck();
      default:
        return this.currentDepth < this.config.maxQueueDepth;
    }
  }

  recordEnqueue(): void {
    this.currentDepth++;
    this.windowEvents.push(Date.now());
    this.updateState();
  }

  recordDequeue(latencyMs: number): void {
    this.currentDepth = Math.max(0, this.currentDepth - 1);
    this.latencies.push(latencyMs);

    // Keep only recent latencies
    if (this.latencies.length > 1000) {
      this.latencies = this.latencies.slice(-500);
    }

    this.updateState();

    if (this.config.strategy === 'adaptive') {
      this.adjustRate();
    }
  }

  recordRejection(): void {
    this.rejectedCount++;
  }

  getMetrics(): BackpressureMetrics {
    return {
      currentDepth: this.currentDepth,
      currentRate: this.currentRate,
      rejectedCount: this.rejectedCount,
      throttledCount: this.throttledCount,
      avgLatencyMs: this.getAvgLatency(),
      state: this.state,
    };
  }

  onStateChange(callback: (state: BackpressureState) => void): () => void {
    const listeners = this.listeners.get('stateChange')!;
    listeners.add(callback);
    return () => listeners.delete(callback);
  }

  private tokenBucketCheck(): boolean {
    this.refillTokens();

    if (this.tokens > 0) {
      this.tokens--;
      return true;
    }

    this.throttledCount++;
    return false;
  }

  private refillTokens(): void {
    const now = Date.now();
    const elapsed = now - this.lastRefill;
    const refillRate = this.config.tokenRefillRate ?? 100;
    const newTokens = Math.floor(elapsed * refillRate / 1000);

    if (newTokens > 0) {
      this.tokens = Math.min(
        this.config.maxQueueDepth,
        this.tokens + newTokens
      );
      this.lastRefill = now;
    }
  }

  private slidingWindowCheck(): boolean {
    const windowSize = this.config.windowSizeMs ?? 60000;
    const now = Date.now();
    const cutoff = now - windowSize;

    this.windowEvents = this.windowEvents.filter(t => t > cutoff);

    if (this.windowEvents.length >= this.config.maxQueueDepth) {
      this.throttledCount++;
      return false;
    }

    return true;
  }

  private adaptiveCheck(): boolean {
    if (this.currentDepth >= this.config.maxQueueDepth) {
      this.rejectedCount++;
      return false;
    }

    const avgLatency = this.getAvgLatency();
    const target = this.config.adaptiveConfig?.targetLatencyMs ?? 100;

    if (avgLatency > target * 2) {
      this.throttledCount++;
      return false;
    }

    return true;
  }

  private adjustRate(): void {
    if (!this.config.adaptiveConfig) return;

    const { targetLatencyMs, minRate, maxRate, adjustmentFactor } = this.config.adaptiveConfig;
    const avgLatency = this.getAvgLatency();

    if (avgLatency > targetLatencyMs) {
      // Decrease rate
      this.currentRate = Math.max(
        minRate,
        this.currentRate * (1 - adjustmentFactor)
      );
    } else if (avgLatency < targetLatencyMs * 0.5) {
      // Increase rate
      this.currentRate = Math.min(
        maxRate,
        this.currentRate * (1 + adjustmentFactor * 0.5)
      );
    }
  }

  private getAvgLatency(): number {
    if (this.latencies.length === 0) return 0;
    const sum = this.latencies.reduce((a, b) => a + b, 0);
    return sum / this.latencies.length;
  }

  private updateState(): void {
    const prevState = this.state;

    if (this.currentDepth >= this.config.maxQueueDepth) {
      this.state = 'blocked';
    } else if (this.currentDepth >= this.config.highWatermark) {
      this.state = 'throttled';
    } else if (this.currentDepth <= this.config.lowWatermark) {
      this.state = 'flowing';
    }

    if (prevState !== this.state) {
      const listeners = this.listeners.get('stateChange')!;
      for (const listener of listeners) {
        listener(this.state);
      }
    }
  }

  reset(): void {
    this.currentDepth = 0;
    this.state = 'flowing';
    this.tokens = this.config.maxQueueDepth;
    this.windowEvents = [];
    this.rejectedCount = 0;
    this.throttledCount = 0;
    this.latencies = [];
  }
}
