import { describe, it, expect, beforeEach } from 'vitest';
import { BackpressureController } from '../src/backpressure';

describe('BackpressureController', () => {
  describe('token-bucket strategy', () => {
    let controller: BackpressureController;

    beforeEach(() => {
      controller = new BackpressureController({
        strategy: 'token-bucket',
        maxQueueDepth: 100,
        highWatermark: 80,
        lowWatermark: 20,
        tokenRefillRate: 10,
      });
    });

    it('should accept messages when tokens available', () => {
      expect(controller.canAccept()).toBe(true);
    });

    it('should transition to throttled state at high watermark', () => {
      for (let i = 0; i < 80; i++) {
        controller.recordEnqueue();
      }
      const metrics = controller.getMetrics();
      expect(metrics.state).toBe('throttled');
    });

    it('should transition to blocked state at max depth', () => {
      for (let i = 0; i < 100; i++) {
        controller.recordEnqueue();
      }
      const metrics = controller.getMetrics();
      expect(metrics.state).toBe('blocked');
    });

    it('should return to flowing after dequeue below low watermark', () => {
      for (let i = 0; i < 50; i++) {
        controller.recordEnqueue();
      }
      for (let i = 0; i < 40; i++) {
        controller.recordDequeue(10);
      }
      const metrics = controller.getMetrics();
      expect(metrics.state).toBe('flowing');
    });
  });

  describe('adaptive strategy', () => {
    let controller: BackpressureController;

    beforeEach(() => {
      controller = new BackpressureController({
        strategy: 'adaptive',
        maxQueueDepth: 100,
        highWatermark: 80,
        lowWatermark: 20,
        adaptiveConfig: {
          targetLatencyMs: 50,
          minRate: 10,
          maxRate: 1000,
          adjustmentFactor: 0.1,
          measurementWindowMs: 5000,
        },
      });
    });

    it('should reduce rate when latency exceeds target', () => {
      const metricsBefore = controller.getMetrics();
      for (let i = 0; i < 100; i++) {
        controller.recordDequeue(200); // High latency
      }
      const metricsAfter = controller.getMetrics();
      expect(metricsAfter.currentRate).toBeLessThan(metricsBefore.currentRate);
    });

    it('should reject when at max depth', () => {
      for (let i = 0; i < 100; i++) {
        controller.recordEnqueue();
      }
      expect(controller.canAccept()).toBe(false);
    });
  });

  describe('metrics and reset', () => {
    it('should reset all counters', () => {
      const controller = new BackpressureController({
        strategy: 'token-bucket',
        maxQueueDepth: 10,
        highWatermark: 8,
        lowWatermark: 2,
      });

      for (let i = 0; i < 10; i++) {
        controller.recordEnqueue();
      }

      controller.reset();
      const metrics = controller.getMetrics();
      expect(metrics.currentDepth).toBe(0);
      expect(metrics.state).toBe('flowing');
    });
  });
});
