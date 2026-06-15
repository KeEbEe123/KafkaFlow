import { create } from 'zustand';
import type {
  BrokerState, MetricsPayload, MetricsPoint,
  PartitionOffsetPayload, GroupRebalancePayload,
} from './types';

const MAX_HISTORY = 60;

const defaultMetrics: MetricsPayload = {
  messages_per_sec: 0,
  avg_latency_ms: 0,
  p99_latency_ms: 0,
  messages_in: 0,
  bytes_in: 0,
  topic_count: 0,
  partition_count: 0,
  consumer_group_count: 0,
};

interface Actions {
  setConnected: (v: boolean) => void;
  setLastEvent: (v: string) => void;
  applyMetrics: (m: MetricsPayload, ts: number) => void;
  applyPartitionOffset: (p: PartitionOffsetPayload) => void;
  applyGroupRebalance: (g: GroupRebalancePayload) => void;
}

export const useStore = create<BrokerState & Actions>((set) => ({
  metrics: defaultMetrics,
  metricsHistory: [],
  partitions: {},
  groups: {},
  connected: false,
  lastEvent: '',

  setConnected: (v) => set({ connected: v }),
  setLastEvent: (v) => set({ lastEvent: v }),

  applyMetrics: (m, ts) =>
    set((s) => {
      const point: MetricsPoint = {
        time: ts,
        msgPerSec: m.messages_per_sec,
        avgLatencyMs: m.avg_latency_ms,
        p99LatencyMs: m.p99_latency_ms,
        bytesIn: m.bytes_in,
      };
      const history = [...s.metricsHistory, point].slice(-MAX_HISTORY);
      return { metrics: m, metricsHistory: history };
    }),

  applyPartitionOffset: (p) =>
    set((s) => {
      const key = `${p.topic}/${p.partition}`;
      return {
        partitions: {
          ...s.partitions,
          [key]: {
            topic: p.topic,
            partition: p.partition,
            hwm: p.offset,
            leader: 1,
          },
        },
      };
    }),

  applyGroupRebalance: (g) =>
    set((s) => ({
      groups: {
        ...s.groups,
        [g.group_id]: {
          id: g.group_id,
          state: 'Stable',
          generation: g.generation,
          members: Object.keys(g.assignment),
          lastRebalance: Date.now(),
        },
      },
    })),
}));
