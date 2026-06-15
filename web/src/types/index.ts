export interface WSEvent {
  type: string;
  ts: number;
  payload: unknown;
}

export interface MetricsPayload {
  messages_per_sec: number;
  avg_latency_ms: number;
  p99_latency_ms: number;
  messages_in: number;
  bytes_in: number;
  topic_count: number;
  partition_count: number;
  consumer_group_count: number;
}

export interface PartitionOffsetPayload {
  topic: string;
  partition: number;
  offset: number;
}

export interface GroupRebalancePayload {
  group_id: string;
  generation: number;
  assignment: Record<string, number[]>;
}

export interface MetricsPoint {
  time: number;
  msgPerSec: number;
  avgLatencyMs: number;
  p99LatencyMs: number;
  bytesIn: number;
}

export interface TopicPartition {
  topic: string;
  partition: number;
  hwm: number;
  leader: number;
  msgPerSec?: number;
}

export interface ConsumerGroup {
  id: string;
  state: string;
  generation: number;
  members: string[];
  lastRebalance?: number;
}

export interface BrokerState {
  metrics: MetricsPayload;
  metricsHistory: MetricsPoint[];
  partitions: Record<string, TopicPartition>;
  groups: Record<string, ConsumerGroup>;
  connected: boolean;
  lastEvent: string;
}
