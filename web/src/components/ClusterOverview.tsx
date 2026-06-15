import { Activity, Database, Users, Zap, Clock } from 'lucide-react';
import { useStore } from '../store';

function fmt(n: number, decimals = 1) {
  return n.toFixed(decimals);
}

function fmtBytes(b: number) {
  if (b > 1_000_000_000) return `${fmt(b / 1_000_000_000)} GB`;
  if (b > 1_000_000) return `${fmt(b / 1_000_000)} MB`;
  if (b > 1_000) return `${fmt(b / 1_000)} KB`;
  return `${b} B`;
}

export function ClusterOverview() {
  const { metrics, connected, lastEvent } = useStore();

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h2 className="text-xl font-semibold text-white">Cluster Overview</h2>
        <div className="flex items-center gap-2">
          <span className={`w-2.5 h-2.5 rounded-full ${connected ? 'bg-emerald-400 animate-pulse' : 'bg-red-500'}`} />
          <span className="text-sm text-slate-400">{connected ? 'Live' : 'Disconnected'}</span>
        </div>
      </div>

      <div className="grid grid-cols-2 gap-3 lg:grid-cols-4">
        <StatCard
          icon={<Zap className="w-5 h-5 text-yellow-400" />}
          label="Throughput"
          value={`${fmt(metrics.messages_per_sec)} msg/s`}
          sub={fmtBytes(metrics.bytes_in) + '/s'}
        />
        <StatCard
          icon={<Clock className="w-5 h-5 text-blue-400" />}
          label="Avg Latency"
          value={`${fmt(metrics.avg_latency_ms)} ms`}
          sub={`p99: ${fmt(metrics.p99_latency_ms)} ms`}
        />
        <StatCard
          icon={<Database className="w-5 h-5 text-purple-400" />}
          label="Topics / Partitions"
          value={`${metrics.topic_count} / ${metrics.partition_count}`}
          sub="active"
        />
        <StatCard
          icon={<Users className="w-5 h-5 text-emerald-400" />}
          label="Consumer Groups"
          value={`${metrics.consumer_group_count}`}
          sub="groups"
        />
      </div>

      <div className="rounded-lg bg-slate-800/50 border border-slate-700 p-3 flex items-center gap-2 text-sm">
        <Activity className="w-4 h-4 text-slate-400 shrink-0" />
        <span className="text-slate-400">Last event:</span>
        <span className="text-slate-200 font-mono">{lastEvent || '—'}</span>
        <span className="ml-auto text-slate-500">
          {metrics.messages_in.toLocaleString()} total msgs
        </span>
      </div>
    </div>
  );
}

function StatCard({ icon, label, value, sub }: { icon: React.ReactNode; label: string; value: string; sub: string }) {
  return (
    <div className="rounded-xl bg-slate-800 border border-slate-700 p-4">
      <div className="flex items-center gap-2 mb-2">
        {icon}
        <span className="text-xs text-slate-400 uppercase tracking-wider">{label}</span>
      </div>
      <div className="text-2xl font-bold text-white tabular-nums">{value}</div>
      <div className="text-xs text-slate-500 mt-0.5">{sub}</div>
    </div>
  );
}
