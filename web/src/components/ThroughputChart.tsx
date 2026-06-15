import {
  ResponsiveContainer,
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  CartesianGrid,
  Legend,
} from 'recharts';
import { useStore } from '../store';

export function ThroughputChart() {
  const history = useStore((s) => s.metricsHistory);

  const data = history.map((p) => ({
    t: new Date(p.time).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' }),
    msgSec: Math.round(p.msgPerSec),
    avgMs: parseFloat(p.avgLatencyMs.toFixed(2)),
    p99Ms: parseFloat(p.p99LatencyMs.toFixed(2)),
  }));

  return (
    <div className="rounded-xl bg-slate-800 border border-slate-700 p-4">
      <h3 className="text-sm font-semibold text-slate-300 mb-4 uppercase tracking-wider">
        Throughput &amp; Latency — 60s window
      </h3>

      <div className="mb-6">
        <p className="text-xs text-slate-500 mb-2">Messages / second</p>
        <ResponsiveContainer width="100%" height={140}>
          <LineChart data={data} margin={{ top: 4, right: 8, bottom: 0, left: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
            <XAxis dataKey="t" tick={{ fontSize: 10, fill: '#64748b' }} interval="preserveStartEnd" />
            <YAxis tick={{ fontSize: 10, fill: '#64748b' }} width={50} />
            <Tooltip
              contentStyle={{ background: '#1e293b', border: '1px solid #334155', borderRadius: 8, fontSize: 12 }}
              labelStyle={{ color: '#94a3b8' }}
            />
            <Line type="monotone" dataKey="msgSec" stroke="#facc15" strokeWidth={2} dot={false} name="msg/sec" />
          </LineChart>
        </ResponsiveContainer>
      </div>

      <div>
        <p className="text-xs text-slate-500 mb-2">Latency (ms)</p>
        <ResponsiveContainer width="100%" height={140}>
          <LineChart data={data} margin={{ top: 4, right: 8, bottom: 0, left: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
            <XAxis dataKey="t" tick={{ fontSize: 10, fill: '#64748b' }} interval="preserveStartEnd" />
            <YAxis tick={{ fontSize: 10, fill: '#64748b' }} width={50} />
            <Tooltip
              contentStyle={{ background: '#1e293b', border: '1px solid #334155', borderRadius: 8, fontSize: 12 }}
              labelStyle={{ color: '#94a3b8' }}
            />
            <Legend wrapperStyle={{ fontSize: 11, color: '#94a3b8' }} />
            <Line type="monotone" dataKey="avgMs" stroke="#60a5fa" strokeWidth={2} dot={false} name="avg ms" />
            <Line type="monotone" dataKey="p99Ms" stroke="#f87171" strokeWidth={2} dot={false} name="p99 ms" strokeDasharray="4 2" />
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
