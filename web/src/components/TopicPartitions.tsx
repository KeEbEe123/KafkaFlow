import { useStore } from '../store';

export function TopicPartitions() {
  const partitions = useStore((s) => s.partitions);
  const entries = Object.values(partitions);

  return (
    <div className="rounded-xl bg-slate-800 border border-slate-700 p-4">
      <h3 className="text-sm font-semibold text-slate-300 mb-4 uppercase tracking-wider">
        Topic Partitions
      </h3>
      {entries.length === 0 ? (
        <p className="text-slate-500 text-sm">No partition data yet — publish some messages.</p>
      ) : (
        <div className="overflow-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-slate-700 text-left text-xs text-slate-500 uppercase tracking-wider">
                <th className="pb-2 pr-4">Topic</th>
                <th className="pb-2 pr-4">Partition</th>
                <th className="pb-2 pr-4">Leader</th>
                <th className="pb-2 pr-4 text-right">High Watermark</th>
              </tr>
            </thead>
            <tbody>
              {entries.map((p) => (
                <tr
                  key={`${p.topic}/${p.partition}`}
                  className="border-b border-slate-700/50 hover:bg-slate-700/30 transition-colors"
                >
                  <td className="py-2 pr-4 font-mono text-emerald-400">{p.topic}</td>
                  <td className="py-2 pr-4 text-slate-300">{p.partition}</td>
                  <td className="py-2 pr-4">
                    <span className="inline-flex items-center gap-1.5">
                      <span className="w-2 h-2 rounded-full bg-blue-400" />
                      <span className="text-slate-300">broker-{p.leader}</span>
                    </span>
                  </td>
                  <td className="py-2 pr-4 text-right font-mono tabular-nums text-yellow-400">
                    {p.hwm.toLocaleString()}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
