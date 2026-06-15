import { useStore } from '../store';

const STATE_COLORS: Record<string, string> = {
  Stable: 'text-emerald-400 bg-emerald-400/10',
  PreRebalance: 'text-yellow-400 bg-yellow-400/10',
  CompletingRebalance: 'text-orange-400 bg-orange-400/10',
  Empty: 'text-slate-500 bg-slate-500/10',
};

function timeAgo(ts?: number) {
  if (!ts) return '—';
  const sec = Math.floor((Date.now() - ts) / 1000);
  if (sec < 60) return `${sec}s ago`;
  return `${Math.floor(sec / 60)}m ago`;
}

export function ConsumerGroups() {
  const groups = useStore((s) => s.groups);
  const entries = Object.values(groups);

  return (
    <div className="rounded-xl bg-slate-800 border border-slate-700 p-4">
      <h3 className="text-sm font-semibold text-slate-300 mb-4 uppercase tracking-wider">
        Consumer Groups
      </h3>
      {entries.length === 0 ? (
        <p className="text-slate-500 text-sm">No consumer groups connected yet.</p>
      ) : (
        <div className="space-y-3">
          {entries.map((g) => (
            <div key={g.id} className="rounded-lg bg-slate-700/40 border border-slate-700 p-3">
              <div className="flex items-center justify-between mb-2">
                <span className="font-mono text-white text-sm">{g.id}</span>
                <span className={`text-xs px-2 py-0.5 rounded-full font-medium ${STATE_COLORS[g.state] ?? 'text-slate-400'}`}>
                  {g.state}
                </span>
              </div>
              <div className="grid grid-cols-3 gap-2 text-xs text-slate-400">
                <div>
                  <span className="block text-slate-500">Generation</span>
                  <span className="text-slate-200 font-mono">{g.generation}</span>
                </div>
                <div>
                  <span className="block text-slate-500">Members</span>
                  <span className="text-slate-200">{g.members.length}</span>
                </div>
                <div>
                  <span className="block text-slate-500">Last Rebalance</span>
                  <span className="text-slate-200">{timeAgo(g.lastRebalance)}</span>
                </div>
              </div>
              {g.members.length > 0 && (
                <div className="mt-2 flex flex-wrap gap-1">
                  {g.members.map((m) => (
                    <span key={m} className="text-xs font-mono bg-slate-600 text-slate-300 px-1.5 py-0.5 rounded">
                      {m.slice(0, 12)}…
                    </span>
                  ))}
                </div>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
