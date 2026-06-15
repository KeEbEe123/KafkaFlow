import { useWebSocket } from './hooks/useWebSocket';
import { ClusterOverview } from './components/ClusterOverview';
import { ThroughputChart } from './components/ThroughputChart';
import { TopicPartitions } from './components/TopicPartitions';
import { ConsumerGroups } from './components/ConsumerGroups';

export default function App() {
  useWebSocket();

  return (
    <div className="min-h-screen bg-slate-900 text-white">
      {/* Header */}
      <header className="border-b border-slate-700/60 bg-slate-900/80 backdrop-blur-sm sticky top-0 z-10">
        <div className="max-w-7xl mx-auto px-4 py-3 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <div className="w-8 h-8 rounded-lg bg-gradient-to-br from-yellow-400 to-orange-500 flex items-center justify-center text-slate-900 font-black text-sm">
              KF
            </div>
            <span className="font-bold text-lg tracking-tight">KafkaFlow</span>
            <span className="text-xs text-slate-500 bg-slate-800 px-2 py-0.5 rounded-full">v0.1</span>
          </div>
          <div className="text-xs text-slate-500">Lightweight Message Broker</div>
        </div>
      </header>

      {/* Main */}
      <main className="max-w-7xl mx-auto px-4 py-6 space-y-6">
        <ClusterOverview />

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <ThroughputChart />
          <ConsumerGroups />
        </div>

        <TopicPartitions />
      </main>
    </div>
  );
}
