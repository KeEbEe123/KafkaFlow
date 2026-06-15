import { useEffect, useRef } from 'react';
import { useStore } from '../store';
import type { WSEvent, MetricsPayload, PartitionOffsetPayload, GroupRebalancePayload } from '../types';

const WS_URL = 'ws://localhost:9095/ws';
const MAX_RETRIES = 10;
const BASE_DELAY = 500;

export function useWebSocket() {
  const wsRef = useRef<WebSocket | null>(null);
  const retriesRef = useRef(0);
  const { setConnected, applyMetrics, applyPartitionOffset, applyGroupRebalance, setLastEvent } = useStore();

  useEffect(() => {
    let timeoutId: ReturnType<typeof setTimeout>;

    function connect() {
      const ws = new WebSocket(WS_URL);
      wsRef.current = ws;

      ws.onopen = () => {
        retriesRef.current = 0;
        setConnected(true);
      };

      ws.onmessage = (e) => {
        try {
          const event: WSEvent = JSON.parse(e.data);
          setLastEvent(event.type);
          switch (event.type) {
            case 'metrics.update':
              applyMetrics(event.payload as MetricsPayload, event.ts);
              break;
            case 'partition.offset':
              applyPartitionOffset(event.payload as PartitionOffsetPayload);
              break;
            case 'group.rebalance':
              applyGroupRebalance(event.payload as GroupRebalancePayload);
              break;
          }
        } catch {
          // ignore malformed events
        }
      };

      ws.onclose = () => {
        setConnected(false);
        wsRef.current = null;
        if (retriesRef.current < MAX_RETRIES) {
          const delay = BASE_DELAY * Math.pow(2, retriesRef.current);
          retriesRef.current++;
          timeoutId = setTimeout(connect, delay);
        }
      };

      ws.onerror = () => {
        ws.close();
      };
    }

    connect();
    return () => {
      clearTimeout(timeoutId);
      wsRef.current?.close();
    };
  }, []);
}
