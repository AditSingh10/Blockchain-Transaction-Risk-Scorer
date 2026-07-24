import React, { act } from 'react';
import { createRoot, Root } from 'react-dom/client';
import {
  useWebSocketContext,
  WebSocketProvider,
} from './WebSocketContext';

class MockWebSocket {
  static OPEN = 1;
  static instances: MockWebSocket[] = [];

  readyState = MockWebSocket.OPEN;
  onopen: (() => void) | null = null;
  onclose: (() => void) | null = null;
  onerror: (() => void) | null = null;
  onmessage: ((event: MessageEvent) => void) | null = null;
  sent: string[] = [];

  constructor(public url: string) {
    MockWebSocket.instances.push(this);
  }

  send(payload: string) {
    this.sent.push(payload);
  }

  close() {
    this.readyState = 3;
    this.onclose?.();
  }
}

const Probe = () => {
  const { transactions, pendingCount, graphNodes } = useWebSocketContext();
  return (
    <output data-testid="stream-state">
      {transactions.length}:{pendingCount}:{graphNodes.length}
    </output>
  );
};

const transactionMessage = (index: number) => JSON.stringify({
  type: 'transaction',
  event_id: `00000000-0000-0000-0000-${String(index).padStart(12, '0')}`,
  stream_id: `${1000 + index}-0`,
  data: {
    tx_id: `tx-${index}`,
    timestamp: 1000 + index,
    amount: index,
    illicit_probability: (index % 100) / 100,
    threshold: 0.9,
    flagged: false,
    inference_latency_ms: 4,
    neighbors: [],
    delivery_mode: 'replay',
  },
});

describe('WebSocket presentation pacing', () => {
  let container: HTMLDivElement;
  let root: Root;
  const testGlobal = globalThis as typeof globalThis & {
    IS_REACT_ACT_ENVIRONMENT: boolean;
  };

  beforeAll(() => {
    testGlobal.IS_REACT_ACT_ENVIRONMENT = true;
  });

  afterAll(() => {
    testGlobal.IS_REACT_ACT_ENVIRONMENT = false;
  });

  beforeEach(() => {
    jest.useFakeTimers();
    MockWebSocket.instances = [];
    window.localStorage.setItem('risk-monitor-last-event-id', '9999-0');
    Object.defineProperty(window, 'WebSocket', {
      configurable: true,
      writable: true,
      value: MockWebSocket,
    });
    Object.defineProperty(global, 'WebSocket', {
      configurable: true,
      writable: true,
      value: MockWebSocket,
    });
    container = document.createElement('div');
    document.body.appendChild(container);
    act(() => {
      root = createRoot(container);
      root.render(
        <WebSocketProvider>
          <Probe />
        </WebSocketProvider>
      );
    });
  });

  afterEach(() => {
    act(() => root.unmount());
    container.remove();
    window.localStorage.clear();
    jest.useRealTimers();
  });

  it('rebuilds from recent history and reveals transactions at 20 tx/s', () => {
    const socket = MockWebSocket.instances[0];
    expect(socket.url).not.toContain('last_event_id');

    act(() => {
      socket.onopen?.();
      socket.onmessage?.(new MessageEvent('message', { data: transactionMessage(1) }));
      socket.onmessage?.(new MessageEvent('message', { data: transactionMessage(2) }));
      socket.onmessage?.(new MessageEvent('message', { data: transactionMessage(3) }));
    });
    expect(container.textContent).toBe('0:3:0');

    act(() => jest.advanceTimersByTime(50));
    expect(container.textContent).toBe('1:2:1');

    act(() => jest.advanceTimersByTime(100));
    expect(container.textContent).toBe('3:0:3');
  });

  it('retains the complete 9,600-transaction local replay in the graph', () => {
    const socket = MockWebSocket.instances[0];
    act(() => {
      socket.onopen?.();
      for (let index = 1; index <= 9_600; index += 1) {
        socket.onmessage?.(
          new MessageEvent('message', { data: transactionMessage(index) })
        );
      }
    });

    act(() => jest.advanceTimersByTime(480_000));
    expect(container.textContent).toBe('9600:0:9600');
  });
});
