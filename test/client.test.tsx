// @vitest-environment jsdom
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, waitFor } from '@testing-library/react';
import React from 'react';
import { RealtimeProvider, useRealtime } from '../src/client';
import { z } from 'zod';

// Mock EventSource
const originalEventSource = global.EventSource;

describe('Realtime Client', () => {
  let mockEventSource: any;
  let mockEventSourceInstances: any[] = [];
  
  // Explicitly define MessageEvent if not available in environment
  if (typeof MessageEvent === 'undefined') {
      global.MessageEvent = class MessageEvent extends Event {
          data: any;
          constructor(type: string, eventInitDict?: MessageEventInit) {
              super(type, eventInitDict);
              this.data = eventInitDict?.data;
          }
      } as any;
  }

  beforeEach(() => {
    mockEventSourceInstances = [];
    
    mockEventSource = vi.fn((url: string, init?: EventSourceInit) => {
        const listeners: Record<string, Function[]> = {};
        
        const instance = {
            url,
            init,
            readyState: 0, // CONNECTING
            close: vi.fn(() => {
                instance.readyState = 2; // CLOSED
            }),
            addEventListener: vi.fn((event: string, cb: Function) => {
                if (!listeners[event]) listeners[event] = [];
                listeners[event].push(cb);
            }),
            removeEventListener: vi.fn(),
            dispatchEvent: vi.fn(),
            // Simulate internal helpers for testing
            emitOpen: () => {
                instance.readyState = 1; // OPEN
                if (instance.onopen) (instance.onopen as EventListener)(new Event('open'));
            },
            emitMessage: (data: any) => {
                if (instance.onmessage) {
                    (instance.onmessage as EventListener)(new MessageEvent('message', { data: JSON.stringify(data) }));
                }
            },
            emitError: () => {
                if (instance.onerror) (instance.onerror as EventListener)(new Event('error'));
            },
            onopen: null,
            onmessage: null,
            onerror: null
        };
        mockEventSourceInstances.push(instance);
        return instance;
    });

    mockEventSource.CONNECTING = 0;
    mockEventSource.OPEN = 1;
    mockEventSource.CLOSED = 2;

    global.EventSource = mockEventSource as any;
  });

  afterEach(() => {
    global.EventSource = originalEventSource;
    vi.clearAllMocks();
  });

  const TestComponent = ({ onData }: { onData: (msg: any) => void }) => {
    // Define a simple schema for typing
    const schema = {
        chat: {
            message: z.object({ text: z.string() })
        }
    };
    
    useRealtime({
        channels: ['chat'],
        events: ['chat.message'],
        onData: onData
    });
    
    return <div>Test Component</div>;
  };

  it('should connect to the correct URL', async () => {
    render(
      <RealtimeProvider api={{ url: '/api/realtime' }}>
        <TestComponent onData={() => {}} />
      </RealtimeProvider>
    );

    // Wait for debounce
    await waitFor(() => {
        expect(mockEventSourceInstances.length).toBe(1);
    }, { timeout: 1000 });

    const instance = mockEventSourceInstances[0];
    expect(instance.url).toContain('/api/realtime');
    expect(instance.url).toContain('channel=chat');
  });

  it('should receive messages', async () => {
    const onData = vi.fn();

    render(
      <RealtimeProvider api={{ url: '/api/realtime' }}>
        <TestComponent onData={onData} />
      </RealtimeProvider>
    );

    // Wait for connection
    await waitFor(() => expect(mockEventSourceInstances.length).toBe(1));
    const instance = mockEventSourceInstances[0];
    
    // Simulate connection open
    instance.emitOpen();

    // Simulate incoming message
    const payload = {
        id: '123',
        event: 'chat.message',
        channel: 'chat',
        data: { text: 'Hello' }
    };
    
    // Wrap in act if necessary, but usually libraries handle it.
    // However, since state updates happen inside provider, we might need to wait.
    instance.emitMessage(payload);

    await waitFor(() => {
        expect(onData).toHaveBeenCalledTimes(1);
    });
    
    expect(onData).toHaveBeenCalledWith(expect.objectContaining({
        event: 'chat.message',
        data: { text: 'Hello' }
    }));
  });

  it('should handle reconnects', async () => {
    vi.useFakeTimers();
    
    render(
      <RealtimeProvider api={{ url: '/api/realtime' }} maxReconnectAttempts={2}>
        <TestComponent onData={() => {}} />
      </RealtimeProvider>
    );

    // Initial connection: wait for debounce (25ms)
    // Provider uses setTimeout(..., 25)
    await vi.advanceTimersByTimeAsync(100);

    // Using fake timers + waitFor can be tricky if waitFor uses real timers or process.nextTick
    // But @testing-library/react's waitFor should work with fake timers if configured.
    // However, sometimes it is safer to just check manually when controlling time strictly.
    expect(mockEventSourceInstances.length).toBe(1);
    
    const instance1 = mockEventSourceInstances[0];
    instance1.emitOpen();
    
    // Simulate error/disconnect
    instance1.readyState = 2; // CLOSED
    instance1.emitError();
    
    // Should wait before reconnecting. Default first attempt is around 1s.
    // Let's fast forward.
    await vi.advanceTimersByTimeAsync(3000);

    // Should have created a second instance
    expect(mockEventSourceInstances.length).toBe(2);
    
    vi.useRealTimers();
  });
  
  it('should filter events based on hook options', async () => {
    // This test does NOT use fake timers, so we rely on real time.
    const onData = vi.fn();
    
    const FilterTestComponent = () => {
        useRealtime({
            channels: ['chat'],
            events: ['chat.message'], 
            onData: onData
        });
        return null;
    };

    render(
      <RealtimeProvider api={{ url: '/api/realtime' }}>
        <FilterTestComponent />
      </RealtimeProvider>
    );

    // Default debounce is 25ms.
    // Wait for connection
    await waitFor(() => expect(mockEventSourceInstances.length).toBe(1), { timeout: 2000 });
    const instance = mockEventSourceInstances[0];
    instance.emitOpen();

    // Emit event we are listening to
    instance.emitMessage({
        id: '1',
        event: 'chat.message',
        channel: 'chat',
        data: { text: 'yes' }
    });
    
    // Emit event we are NOT listening to
    instance.emitMessage({
        id: '2',
        event: 'chat.typing',
        channel: 'chat',
        data: { userId: '1' }
    });

    // waitFor will poll until assertion passes.
    // But we also want to ensure the second call DOES NOT happen.
    // If we just wait for "Times(1)", it might pass immediately after the first event, 
    // and ignore if a second event comes later (false positive).
    // Or if the first event is slow, it waits.
    
    // To properly test "not called", we usually need to wait a bit to ensure silence.
    // But let's verify the first one first.
    await waitFor(() => {
        expect(onData).toHaveBeenCalledTimes(1);
    }, { timeout: 2000 });
    
    // Check specific calls
    expect(onData).toHaveBeenCalledWith(expect.objectContaining({
        event: 'chat.message'
    }));
  });
});
