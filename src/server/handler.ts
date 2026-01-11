import type { Opts, Realtime } from "./realtime.js";
import {
  userEvent,
  type SystemEvent,
  type UserEvent,
} from "../shared/types.js";
import { compareStreamIds, parseStreamResponse } from "./utils.js";

export function handle<T extends Opts>(config: {
  realtime: Realtime<T>;
  /**
   * Maximum number of missed messages to retrieve from history upon reconnection.
   * Defaults to 2000. Increase this if your application expects high message volume
   * and needs to ensure clients catch up on all history after long disconnections.
   * Warning: Setting this too high may cause memory issues on the server.
   */
  maxRecoveryLimit?: number;
  /**
   * Middleware to authorize the request (e.g. check session cookie, check channel permissions).
   * Return a Response to block the request, or nothing to allow.
   */
  middleware?: ({
    request,
    channels,
  }: {
    request: Request;
    channels: string[];
  }) => Response | void | Promise<Response | void>;
}): (request: Request) => Promise<Response | void> {
  return async (request: Request) => {
    const requestStartTime = Date.now();
    const { searchParams } = new URL(request.url);
    const rawChannels =
      searchParams.getAll("channel").length > 0
        ? searchParams.getAll("channel")
        : ["default"];
    const channels = [...new Set(rawChannels)];

    const redis = config.realtime._redis;
    const logger = config.realtime._logger;
    const subscriptionManager = config.realtime._subscriptionManager;
    const maxRecoveryLimit = config.maxRecoveryLimit ?? 2000;

    if (config.middleware) {
      const result = await config.middleware({ request, channels });
      if (result) return result;
    }

    if (!redis || !subscriptionManager) {
      logger.error("No Redis instance provided to Realtime");
      return new Response(JSON.stringify({ error: "Redis not configured" }), {
        status: 500,
        headers: { "Content-Type": "application/json" },
      });
    }

    let cleanup: (() => Promise<void>) | undefined;
    // No dedicated subscriber connection anymore
    const unsubs: (() => void)[] = [];

    let reconnectTimeout: NodeJS.Timeout | undefined;
    let keepaliveInterval: NodeJS.Timeout | undefined;
    let isClosed = false;
    let handleAbort: (() => Promise<void>) | undefined;

    const stream = new ReadableStream({
      async start(controller) {
        if (request.signal.aborted) {
          controller.close();
          return;
        }

        cleanup = async () => {
          if (isClosed) return;
          isClosed = true;

          clearTimeout(reconnectTimeout);
          clearInterval(keepaliveInterval);

          if (handleAbort) {
            request.signal.removeEventListener("abort", handleAbort);
          }

          // Unsubscribe from manager
          unsubs.forEach((unsub) => unsub());

          try {
            if (!request.signal.aborted) controller.close();
            logger.info("✅ Connection closed successfully.");
          } catch (err) {
            logger.error("⚠️ Error closing controller:", err);
          }
        };

        handleAbort = async () => {
          await cleanup?.();
        };

        request.signal.addEventListener("abort", handleAbort);

        const safeEnqueue = (data: Uint8Array) => {
          if (isClosed) return;

          // Backpressure check:
          // If the client is too slow (internal queue is full), we drop the message
          // to prevent server memory exhaustion (OOM).
          if (controller.desiredSize && controller.desiredSize <= 0) {
            logger.warn?.("⚠️ Client too slow, dropping message to prevent OOM.");
            return;
          }

          try {
            controller.enqueue(data);
          } catch (err) {
            logger.error("⚠️ Error closing controller:", err);
          }
        };

        const elapsedMs = Date.now() - requestStartTime;
        const remainingMs = config.realtime._maxDurationSecs * 1000 - elapsedMs;
        const streamDurationMs = Math.max(remainingMs - 2000, 1000);

        reconnectTimeout = setTimeout(async () => {
          const reconnectEvent: SystemEvent = {
            type: "reconnect",
            timestamp: Date.now(),
          };

          safeEnqueue(json(reconnectEvent));

          await cleanup?.();
        }, streamDurationMs);

        let buffer: UserEvent[] = [];
        let isHistoryReplayed = false;
        const lastHistoryIds = new Map<string, string>();

        const onManagerMessage = (
          message: UserEvent,
          encodedMessage: Uint8Array
        ) => {
          // We don't need to parse/filter here anymore, manager does it.
          // We just need to handle buffer/replay logic.
          logger.debug?.("⬇️  Received event:", message);

          if (!isHistoryReplayed) {
            buffer.push(message);
          } else {
            // Use pre-encoded message for broadcasting optimization!
            safeEnqueue(encodedMessage);
          }
        };

        // Create a function that runs the pipeline but returns the raw results/state
        // instead of processing them immediately, to allow parallel execution.
        const executeHistoryPipeline = async () => {
          const pipeline = redis.pipeline();
          const channelAcks = new Map<string, string>();

          for (const channel of channels) {
            const connectedEvent: SystemEvent = {
              type: "connected",
              channel,
            };
            safeEnqueue(json(connectedEvent));

            const lastAck =
              searchParams.get(`last_ack_${channel}`) ?? String(Date.now());
            channelAcks.set(channel, lastAck);

            pipeline.xrange(
              channel,
              `(${lastAck}`,
              "+",
              "COUNT",
              maxRecoveryLimit
            );
          }

          try {
            return await pipeline.exec();
          } catch (error) {
            logger.error("Error executing history pipeline:", error as string);
            return null;
          }
        };

        const processHistoryResults = (
          results: [error: Error | null, result: unknown][] | null
        ) => {
          if (!results) return;

          results.forEach((result, index) => {
            const [err, rawMissing] = result;
            const channel = channels[index];

            if (!channel) return;

            if (err) {
              logger.error(
                `Error fetching history for channel ${channel}:`,
                err
              );
              return;
            }

            const missingMessages = parseStreamResponse(rawMissing);

            if (missingMessages.length > 0) {
              missingMessages.forEach((value) => {
                const eventWithId = value;
                const event = userEvent.safeParse(eventWithId);
                if (event.success) safeEnqueue(json(event.data));
              });
              lastHistoryIds.set(
                channel,
                (missingMessages[missingMessages.length - 1]
                  ?.id as string) ?? ""
              );
            }
          });
        };

        const flushBuffer = () => {
          for (const msg of buffer) {
            const channelLastId = lastHistoryIds.get(msg.channel);
            if (channelLastId && compareStreamIds(msg.id, channelLastId) <= 0)
              continue;
            safeEnqueue(json(msg));
          }
          buffer = [];
          isHistoryReplayed = true;
          logger.info("✅ Subscription established:", { channels } as any);
        };

        try {
          // Optimization: Run Subscription and History Fetch in PARALLEL
          // This reduces the handshake latency by ~50% in 1-to-1 scenarios.
          const [_, historyResults] = await Promise.all([
            Promise.all(
              channels.map(async (channel) => {
                const unsub = await subscriptionManager.subscribe(
                  channel,
                  onManagerMessage
                );
                unsubs.push(unsub);
              })
            ),
            executeHistoryPipeline(),
          ]);

          // Process history first
          processHistoryResults(historyResults);
          
          // Then flush any real-time messages that arrived during the handshake
          flushBuffer();
          
        } catch (err: unknown) {
          const errorMessage =
            err instanceof Error ? err.message : "Unknown error";
          logger.error("⚠️ Redis subscriber error:", errorMessage);
          const errorEvent: SystemEvent = {
            type: "error",
            error: errorMessage,
          };
          safeEnqueue(json(errorEvent));
        }

        keepaliveInterval = setInterval(() => {
          const pingEvent: SystemEvent = {
            type: "ping",
            timestamp: Date.now(),
          };
          safeEnqueue(json(pingEvent));
        }, 60_000);
      },

      async cancel() {
        if (isClosed) return;
        await cleanup?.();
      },
    });

    return new StreamingResponse(stream);
  };
}

const encoder = new TextEncoder();

export function json(data: SystemEvent | UserEvent) {
  return encoder.encode(`data: ${JSON.stringify(data)}\n\n`);
}

export class StreamingResponse extends Response {
  constructor(res: ReadableStream<Uint8Array>, init?: ResponseInit) {
    super(res as unknown as BodyInit, {
      ...init,
      status: 200,
      headers: {
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache",
        Connection: "keep-alive",
        "Content-Encoding": "none",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "Cache-Control",
        ...init?.headers,
      },
    });
  }
}
