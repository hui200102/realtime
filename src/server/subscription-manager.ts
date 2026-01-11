import type Redis from "ioredis";
import { type UserEvent, userEvent, Logger } from "../shared/types.js";

type Listener = (message: UserEvent, encodedMessage: Uint8Array) => void;

const encoder = new TextEncoder();

export class SubscriptionManager {
  private redis: Redis;
  private subRedis: Redis;
  // Map<ChannelName, Set<Listener>>
  private listeners: Map<string, Set<Listener>> = new Map();
  private unsubscribeTimers: Map<string, NodeJS.Timeout> = new Map();
  private logger: Logger;

  constructor(redis: Redis, logger: Logger) {
    this.redis = redis;
    // Create the single global subscription connection
    this.subRedis = redis.duplicate();
    this.logger = logger;

    this.setupMessageListener();
  }

  private setupMessageListener() {
    this.subRedis.on("message", (channel, messageStr) => {
      const handlers = this.listeners.get(channel);
      if (!handlers || handlers.size === 0) return;

      try {
        let payload: unknown;
        // Optimization: Simple check to avoid try-catch on non-JSON strings
        if (messageStr.startsWith("{") || messageStr.startsWith("[")) {
          try {
            payload = JSON.parse(messageStr);
          } catch {
            payload = { data: messageStr };
          }
        } else {
          payload = { data: messageStr };
        }

        // We only care about UserEvents here for distribution
        // System events (like ping) are handled locally in the handler now
        const result = userEvent.safeParse(payload);

        if (result.success) {
          const encodedMessage = encoder.encode(
            `data: ${JSON.stringify(result.data)}\n\n`
          );

          this.logger.debug?.(
            `[SubscriptionManager] Dispatching message to ${handlers.size} listeners on ${channel}`
          );
          handlers.forEach((listener) => {
            try {
              listener(result.data, encodedMessage);
            } catch (listenerErr) {
              this.logger.error(
                `[SubscriptionManager] Error in listener for ${channel}:`,
                listenerErr
              );
            }
          });
        }
      } catch (err) {
        this.logger.error(
          `[SubscriptionManager] Error processing message on ${channel}:`,
          err
        );
      }
    });

    this.subRedis.on("error", (err) => {
      this.logger.error("[SubscriptionManager] Redis subscription error:", err);
    });
  }

  public async subscribe(
    channel: string,
    listener: Listener
  ): Promise<() => void> {
    if (this.unsubscribeTimers.has(channel)) {
      clearTimeout(this.unsubscribeTimers.get(channel)!);
      this.unsubscribeTimers.delete(channel);
      this.logger.debug?.(
        `[SubscriptionManager] Cancelled pending unsubscribe for: ${channel}`
      );
    }

    if (!this.listeners.has(channel)) {
      this.listeners.set(channel, new Set());
      // If this is the first listener for this channel, subscribe in Redis
      this.logger.debug?.(
        `[SubscriptionManager] Subscribing to Redis channel: ${channel}`
      );
      await this.subRedis.subscribe(channel);
    }

    const channelListeners = this.listeners.get(channel)!;
    channelListeners.add(listener);

    // Return cleanup function
    return () => {
      const currentListeners = this.listeners.get(channel);
      if (currentListeners) {
        currentListeners.delete(listener);

        if (currentListeners.size === 0) {
          // Debounce unsubscribe
          if (this.unsubscribeTimers.has(channel)) {
            clearTimeout(this.unsubscribeTimers.get(channel)!);
          }

          const timer = setTimeout(() => {
            this.listeners.delete(channel);
            this.unsubscribeTimers.delete(channel);

            this.logger.debug?.(
              `[SubscriptionManager] Unsubscribing from Redis channel: ${channel}`
            );
            this.subRedis.unsubscribe(channel).catch((err) => {
              this.logger.error(
                `[SubscriptionManager] Error unsubscribing from ${channel}:`,
                err
              );
            });
          }, 2000);

          this.unsubscribeTimers.set(channel, timer);
        }
      }
    };
  }

  public async disconnect() {
    await this.subRedis.quit().catch(() => this.subRedis.disconnect());
  }
}
