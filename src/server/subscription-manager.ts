import type Redis from "ioredis"
import { type UserEvent, userEvent } from "../shared/types.js"

type Listener = (message: UserEvent) => void

export class SubscriptionManager {
  private redis: Redis
  private subRedis: Redis
  // Map<ChannelName, Set<Listener>>
  private listeners: Map<string, Set<Listener>> = new Map()
  private unsubscribeTimers: Map<string, NodeJS.Timeout> = new Map()
  private verbose: boolean

  constructor(redis: Redis, verbose: boolean = false) {
    this.redis = redis
    // Create the single global subscription connection
    this.subRedis = redis.duplicate()
    this.verbose = verbose

    this.setupMessageListener()
  }

  private setupMessageListener() {
    this.subRedis.on("message", (channel, messageStr) => {
      const handlers = this.listeners.get(channel)
      if (!handlers || handlers.size === 0) return

      try {
        let payload: unknown
        // Optimization: Simple check to avoid try-catch on non-JSON strings
        if (messageStr.startsWith('{') || messageStr.startsWith('[')) {
            try {
                payload = JSON.parse(messageStr)
            } catch {
                payload = { data: messageStr }
            }
        } else {
            payload = { data: messageStr }
        }

        // We only care about UserEvents here for distribution
        // System events (like ping) are handled locally in the handler now
        const result = userEvent.safeParse(payload)
        
        if (result.success) {
          if (this.verbose) {
            console.log(`[SubscriptionManager] Dispatching message to ${handlers.size} listeners on ${channel}`)
          }
          handlers.forEach((listener) => {
              try {
                  listener(result.data)
              } catch (listenerErr) {
                  console.error(`[SubscriptionManager] Error in listener for ${channel}:`, listenerErr)
              }
          })
        }
      } catch (err) {
        console.error(`[SubscriptionManager] Error processing message on ${channel}:`, err)
      }
    })
    
    this.subRedis.on("error", (err) => {
        console.error("[SubscriptionManager] Redis subscription error:", err)
    })
  }

  public async subscribe(channel: string, listener: Listener): Promise<() => void> {
    if (this.unsubscribeTimers.has(channel)) {
      clearTimeout(this.unsubscribeTimers.get(channel)!)
      this.unsubscribeTimers.delete(channel)
      if (this.verbose) console.log(`[SubscriptionManager] Cancelled pending unsubscribe for: ${channel}`)
    }

    if (!this.listeners.has(channel)) {
      this.listeners.set(channel, new Set())
      // If this is the first listener for this channel, subscribe in Redis
      if (this.verbose) console.log(`[SubscriptionManager] Subscribing to Redis channel: ${channel}`)
      await this.subRedis.subscribe(channel)
    }

    const channelListeners = this.listeners.get(channel)!
    channelListeners.add(listener)

    // Return cleanup function
    return () => {
      const currentListeners = this.listeners.get(channel)
      if (currentListeners) {
        currentListeners.delete(listener)
        
        if (currentListeners.size === 0) {
          // Debounce unsubscribe
          if (this.unsubscribeTimers.has(channel)) {
            clearTimeout(this.unsubscribeTimers.get(channel)!)
          }

          const timer = setTimeout(() => {
            this.listeners.delete(channel)
            this.unsubscribeTimers.delete(channel)
            
            if (this.verbose) console.log(`[SubscriptionManager] Unsubscribing from Redis channel: ${channel}`)
            this.subRedis.unsubscribe(channel).catch((err) => {
               console.error(`[SubscriptionManager] Error unsubscribing from ${channel}:`, err)
            })
          }, 2000)

          this.unsubscribeTimers.set(channel, timer)
        }
      }
    }
  }

  public async disconnect() {
    await this.subRedis.quit().catch(() => this.subRedis.disconnect())
  }
}
