import Redis from "ioredis"
import * as z from "zod/v4/core"
import {
  EventPaths,
  EventPayloadUnion,
  HistoryArgs,
  userEvent,
  Logger,
  type UserEvent
} from "../shared/types.js"
import { compareStreamIds, parseStreamResponse } from "./utils.js"
import { SubscriptionManager } from "./subscription-manager.js"

const DEFAULT_VERCEL_FLUID_TIMEOUT = 300

type Schema = Record<string, z.$ZodType | Record<string, unknown>>

export type HistoryConfig = {
  maxLength?: number
  expireAfterSecs?: number
}

export type Opts = {
  schema?: Schema
  redis?: Redis | undefined
  maxDurationSecs?: number
  verbose?: boolean
  logger?: Logger
  history?: HistoryConfig | boolean
}

class RealtimeBase<T extends Opts> {
  private channels: Record<string, RealtimeChannel<T>> = {}
  private _schema: Schema
  private _verbose: boolean
  private _history: HistoryConfig

  /** @internal */
  public readonly _redis?: Redis | undefined
  
  /** @internal */
  public readonly _subscriptionManager?: SubscriptionManager

  /** @internal */
  public readonly _maxDurationSecs: number

  /** @internal */
  public readonly _logger: Logger

  constructor(data: T) {
    Object.assign(this, data)
    this._schema = data.schema || {}
    this._redis = data.redis
    this._maxDurationSecs = data.maxDurationSecs ?? DEFAULT_VERCEL_FLUID_TIMEOUT
    this._verbose = data.verbose ?? false
    this._history = typeof data.history === "boolean" ? {} : data.history ?? {}
    
    // Default logger
    this._logger = data.logger ?? {
        info: (...args: unknown[]) => {
            if (this._verbose) console.log(...args)
        },
        warn: (...args: unknown[]) => {
             console.warn(...args)
        },
        error: (...args: unknown[]) => {
             console.error(...args)
        },
        debug: (...args: unknown[]) => {
            if (this._verbose) console.debug(...args)
        }
    }

    if (this._redis) {
        this._subscriptionManager = new SubscriptionManager(this._redis, this._verbose)
    }

    // Pre-create default channel handler
    // But we don't assign it to this (RealtimeBase doesn't have subscribe/emit directly on itself strictly speaking, 
    // but the intersection type Realtime<T> implies it does via delegation or mixin).
    // The constructor assigns createEventHandlers return value to 'this'.
    // This is a bit hacky for type safety.
    Object.assign(this, this.createEventHandlers("default"))
  }

  private createEventHandlers(channel: string, historyOverride?: HistoryConfig | boolean): RealtimeChannel<T> {
    const historyConfig = {
      ...this._history,
      ...(typeof historyOverride === "boolean" ? {} : historyOverride ?? {}),
    }
    let unsubscribe: undefined | (() => void) = undefined
    let pingInterval: undefined | NodeJS.Timeout = undefined

    const startPingInterval = () => {
      pingInterval = setInterval(() => {
        this._redis?.publish(channel, JSON.stringify({ type: "ping", timestamp: Date.now() }))
      }, 60_000)
    }

    const stopPingInterval = () => {
      if (pingInterval) clearInterval(pingInterval)
    }
    
    const historyFunc = async (args?: HistoryArgs) => {
      const redis = this._redis
      if (!redis) throw new Error("Redis not configured.")

      const start = args?.start ? String(args.start) : "-"
      const end = args?.end ? String(args.end) : "+"
      const limit = Math.min(args?.limit ?? 1000, 1000)

      const rawHistory = await redis.xrange(channel, start, end, 'COUNT', limit);
      
      const historyMessages = parseStreamResponse(rawHistory);

      return historyMessages
        .map((value) => {
          if (typeof value === "object" && value !== null) {
            const { id, channel, event, data } = value as { id: string, channel: string, event: string, data: unknown }
            return { data, event, id, channel } as HistoryMessage
          }
          return null
        })
        .filter((item): item is HistoryMessage => item !== null)
    }

    const unsubscribeFunc = () => {
      if (unsubscribe) {
        unsubscribe()
        this._logger.info("✅ Connection closed successfully.")
      }
    }

    const subscribeFunc = async <E extends EventPaths<T["schema"]>>({
      events,
      onData,
      history,
    }: SubscribeArgs<T, E>): Promise<() => void> => {
      const redis = this._redis
      if (!redis) throw new Error("Redis not configured.")
      
      const subManager = this._subscriptionManager
      if (!subManager) throw new Error("SubscriptionManager not initialized.")

      const buffer: UserEvent[] = []
      let isHistoryReplayed = false
      let lastHistoryId: string | null = null

          const onMessage = (message: UserEvent) => {
          if (events && !events.includes(message.event as E)) return

          if (!isHistoryReplayed) {
            buffer.push(message)
          } else {
            onData(message as unknown as EventPayloadUnion<T["schema"], E>)
          }
      }

      const unsubFromManager = await subManager.subscribe(channel, onMessage)

      try {
           if (history) {
              const start =
                typeof history === "object" && history.start ? String(history.start) : "-"
              const end =
                typeof history === "object" && history.end ? String(history.end) : "+"
              const limit = typeof history === "object" ? history.limit : undefined
              
              let rawMessages: unknown[] = [];
              if (limit) {
                  rawMessages = await redis.xrange(channel, start, end, 'COUNT', limit);
              } else {
                  rawMessages = await redis.xrange(channel, start, end);
              }

              const messages = parseStreamResponse(rawMessages);

              for (const message of messages) {
                const typedMessage = message as { event?: string, [key: string]: unknown };
                if (!typedMessage.event || (events && !events.includes(typedMessage.event as E))) continue

                const result = userEvent.safeParse(message)
                if (result.success) {
                  onData(result.data as unknown as EventPayloadUnion<T["schema"], E>)
                }
              }

              if (messages.length > 0) {
                lastHistoryId = (messages[messages.length - 1]?.id as string) ?? null
              }
            }

            for (const message of buffer) {
              if (lastHistoryId && compareStreamIds(message.id, lastHistoryId) <= 0)
                continue
              onData(message as unknown as EventPayloadUnion<T["schema"], E>)
            }

            buffer.length = 0
            isHistoryReplayed = true
            startPingInterval()
            
      } catch (err) {
          unsubFromManager()
          throw err
      }
      
      unsubscribe = () => {
          stopPingInterval();
          unsubFromManager();
      }
      return unsubscribe
    }

    const findSchema = (path: string[]): z.$ZodType | undefined => {
      let current: unknown = this._schema

      for (const key of path) {
        if (!current || typeof current !== "object") return undefined
        current = (current as Record<string, unknown>)[key]
      }

      // Check if it looks like a Zod schema
      const typedCurrent = current as { _zod?: z.$ZodType, _def?: unknown }
      return typedCurrent?._zod || typedCurrent?._def ? (current as z.$ZodType) : undefined
    }

    const emitFunc = async <K extends EventPath<T>>(event: K, data: EventData<T, K>, opts?: { history?: HistoryConfig | boolean }) => {
      const pathParts = (event as string).split(".")
      const schema = findSchema(pathParts)

      if (schema) {
        z.parse(schema, data)
      }

      if (!this._redis) {
        this._logger.warn("No Redis instance provided to Realtime.")
        return
      }

      const currentHistoryConfig = {
        ...historyConfig,
        ...(typeof opts?.history === "boolean" ? {} : opts?.history ?? {}),
      }
      
      const xaddArgs: (string | number)[] = [channel];
      
      if (currentHistoryConfig.maxLength) {
          xaddArgs.push('MAXLEN', '~', currentHistoryConfig.maxLength);
      }
      
      xaddArgs.push('*'); // ID
      
      const dataStr = typeof data === 'object' ? JSON.stringify(data) : String(data);
      
      xaddArgs.push('data', dataStr);
      xaddArgs.push('event', event);
      xaddArgs.push('channel', channel);

      // @ts-expect-error - ioredis spread args
      const id = (await this._redis.xadd(...xaddArgs)) as string

      const payload: UserEvent = {
        data,
        event,
        channel,
        id,
      }

      const pipeline = this._redis.pipeline()

      if (currentHistoryConfig.expireAfterSecs) {
        pipeline.expire(channel, currentHistoryConfig.expireAfterSecs)
      }

      pipeline.publish(channel, JSON.stringify(payload))

      await pipeline.exec()

      this._logger.info(`⬆️  Emitted event:`, {
        id,
        data,
        event,
        channel,
      })
    }

    return {
        history: historyFunc,
        unsubscribe: unsubscribeFunc,
        subscribe: subscribeFunc,
        emit: emitFunc
    }
  }

  channel<N extends string>(channel: N, history?: HistoryConfig | boolean): RealtimeChannel<T> {
    if (!this.channels[channel]) {
      this.channels[channel] = this.createEventHandlers(channel, history)
    }

    return this.channels[channel]
  }
}

type SchemaPaths<T, Prefix extends string = ""> = {
  [K in keyof T]: K extends string
    ? T[K] extends z.$ZodType
      ? Prefix extends ""
        ? K
        : `${Prefix}${K}`
      : T[K] extends object
      ? SchemaPaths<T[K], `${Prefix}${K}.`>
      : never
    : never
}[keyof T]

export type EventPath<T extends Opts> = T["schema"] extends Schema
  ? SchemaPaths<T["schema"]>
  : never

type SchemaValue<T, Path extends string> = Path extends `${infer First}.${infer Rest}`
  ? First extends keyof T
    ? SchemaValue<T[First], Rest>
    : never
  : Path extends keyof T
  ? T[Path]
  : never

export type EventData<T extends Opts, K extends string> = T["schema"] extends Schema
  ? SchemaValue<T["schema"], K> extends z.$ZodType
    ? z.infer<SchemaValue<T["schema"], K>>
    : never
  : never

export type HistoryMessage = {
  id: string
  event: string
  channel: string
  data: unknown
}

type SubscribeArgs<T extends Opts, E extends EventPaths<T["schema"]>> = {
  events: readonly E[]
  onData: (arg: EventPayloadUnion<T["schema"], E>) => void
  history?: boolean | HistoryArgs
}

type RealtimeChannel<T extends Opts> = {
  subscribe: <E extends EventPaths<T["schema"]>>(
    args: SubscribeArgs<T, E>
  ) => Promise<() => void>
  unsubscribe: () => void
  emit: <K extends EventPath<T>>(
    event: K,
    data: EventData<T, K>,
    opts?: { history?: HistoryConfig | boolean }
  ) => Promise<void>
  history: (params?: HistoryArgs) => Promise<HistoryMessage[]>
}

export type Realtime<T extends Opts> = RealtimeBase<T> & {
  channel: (name: string, history?: HistoryConfig | boolean) => RealtimeChannel<T>
} & RealtimeChannel<T>

type InferSchemaRecursive<T> = {
  [K in keyof T]: T[K] extends z.$ZodType
    ? z.infer<T[K]>
    : T[K] extends object
    ? InferSchemaRecursive<T[K]>
    : never
}

export type InferSchema<T extends Schema> = InferSchemaRecursive<T>

export type InferRealtimeEvents<T> = T extends Realtime<infer R>
  ? NonNullable<R["schema"]>
  : never

export const Realtime = RealtimeBase as new <T extends Opts>(data?: T) => Realtime<T>
