import {
    Redis,
    Cluster,
    type RedisOptions,
    type ClusterOptions,
    type ClusterNode
} from "ioredis"

/**
 * Connection factory that returns either a single-node ioredis Redis or
 * ioredis Cluster client based on the `cluster` flag.
 *
 * IMPORTANT: returns `Redis` typed even when actually a Cluster instance.
 * ioredis Cluster implements the same command surface (read/write
 * commands, EVAL, MULTI, pipeline), so existing call sites work
 * unchanged at runtime. We cast through `unknown` to keep the type
 * signature unchanged across the ~10 call sites; this is the pattern
 * used by ioredis docs for mixed-mode codebases.
 *
 * Cluster mode notes for callers:
 * - All keys touched within a single MULTI / EVAL / multi-key pipeline
 *   block MUST hash to the same slot. Use Redis hash tags `{tag}` to
 *   force same-slot grouping (e.g. `prefix:{tag}:key1` and
 *   `prefix:{tag}:key2` will share a slot).
 * - Independent commands in a pipeline are auto-routed to the owning
 *   node by ioredis Cluster — pipelines without hash tags work but lose
 *   atomicity guarantees.
 * - BullMQ queues require `prefix: '{tag}'` so all internal keys hash
 *   to the same slot.
 */
export function createRedis(
    endpoint: string,
    opts?: {
        cluster?: boolean
        redisOptions?: RedisOptions
        clusterOptions?: ClusterOptions
    }
): Redis {
    if (opts?.cluster) {
        const { host, port, tls, password, username } =
            parseEndpoint(endpoint)
        const nodes: ClusterNode[] = [{ host, port }]
        const cluster = new Cluster(nodes, {
            redisOptions: {
                tls,
                password,
                username,
                ...opts.redisOptions
            },
            // Re-fetch CLUSTER SLOTS topology every 5s — picks up
            // master/replica role changes within seconds of an
            // ElastiCache failover.
            slotsRefreshInterval: 5000,
            slotsRefreshTimeout: 2000,
            // Follow MOVED redirects up to 16 hops (covers resharding).
            maxRedirections: 16,
            ...opts.clusterOptions
        })
        // ioredis Cluster implements the same command surface as Redis
        // for the ops alto uses. Cast keeps existing typings intact.
        return cluster as unknown as Redis
    }
    return opts?.redisOptions
        ? new Redis(endpoint, opts.redisOptions)
        : new Redis(endpoint)
}

interface ParsedEndpoint {
    host: string
    port: number
    tls?: Record<string, never>
    password?: string
    username?: string
}

const REDIS_SCHEME_RE = /^rediss?:\/\//

function parseEndpoint(endpoint: string): ParsedEndpoint {
    // Accept "host:port", "redis://...", "rediss://..." and parse uniformly.
    const hasScheme = REDIS_SCHEME_RE.test(endpoint)
    const url = new URL(hasScheme ? endpoint : `redis://${endpoint}`)
    return {
        host: url.hostname,
        port: Number.parseInt(url.port || "6379", 10),
        tls: url.protocol === "rediss:" ? {} : undefined,
        password: url.password ? decodeURIComponent(url.password) : undefined,
        username: url.username ? decodeURIComponent(url.username) : undefined
    }
}
