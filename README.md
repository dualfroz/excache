# Excache

Excache is an in-memory data store server written in Elixir that speaks the Redis
protocol (RESP2), so `redis-cli` and ordinary Redis client libraries can talk to it.
The keyspace is split across supervised shard processes backed by ETS. On top of
strings, lists, hashes, sets and sorted sets it implements key expiry,
`maxmemory` eviction with the Redis LRU, LFU, TTL and random policies, AOF logging
and snapshots, pub/sub with keyspace notifications, streams with consumer groups,
bitmaps and bit fields, HyperLogLog, `MULTI`/`EXEC` transactions with `WATCH`, and
blocking list commands.

## Features

- **RESP2 server.** Multi-bulk requests and inline commands, pipelining, one
  process per connection under a `DynamicSupervisor`, a pool of acceptors on a
  shared listen socket.
- **Sharded keyspace.** Keys are hashed with `:erlang.phash2/1` onto a fixed
  number of shard processes (8 by default). Each shard owns one ETS table and
  runs every operation on its keys, so read-modify-write commands are atomic.
- **Expiry.** `EX`/`PX` on `SET`, `EXPIRE`, `PEXPIRE`, and `EXPIREAT` with `NX`,
  `XX`, `GT` and `LT`. Expired keys are hidden on access and removed by a periodic
  sweeper, which also emits `expired` notifications.
- **Memory accounting and eviction.** Per-entry byte estimates built from
  `:erts_debug.size/1`, off-heap binaries and the ETS per-row overhead.
  `maxmemory` with the policies `noeviction`, `allkeys-lru`, `allkeys-lfu`,
  `allkeys-random`, `volatile-lru`, `volatile-lfu`, `volatile-ttl` and
  `volatile-random`. Eviction samples `maxmemory-samples` candidates per pass into
  a pool that persists between passes. The LFU counter grows logarithmically and
  decays over time (`lfu-log-factor`, `lfu-decay-time`). Write commands that can
  grow memory get an `OOM` error when usage is over the limit and the policy
  cannot free enough (always the case under `noeviction`).
- **Persistence.** An append-only file of write commands in RESP format with
  `always`, `everysec` or no explicit fsync, replayed on startup. Snapshots of the
  whole keyspace with `SAVE` and `BGSAVE`, loaded by the shards on startup
  (entries that have already expired are skipped). Once a snapshot is safely on
  disk the AOF is emptied, so startup replays only the writes made after it.
- **Pub/sub.** `SUBSCRIBE`, `UNSUBSCRIBE`, `PUBLISH`. Keyspace notifications are
  published on `__keyspace@0__:<key>` and `__keyevent@0__:<event>` for the core
  string, list, hash, set, sorted set and expiry commands.
- **Sorted sets** on a skip list with span counters, so rank and index range
  queries are O(log n).
- **Streams** with auto and explicit IDs, `MAXLEN`/`MINID` trimming, consumer
  groups, pending entry lists, claiming and auto-claiming of idle entries.
- **Bitmaps** including `BITOP`, `BITPOS` and `BITFIELD` with signed and
  unsigned fields and `WRAP`/`SAT`/`FAIL` overflow handling.
- **HyperLogLog** cardinality estimation with merge.
- **Transactions.** `MULTI`/`EXEC`/`DISCARD` with queued command validation
  (`EXECABORT`) and optimistic locking through `WATCH`/`UNWATCH`.
- **Blocking commands.** `BLPOP`, `BRPOP`, `BLMOVE`, `BRPOPLPUSH` and `BLMPOP`
  with timeouts; clients are served in arrival order when a key receives data.
- **Introspection.** `INFO` sections, `CLIENT` management, `COMMAND` with
  arity, flags and key positions for every supported command, `MEMORY USAGE`,
  `MEMORY STATS`, `MEMORY DOCTOR`, `OBJECT`, runtime `CONFIG GET`/`CONFIG SET`.

## Supported commands

| Group | Commands |
|---|---|
| Connection | `PING` `ECHO` `SELECT` `QUIT` `CLIENT` (`ID` `INFO` `GETNAME` `SETNAME` `LIST` `KILL` `HELP`) |
| Server | `INFO` `COMMAND` (`COUNT` `LIST` `INFO` `DOCS` `GETKEYS`) `CONFIG` (`GET` `SET` `RESETSTAT`) `MEMORY` (`USAGE` `STATS` `DOCTOR`) `DBSIZE` `FLUSHDB` `SAVE` `BGSAVE` |
| Generic keys | `DEL` `UNLINK` `EXISTS` `TOUCH` `KEYS` `SCAN` `TYPE` `OBJECT` (`ENCODING` `REFCOUNT` `IDLETIME` `HELP`) `RENAME` `RENAMENX` `COPY` `EXPIRE` `PEXPIRE` `EXPIREAT` `EXPIRETIME` `PEXPIRETIME` `TTL` `PTTL` `PERSIST` |
| Strings | `GET` `SET` `SETNX` `GETSET` `GETDEL` `APPEND` `STRLEN` `SETRANGE` `GETRANGE` `INCR` `DECR` `INCRBY` `DECRBY` `INCRBYFLOAT` `MSET` `MGET` |
| Lists | `LPUSH` `RPUSH` `LPOP` `RPOP` `LRANGE` `LLEN` `LINDEX` `LSET` `LREM` `LTRIM` `LINSERT` `LPOS` `RPOPLPUSH` |
| Blocking lists | `BLPOP` `BRPOP` `BLMOVE` `BRPOPLPUSH` `BLMPOP` |
| Hashes | `HSET` `HSETNX` `HGET` `HMGET` `HDEL` `HGETALL` `HKEYS` `HVALS` `HLEN` `HEXISTS` `HINCRBY` `HSCAN` |
| Sets | `SADD` `SREM` `SMEMBERS` `SISMEMBER` `SMISMEMBER` `SCARD` `SPOP` `SRANDMEMBER` `SMOVE` `SINTER` `SUNION` `SDIFF` `SINTERCARD` `SINTERSTORE` `SUNIONSTORE` `SDIFFSTORE` `SSCAN` |
| Sorted sets | `ZADD` `ZINCRBY` `ZREM` `ZSCORE` `ZMSCORE` `ZRANK` `ZREVRANK` `ZCARD` `ZCOUNT` `ZRANGE` `ZREVRANGE` `ZRANGEBYSCORE` `ZPOPMIN` `ZPOPMAX` `ZSCAN` |
| Streams | `XADD` `XLEN` `XRANGE` `XREVRANGE` `XDEL` `XTRIM` `XSETID` `XREAD` `XINFO STREAM` |
| Consumer groups | `XGROUP` (`CREATE` `DESTROY` `CREATECONSUMER` `DELCONSUMER` `SETID`) `XREADGROUP` `XACK` `XPENDING` `XCLAIM` `XAUTOCLAIM` |
| Bitmaps | `SETBIT` `GETBIT` `BITCOUNT` `BITPOS` `BITOP` `BITFIELD` |
| HyperLogLog | `PFADD` `PFCOUNT` `PFMERGE` |
| Pub/sub | `SUBSCRIBE` `UNSUBSCRIBE` `PUBLISH` |
| Transactions | `MULTI` `EXEC` `DISCARD` `WATCH` `UNWATCH` |

`SCAN`, `HSCAN`, `SSCAN` and `ZSCAN` accept `MATCH` and `COUNT`; `SCAN` also
accepts `TYPE`. `CLIENT LIST` accepts `TYPE` and `ID` filters.

## Requirements

- Elixir 1.18 or newer
- Erlang/OTP 25 or newer

There are no runtime dependencies outside the standard library.

## Build and run

```sh
mix compile
mix run --no-halt
```

The server listens on port 6379 by default. Startup is configured through
environment variables read by `config/config.exs`:

| Variable | Meaning |
|---|---|
| `EXCACHE_PORT` | TCP port (default `6379`) |
| `EXCACHE_SNAPSHOT_PATH` | snapshot file (default `excache.snapshot` in the application `priv` directory) |
| `EXCACHE_AOF_PATH` | append-only file (default `excache.aof` in the application `priv` directory) |

Other application settings in `config/config.exs`: `shard_count`,
`sweep_interval` (expiry sweep period in milliseconds), `aof_fsync`
(`:always`, `:everysec` or anything else for no explicit fsync) and
`keyspace_notifications`.

Example on a non-default port with files in a data directory:

```sh
EXCACHE_PORT=6380 \
EXCACHE_SNAPSHOT_PATH=data/dump.snapshot \
EXCACHE_AOF_PATH=data/appendonly.aof \
mix run --no-halt
```

To get an interactive shell with the server running, use `iex -S mix`.

## Connecting with redis-cli

```sh
redis-cli -p 6379
```

```
127.0.0.1:6379> SET greeting hello EX 60
OK
127.0.0.1:6379> TTL greeting
(integer) 60
127.0.0.1:6379> ZADD board 10 alice 20 bob
(integer) 2
127.0.0.1:6379> ZRANGE board 0 -1 WITHSCORES
1) "alice"
2) "10"
3) "bob"
4) "20"
127.0.0.1:6379> XADD events * type login user alice
"1700000000000-0"
127.0.0.1:6379> XGROUP CREATE events workers 0
OK
127.0.0.1:6379> XREADGROUP GROUP workers w1 COUNT 1 STREAMS events >
1) 1) "events"
   2) 1) 1) "1700000000000-0"
         2) 1) "type"
            2) "login"
            3) "user"
            4) "alice"
127.0.0.1:6379> PFADD visitors a b c
(integer) 1
127.0.0.1:6379> PFCOUNT visitors
(integer) 3
```

Memory limits and eviction can be changed at runtime:

```
127.0.0.1:6379> CONFIG SET maxmemory 100mb maxmemory-policy allkeys-lfu
OK
127.0.0.1:6379> CONFIG GET maxmemory*
```

Runtime parameters: `maxmemory`, `maxmemory-policy`, `maxmemory-samples`,
`lfu-log-factor`, `lfu-decay-time` and `snapshot_path` are writable; `port` and
`aof_path` are read-only.

To watch keyspace notifications:

```sh
redis-cli -p 6379 SUBSCRIBE __keyevent@0__:expired
```

## Tests

```sh
mix test
```

The suite covers the RESP parser, every command group over real TCP
connections, AOF replay, snapshots, transactions, blocking commands, the skip
list, memory estimates and eviction policies. Tests start their own listener on
an ephemeral port and write AOF and snapshot files to `tmp/`.

## Project layout

```
lib/excache/
  application.ex        supervision tree, AOF replay, listener start
  server/               TCP acceptor and per-connection process
  resp/                 RESP parser and encoder
  command.ex            command dispatch, AOF logging, OOM checks
  command/              command groups (keyspace, streams, bitmaps, blocking,
                        transactions, server, introspection, HyperLogLog)
  store.ex              routing of operations to shards
  store/                shard processes and the operations they run on ETS
  skip_list.ex          sorted set index
  stream.ex, stream/    stream entries and consumer groups
  bitmap.ex, bitfield.ex
  hyper_log_log.ex
  blocking.ex           queue of clients blocked on keys
  pubsub.ex, notify.ex  channels and keyspace notifications
  expiry.ex             periodic sweep of expired keys
  memory.ex             per-entry memory estimates
  eviction.ex, eviction/ eviction policies, access tracker, runner
  aof.ex, persistence.ex
  config.ex             runtime CONFIG parameters
  clients.ex            client registry for CLIENT and INFO
test/                   ExUnit suite and a minimal RESP test client
```

## Limitations

- RESP2 only; `HELLO` and RESP3 are not supported.
- A single logical database. `SELECT` replies `OK` and all notifications use
  database 0.
- No pattern subscriptions (`PSUBSCRIBE`), no `AUTH`/ACL, no replication,
  cluster mode, Lua scripting or modules.
- `XREAD` and `XREADGROUP` accept `BLOCK` but always return immediately.
- `XINFO` supports the `STREAM` subcommand only.
- `BGSAVE` does not block the calling client, but write commands from all
  clients wait until the snapshot is on disk; reads are not affected.
- The snapshot format is Erlang external term format, not RDB.

## License

MIT, see [LICENSE](LICENSE).
