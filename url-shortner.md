# URL Shortener — Design Notes

## Problem statement

> Design a URL shortening service like bit.ly. A user gives you a long URL, you
> return a short code; when someone hits the short URL they get redirected to the
> long one.

**Requirements (clarified up front):**

- Scale: **100M new URLs/day**, read:write ≈ **100:1**, redirect p99 < 100ms
- URLs never expire
- Out of scope: auth, real DNS, multi-region, analytics (until follow-up)

## Solution overview

### 1. Capacity math

```
100M new URLs/day ÷ 100k s  ≈ 1,000 write QPS   (peak ~3k)  → 1 Postgres, easy
× 100 B/row                 = 10^10 B = 10 GB/day ≈ 4 TB/yr → no sharding
reads 100:1                 = ~100k read QPS                  → THE problem → Redis
```

Reference anchors: 1M/day ≈ 12 QPS. bytes ÷ 10⁹ = GB, ÷ 10¹² = TB.
Per box: Postgres ~1-10k writes/s, ~10-50k reads/s. Redis ~100k-1M ops/s.
Cache at 90% hit → DB only sees ~10k reads/s.

### 2. API

```
POST /api/v1/urls      { longUrl, customAlias? } → 201 { code, shortUrl }
GET  /{code}           → 301 | 302 Location: longUrl
GET  /api/v1/urls/{code}/stats  (off critical path)
```

### 3. Data model

```
urls: id BIGINT PK, code VARCHAR(7) UNIQUE, long_url TEXT, created_at, clicks BIGINT
index on code — that's the lookup key. One table.
```

### 4. High-level architecture

```
Client → DNS → L7 LB (TLS term) → App servers (stateless, horizontal)
                          ├─ Redis (ElastiCache): code → longUrl   ← read path
                          └─ Aurora Postgres: urls table          ← source of truth
```

- **Read path:** cache hit → redirect. Miss → DB → write back (cache-aside) → redirect.
- **Write path:** generate code → insert → respond. No cache write (read path self-populates).

### 5. Deep dive: code generation — counter + base62

- **Counter → base62:** unique by construction, zero collision logic. `62^7 ≈ 3.5T` vs
  `100M × 365 × 10yr ≈ 365B` needed → **~9.6× headroom**, proven by math.
- **KGS** (pre-generate ID batches): no per-request uniqueness check; some IDs lost if a
  server dies. Reach for it at higher write scale.
- **Hash(longUrl) → 7 chars:** dedup becomes a "feature", but collision handling +
  check-on-insert is ugly. Avoid as the primary approach.

### 6. Deep dive: cache read path + 301 vs 302

- **Cache stampede / hot key:** single-flight per key (one refresh, rest wait or serve stale).
- **301 vs 302:** 301 = permanent, browsers cache it → less load, worse click tracking.
  302 = every request hits us → better analytics, more load. **302 when click tracking matters.**

### 7. DB choice — Postgres (Aurora) over Cassandra/Scylla

The cache eats the reads, so the DB only sees ~1k writes/s + ~10k misses/s — a
**single-node ACID shape**, not a distributed-DB shape.

- Cassandra/Scylla shine at: write-heavy, wide rows, multi-master, no joins. **Reads are
  their weak side.** This workload is 100:1 read:write → wrong tool.
- Scylla is right when: 10M+ writes/s of time-series/wide-row data (e.g. click analytics at scale).

**Why no sharding:** 4 TB/yr fits one box; 1k writes/s is a tenth of one box; reads are
handled by cache. Shard only when: writes > ~10k/s sustained, storage > one box,
**working set won't fit RAM** (read amplification), or multi-region writes. It's driven by
storage growth + read amplification, NOT write QPS.

### 8. Sizing (AWS)

| Box | Size | Why |
| --- | --- | --- |
| Aurora Postgres writer | 2 vCPU / 16 GB | 1k writes/s ≈ 10-15% CPU; 10k misses/s ≈ 40-50% util peak |
| Aurora Postgres reader | 2 vCPU / 16 GB, idle | HA/failover only, NOT load |
| ElastiCache Redis | 1 node, 16 GB | top ~100M hot codes × 100 B ≈ 10 GB working set |

The cost argument: the cache is the *frugal* option, not an extra. Without it you need
3-4 Postgres replicas — each a full ~4 TB copy — instead of one 16 GB Redis node. And
caching is cheap *for this data*: a URL mapping is insert-only + near-immutable, so slight
staleness is harmless and there's nothing to invalidate. (Cache coherence only bites for
mutable, correctness-critical data — inventory, wallet — not here.)
Buy for the numbers; scale when a metric crosses ~60% sustained.

### 9. Failures

- **Redis down:** DB serves reads directly — degraded, latency may exceed budget, but up.
- **DB down:** cached codes still redirect; uncached codes fail → fail fast, don't queue.
- **Custom alias race:** two users, same alias → atomic `INSERT ... ON CONFLICT DO NOTHING`;
  loser gets 0 rows → returns a different alias.

### 10. Scale path (10x)

Shard `urls` by code — hash/consistent-hashing (min rebalance on node add) or range.
More Redis nodes + replication; multi-region read replicas near users. Analytics stays off
the critical path: `redirect → Kafka → Flink → ClickHouse`. **Analytics can be eventually
consistent. Redirects cannot.**

## Summary

> Read-heavy, cache-first, counter → base62, Aurora Postgres as source of truth,
> cache-aside read path, 302, analytics async. Start simple; scale only the part
> the numbers say breaks.