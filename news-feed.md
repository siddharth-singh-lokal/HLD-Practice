# News Feed — Design Notes

## Problem statement

> Design a news feed like Facebook/Instagram/Twitter. Users follow others. When
> someone posts, followers see it in their feed. Support pagination, near-real-time
> visibility, and celebrities with millions of followers.

**Requirements (clarified up front):**

- Scale: **100M DAU**, avg **~200 followers**, ~0.1% of accounts are "celebrities" (>1M followers)
- Posts: ~5M/day (5% of DAU post) ≈ **~50-100 write QPS** — trivial
- Reads: ~10 feed loads/user/day ≈ 1B/day ≈ **~10-15k read QPS**, peak 2-3x → **read:write ≈ 200:1**
- Feed load **p99 < 200ms**; post visible to followers within seconds (eventual consistency OK)
- Out of scope: ML ranking (feed is chronological; ranking = separate layer), ads, media/CDN, comment threading

## Solution overview

### 1. Capacity math

```
posts ~5M/day ÷ 100k s   ≈ 50-100 write QPS      → trivial
reads 1B/day ÷ 100k      ≈ 10-15k read QPS       → feed cache (Redis) serves this
storage 5M × 1 KB        ≈ 5 GB/day ≈ 1.8 TB/yr  → one Postgres, no sharding
```

### 2. API

```
POST /api/v1/posts            { content, media_ids? } → 201 { post_id, created_at }
GET  /api/v1/feed?cursor=<ts> &page_size=20          → { posts[], next_cursor }
POST /api/v1/users/{uid}/follow        (DELETE to unfollow)
```

Pagination by cursor (last timestamp), never OFFSET — OFFSET degrades with depth.

### 3. Data model

```
posts:          post_id PK, user_id, content, media, created_at, deleted flag (lazy delete)
user_timeline:  (user_id, created_at) → author's own posts   (Postgres now; Cassandra at scale)
feed:{user_id}: Redis SORTED SET — member = post_id, score = timestamp, keep top 500
```

**Why sorted set, not a list:** `ZADD` is idempotent (no dupes on fan-out retry),
`ZREM` removes one post (deletes/moderation), `ZREMRANGEBYRANK` trims to 500,
`ZREVRANGEBYSCORE` does cursor pagination. A list can't dedup or remove a single item.

### 4. High-level architecture

```
Client → DNS → L7 LB → Post Service / Feed Service (stateless, horizontal)
                          ├─ Postgres: posts + user_timeline      ← source of truth
                          ├─ Redis: feed:{user_id} sorted sets    ← read path
                          ├─ Kafka: "new-post" events             ← write-side decoupling
                          └─ Fan-Out workers (consumer group)     ← do the fan-out
```

### 5. Deep dive: fan-out on write vs read

| | Fan-out on write (push) | Fan-out on read (pull) |
| --- | --- | --- |
| Post cost | **O(followers)** writes — copy post to every follower's feed | **O(1)** — just store it |
| Feed load cost | **O(1)** — prebuilt list | **O(following)** — query each followed user, merge |
| Good when | Read-heavy, small fan-outs (most users) | Huge fan-outs (celebrities), inactive readers |
| Breaks when | Celebrity with 50M followers → 50M writes/post | Everyone reads → merge cost at read time |

Read-heavy (200:1) argues for push: a post costs ~200 writes, but each copy is read
~10 times, so push is net cheaper. Push only breaks when a fan-out is too big to copy.

### 6. Deep dive: hybrid + threshold math (the production answer)

- **Normal users:** fan-out on write → push post_id into every follower's feed (Redis)
- **Celebrities (> threshold):** fan-out on read → just store the post; followers fetch
  it separately and merge at read time
- Threshold ~**10k-100k followers**, tuned to your write budget:
  - `5M posts/day × 200 avg followers ≈ 1B fan-out writes/day` → manageable
  - `celebrity post × 50M followers` → impossible → pull

```
post → if followers ≤ threshold: ZADD to each follower's feed (async)
       if followers > threshold:  just store, no fan-out
feed → read pre-computed feed (Redis) + fetch followed celebrities' recent posts
       → merge → return
```

Only materialize feeds for **active users** — inactive users fall back to pull
(push work is wasted if the feed is never read).

### 7. Deep dive: Kafka's role (and where it isn't)

```
POST /posts → validate + store in Postgres → publish "new-post" event → 201 (immediately)
                                                    ↓
                              Fan-Out workers (consumer group) → ZADD to follower feeds
```

- **Rule: async side effects never block the sync API response.** Fan-out happens after 201.
- Kafka = decoupling buffer: absorbs post bursts; consumer group = horizontal scaling;
  at-least-once + idempotent handlers, DLQ, lag alert (>60s → scale workers).
- Fan-out handler is `ZADD post_id → feed` — naturally **idempotent**, retries are safe.
- **Kafka is NOT in the read path.** Reads = Redis + celebrity merge. No queue involved.

### 8. Sizing (AWS)

| Box | Size | Why |
| --- | --- | --- |
| Postgres (posts) | 2 vCPU / 16 GB + idle reader | 5M posts/day ≈ 5 GB/day → 1.8 TB/yr, no sharding; reader = HA |
| Redis (feeds) | Cluster, ~6-8 nodes × 64 GB | ~4-8 KB per materialized feed × active users ≈ 400-800 GB; only active users materialized |
| Kafka | 3 brokers, modest | carries post + graph events (~100 QPS), NOT the fan-out itself |

### 9. Failures / edge cases

- **Post deleted:** never remove from millions of feeds (eager = O(followers), minutes).
  Mark deleted in DB; at read time batch-check validity, skip deleted; they age out of
  the 500-entry feed. Background consumer cleans up best-effort.
- **Unfollow:** lazy-filter at read time; rebuild feed periodically.
- **Cold start** (new user / long-inactive): fall back to fan-out on read — pull last
  posts from followed users, build the feed cache on demand.
- **Viral post storm → Kafka lag:** scale fan-out consumers; temporarily extend the
  hybrid threshold (more pull); prioritize active users.
- **Redis down:** degrade to fan-out on read from user_timeline — slower, but up.

### 10. Scale path (10x)

Shard posts by user_id (isolates hot users); Cassandra for user_timeline; multi-region —
home feed in-region, celebrity posts replicated globally.

## Summary

> Post → store → Kafka → fan-out workers → Redis sorted-set feeds (push for normal
> users, pull for celebrities, merged at read time). Async side effects never block
> the post API. Lazy delete, cursor pagination.