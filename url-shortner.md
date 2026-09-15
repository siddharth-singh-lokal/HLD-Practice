# URL Shortener — System Design Learnings

Problem Statement: https://thedesignround.com/system-design/problems/01-url-shortener

Main takeaway: start simple, do the capacity math, identify the bottleneck, then scale the design only where needed.

### Core design

```text
Client
  ↓
LB / API Gateway
  ↓
URL Service
  ↓
Redis
  ↓
Cassandra
```

- Very read-heavy system → optimize the redirect path first.
- Redis for fast/hot reads, Cassandra for durable scalable storage.
- Keep the service stateless so it can scale horizontally.

### Back-of-the-envelope

~100M URLs/month, ~100:1 read/write.

→ ~38 writes/sec
→ ~3.8K reads/sec average
→ ~19K reads/sec peak

7-char Base62:

`62^7 ≈ 3.5T`

So 7 chars gives plenty of room.

### ID generation

**Hash**

- Deterministic
- Collision handling needed

**Counter**

- Simple + unique
- Central bottleneck / predictable IDs

**KGS**

- Pre-generate IDs in batches
- No collision check on every request
- Good for short + unique IDs
- Some unused IDs can be lost if a server dies

Main thing to remember: **hash if deterministic mapping matters, KGS if I mainly need short + unique IDs at scale.**

### Read path

```text
GET /abc123
     ↓
Redis
  hit → redirect
  miss → Cassandra → Redis → redirect
```

Cache-aside.

Things to think about:

- Hot keys / viral URLs
- Cache stampede
- Redis failure → Cassandra fallback

### 301 vs 302

**301**

- More caching
- Less load
- Analytics can become less accurate

**302**

- Request keeps coming back to us
- Better analytics / more control
- More load

→ Use **302 when click tracking matters**.

### Analytics

Keep it off the critical path:

```text
Redirect → Kafka → Flink → ClickHouse
```

- Kafka decouples/buffers events
- Flink processes/aggregates
- ClickHouse for analytics queries

**Analytics can be eventually consistent. Redirects cannot.**

### Custom aliases

Two users might request the same alias at the same time.

Bloom filter can optimize the lookup, but correctness should come from an atomic DB operation like:

`INSERT IF NOT EXISTS`

### Scaling

Start with:

```text
LB → URL Service → Redis → DB
```

Then add things only when needed:

- KGS for ID generation
- Cassandra for larger scale
- Kafka/Flink/ClickHouse for analytics
- Multi-DC + global routing for global scale
- CDN/L1 cache for very hot redirects

### Failure cases

- Redis down → DB fallback
- Kafka/Flink down → analytics delayed, redirects still work
- KGS down → use already allocated IDs until replenished
- DC down → fail over to another region

### Other stuff worth remembering

- Rate limiting + malicious URL checks
- URL expiry / TTL
- URL normalization / dedup
- Multi-region replication
- Monitor redirect latency, cache hit rate, error rate, Kafka lag, KGS pool

### Complete flow

```text
Requirements
    ↓
Back-of-envelope
    ↓
Simple architecture
    ↓
Read path + cache
    ↓
ID generation
    ↓
301 vs 302
    ↓
Analytics
    ↓
Hot keys / failures
    ↓
Multi-region if needed
```

**Big takeaway:** don't memorize the final architecture.

Remember the progression:

**read-heavy → cache → scalable DB → ID generation → async analytics → handle hot keys/failures → multi-region**
