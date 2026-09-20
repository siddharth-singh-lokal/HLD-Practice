# HLD Practice

System design reference notes — scale math, architecture sketches, tradeoffs,
and evolution paths (MVP → production) for classic HLD problems.

## Notes

| File | System | Core decisions |
| --- | --- | --- |
| `url-shortner.md` | URL shortener | read-heavy cache-first, counter → base62, 302, Aurora Postgres + Redis |
| `news-feed.md` | News feed | fan-out on write vs read, hybrid + celebrity threshold, Redis sorted-set feed, Kafka for async fan-out |
| `rate_limiter.md` | Distributed rate limiter | sliding window over shared store, consistent hashing, fail-open vs fail-closed |

More coming as the sprint progresses.
