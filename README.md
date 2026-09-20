# HLD Practice

System design reference notes — scale math, architecture sketches, tradeoffs,
and evolution paths (MVP → production) for classic HLD problems.

## Notes

| File | System | Core decisions |
| --- | --- | --- |
| `url-shortner.md` | URL shortener | read-heavy cache-first, counter → base62, 302, Aurora Postgres + Redis |
| `rate_limiter.md` | Distributed rate limiter | sliding window over shared store, consistent hashing, fail-open vs fail-closed |

More coming as the sprint progresses.
