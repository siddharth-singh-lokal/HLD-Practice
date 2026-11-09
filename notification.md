# Notification System

Notifications are the async problem: every send must survive retries, duplicates, flaky
providers and users who opted out. The volume is high but the writes are trivial — the
difficulty is reliability (at-least-once without duplicates) and not spamming users.

## Concepts

| Concept | One line |
| --- | --- |
| At-least-once delivery | Every message is delivered at least once; duplicates are possible and must be handled |
| Idempotent consumer | Consumers dedupe by event ID, making duplicates harmless |
| Outbox pattern | The database write and the event publish happen in one transaction; a relay publishes the event |
| Dual-write problem | Writing to the DB and publishing to a broker as two steps can diverge on crash |
| Poison message | A message that always fails; move it to a DLQ after N attempts so it can't block the queue |
| Dead-letter queue (DLQ) | Where repeatedly failing messages go, with the error attached |
| Exponential backoff + jitter | Retry waits grow (100ms → 200ms → 400ms) with randomness so retries don't stampede |
| Fan-out | One event feeds many independent consumers |
| Provider adapter | A common interface over FCM, APNs, SES, Twilio so the service doesn't know each provider |
| Circuit breaker | Stop calling a failing provider for a cooling period instead of hammering it |
| Consumer lag | How many messages are waiting unprocessed; the key health metric |
| Preference filtering | Respect opt-in/out per channel before sending; spam causes uninstalls |

## Requirements

**Functional**
- Users register their devices (push tokens) and channels.
- Send transactional notifications (OTP, payment updates) to a single user.
- Send bulk notifications (marketing, announcements) to segments of users.
- Channels: push (FCM/APNs), email, SMS, in-app.
- Users can view delivery status; failures are retried or surfaced.

**Out of scope:** chat between users, in-app feed computation, personalisation/ML.

**Non-functional**
- At-least-once delivery; duplicates must be deduplicated, never sent twice.
- Push p95 under 5 seconds for transactional sends.
- Respect provider rate limits and quotas.
- Survive provider outages (queue instead of failing).
- Per-user per-channel rate limits; preference checks before every send.

## Scale

| Quantity | Assumption | Works out to | What it decides |
| --- | --- | --- | --- |
| Transactional events | 10M/day | ~120/s average | Small; one service + Kafka handles it |
| Peak (offer at 8 pm) | 20–50× average | ~3–6k/s | Workers auto-scale; queues absorb the burst |
| Bulk campaign | 100M sends over hours | throughput-bound by provider quota | Not infrastructure-bound; schedule and batch |
| Storage | 10M notifications/day × ~0.5 KB | ~5 GB/day | Retention 30–90 days; status updates dominate writes |

**Consequence:** the system is not throughput-bound for normal traffic. The design targets
reliability (no lost or duplicate sends) and provider chaos, not raw scale.

## Core entities

User, Device (per-channel token), Channel (push/email/SMS/in-app), Template (rendered with
data), Notification (one logical send), Provider (FCM/APNs/SES/Twilio), Preference
(opt-in/out per channel), Segment (for bulk).

## API

```
POST /notifications {user_ids|segment, template_id, data, channel, priority}
                Idempotency-Key  -> 202 + job_id
GET  /notifications/{id}                    -> status: PENDING | SENT | FAILED | DROPPED
POST /users/{id}/devices                    -> register a push token
POST /users/{id}/devices/{token}            -> unregister
PUT  /users/{id}/preferences                -> opt-in/out per channel
POST /providers/callbacks                   -> signed delivery receipts from providers
```

Design notes:
- The caller sends an idempotency key; a retry returns the same job instead of double-sending.
- The server resolves segments, filters preferences and renders templates; clients send
  only ids and data.
- Delivery receipts arrive on a signed callback — asynchronously, never in the request path.

## Data model

```sql
users          (id, phone, email, created_at)
devices        (id, user_id, channel, token UNIQUE, provider, created_at, last_seen_at)
templates      (id, channel, subject, body_template, created_at)
preferences    (user_id, channel, opted_in, PRIMARY KEY (user_id, channel))

notifications  (id, user_id, channel, template_id, data JSONB, status, attempts,
                scheduled_at, idempotency_key, created_at)
                 UNIQUE (user_id, idempotency_key)      -- dedupe at the entry point
                 -- status: PENDING | ENQUEUED | SENT | FAILED | DROPPED

outbox         (id, notification_id, event_type, payload, created_at, published_at)

delivery_log   (notification_id, provider, provider_message_id UNIQUE,
                delivered_at, error_code, raw_callback JSONB)
```

Why each choice:
- `UNIQUE (user_id, idempotency_key)` makes client retries harmless at the entry point.
- A notification row per logical send gives status tracking and audit; the `delivery_log`
  records what the provider actually reported.
- The outbox table makes the DB write and the Kafka publish atomic (see Deep diversions 2).

## High-level design

**Flow — send.** User/service → API gateway → Notification service: validate, dedupe by
idempotency key, resolve segment, filter preferences, render template, then write the
notification row + outbox row **in one transaction**. A relay publishes outbox rows to
Kafka (one topic per channel). Per-channel consumer workers pull, rate-limit per
user/channel/hour, and call the provider through its adapter. Failures retry with
exponential backoff + jitter; after N attempts the message goes to a DLQ with the error
attached and an alert fires.

**Flow — status.** The notification row drives `GET /notifications/{id}`. Provider
callbacks update `delivery_log` and the row's status; a reconciliation job periodically
queries providers for anything that went silent.

**Flow — bulk.** A campaign creates a job; workers fan out over the segment in batches,
respecting per-user preferences and rate limits, and can be paused/throttled without
losing progress (position is tracked in the job).

Read it top to bottom: nothing sends synchronously inside a request. The entry service
does validation and one transaction; everything after is queue-backed. Note what's absent:
no per-entity microservices, no in-memory message holds — Kafka is the buffer, the DB is
the record.

## Deep discussions

### 1. At-least-once, dedupe, and the DLQ

Brokers deliver at-least-once: a consumer crash between processing and commit produces a
duplicate. Therefore consumers must be idempotent:
- Dedupe by event ID — check a processed-events table (or the notification row's status)
  before sending; commit after processing, not before.
- Poison messages (bad templates, dead tokens, provider bugs) must not block their
  partition: after N retries with backoff + jitter, move them to a DLQ with the error
  attached and alert. One undead message otherwise stalls everything behind it in order.
- Committing offsets *after* processing is what turns "at-least-once" into the safe
  default; combination with dedupe gives effectively-once behaviour.

### 2. The dual-write problem and the outbox pattern

The bug being avoided: the service writes a notification row to Postgres, then publishes
to Kafka — if it crashes between the two, the DB has the notification but no event was
sent (or the reverse). The fix is the outbox pattern:
1. In the same transaction as the notification row, insert an `outbox` row describing the
   event.
2. A relay reads new outbox rows and publishes them to Kafka, then marks them published.
3. If the relay crashes, it resends — consumers must be idempotent (point 1).

The event is published if and only if the notification committed. The relay can poll the
table or read the database's change log via CDC; CDC additionally catches writes made by
scripts or manual fixes that bypass the service.

### 3. Provider handling

Providers are different and flaky:
- Each channel gets an adapter implementing one interface (validate → send → parse
  callback) because FCM, APNs, SES and Twilio differ in API, quotas and semantics.
- Rate limit per user per channel per hour at the worker, and respect each provider's
  aggregate quota — exceeding it gets the account throttled or banned.
- If a provider is failing, trip a circuit breaker and let the queue back up rather than
  hammering it; resume when it recovers. Delivery receipts (webhooks) update the log, and
  a reconciliation job queries the provider's status API for anything that never
  reported — never trust callbacks alone.

### 4. Rate limits and preferences

Spam is the real product risk: users opt out or uninstall. Before any send, check
preferences; enforce per-user per-channel/hour limits; priority tiers keep transactional
(OTP) ahead of marketing, which can be batched and delayed. Drops due to preferences are
recorded as DROPPED, not FAILED, so dashboards distinguish "chose not to" from "broke".

### 5. Bulk fan-out

A 100M-user campaign is not 100M synchronous sends:
- The campaign is a job with position; workers consume the segment in batches.
- Render templates in bulk; batch provider API calls where supported; throttle to the
  provider quota; pause/resume without losing progress.
- Segments are resolved against the user store at send time, and preference filtering is
  applied per user — marketing never bypasses the preference check.

### 6. Ordering, when it matters

Per-user ordering (e.g., OTP after payment event) comes from keying Kafka partitions by
`user_id` so events for one user stay in order in one partition, consumed by one worker.
Marketing has no ordering requirement and can use any partition scheme. Don't pay for
global ordering unless a requirement demands it.

## Failure handling

Common mistakes that break this design:
- Sending synchronously in the request path — the user waits on FCM.
- No dedupe — retries double-send OTPs and notifications.
- No DLQ — a poison message stalls its entire partition.
- Ignoring provider rate limits — account throttled or banned.
- No preference check — spam drives uninstalls.
- Unbounded retries with no cap — retry storms during a provider outage.
- Committing offsets before processing — crashes lose messages silently.

**Operational picture.** The queue is the first thing that breaks visibility: watch
consumer lag, DLQ depth, per-channel delivery rate and p99 send latency, and uninstall
rate as the spam proxy. Alert on lag growth (workers falling behind), DLQ ingress
(business bug or provider issue), and callback mismatch (sent but never reported → the
reconciliation job fails). SLO: 99.9% of transactional sends accepted, p95 delivery under
5 seconds. Roll out per channel (canary email first, push last).

## Variants

**Marketing email campaigns.** Same pipeline; bulk-templated, bounce-handling matters
(hard vs soft bounces → suppression lists), deliverability (SPF/DKIM/DMARC) and provider
reputation are the hard parts, not scale.

**In-app notifications.** Same pipeline with an SSE/polling read path; the write side is
identical.

**Payment webhooks.** Same reliability skeleton — at-least-once, dedupe by event ID,
reconciliation as the safety net — with higher stakes: never refund or charge twice.

**Chat messages.** Different: real-time, bidirectional, ordering per conversation, and
WebSocket/HTTP2 push instead of fire-and-forget providers — notification semantics don't
apply.

**LLM streaming.** Notifications of token streams need SSE/WebSocket long-lived
connections and idempotent consumers for partial events; the write side is the same
outbox queue machinery.

## Summary

1. Everything is async; nothing sends inside a request.
2. At-least-once + idempotent consumers + DLQ handles all duplicates and poison messages.
3. The outbox pattern keeps the DB write and the event publish atomic.
4. Per-channel adapters, rate limits, circuit breakers and preferences protect users and
   providers.
5. Watch consumer lag and DLQ depth; reconcile with providers — callbacks alone are never
   the source of truth.