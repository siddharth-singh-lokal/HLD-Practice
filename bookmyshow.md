# Ticket Booking (BookMyShow)

Booking is the classic contention problem: many users want the same few seats at the same
instant, and the system must never sell one seat twice. Volume is small; the difficulty is
correctness under concurrency and surviving release-day spikes.

## Concepts

| Concept | One line |
| --- | --- |
| Conditional update | Put the expected state in WHERE, check the row count; the database picks exactly one winner |
| All-or-nothing hold | Hold every requested seat in one transaction, or none of them |
| Hold with expiry | A reservation that lapses by itself if the user doesn't pay in time |
| Lazy expiry | Treat an expired hold as free at read time instead of waiting for a cleanup job |
| Deadlock | Two transactions each wait for a row the other holds; avoid by locking rows in a fixed order |
| Pessimistic vs optimistic locking | Lock rows before changing them vs check a version number when writing |
| Unique constraint | The database refuses a duplicate whatever the code does; the last line of defence |
| Saga with compensation | Hold, pay, confirm as separate steps; if confirm fails after payment, refund |
| Admission control (waiting room) | Let users into the booking path at a rate the database can handle |
| Partition key by transaction scope | Shard so that everything one hold touches lives together |
| Stale reads, fresh writes | The seat map can be a few seconds old because the hold is checked against the database |

## Requirements

**Functional**
- Users can browse movies playing in their city on a date, and see theatres and showtimes.
- Users can see a live seat map for a show.
- Users can select up to 10 seats and hold them for 10 minutes while paying.
- Users can pay, get a confirmed ticket, and cancel under the refund policy.

**Out of scope:** reviews, food and beverage, offers, dynamic pricing.

**Non-functional**
- Never double-book. Strong consistency for seat state; every design decision serves this.
- Seat map p99 under 200 ms, and a few seconds of staleness is acceptable.
- Survive a flash crowd: a blockbuster's first-day shows open and a huge number of
  users arrive within minutes.
- Fairness: users who arrive first should generally get in first.

## Scale

| Quantity | Assumption | Works out to | What it decides |
| --- | --- | --- | --- |
| Seat-shows per day | 8,000 screens × 5 shows × 250 seats | 10M seat rows a day, ~1 GB | Small: Postgres holds months of it |
| Bookings | ~1.5M tickets a day | ~20/s average | Writes are tiny on average |
| Release-day spike | 1M users in the first 10 minutes | ~1,700 arrivals a second, all on a few shows | Contention and spikes, not volume, are the problem |

**Consequence:** average load fits one database; the design is for the spike and for the
contention on hot shows.

## Core entities

City, Theatre, Screen, Seat (the physical seat in a screen), Movie, Show (a movie on a
screen at a time), **ShowSeat** (one seat for one show — the row people fight over),
Booking, Payment, Ticket.

## API

```
GET  /cities/{city}/movies?date=
GET  /movies/{id}/shows?city=&date=              -> theatres + showtimes
GET  /shows/{id}/seats                           -> seat map: seat_id, status, price tier

POST /bookings  {show_id, seat_ids}   Idempotency-Key
                -> {booking_id, status: HELD, expires_at, amount}
POST /bookings/{id}/pay               -> payment session
GET  /bookings/{id}                   -> status, tickets
POST /bookings/{id}/cancel

POST /payments/webhook                (signed callback from the payment provider)
```

Design notes:
- The user comes from the auth token, and the server computes the amount from the show's
  price tiers; the client sends only seat ids.
- `POST /bookings` takes an idempotency key: a retry on a flaky network returns the same
  hold instead of trying to grab the seats again.
- The seat map response is cacheable for a few seconds. Correctness doesn't depend on it,
  because the hold is checked against the database.

## Data model

Everything lives in Postgres: the data is relational, needs transactions, and is small.
Movie and show search can move to Elasticsearch later, but isn't needed to start.

```sql
theatres   (id, city_id, name, address)
screens    (id, theatre_id, name)
seats      (id, screen_id, row_label, number, tier)          -- the physical layout
movies     (id, title, language, duration_min)
shows      (id, movie_id, screen_id, starts_at, prices_by_tier)
             INDEX (movie_id, starts_at)

show_seats (show_id, seat_id, status, booking_id, hold_expires_at)
             PRIMARY KEY (show_id, seat_id)
             -- status: AVAILABLE | HELD | BOOKED

bookings   (id, user_id, show_id, status, amount_paise, expires_at,
            idempotency_key, created_at)
             UNIQUE (user_id, idempotency_key)
             -- status: HELD | CONFIRMED | EXPIRED | CANCELLED

tickets    (id, booking_id, show_id, seat_id)
             UNIQUE (show_id, seat_id)                        -- the last line of defence

payments   (id, booking_id, psp_reference UNIQUE, status, amount_paise)
outbox     (id, booking_id, event_type, payload, created_at, published_at)
```

Why each choice:
- `show_seats` is created when the show is scheduled, one row per seat (250 rows per show).
  Holding a seat is then an UPDATE of an existing row, which locks cleanly. If seats were
  rows you INSERT on booking, two people could both check "no row yet" and both insert.
- The primary key `(show_id, seat_id)` keeps all of a show's seats together, which matters
  when sharding.
- `tickets` has `UNIQUE (show_id, seat_id)`. Even if a bug in the hold logic slipped
  through, the database refuses a second ticket for the same seat. A storage-layer backstop.
- Money in integer paise, never floats.
- `UNIQUE (user_id, idempotency_key)` makes client retries harmless.

## Low-level design sketch

Classes: Movie, Theatre, Screen, Seat, Show, ShowSeat, Booking, Payment, Ticket, plus a
BookingService exposing `holdSeats`, `confirm`, `cancel`.

```java
interface SeatLockProvider {
    boolean lock(showId, seatIds, bookingId, ttl);
    void unlock(showId, seatIds, bookingId);
}
DbSeatLockProvider    implements SeatLockProvider   // conditional UPDATE
RedisSeatLockProvider implements SeatLockProvider   // SET NX PX + Lua
```

The point: the locking strategy can change (database today, Redis at higher scale) without
touching BookingService. That's the Strategy pattern and the Open/Closed principle in one
line.

## High-level design

**Flow A — browse.** User → CDN and API gateway → Catalog service → Postgres read replica
behind a cache. Movies and showtimes change a few times a day, so this is almost all cache
hits.

**Flow B — seat map.** User → Seat map service → Redis, which holds each show's seat map
for a couple of seconds. On a miss, read `show_seats` from a replica and refill. Thousands
of people refreshing one hot show hit Redis, not the database.

**Flow C — hold.** User → waiting room (only switched on for hot shows) → Booking service →
one transaction on the Postgres primary: mark every requested seat HELD with a 10-minute
expiry, or none of them, and create the booking as HELD. The response carries `expires_at`
so the app can show a countdown.

**Flow D — pay and confirm.** User pays through the payment provider (PSP). The PSP's
webhook → Payment service → Booking service: if the hold is still ours, mark the seats
BOOKED, the booking CONFIRMED, insert the tickets and an outbox row, all in one
transaction. The event goes to Kafka; Notification sends the ticket and the seat map
refreshes.

**Flow E — expiry.** Unpaid holds stop counting the moment they expire (the hold query
treats them as free). A sweeper job every minute resets them to AVAILABLE and marks the
bookings EXPIRED, so the seat map looks right.

Read it top to bottom: browse and seat-map traffic never reaches the primary database;
only holds, confirms and the sweeper write to it, and only through the Booking service.
Note what's absent: no distributed lock service, no microservice per entity. One Postgres
primary is enough for the writes; the design work goes into the deep dives.

## Deep discussions

### 1. Holding several seats atomically

The user picks seats A5, A6 and A7. Either all three become theirs, or none do: half a
family booking is worse than none. One statement inside a transaction, then check the row
count:

```sql
BEGIN;
UPDATE show_seats
SET status = 'HELD', booking_id = :booking, hold_expires_at = now() + interval '10 minutes'
WHERE show_id = :show
  AND seat_id = ANY(:seats)
  AND (status = 'AVAILABLE' OR (status = 'HELD' AND hold_expires_at < now()));
-- rows updated = 3?  -> insert the booking row, COMMIT
-- rows updated < 3?  -> ROLLBACK, return "some seats were just taken" + fresh seat map
COMMIT;
```

Why this is safe: when two users target A6 at the same instant, Postgres locks the A6 row
for whichever update reaches it first. The second waits, and when the first commits, it
re-checks its WHERE clause against the new row: A6 is now HELD and not expired, so it
doesn't match. The second user's row count comes back short, and they roll back. No double
hold is possible, and no application-level lock is needed.

**Deadlocks, and how to avoid them.** User X holds A5 then wants A6; user Y holds A6 then
wants A5. Each waits for the other forever. Postgres detects it and aborts one transaction,
which is safe but wastes work. Avoid it by always locking seats in the same order, sorted
by seat id. If you need to inspect the seats before deciding (say, a rule that you can't
leave a single empty seat between two booked ones), lock them explicitly in order first:

```sql
SELECT seat_id, status, hold_expires_at FROM show_seats
WHERE show_id = :show AND seat_id = ANY(:seats)
ORDER BY seat_id
FOR UPDATE;          -- pessimistic: rows stay locked until COMMIT
```

**Pessimistic vs optimistic locking.** Pessimistic (`FOR UPDATE`) locks first and is right
when conflicts are likely, as on a hot show. Optimistic adds a version column and writes
only `WHERE version = :seen`, retrying on failure; it's right when conflicts are rare,
such as a user editing their profile. The conditional update above is a single-statement
form of the optimistic idea and the simplest correct answer here.

### 2. Expiry

Two mechanisms together:
- **Lazy expiry.** The hold query already treats `HELD AND hold_expires_at < now()` as
  free, so an expired hold stops blocking anyone the instant it expires, even if nothing
  has cleaned it up.
- **A sweeper** runs every minute and resets expired rows to AVAILABLE and their bookings
  to EXPIRED, so the seat map shows them as free and reports stay accurate.

Why not the sweeper alone: for up to a minute after expiry, a seat would look taken when
it isn't, and on a sold-out show that's lost revenue. Why not Redis key-expiry events:
they're fire-and-forget, and a missed event means a seat stuck forever.

### 3. Payment succeeds after the hold expired

The classic race: the user pays at 9 minutes 58 seconds; the provider's webhook arrives at
10 minutes 5 seconds. Meanwhile someone else may have taken the seats. Confirm only if the
hold is still ours:

```sql
UPDATE show_seats SET status = 'BOOKED'
WHERE booking_id = :b AND status = 'HELD' AND hold_expires_at > now() - interval '2 minutes';
-- (a small grace window for in-flight payments, as long as nobody else re-held the seat)
-- rows = seats in the booking -> insert tickets, booking CONFIRMED
-- fewer rows                  -> the seats are gone: refund automatically, booking EXPIRED
```

Two things make this rare. Extend the hold once (say by 5 minutes) when the user starts
paying. And because `booking_id` is still on the row, a seat that nobody re-held can still
be confirmed after a short delay.

**This is a saga.** Hold → pay → confirm are separate steps across your database and an
external provider; there's no single transaction around them. Each step has a
compensation: if confirm fails after payment, the compensation is a refund.

**A payment timeout is an unknown outcome.** If the call to the provider times out, the
money may have moved. Mark the payment pending, query the provider with the same
reference, and let the webhook or reconciliation settle it. Never charge again.

### 4. The release-day flash crowd

The database can't autoscale in seconds; protect it instead.
- **Virtual waiting room.** When a show is hot, users land in a queue page and get a
  position. The waiting room admits users to the booking path at a fixed rate (say a few
  hundred a second), handing each a signed token valid for 10 minutes. The Booking service
  rejects requests without a valid token. Users see a fair queue instead of errors.
- **Serve everything else from cache.** Seat maps from Redis, show pages from the CDN.
  Only holds reach the primary database.
- **Per-user limits.** At most one active hold per user per show, and rate limiting at the
  gateway, which also blunts bots.
- **Scale out ahead of time.** The release time is known in advance, so add capacity
  before 10 am rather than reacting.

**Admission control:** when one component (here the primary database) can't scale quickly,
limit how much work reaches it rather than letting everything queue and time out. A
waiting room is admission control with a friendly face.

### 5. Holding seats in Redis instead

A common follow-up: can you avoid hitting the database on every hold?

```sql
SET seat:{show}:{seat} {booking} NX PX 600000      -- one seat, 10-minute expiry
```

For several seats, a Lua script checks all the keys and sets them together, because Redis
runs a script without interleaving other commands. It's faster than Postgres. The cost:
you now have two sources of truth, and Redis replicates asynchronously, so a failover can
lose a few holds and let two people hold the same seat. The fix is that confirmation still
goes through Postgres, where the ticket's `UNIQUE (show_id, seat_id)` constraint catches
any duplicate and triggers a refund.

**Conclusion:** Postgres alone is simpler and handles this load. Move holds to Redis only
if the hold rate outgrew the database, and keep the database as the final judge at confirm.

### 6. Sharding

If one Postgres primary isn't enough, shard by `show_id` (or by city, which keeps a whole
city's data together). The rule: choose the partition key so that one transaction touches
one shard. A hold only ever touches seats of a single show, so with `show_id` as the key,
every hold stays a single-shard transaction. Shard by `user_id` instead and a popular
show's seats would scatter across shards, turning every hold into a cross-shard
transaction.

### 7. How fresh is the seat map?

- Cache each show's map in Redis with a 1–2 second expiry, and delete it on every hold,
  confirm or release event.
- For very hot shows, push changes to open clients over SSE so seats grey out as others
  take them.
- The client will still sometimes pick a seat that was just taken. That's fine: the hold
  fails cleanly, and the response carries a fresh map. The seat map is a hint; the hold
  is the truth.

## Failure handling

Common mistakes that break this design:
- "Check availability, then book" as two separate steps with no lock or condition between
  them.
- Locking the whole show or the whole table to be safe; it kills throughput on exactly the
  shows that matter.
- Holding seats only in application memory or a local cache, which breaks with more than
  one server.
- No expiry on holds, so abandoned carts block seats forever.
- Treating a payment timeout as a failure and letting the user pay again.
- Microservices for every entity before the requirements are agreed.

**Operational picture.** The primary database is the first thing that breaks, on a release
morning. Watch: hold p99 latency, hold conflict rate, waiting-room queue length,
payment-to-confirm failures (refunds issued), and the count of expired holds the sweeper
is releasing. If the database primary fails, fail over to a synchronous replica; holds
live in the database, so they survive, and payments in flight resolve through webhooks and
reconciliation.

## Variants: same skeleton, different inventory

**Airline.** Seats on a flight are sold first as a count per fare class, and seat numbers
are chosen later. So the hold is a conditional decrement on a counter,
`UPDATE fare_inventory SET left = left - :n WHERE flight_id = ? AND fare_class = ? AND left >= :n`,
not a row per seat. Airlines deliberately overbook a little, so the check becomes
`left >= :n - allowed_overbooking`; that's a business rule, not a bug.

**IRCTC (train).** Inventory is a count per train, date, class and quota (general, Tatkal,
ladies). When it reaches zero, bookings go to RAC and then a numbered waitlist: an ordered
queue per train, date and class, promoted automatically when someone cancels. Tatkal
opening at 10 am is the flash crowd, so the waiting room matters even more.

**Hotel.** Inventory is per room type per night, so a three-night stay must reserve three
rows at once. With a hotel aggregator, availability comes from external suppliers: cache
it for search, but confirm with the supplier at booking time, and treat a supplier timeout
as an unknown outcome, just like a payment.

## Summary

1. Volume is small; contention is the problem.
2. `show_seats` rows exist before anyone books; a hold is one conditional UPDATE with a
   row-count check, all or nothing.
3. Holds expire: lazy expiry in the query plus a sweeper.
4. Confirm checks the hold is still yours; otherwise refund. `UNIQUE (show_id, seat_id)`
   on tickets is the backstop.
5. Release day: waiting room, cache the seat map, one hold per user, shard by `show_id`.