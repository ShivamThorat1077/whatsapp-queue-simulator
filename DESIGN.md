# WhatsApp Announcement Queue Simulator — Design Notes

## Why asyncio, not threading or multiprocessing

This workload is **I/O-bound**: every unit of work is "send an HTTP request, wait
for a response." The CPU does almost nothing — it's waiting on the network for
most of the lifetime of each task. That's exactly the case asyncio is built for.

**Threading** would technically work here too — threads also release the GIL
during I/O wait, so a thread pool calling `requests.post()` could reach similar
throughput. But it costs more to get there and gives you less control:

- Each OS thread costs ~1-8MB of stack space and real context-switch overhead.
  Coroutines cost a few hundred bytes each. To have 500+ deliveries in flight
  with headroom, threads scale worse per unit of concurrency.
- Coordination gets harder. My priority queue, semaphore, and rate limiter all
  need to be touched by every worker on every message. With `asyncio` those are
  single-threaded data structures — no locks needed except where I explicitly
  want one (the rate limiter's token refill). With threads, `queue.PriorityQueue`
  is thread-safe but the *rate limiter state* (tokens, current rate) would need
  its own locking, and it's easy to get subtly wrong under contention.
- Cancellation and backoff timing are precise with `asyncio.sleep` inside a
  coroutine. Getting a thread to "wait 0.6s, but only for its own retry, without
  blocking the others" means real per-thread timers instead of one event loop
  the runtime already schedules for you.

**Multiprocessing** is close to the wrong tool entirely. It buys you parallel
*CPU* work by paying for process isolation (each process gets its own memory
space, its own copy of the interpreter, IPC/serialization to share state). None
of that helps here — there's no CPU-bound work to parallelize, and the
isolation is actively counterproductive: the whole point of this system is a
**single shared priority queue** and a **single shared rate limit budget**.
Splitting workers across processes means either:
- sharding the queue and rate limit per process (so a billing message that
  lands in process B can't preempt announcements queued in process A — this
  breaks the priority requirement outright), or
- standing up a shared-state layer (Redis, a manager process) just to
  coordinate what asyncio gives you for free in one process.

So: asyncio wins because the bottleneck is waiting on a network call, and the
things that need to be strictly ordered and coordinated (queue, rate budget)
are naturally single-owner in a single event loop.

## What would break if we switched

- **Switching to threading**: the system would likely still function, but
  under real load I'd expect the rate limiter to occasionally either over- or
  under-throttle due to lock contention/interleaving on the token bucket, and
  memory footprint would grow linearly with concurrency (500 threads is not
  realistic; 500 coroutines is nothing). Debugging ordering bugs also gets
  harder — threads interleave non-deterministically at the bytecode level;
  coroutines only yield at `await` points, so the set of places a race can
  happen is much smaller and easier to reason about.
- **Switching to multiprocessing**: priority preemption breaks unless I add
  a shared coordination layer, at which point I've reinvented a message broker
  (which, honestly, is what you'd actually want at real WhatsApp-Business-API
  scale — see below).

## Where asyncio *would* stop being the right answer

If this needed to survive a process crash without losing in-flight messages,
or scale past what one process/host can push, I'd reach for a real broker
(e.g., Redis-backed priority queue, or RabbitMQ with priority queues) and treat
this asyncio worker as *one consumer* of that broker rather than the whole
system. asyncio here is the right tool for "single-process concurrency,"
not for durability or horizontal scale — those are separate problems this
trial isn't asking me to solve.

## Concurrency model

Two independent controls, deliberately separate:

- **`asyncio.Semaphore(10)`** caps how many requests are physically in flight
  at once — this is a hard ceiling on concurrency, independent of pace.
- **`AdaptiveRateLimiter`** (token bucket) paces *how fast* new requests start,
  targeting 25/sec normally. On a 429 it halves its rate immediately; on
  success it creeps back up 5% at a time. This is what makes backpressure
  systemic rather than per-message: one 429 slows the *whole* pipeline down for
  a bit, instead of just delaying the one message that got throttled, which is
  what actually prevents a retry storm from making the 429s worse.

## Priority queue

`asyncio.PriorityQueue` ordered on `(priority, sequence_number)`. Billing = 0,
announcements = 1, so billing always sorts first; the sequence number is a
tiebreaker so two messages of equal priority don't need their `Message` objects
to be comparable.

This gives **queue-level preemption**: a billing webhook that lands while
hundreds of announcements are already queued gets picked up by the next free
worker before any of those announcements, regardless of insertion order.

Note this only proves something if billing arrives *after* the backlog
already exists. Early iterations of this simulator preloaded all billing
messages into the initial batch alongside announcements — which is a mistake,
because a priority queue and a plain sorted-then-processed-sequentially list
produce identical output in that setup (both just do billing first, since
everything was sorted before any worker started pulling). That's not a test
of preemption, it's a test of sorting.

The actual test (`client.py`, `billing_arrivals`) enqueues 500 announcements
as backlog, then trickles 20 "billing webhooks" in one at a time over the
whole run, each logging how many announcements are sitting ahead of it in the
queue at that instant. The evidence is in the delivery latency: a billing
message that arrives with 439 announcements ahead of it clears in ~450ms —
about the same time as one arriving with 42 announcements ahead. If this were
sequential processing of a sorted list, wait time would scale with queue
position. It doesn't, because every worker's next `queue.get()` re-evaluates
priority fresh rather than working through a fixed order.

What this *doesn't* do — and what no queue design can do — is preempt a
request that a worker has already started sending. Once a worker has pulled a
message and is inside `await session.post(...)`, it finishes that call before
picking up anything else. That's a property of "one worker handles one message
at a time," not a limitation of the priority queue. If true mid-flight
preemption mattered, the fix would be more workers (lower the odds any given
worker is busy when billing arrives), not a different queue.

## A bug found by actually running this, not just reasoning about it

The first time I ran the simulation, throughput fell off a cliff early and
took a long time to recover. The cause: with 10 concurrent workers, a single
moment of exceeding the server's rate limit produces roughly 10
*simultaneous* 429s — one per worker in flight — not one. Each of those
independently called `throttle()` on the rate limiter, which halves the rate.
Ten near-simultaneous halvings collapsed the rate from 25/s to about 0.02/s
in one burst (floored at a `min_rate` of 1.0), and recovery at a flat +5% per
success meant it needed dozens of successful requests just to climb back to
a sane rate.

Fix: debounce the throttle signal (one cut per 0.5s cooldown, since
concurrent 429s from the same moment shouldn't compound), and make recovery
step by an absolute floor rather than only a percentage, so escaping a
near-zero rate doesn't take forever. After the fix, the system converges to
right around the server's real capacity (15 req/sec) and delivers all 520
messages in ~35s across repeated runs — close to the theoretical minimum
(520 / 15 ≈ 34.7s) — with zero dead-lettered messages every time.

I'm noting this because it's the kind of thing that only shows up when you
actually run the concurrency model under real timing, rather than reading
the code and convincing yourself it's correct.

## `dead_lettered` vs. `permanent_failure`: not the same number

These two counters answer different questions, and they will not always
match. `permanent_failure` counts messages that got a definitive "do not
retry this" signal (a 400 whose error code isn't a throttle code — the
request itself was invalid). `dead_lettered` counts every message that
ends up permanently undelivered, for *either* reason: a permanent failure,
or a message that exhausted its retry budget on an ordinary retryable
error (429, 500, 503) purely from bad timing.

This showed up in an actual run: `delivered=518 retried=141
dead_lettered=2 permanent_failure=1`. One message was dead-lettered for a
genuine permanent reason (code 100). The other — message `306` — was
dead-lettered after exhausting all 5 attempts on nothing but ordinary
`429-burst` responses; it just kept getting unlucky with timing until its
budget ran out. Both are correctly dead-lettered, but for different
reasons, and only one of them is a "permanent failure" in the strict
sense. The invariant is `dead_lettered >= permanent_failure`, not
equality — the `reason=` field on each `DEAD` log line is what tells you
which case actually happened for any given message.

## Not every failure should be retried

429, 500, and 503 all mean "try again later" — but a 400 can mean two very
different things. Meta's real Graph API sometimes returns a 400 whose body
contains a rate-limit error code (130429, 80007, 4) — a rate limit wearing
a 400's clothes — which should be retried like a 429. Other 400s mean the
request itself was invalid, and retrying it would just fail the same way
forever. `client.py` checks the error code inside the 400 body to tell these
apart; `server.py` generates both kinds (rarely) so this logic is actually
exercised rather than sitting as dead code. This is also why
`dead_lettered` is no longer always 0 in a normal run — a handful of
genuinely permanent failures are expected and correctly *not* retried,
which is the point being demonstrated.

## Retry budget: billing gets more attempts than announcements

Priority determines queue order, but on its own that only protects a message
from *waiting* — every message still shares a retry budget. I gave billing
messages a larger budget (10 attempts vs. 5 for announcements), since
billing is the traffic we've explicitly said matters more; if anything is
going to survive a bad stretch of retries, it should be that.

Exponential backoff with jitter (`0.25 * 2^attempt + random jitter`), capped at
8s, up to 5 attempts, then dead-lettered (logged, not silently dropped). A 429
response overrides the computed backoff with the server's `Retry-After` value
when present, since the provider is telling us exactly how long it wants to be
left alone — that's better information than a fixed backoff schedule.

Retried messages go back into the *same* priority queue at their original
priority, so a billing message that fails and retries doesn't lose its place
in line relative to announcements.