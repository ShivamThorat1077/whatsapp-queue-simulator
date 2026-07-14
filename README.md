# WhatsApp Announcement Queue Simulator

## Setup
```
pip install -r requirements.txt
```

## Run
```
python client.py
```

Starts a local mock WhatsApp-like API (`server.py`, a real aiohttp server
with a deterministic token-bucket rate limit) and delivers a batch of 500
announcements plus 20 billing webhooks — arriving one at a time throughout
the run, after most of the backlog already exists, so priority preemption is
actually being tested rather than just sorted once at the start.

See `DESIGN.md` for the reasoning behind the concurrency model, including a
real rate-limiter bug found and fixed while testing this.

## Files
- `client.py` — priority queue, worker pool, retry/backoff logic
- `rate_limiter.py` — adaptive token-bucket rate limiter (client-side backpressure)
- `server.py` — mock downstream API with a real, deterministic rate limit
- `DESIGN.md` — design rationale and tradeoffs

## AI tools disclosure

I used Claude (Anthropic) as a coding assistant throughout this project.

* **Initial implementation:** the first draft of `client.py`, `server.py`, and
  `rate_limiter.py` was scaffolded with Claude based on my direction on the
  architecture (priority queue + worker pool + semaphore + adaptive rate
  limiter). I reviewed the generated code line by line before treating it as
  a starting point, and modified the backoff strategy (capped exponential
  with jitter + Retry-After override from the server), the retry budget
  (10 attempts for billing vs 5 for announcements), and the queue priority
  logic (billing webhooks trickle in mid-run so preemption is actually
  tested, not just sorted once at the start) to match how I wanted it to
  behave.

* **Debugging:** I ran the simulator myself, repeatedly, on my own machine.
  A real bug showed up — the rate limiter collapsing to near-zero under a
  burst of simultaneous 429s. I worked with Claude to diagnose the cause and
  fix it, then re-ran the simulator multiple times to confirm the fix held:
  throughput converged to the server's real capacity (~15 req/s) after a 429
  burst, all 520 messages delivered in ~35s with zero dead-lettered across
  repeated runs.

* **Scope:** I deliberately left out an extra load-testing scenario Claude
  had proposed (a 5x-overload stress test with a naive fire-and-forget
  baseline) — it wasn't part of the original ask, and I wanted the
  submission focused on the stated requirements.

