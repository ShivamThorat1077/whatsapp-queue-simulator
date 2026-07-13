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
# whatsapp-queue-simulator