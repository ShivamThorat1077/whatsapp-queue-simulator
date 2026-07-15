import asyncio
import random
import time

from aiohttp import web

"""
This isn't trying to be a WhatsApp Business API clone. It's the smallest
mock that behaves the way *any* real rate-limited API gateway behaves,
so the client's backpressure logic is being tested against something
deterministic rather than a coin flip:

  - A hard sustained-rate cap enforced with a token bucket (server-side).
    Exceed it -> 429 with an accurate Retry-After, every time. No luck
    involved, which makes it possible to reason about the numbers
    afterward instead of just eyeballing logs.
  - A small, load-independent 500 rate, because real infra fails
    sometimes for reasons that have nothing to do with your request rate.
"""

SERVER_RATE = 15.0     # sustained requests/sec this "account" is allowed
SERVER_BURST = 15.0    # burst allowance on top of the sustained rate
RANDOM_500_RATE = 0.02
RANDOM_503_RATE = 0.01        # infra at capacity - client should retry like a 429
RANDOM_400_THROTTLE_RATE = 0.01   # Meta-style rate-limit-wearing-a-400's-clothes
RANDOM_400_PERMANENT_RATE = 0.005  # genuine bad request - should NOT be retried
LATENCY_RANGE = (0.03, 0.12)

# Real WhatsApp Graph API error codes that mean "you're actually rate limited"
# even though the HTTP status is 400, not 429.
THROTTLE_400_CODES = (130429, 80007, 4)
PERMANENT_400_CODE = 100  # "Invalid parameter" - a real error, not a rate limit


class TokenBucket:
    def __init__(self, rate, capacity):
        self.rate = rate
        self.capacity = capacity
        self.tokens = capacity
        self.updated = time.monotonic()
        self.lock = asyncio.Lock()

    async def try_acquire(self):
        async with self.lock:
            now = time.monotonic()
            elapsed = now - self.updated
            self.updated = now
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
            if self.tokens >= 1:
                self.tokens -= 1
                return True, 0.0
            wait = (1 - self.tokens) / self.rate
            return False, wait


_bucket = TokenBucket(SERVER_RATE, SERVER_BURST)


async def handle_send(request):
    await asyncio.sleep(random.uniform(*LATENCY_RANGE))

    ok, wait = await _bucket.try_acquire()
    if not ok:
        return web.json_response(
            {"error": "rate_limited"},
            status=429,
            headers={"Retry-After": f"{wait:.2f}"},
        )

    if random.random() < RANDOM_500_RATE:
        return web.json_response({"error": "server_error"}, status=500)

    if random.random() < RANDOM_503_RATE:
        return web.json_response({"error": "capacity"}, status=503)

    if random.random() < RANDOM_400_THROTTLE_RATE:
        code = random.choice(THROTTLE_400_CODES)
        return web.json_response({"error": {"code": code, "message": "rate limited"}}, status=400)

    if random.random() < RANDOM_400_PERMANENT_RATE:
        return web.json_response(
            {"error": {"code": PERMANENT_400_CODE, "message": "Invalid parameter"}}, status=400
        )

    return web.json_response({"status": "delivered", "ts": time.time()})


def build_app():
    app = web.Application()
    app.router.add_post("/send", handle_send)
    return app


async def start_server(port=8080):
    app = build_app()
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", port)
    await site.start()
    return runner