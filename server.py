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
LATENCY_RANGE = (0.03, 0.12)


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