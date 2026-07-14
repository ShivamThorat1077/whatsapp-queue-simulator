import asyncio
import time


class AdaptiveRateLimiter:
    """
    Token-bucket limiter that paces outbound requests to a target rate,
    and adapts that rate down when the downstream API signals it's
    overloaded (429s), then slowly recovers when things go well again.

    This is what turns "backpressure" from a buzzword into actual
    behavior: instead of catching a 429 and just sleeping once, the
    *whole system* slows itself down for a while, so we stop hammering
    a struggling API with the next 50 requests that are already in flight.
    """

    def __init__(self, rate: float, capacity: float, min_rate: float = 1.0):
        self.max_rate = rate
        self.rate = rate
        self.capacity = capacity
        self.tokens = capacity
        self.min_rate = min_rate
        self.last_refill = time.monotonic()
        self._lock = asyncio.Lock()
        self._last_throttle_at = 0.0
        self._throttle_cooldown = 0.5

    async def acquire(self):
        while True:
            async with self._lock:
                now = time.monotonic()
                elapsed = now - self.last_refill
                self.last_refill = now
                self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)

                if self.tokens >= 1:
                    self.tokens -= 1
                    return

                wait = (1 - self.tokens) / self.rate

            # Sleep OUTSIDE the lock so other workers aren't blocked from
            # acquiring tokens that may already be available.
            await asyncio.sleep(wait)

    def throttle(self, factor: float = 0.5):
        """
        Called on a 429 (or harder signals like 503): cut the rate by
        `factor`. Debounced with a short cooldown, because concurrency=10
        means a single overshoot event produces ~10 near-simultaneous 429s
        for the *same* underlying spike, not 10 independent ones. Without
        the cooldown, one burst can compound-halve the rate 10 times in a
        row (25 -> ~0.02/s), collapsing far below where it needs to be and
        taking dozens of successful requests to climb back out of.
        """
        now = time.monotonic()
        if now - self._last_throttle_at < self._throttle_cooldown:
            return
        self._last_throttle_at = now
        self.rate = max(self.min_rate, self.rate * factor)

    def recover(self):
        """Called on a success: climb back toward max_rate. Uses a floor
        on the step size (not just a percentage) so recovery from a very
        low rate isn't glacial - 5% of 1.0 is nothing, but +1.0 flat moves."""
        if self.rate < self.max_rate:
            step = max(1.0, self.rate * 0.1)
            self.rate = min(self.max_rate, self.rate + step)