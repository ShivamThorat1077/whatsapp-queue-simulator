import asyncio
import itertools
import logging
import random
import sys
import time
from dataclasses import dataclass, field

import aiohttp

from rate_limiter import AdaptiveRateLimiter
from server import start_server

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d | %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("queue-sim")

BILLING = 0
ANNOUNCEMENT = 1

API_URL = "http://127.0.0.1:8080/send"

# error.code values that mean "this is actually a rate limit wearing a 400's
# clothes" per Meta's docs (130429 = throughput, 80007 = WABA-level, 4 = app
# call limit). Anything else in a 400 body is a genuine permanent failure.
THROTTLE_400_CODES = {130429, 80007, 4}

seq_counter = itertools.count()


@dataclass
class Message:
    id: int
    priority: int
    recipient: str
    body: str
    attempts: int = 0
    max_attempts: int = 5
    enqueued_at: float = field(default_factory=time.monotonic)

    def __post_init__(self):
        # Priority determines queue order, but under sustained heavy
        # overload that alone isn't enough: a billing message can burn
        # through the same fixed retry budget as an announcement and get
        # dead-lettered despite always jumping the queue. Load-testing at
        # 5x the server's real capacity surfaced exactly this - billing
        # messages dying with reason=429-burst after 5 attempts, same as
        # announcements. Giving billing a bigger retry budget means
        # priority protects against both wait time *and* permanent loss.
        if self.priority == BILLING:
            self.max_attempts = max(self.max_attempts, 10)


async def deliver(session, sem, limiter, msg, queue, stats):
    async with sem:
        await limiter.acquire()
        t0 = time.monotonic()
        label = "BILLING" if msg.priority == BILLING else "ANNOUNCE"
        try:
            async with session.post(
                API_URL,
                json={"id": msg.id, "body": msg.body},
                timeout=aiohttp.ClientTimeout(total=5),
            ) as resp:
                elapsed_ms = (time.monotonic() - t0) * 1000

                if resp.status == 200:
                    limiter.recover()
                    stats["delivered"] += 1
                    if msg.priority == BILLING and msg.attempts == 0:
                        wait_ms = (time.monotonic() - msg.enqueued_at) * 1000
                        log.info(f"OK    id={msg.id:<6} {label:<8} attempt=1 {elapsed_ms:.0f}ms  "
                                 f"(queue->delivered in {wait_ms:.0f}ms)")
                    else:
                        log.info(f"OK    id={msg.id:<6} {label:<8} attempt={msg.attempts+1} {elapsed_ms:.0f}ms")
                    return

                if resp.status == 429:
                    limiter.throttle()
                    retry_after = float(resp.headers.get("Retry-After", 0.5))
                    await _retry(msg, queue, stats, reason="429-burst", delay=retry_after)
                    return

                if resp.status == 503:
                    # "core app under heavy load" -> per Meta's guidance, halt
                    # and slow down harder than a plain 429.
                    limiter.throttle(factor=0.35)
                    await _retry(msg, queue, stats, reason="503-capacity", delay=None)
                    return

                if resp.status == 400:
                    payload = await resp.json()
                    code = payload.get("error", {}).get("code")
                    if code in THROTTLE_400_CODES:
                        limiter.throttle()
                        await _retry(msg, queue, stats, reason=f"400/{code}-throughput", delay=None)
                    else:
                        stats["dead_lettered"] += 1
                        stats["permanent_failure"] += 1
                        log.error(f"PERM  id={msg.id:<6} {label:<8} code={code} — permanent, not retrying")
                    return

                await _retry(msg, queue, stats, reason=str(resp.status), delay=None)

        except (asyncio.TimeoutError, aiohttp.ClientError) as e:
            await _retry(msg, queue, stats, reason=type(e).__name__, delay=None)


async def _retry(msg, queue, stats, reason, delay):
    msg.attempts += 1
    label = "BILLING" if msg.priority == BILLING else "ANNOUNCE"
    if msg.attempts >= msg.max_attempts:
        stats["dead_lettered"] += 1
        log.warning(f"DEAD  id={msg.id:<6} {label:<8} reason={reason} attempts={msg.attempts}")
        return

    backoff = delay if delay is not None else min(8.0, 0.25 * (2 ** msg.attempts))
    backoff += random.uniform(0, 0.2)
    stats["retried"] += 1
    log.info(f"RETRY id={msg.id:<6} {label:<8} reason={reason:<16} attempt={msg.attempts} wait={backoff:.2f}s")
    await asyncio.sleep(backoff)
    await queue.put((msg.priority, next(seq_counter), msg))


async def worker(name, queue, session, sem, limiter, stats):
    while True:
        priority, seq, msg = await queue.get()
        try:
            await deliver(session, sem, limiter, msg, queue, stats)
        finally:
            queue.task_done()


def make_batch(n_announcements=500):
    mid = itertools.count(1)
    msgs = [
        Message(id=next(mid), priority=ANNOUNCEMENT, recipient="user", body="Service update")
        for _ in range(n_announcements)
    ]
    random.shuffle(msgs)
    return msgs


async def billing_arrivals(queue, count=20, gap_range=(0.4, 1.4)):
    bid = itertools.count(1)
    for _ in range(count):
        await asyncio.sleep(random.uniform(*gap_range))
        ahead = queue.qsize()
        msg = Message(id=100000 + next(bid), priority=BILLING, recipient="urgent-user", body="Invoice/payment event")
        log.info(f"ARRIVE id={msg.id} BILLING  <- webhook arrives, {ahead} announcements waiting ahead of it")
        await queue.put((msg.priority, next(seq_counter), msg))


async def run_simulation(
    n_announcements=500,
    n_billing=20,
    max_concurrency=10,
    base_rate=25.0,
    billing_gap_range=(0.4, 1.4),
    start_own_server=True,
    server_port=8080,
):
    """
    Runs one full batch through the hardened pipeline and returns
    (stats, elapsed_seconds). Exposed as a function (not just a __main__
    block) so it's reusable if a test harness needs to call it repeatedly
    with different concurrency/rate settings without duplicating the wiring.
    """
    global API_URL
    API_URL = f"http://127.0.0.1:{server_port}/send"

    runner = await start_server(port=server_port) if start_own_server else None

    queue = asyncio.PriorityQueue()
    batch = make_batch(n_announcements)
    for m in batch:
        await queue.put((m.priority, next(seq_counter), m))
    log.info(f"Enqueued {len(batch)} announcements as backlog "
             f"(concurrency={max_concurrency}, target_rate={base_rate}/s)")

    sem = asyncio.Semaphore(max_concurrency)
    limiter = AdaptiveRateLimiter(rate=base_rate, capacity=base_rate)
    stats = {"delivered": 0, "retried": 0, "dead_lettered": 0, "permanent_failure": 0}

    async with aiohttp.ClientSession() as session:
        workers = [
            asyncio.create_task(worker(f"w{i}", queue, session, sem, limiter, stats))
            for i in range(max_concurrency)
        ]
        billing_task = asyncio.create_task(billing_arrivals(queue, count=n_billing, gap_range=billing_gap_range))

        t_start = time.monotonic()
        await billing_task
        await queue.join()
        elapsed = time.monotonic() - t_start

        for w in workers:
            w.cancel()
        await asyncio.gather(*workers, return_exceptions=True)

    if runner is not None:
        await runner.cleanup()

    log.info("---- SUMMARY ----")
    log.info(
        f"delivered={stats['delivered']} retried={stats['retried']} "
        f"dead_lettered={stats['dead_lettered']} permanent_failure={stats['permanent_failure']} "
        f"elapsed={elapsed:.2f}s"
    )
    return stats, elapsed


if __name__ == "__main__":
    asyncio.run(run_simulation())