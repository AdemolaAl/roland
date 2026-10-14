"""Global outbound send pacer for the Telegram bot.

The old `rate_limited_send` enforced a min-interval using a single shared
timestamp WITHOUT a lock. Under concurrency (25 dispatcher workers + the v2
scheduler all sending at once) every coroutine read the same `last_call`,
slept the same amount, then fired simultaneously — so the pacing did nothing
and bursts tripped Telegram's flood limits (FLOOD_WAIT → 429 transport flood).

`SendPacer` fixes that: acquisition is serialized by an asyncio.Lock, so N
concurrent callers are released one-at-a-time, each spaced by `min_interval`.
That converts uncontrolled bursts into a steady aggregate rate shared by every
sender in the process (onboarding AND the scheduler), keeping us under the
flood threshold.
"""

import asyncio
import time


class SendPacer:
    """Spaces awaited sends to at most one per `min_interval` seconds,
    enforced atomically across all concurrent callers."""

    def __init__(self, min_interval: float):
        if min_interval < 0:
            raise ValueError("min_interval must be >= 0")
        self.min_interval = min_interval
        self._lock = asyncio.Lock()
        self._last = 0.0  # monotonic timestamp of the last release

    async def wait(self):
        """Block until enough time has elapsed since the previous caller was
        released. Callers queue on the lock, so they are spaced — not released
        in a thundering herd."""
        async with self._lock:
            now = time.monotonic()
            delta = now - self._last
            if delta < self.min_interval:
                await asyncio.sleep(self.min_interval - delta)
            self._last = time.monotonic()
