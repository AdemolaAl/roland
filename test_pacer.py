"""Standalone test for SendPacer (no pytest dependency).

Run: python3 test_pacer.py
"""

import asyncio
import time

from pacer import SendPacer


async def _concurrent_spacing():
    """10 concurrent callers must be released spaced by >= min_interval."""
    interval = 0.05
    pacer = SendPacer(interval)
    release_times = []

    async def caller():
        await pacer.wait()
        release_times.append(time.monotonic())

    start = time.monotonic()
    await asyncio.gather(*(caller() for _ in range(10)))

    release_times.sort()
    gaps = [b - a for a, b in zip(release_times, release_times[1:])]

    # Every consecutive pair must be spaced by at least the interval
    # (small scheduler slop tolerated).
    slop = 0.005
    for i, gap in enumerate(gaps):
        assert gap >= interval - slop, f"gap {i}={gap:.4f}s < interval {interval}s"

    # Total wall time for 10 callers should be ~9 intervals, NOT ~0
    # (which is what the old lock-less burst produced).
    total = release_times[-1] - start
    assert total >= 9 * interval - slop, f"total {total:.4f}s too short — no real pacing"
    print(f"  concurrent_spacing OK (10 callers in {total:.3f}s, min gap {min(gaps):.4f}s)")


async def _zero_interval_is_noop():
    pacer = SendPacer(0)
    start = time.monotonic()
    await asyncio.gather(*(pacer.wait() for _ in range(50)))
    assert time.monotonic() - start < 0.05
    print("  zero_interval_is_noop OK")


async def main():
    await _concurrent_spacing()
    await _zero_interval_is_noop()
    print("All pacer tests passed.")


if __name__ == "__main__":
    asyncio.run(main())
