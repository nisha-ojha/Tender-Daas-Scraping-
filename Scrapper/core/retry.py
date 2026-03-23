"""
core/retry.py
-------------
Retry logic for flaky operations (page loads, API calls, etc.)

WHAT IS EXPONENTIAL BACKOFF?
  Attempt 1 fails → wait 2 seconds
  Attempt 2 fails → wait 4 seconds (2 × 2)
  Attempt 3 fails → wait 8 seconds (2 × 4)
  After max retries → give up and raise the error

WHY JITTER?
  If 5 scrapers all fail at the same time and retry after exactly 2 seconds,
  they all hit the server at once again. Adding a random 0.5–1.5s "jitter"
  spreads them out so the server doesn't get overwhelmed.
"""

import asyncio
import random
import time
import functools


async def retry_async(func, max_retries=3, base_delay=2, operation_name="operation"):
    """
    Run an async function with retry + exponential backoff.

    Args:
        func: An async function (no arguments) to execute
        max_retries: How many times to try before giving up
        base_delay: Starting delay in seconds (doubles each retry)
        operation_name: What to call this in log messages

    Returns:
        Whatever func() returns on success

    Raises:
        The last exception if all retries fail
    """
    last_error = None

    for attempt in range(max_retries):
        try:
            return await func()
        except Exception as e:
            last_error = e

            if attempt == max_retries - 1:
                # Last attempt — give up
                print(f"  [RETRY] {operation_name}: FAILED after {max_retries} attempts")
                raise

            # Calculate wait time: base_delay × 2^attempt + random jitter
            delay = base_delay * (2 ** attempt)
            jitter = random.uniform(0.5, 1.5)
            wait = delay + jitter

            print(f"  [RETRY] {operation_name}: Attempt {attempt + 1}/{max_retries} "
                  f"failed ({type(e).__name__}: {str(e)[:100]})")
            print(f"  [RETRY] Waiting {wait:.1f}s before next attempt...")

            await asyncio.sleep(wait)

    raise last_error  # Should never reach here, but just in case


def retry_sync(func, max_retries=3, base_delay=2, operation_name="operation"):
    """
    Same as retry_async but for regular (non-async) functions.
    Use this for database operations, file downloads, etc.
    """
    last_error = None

    for attempt in range(max_retries):
        try:
            return func()
        except Exception as e:
            last_error = e

            if attempt == max_retries - 1:
                print(f"  [RETRY] {operation_name}: FAILED after {max_retries} attempts")
                raise

            delay = base_delay * (2 ** attempt)
            jitter = random.uniform(0.5, 1.5)
            wait = delay + jitter

            print(f"  [RETRY] {operation_name}: Attempt {attempt + 1}/{max_retries} "
                  f"failed ({type(e).__name__}: {str(e)[:100]})")
            print(f"  [RETRY] Waiting {wait:.1f}s before next attempt...")

            time.sleep(wait)

    raise last_error


# ─── Quick self-test ─────────────────────────────────────────
if __name__ == "__main__":
    import asyncio

    print("Testing retry logic...")

    # Test 1: Function that fails twice then succeeds
    call_count = 0

    async def flaky_function():
        global call_count
        call_count += 1
        if call_count < 3:
            raise ConnectionError(f"Simulated failure #{call_count}")
        return "Success!"

    result = asyncio.run(
        retry_async(flaky_function, max_retries=3, base_delay=1, operation_name="test")
    )
    print(f"✓ Retry test PASSED! Result: {result} (took {call_count} attempts)")

    # Test 2: Function that always fails
    print("\nTesting max retry failure...")
    async def always_fails():
        raise TimeoutError("Server not responding")

    try:
        asyncio.run(
            retry_async(always_fails, max_retries=2, base_delay=0.5, operation_name="fail-test")
        )
    except TimeoutError:
        print("✓ Max retry test PASSED! (correctly gave up after 2 attempts)")
