"""Per-proxy rate limiting and circuit breaking for STF portal extraction.

Replaces the global ProxyPool + GlobalRateLimiter + per-extractor circuit
breaker with a single thread-safe component that tracks state per IP.
"""

from __future__ import annotations

import logging
import random
import threading
import time
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class _ProxyState:
    """Mutable state for a single proxy (or direct connection)."""

    proxy_url: str | None
    next_allowed_at: float = 0.0
    consecutive_403: int = 0
    circuit_open_until: float = 0.0
    total_requests: int = 0
    total_403s: int = 0


class ProxyManager:
    """Thread-safe per-proxy rate limiter, circuit breaker, and selector.

    Shared across all worker threads.  Each proxy has independent rate
    limiting and circuit breaker state so that a WAF block on one IP
    does not affect others.

    The ``acquire()`` method selects the proxy that has rested the
    longest, sleeps until its rate-limit slot opens, and returns the
    proxy URL for the caller to use.
    """

    def __init__(
        self,
        proxy_urls: list[str],
        per_proxy_rate: float = 1.0,
        jitter_range: tuple[float, float] = (0.8, 1.3),
        circuit_threshold: int = 5,
        circuit_cooldown: float = 120.0,
    ) -> None:
        self._per_proxy_rate = per_proxy_rate
        self._jitter_lo, self._jitter_hi = jitter_range
        self._circuit_threshold = circuit_threshold
        self._circuit_cooldown = circuit_cooldown
        self._lock = threading.Lock()
        # Direct connection (None) is always included
        self._proxies: list[_ProxyState] = [_ProxyState(proxy_url=None)]
        for url in proxy_urls:
            self._proxies.append(_ProxyState(proxy_url=url))
        self._index: dict[str | None, _ProxyState] = {s.proxy_url: s for s in self._proxies}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def acquire(self) -> tuple[str | None, float]:
        """Select the best proxy, wait for its rate-limit slot, return ``(proxy_url, delay)``.

        If all proxies have their circuit open, blocks until the soonest
        cooldown expires, then retries.
        """
        while True:
            with self._lock:
                now = time.monotonic()
                available = self._available_proxies(now)

                if not available:
                    soonest = min(s.circuit_open_until for s in self._proxies)
                    wait = max(0.0, soonest - now)
                    logger.warning(
                        "All %d proxies circuit-broken — waiting %.1fs for cooldown",
                        len(self._proxies),
                        wait,
                    )
                    # Release lock before sleeping
                else:
                    # Pick the proxy that has rested the longest
                    best = min(available, key=lambda s: s.next_allowed_at)
                    delay = max(0.0, best.next_allowed_at - now)
                    jitter = random.uniform(self._jitter_lo, self._jitter_hi)  # noqa: S311
                    best.next_allowed_at = max(now, best.next_allowed_at) + self._per_proxy_rate * jitter
                    best.total_requests += 1
                    proxy_url = best.proxy_url
                    # Release lock before sleeping
                    if delay > 0:
                        time.sleep(delay)
                    return proxy_url, delay

            # All broken — sleep outside lock, then retry
            time.sleep(wait)

    def record_success(self, proxy: str | None) -> None:
        """Reset consecutive 403 counter for the proxy."""
        with self._lock:
            state = self._index.get(proxy)
            if state:
                state.consecutive_403 = 0

    def record_403(self, proxy: str | None) -> None:
        """Increment 403 counter; open circuit if threshold reached."""
        with self._lock:
            state = self._index.get(proxy)
            if not state:
                return
            state.consecutive_403 += 1
            state.total_403s += 1
            if state.consecutive_403 >= self._circuit_threshold:
                state.circuit_open_until = time.monotonic() + self._circuit_cooldown
                logger.warning(
                    "Circuit open for proxy %s: %d consecutive 403s — cooldown %.0fs",
                    state.proxy_url or "direct",
                    state.consecutive_403,
                    self._circuit_cooldown,
                )

    def is_circuit_open(self, proxy: str | None) -> bool:
        """Check if a proxy's circuit breaker is currently open."""
        with self._lock:
            state = self._index.get(proxy)
            if not state:
                return False
            return time.monotonic() < state.circuit_open_until

    def __len__(self) -> int:
        return len(self._proxies)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _available_proxies(self, now: float) -> list[_ProxyState]:
        """Return proxies with closed circuit (or expired cooldown).

        Must be called while holding ``self._lock``.
        """
        available: list[_ProxyState] = []
        for state in self._proxies:
            if now >= state.circuit_open_until:
                # Cooldown expired — reset
                if state.consecutive_403 >= self._circuit_threshold:
                    state.consecutive_403 = 0
                    state.circuit_open_until = 0.0
                available.append(state)
        return available
