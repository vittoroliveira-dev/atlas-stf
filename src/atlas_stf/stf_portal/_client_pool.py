"""Thread-safe httpx.Client pool with deferred close for STF portal extraction.

Invariants:
- ``_lock`` protects ALL mutations to client caches and ``_retired`` list.
- Rotation retires (not closes) clients — deferred close prevents mid-request destruction.
- ``close()`` is the ONLY method that destroys retired clients.
"""

from __future__ import annotations

import random
import threading

import httpx

from ._http import BROWSER_HEADERS, USER_AGENTS


class ClientPool:
    """Manages httpx.Client lifecycle: creation, caching, rotation, and shutdown.

    Thread-safe.  All cache mutations are serialized via a single lock.
    Retired clients are kept alive until ``close()`` to avoid destroying
    a client that another thread is still using mid-request.
    """

    def __init__(self, *, timeout: float, ignore_tls: bool) -> None:
        self._timeout = timeout
        self._ignore_tls = ignore_tls
        self._lock = threading.Lock()
        self._client: httpx.Client | None = None
        self._nofollow_client: httpx.Client | None = None
        self._proxy_clients: dict[str | None, httpx.Client] = {}
        self._proxy_nofollow_clients: dict[str | None, httpx.Client] = {}
        self._retired: list[httpx.Client] = []

    # --- Public: client resolution (must hold lock) ---

    def pick_user_agent(self) -> str:
        return random.choice(USER_AGENTS)  # noqa: S311

    def get_client(self) -> httpx.Client:
        """Get or create default client. Must be called while holding lock."""
        if self._client is None:
            headers = {**BROWSER_HEADERS, "User-Agent": self.pick_user_agent()}
            self._client = httpx.Client(
                timeout=self._timeout,
                follow_redirects=True,
                headers=headers,
                verify=not self._ignore_tls,
            )
        return self._client

    def get_nofollow_client(self) -> httpx.Client:
        """Get or create no-redirect client. Must be called while holding lock."""
        if self._nofollow_client is None:
            headers = {**BROWSER_HEADERS, "User-Agent": self.pick_user_agent()}
            self._nofollow_client = httpx.Client(
                timeout=self._timeout,
                follow_redirects=False,
                headers=headers,
                verify=not self._ignore_tls,
            )
        return self._nofollow_client

    def get_client_for_proxy(self, proxy: str | None, *, follow_redirects: bool = True) -> httpx.Client:
        """Get or create client for a proxy. Must be called while holding lock."""
        cache = self._proxy_clients if follow_redirects else self._proxy_nofollow_clients
        if proxy in cache:
            return cache[proxy]
        headers = {**BROWSER_HEADERS, "User-Agent": self.pick_user_agent()}
        client = httpx.Client(
            timeout=self._timeout,
            follow_redirects=follow_redirects,
            headers=headers,
            verify=not self._ignore_tls,
            proxy=proxy,
        )
        cache[proxy] = client
        return client

    def resolve(self, proxy: str | None, *, follow_redirects: bool = True, force_new: bool = False) -> httpx.Client:
        """Resolve the right client. Must be called while holding lock."""
        if proxy:
            if force_new:
                old = self._proxy_clients.pop(proxy, None)
                if old:
                    self._retired.append(old)
                old_nf = self._proxy_nofollow_clients.pop(proxy, None)
                if old_nf:
                    self._retired.append(old_nf)
            return self.get_client_for_proxy(proxy, follow_redirects=follow_redirects)
        if follow_redirects:
            if force_new:
                if self._client:
                    self._retired.append(self._client)
                self._client = None
            return self.get_client()
        if force_new:
            if self._nofollow_client:
                self._retired.append(self._nofollow_client)
            self._nofollow_client = None
        return self.get_nofollow_client()

    # --- Public: rotation (acquires lock) ---

    def rotate_for_proxy(self, proxy: str | None) -> None:
        """Retire cached clients for a single proxy (deferred close)."""
        with self._lock:
            if proxy is None:
                if self._client:
                    self._retired.append(self._client)
                    self._client = None
                if self._nofollow_client:
                    self._retired.append(self._nofollow_client)
                    self._nofollow_client = None
            else:
                old = self._proxy_clients.pop(proxy, None)
                if old:
                    self._retired.append(old)
                old_nf = self._proxy_nofollow_clients.pop(proxy, None)
                if old_nf:
                    self._retired.append(old_nf)

    # --- Lifecycle ---

    def close(self) -> None:
        """Close all active and retired clients. Call once at shutdown."""
        if self._client:
            self._client.close()
            self._client = None
        if self._nofollow_client:
            self._nofollow_client.close()
            self._nofollow_client = None
        for c in self._proxy_clients.values():
            c.close()
        self._proxy_clients.clear()
        for c in self._proxy_nofollow_clients.values():
            c.close()
        self._proxy_nofollow_clients.clear()
        for c in self._retired:
            c.close()
        self._retired.clear()
