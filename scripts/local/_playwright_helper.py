"""
Shared Playwright helper for Method-6 (browser automation) ingests.
================================================================

Background
----------
Most funder sites publish their grantee/fellow data as either static HTML,
WP REST, Drupal JSON:API, or a simple JSON XHR — covered by Methods 1-5 on
the runbook ladder. A small but high-value cohort sits behind one of:

  - Cloudflare JS challenge / Turnstile (e.g. BWF, Sloan, Klingenstein
    until 2026-05-27 — confirmed bypassed by this helper)
  - Client-side React/Next.js pagination (Mellon, Spencer)
  - Dynamic Server Action IDs that aren't reachable from outside the
    React runtime (Mellon's grant database)

For those, this helper wraps `playwright.sync_api` with three project
conventions baked in: per-request polite throttle, stealth-mode UA + nav
headers, and a context manager that always cleans up the browser. Subclass
or import this from any `scripts/local/{funder}_to_s3.py` that needs
Method 6.

Usage
-----
    from _playwright_helper import PlaywrightSession

    with PlaywrightSession(min_interval_s=0.4) as session:
        html = session.fetch("https://example.org/grantees/")
        # Or, for SPA pages where you need to wait for hydration:
        html = session.fetch(
            "https://example.org/grantees/",
            wait_for_selector="article.grantee-card",
            wait_for_selector_timeout_ms=10000,
        )

Requirements
------------
    pip install playwright playwright-stealth
    playwright install chromium

Maintainer notes
----------------
- First Method-6 contractor ingest on the project: Klingenstein-Simons
  Fellowship in Neuroscience (priority 147, 2026-05-27). Any future
  Method-6 ingest should import this module rather than re-implementing
  the stealth boilerplate.
- The throttle is enforced inside this helper, not by the caller. Callers
  just call `fetch()` and the helper sleeps as needed between requests.
- `playwright-stealth`'s API changed between 1.x and 2.x. This helper
  handles both — the `Stealth().apply_stealth_sync(context)` form (>=2.x)
  is preferred; the older `stealth_sync(page)` form is a fallback.
"""

from __future__ import annotations

import time
from typing import Optional

from playwright.sync_api import sync_playwright, Page, Response

# Default UA — a recent macOS Chrome string. The point is to look like a
# real browser; the actual version isn't critical and is auto-updated by
# the bundled Chromium binary anyway.
_DEFAULT_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


class PlaywrightSession:
    """Context manager that owns a single browser + page across many requests.

    Methods
    -------
    fetch(url, **kwargs) -> str
        HTTP-style: navigate to `url`, return rendered HTML after JS hydration.

    fetch_with_response(url, **kwargs) -> tuple[Optional[Response], str]
        Same but also returns the goto response object (status, etc.).

    recreate_context() -> Page
        Close the live context and open a fresh stealth context+page. Used by
        callers fighting Cloudflare challenge escalation that hard-sticks on a
        reused context (call it before each page / retry).
    """

    def __init__(
        self,
        *,
        min_interval_s: float = 0.5,
        user_agent: str = _DEFAULT_UA,
        viewport: Optional[dict] = None,
        headless: bool = True,
    ) -> None:
        self._min_interval_s = min_interval_s
        self._user_agent = user_agent
        self._viewport = viewport or {"width": 1280, "height": 800}
        self._headless = headless
        self._last_request_t = 0.0
        # Filled in __enter__
        self._pw_cm = None
        self._pw = None
        self._browser = None
        self._context = None
        self._page = None

    # ----- context manager -------------------------------------------------

    def __enter__(self) -> "PlaywrightSession":
        self._pw_cm = sync_playwright()
        self._pw = self._pw_cm.__enter__()
        self._browser = self._pw.chromium.launch(headless=self._headless)
        # Resolve the stealth API once. The modern playwright-stealth >=2.x
        # `Stealth().apply_stealth_sync(context)` form is preferred; the older
        # `stealth_sync(page)` form is the 1.x fallback. We hold the resolved
        # callable so `_make_context_and_page` can re-apply it on every fresh
        # context (see `recreate_context`).
        self._stealth = None
        self._legacy_stealth = None
        try:
            from playwright_stealth import Stealth  # type: ignore
            self._stealth = Stealth()
        except ImportError:
            try:
                from playwright_stealth import stealth_sync  # type: ignore
                self._legacy_stealth = stealth_sync
            except ImportError:
                pass
        self._make_context_and_page()
        return self

    def _make_context_and_page(self) -> None:
        """Create a fresh stealth context + page on the live browser."""
        self._context = self._browser.new_context(
            user_agent=self._user_agent,
            viewport=self._viewport,
        )
        if self._stealth is not None:
            self._stealth.apply_stealth_sync(self._context)
        self._page = self._context.new_page()
        if self._legacy_stealth:
            self._legacy_stealth(self._page)

    def recreate_context(self) -> "Page":
        """Close the current context and spin up a brand-new stealth context+page.

        Some Cloudflare deployments (sloan.org) let the *first* navigation on a
        fresh context clear the JS challenge, then hard-stick the challenge on
        every subsequent navigation that reuses the same context. Recreating the
        context per page defeats that escalation: each page (and each retry) gets
        a clean context whose first navigation clears. Returns the new page so
        callers can `evaluate(...)` against it directly.
        """
        if self._context is not None:
            try:
                self._context.close()
            except Exception:
                pass
        self._make_context_and_page()
        self._last_request_t = 0.0
        return self._page

    def __exit__(self, exc_type, exc, tb) -> None:
        try:
            if self._browser:
                self._browser.close()
        finally:
            if self._pw_cm:
                self._pw_cm.__exit__(exc_type, exc, tb)

    # ----- fetch -----------------------------------------------------------

    def _throttle(self) -> None:
        elapsed = time.monotonic() - self._last_request_t
        if elapsed < self._min_interval_s:
            time.sleep(self._min_interval_s - elapsed)

    def fetch_with_response(
        self,
        url: str,
        *,
        wait_until: str = "domcontentloaded",
        timeout_ms: int = 30000,
        wait_for_selector: Optional[str] = None,
        wait_for_selector_timeout_ms: int = 10000,
    ) -> tuple[Optional[Response], str]:
        if self._page is None:
            raise RuntimeError("use as a context manager (`with PlaywrightSession() as s:`)")
        self._throttle()
        resp = self._page.goto(url, wait_until=wait_until, timeout=timeout_ms)
        if wait_for_selector:
            self._page.wait_for_selector(wait_for_selector, timeout=wait_for_selector_timeout_ms)
        self._last_request_t = time.monotonic()
        return resp, self._page.content()

    def fetch(self, url: str, **kwargs) -> str:
        _, html = self.fetch_with_response(url, **kwargs)
        return html

    # Optional escape hatch for callers that need direct page access
    # (e.g. for `evaluate(js)` or `inner_text(selector)`).
    @property
    def page(self) -> Page:
        if self._page is None:
            raise RuntimeError("use as a context manager")
        return self._page
