"""
Base scraper with shared HTTP session management and rate limiting.
"""

import logging
import time
from typing import Optional

import requests

from .. import config

logger = logging.getLogger(__name__)


class BaseScraper:
    """Base class for all scrapers with rate-limited HTTP requests."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                f"MarketingResearchAgent/1.0 "
                f"(mailto:{config.CROSSREF_MAILTO})"
            ),
        })
        self._last_request_time = 0.0

    def _rate_limited_get(
        self,
        url: str,
        params: Optional[dict] = None,
        headers: Optional[dict] = None,
        timeout: int = config.REQUEST_TIMEOUT,
    ) -> Optional[requests.Response]:
        """Make a GET request with rate limiting and error handling."""
        elapsed = time.time() - self._last_request_time
        if elapsed < config.REQUEST_DELAY:
            time.sleep(config.REQUEST_DELAY - elapsed)

        try:
            self._last_request_time = time.time()
            response = self.session.get(
                url, params=params, headers=headers, timeout=timeout
            )
            response.raise_for_status()
            return response
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 429:
                retry_after = int(e.response.headers.get("Retry-After", 5))
                logger.warning("Rate limited. Waiting %d seconds...", retry_after)
                time.sleep(retry_after)
                return self._rate_limited_get(url, params, headers, timeout)
            logger.error("HTTP error fetching %s: %s", url, e)
            return None
        except requests.exceptions.RequestException as e:
            logger.error("Request error fetching %s: %s", url, e)
            return None

    def close(self):
        """Close the HTTP session."""
        self.session.close()
