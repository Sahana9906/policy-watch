import logging
from typing import Any

import httpx

logger = logging.getLogger(__name__)


class FetcherError(Exception):
    """Raised when regulation content cannot be fetched."""


async def fetch_url(source_url: str) -> dict[str, Any]:
    timeout = httpx.Timeout(10.0, connect=5.0)
    headers = {
        "User-Agent": "PolicyWatch/2.0 regulation monitor",
        "Accept": "text/html,application/xhtml+xml,application/pdf,*/*;q=0.8",
    }

    try:
        async with httpx.AsyncClient(
            timeout=timeout,
            follow_redirects=True,
            headers=headers,
        ) as client:
            response = await client.get(source_url)
            response.raise_for_status()
            return {
                "content": response.content,
                "content_type": response.headers.get("content-type"),
                "status_code": response.status_code,
                "final_url": str(response.url),
            }
    except httpx.HTTPError as exc:
        logger.exception("Failed to fetch regulation source_url=%s", source_url)
        raise FetcherError(f"Failed to fetch {source_url}: {exc}") from exc


async def fetch_regulation_content(source_url: str) -> dict[str, Any]:
    return await fetch_url(source_url)
