import logging
from typing import Dict, Any

import httpx

# Configure logger
logger = logging.getLogger(__name__)


class FetchToolError(Exception):
    """
    Base exception for errors occurring during the fetch process.
    """
    pass


async def fetch_regulation_content(source_url: str) -> Dict[str, Any]:
    """
    Asynchronously fetches content from a regulation source URL.

    Supports:
    - HTML detection
    - PDF detection
    - 10-second timeout
    - production-style error handling
    """

    # Timeout configuration
    timeout = httpx.Timeout(
        10.0,
        connect=5.0,
    )

    # Browser-like headers
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": (
            "text/html,"
            "application/xhtml+xml,"
            "application/xml;q=0.9,"
            "application/pdf,"
            "*/*;q=0.8"
        ),
    }

    async with httpx.AsyncClient(
        timeout=timeout,
        follow_redirects=True,
        headers=headers,
    ) as client:

        try:
            logger.info(f"Fetching URL: {source_url}")

            response = await client.get(source_url)

            # Validate response
            if response.status_code != 200:

                logger.error(
                    f"Fetch failed for {source_url} "
                    f"with status code {response.status_code}"
                )

                raise FetchToolError(
                    f"Request failed with status "
                    f"{response.status_code}"
                )

            # Detect content type
            content_type = response.headers.get(
                "Content-Type",
                "",
            ).lower()

            is_pdf = "application/pdf" in content_type

            is_html = (
                "text/html" in content_type
                or "application/xhtml+xml" in content_type
            )

            logger.info(
                f"Successfully fetched {source_url} "
                f"(Type: {content_type})"
            )

            return {
                "content": response.content,
                "content_type": content_type,
                "status_code": response.status_code,
                "is_pdf": is_pdf,
                "is_html": is_html,
            }

        except httpx.TimeoutException as e:

            logger.error(
                f"Timeout error while fetching "
                f"{source_url}: {str(e)}"
            )

            raise FetchToolError(
                f"Timeout fetching content from "
                f"{source_url}"
            ) from e

        except httpx.ConnectError as e:

            logger.error(
                f"Connection error while fetching "
                f"{source_url}: {str(e)}"
            )

            raise FetchToolError(
                f"Connection error fetching content from "
                f"{source_url}"
            ) from e

        except httpx.RequestError as e:

            logger.error(
                f"HTTPX request error while fetching "
                f"{source_url}: {str(e)}"
            )

            raise FetchToolError(
                f"An error occurred while requesting "
                f"{source_url}: {str(e)}"
            ) from e

        except Exception as e:

            logger.error(
                f"Unexpected error while fetching "
                f"{source_url}: {str(e)}"
            )

            raise FetchToolError(
                f"Unexpected error during fetch: {str(e)}"
            ) from e

