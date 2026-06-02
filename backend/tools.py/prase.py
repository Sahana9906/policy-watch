import logging
from typing import Optional

from bs4 import BeautifulSoup

# Configure logger
logger = logging.getLogger(__name__)


class ParsingError(Exception):
    """
    Base exception for errors occurring during the parsing process.
    """
    pass


def normalize_whitespace(text: str) -> str:
    """
    Removes redundant whitespace, newlines,
    and tabs from the text.
    """

    return " ".join(text.split())


def parse_html_to_text(html_content: str) -> str:
    """
    Parses HTML content and extracts
    visible regulation body text.
    """

    if not html_content:

        logger.error(
            "Received empty HTML content for parsing."
        )

        raise ParsingError(
            "HTML content is empty."
        )

    try:
        logger.info(
            "Initializing BeautifulSoup parsing."
        )

        soup = BeautifulSoup(
            html_content,
            "lxml",
        )

        # Remove non-visible elements
        for element in soup(
            [
                "script",
                "style",
                "header",
                "footer",
                "nav",
                "aside",
                "form",
            ]
        ):
            element.decompose()

        # Extract visible text
        raw_text = soup.get_text(
            separator=" ",
            strip=True,
        )

        if not raw_text:

            logger.warning(
                "No text content extracted from HTML."
            )

            return ""

        # Normalize extracted text
        normalized_text = normalize_whitespace(
            raw_text
        )

        logger.info(
            f"Successfully parsed HTML. "
            f"Extracted {len(normalized_text)} characters."
        )

        return normalized_text

    except Exception as e:

        logger.error(
            f"Unexpected error during HTML parsing: {str(e)}"
        )

        raise ParsingError(
            f"Failed to parse HTML content: {str(e)}"
        ) from e


async def run_parsing_tool(
    html_content: bytes,
    encoding: str = "utf-8",
) -> str:
    """
    Async-compatible parsing tool entrypoint.
    """

    try:
        decoded_html = html_content.decode(
            encoding,
            errors="replace",
        )

        return parse_html_to_text(decoded_html)

    except Exception as e:

        logger.error(
            f"Failed to decode or parse content: {str(e)}"
        )

        raise ParsingError(
            f"Tool execution failed: {str(e)}"
        ) from e