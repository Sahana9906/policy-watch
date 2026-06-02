
from bs4 import BeautifulSoup


class ParsingError(Exception):
    """Raised when fetched content cannot be parsed."""


def is_pdf(content: bytes | str) -> bool:
    if isinstance(content, str):
        content = content.encode("utf-8", errors="ignore")
    return content.lstrip().startswith(b"%PDF")


def normalize_whitespace(text: str) -> str:
    return " ".join(text.split())


def parse_html_to_text(html_content: bytes | str) -> str:
    if isinstance(html_content, bytes):
        html_content = html_content.decode("utf-8", errors="replace")
    if not html_content:
        raise ParsingError("HTML content is empty.")

    soup = BeautifulSoup(html_content, "html.parser")
    for element in soup(["script", "style", "header", "footer", "nav", "aside", "form"]):
        element.decompose()

    text = normalize_whitespace(soup.get_text(separator=" ", strip=True))
    if not text:
        raise ParsingError("No visible text extracted from HTML.")
    return text


async def run_parsing_tool(html_content: bytes, encoding: str = "utf-8") -> str:
    return parse_html_to_text(html_content.decode(encoding, errors="replace"))
