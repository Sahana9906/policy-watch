from urllib.parse import urlparse


ALLOWED_URL_SCHEMES = {"http", "https"}


class UrlValidationError(ValueError):
    pass


def validate_source_url(source_url: str) -> str:
    parsed = urlparse(source_url.strip())

    if parsed.scheme not in ALLOWED_URL_SCHEMES:
        raise UrlValidationError("URL must use http or https.")

    if not parsed.netloc:
        raise UrlValidationError("URL must include a valid host.")

    return source_url.strip()

