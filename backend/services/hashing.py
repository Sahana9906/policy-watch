import hashlib
import logging
from typing import Union

logger = logging.getLogger(__name__)


class HashingError(Exception):
    """Raised when content cannot be hashed."""


def generate_sha256_hash(
    content: Union[str, bytes],
    encoding: str = "utf-8",
) -> str:
    if isinstance(content, str):
        data = content.encode(encoding)
    elif isinstance(content, bytes):
        data = content
    else:
        raise HashingError(f"Unsupported content type for hashing: {type(content)}")

    return hashlib.sha256(data).hexdigest()


def hash_regulation_data(content: Union[str, bytes]) -> str:
    return generate_sha256_hash(content)


def compute_sha256(content: Union[str, bytes]) -> str:
    return generate_sha256_hash(content)
