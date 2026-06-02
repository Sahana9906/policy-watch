import hashlib
import logging
from typing import Union

# Configure logger
logger = logging.getLogger(__name__)


class HashToolError(Exception):
    """
    Base exception for errors occurring during the hashing process.
    """
    pass


def generate_sha256_hash(
    content: Union[str, bytes],
    encoding: str = "utf-8",
) -> str:
    """
    Generates a deterministic SHA-256 hexadecimal hash
    for the provided content.
    """

    try:

        if isinstance(content, str):

            logger.debug(
                "Hashing string content after encoding."
            )

            data_bytes = content.encode(encoding)

        elif isinstance(content, bytes):

            logger.debug(
                "Hashing raw byte content."
            )

            data_bytes = content

        else:

            logger.error(
                f"Unsupported content type for hashing: "
                f"{type(content)}"
            )

            raise HashToolError(
                f"Cannot hash object of type {type(content)}"
            )

        hash_object = hashlib.sha256(data_bytes)

        hex_digest = hash_object.hexdigest()

        logger.info(
            "Successfully generated SHA-256 hash."
        )

        return hex_digest

    except (UnicodeEncodeError, AttributeError) as e:

        logger.error(
            f"Encoding error during hashing: {str(e)}"
        )

        raise HashToolError(
            f"Failed to encode content for hashing: {str(e)}"
        ) from e

    except Exception as e:

        logger.error(
            f"Unexpected error during hashing process: {str(e)}"
        )

        raise HashToolError(
            f"Unexpected hashing failure: {str(e)}"
        ) from e


def hash_regulation_data(
    data: Union[str, bytes],
) -> str:
    """
    ADK-compatible wrapper for generating
    a regulation content hash.
    """

    return generate_sha256_hash(data)