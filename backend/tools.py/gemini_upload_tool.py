
import os
import logging
import asyncio
import tempfile

from typing import Dict, Any

from dotenv import load_dotenv

import google.generativeai as genai

# Load environment variables
load_dotenv()

# Configure logger
logger = logging.getLogger(__name__)


class GeminiUploadError(Exception):
    """
    Base exception for Gemini upload errors.
    """
    pass


async def upload_pdf_to_gemini(
    pdf_bytes: bytes,
    filename: str,
) -> Dict[str, Any]:
    """
    Uploads raw PDF bytes to Gemini File API.
    """

    api_key = os.getenv("GEMINI_API_KEY")

    if not api_key:

        logger.error(
            "GEMINI_API_KEY not found in environment."
        )

        raise GeminiUploadError(
            "Missing GEMINI_API_KEY in environment variables."
        )

    # Configure Gemini SDK
    genai.configure(api_key=api_key)

    # Validate PDF
    if not pdf_bytes or len(pdf_bytes) < 4:

        raise GeminiUploadError(
            "PDF content is empty or invalid."
        )

    if not pdf_bytes.startswith(b"%PDF"):

        logger.warning(
            f"File {filename} does not contain a standard PDF header."
        )

    tmp_path = None

    try:
        # Create temporary PDF file
        with tempfile.NamedTemporaryFile(
            delete=False,
            suffix=".pdf",
        ) as tmp_file:

            tmp_file.write(pdf_bytes)

            tmp_path = tmp_file.name

        logger.info(
            f"Uploading {filename} to Gemini File API..."
        )

        # Run blocking upload in thread
        file_meta = await asyncio.to_thread(
            genai.upload_file,
            path=tmp_path,
            display_name=filename,
            mime_type="application/pdf",
        )

        logger.info(
            f"Upload successful. File URI: {file_meta.uri}"
        )

        return {
            "file_uri": file_meta.uri,
            "file_name": file_meta.name,
            "display_name": file_meta.display_name,
            "mime_type": file_meta.mime_type,
            "upload_metadata": {
                "name": file_meta.name,
                "uri": file_meta.uri,
                "create_time": getattr(
                    file_meta,
                    "create_time",
                    None,
                ),
                "expiration_time": getattr(
                    file_meta,
                    "expiration_time",
                    None,
                ),
            },
        }

    except Exception as e:

        logger.error(
            f"Failed to upload {filename} to Gemini: {str(e)}"
        )

        raise GeminiUploadError(
            f"Gemini API upload failed: {str(e)}"
        ) from e

    finally:
        # Cleanup temporary file
        if tmp_path and os.path.exists(tmp_path):

            try:
                os.remove(tmp_path)

            except OSError as e:

                logger.warning(
                    f"Failed to remove temporary file "
                    f"{tmp_path}: {e}"
                )


async def run_gemini_upload_tool(
    pdf_data: bytes,
    source_filename: str,
) -> Dict[str, Any]:
    """
    ADK-compatible wrapper entrypoint.
    """

    return await upload_pdf_to_gemini(
        pdf_data,
        source_filename,
    )