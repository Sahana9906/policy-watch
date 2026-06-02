
import asyncio
import logging
import os
import tempfile

import google.generativeai as genai

logger = logging.getLogger(__name__)


class GeminiUploadError(Exception):
    """Raised when a PDF cannot be uploaded to Gemini."""


async def upload_pdf_to_gemini(pdf_bytes: bytes, filename: str) -> dict[str, object]:
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise GeminiUploadError("Missing GEMINI_API_KEY in environment variables.")
    if not pdf_bytes:
        raise GeminiUploadError("PDF content is empty.")

    genai.configure(api_key=api_key)
    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp_file:
            tmp_file.write(pdf_bytes)
            tmp_path = tmp_file.name

        file_meta = await asyncio.to_thread(
            genai.upload_file,
            path=tmp_path,
            display_name=filename,
            mime_type="application/pdf",
        )
        return {
            "file_uri": getattr(file_meta, "uri", None),
            "file_name": getattr(file_meta, "name", None),
            "display_name": getattr(file_meta, "display_name", filename),
        }
    except Exception as exc:
        logger.exception("Gemini PDF upload failed filename=%s", filename)
        raise GeminiUploadError(str(exc)) from exc
    finally:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except OSError:
                logger.warning("Failed to remove temporary Gemini upload file %s", tmp_path)


async def run_gemini_upload_tool(
    pdf_data: bytes,
    source_filename: str,
) -> dict[str, object]:
    return await upload_pdf_to_gemini(pdf_data, source_filename)
