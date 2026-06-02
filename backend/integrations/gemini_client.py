import asyncio
import logging
import os

import google.generativeai as genai
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

DEFAULT_GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-1.5-flash")


class GeminiClientError(Exception):
    pass


class GeminiClient:
    def __init__(
        self,
        api_key: str | None = None,
        model_name: str = DEFAULT_GEMINI_MODEL,
    ):
        self.api_key = api_key or os.getenv("GEMINI_API_KEY")
        self.model_name = model_name

        if not self.api_key:
            raise GeminiClientError("GEMINI_API_KEY is not configured.")

        genai.configure(api_key=self.api_key)
        self.model = genai.GenerativeModel(self.model_name)

    async def generate_text(self, prompt: str) -> str:
        try:
            response = await asyncio.to_thread(
                self.model.generate_content,
                prompt,
            )
            text = getattr(response, "text", None)
            if not text:
                raise GeminiClientError("Gemini returned an empty response.")
            return text
        except Exception as exc:
            logger.exception("Gemini text generation failed.")
            raise GeminiClientError(str(exc)) from exc

