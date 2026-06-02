import asyncio
import importlib
import logging
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Callable

from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from core.workflow_states import INITIAL_STAGE
from db import queries
from db.models import Regulation, RegulationSnapshot
from services.workflow_service import WorkflowService

logger = logging.getLogger(__name__)

DEFAULT_FETCH_TIMEOUT_SECONDS = 30
RETRY_ATTEMPTS = 2
RETRY_DELAY_SECONDS = 0.5


class ContentKind(StrEnum):
    HTML = "html"
    PDF = "pdf"


class IngestionStatus(StrEnum):
    CHANGED = "changed"
    NO_CHANGE = "no_change"
    FAILED = "failed"


class IngestionServiceError(Exception):
    """Base exception for ingestion failures."""


class FetchError(IngestionServiceError):
    """Raised when source content cannot be fetched."""


class ContentDetectionError(IngestionServiceError):
    """Raised when content cannot be classified as HTML or PDF."""


class ParseError(IngestionServiceError):
    """Raised when HTML/PDF parsing coordination fails."""


@dataclass(slots=True)
class FetchResult:
    content: bytes
    status_code: int
    content_type: str | None = None
    final_url: str | None = None


@dataclass(slots=True)
class ParsedRegulationContent:
    content_kind: ContentKind
    content_hash: str
    content_text: str | None = None
    gemini_file_id: str | None = None


@dataclass(slots=True)
class IngestionResult:
    status: IngestionStatus
    regulation_id: int
    content_hash: str
    previous_hash: str | None
    snapshot_id: int | None = None
    workflow_run_id: int | None = None
    content_kind: ContentKind | None = None
    message: str | None = None


def _import_first(module_names: tuple[str, ...]) -> Any:
    last_error: Exception | None = None
    for module_name in module_names:
        try:
            return importlib.import_module(module_name)
        except Exception as exc:
            last_error = exc

    raise ImportError(
        f"Could not import any of: {', '.join(module_names)}"
    ) from last_error


fetch_tool = _import_first(
    (
        "tools.fetch_tool",
        "tools.fetch",
        "services.fetcher",
    )
)
parse_tool = _import_first(
    (
        "tools.parse",
        "tools.prase",
        "services.parser",
    )
)
hash_tool = _import_first(
    (
        "tools.hash_tool",
        "services.hashing",
    )
)
gemini_upload_tool = _import_first(
    (
        "tools.gemini_upload_tool",
        "services.gemini_service",
    )
)


def _callable_from(module: Any, names: tuple[str, ...]) -> Callable[..., Any]:
    for name in names:
        value = getattr(module, name, None)
        if callable(value):
            return value

    raise AttributeError(
        f"{module.__name__} does not expose any of: {', '.join(names)}"
    )


async def _maybe_await(value: Any) -> Any:
    if asyncio.iscoroutine(value):
        return await value
    return value


async def _retry_async(
    operation_name: str,
    func: Callable[[], Any],
    attempts: int = RETRY_ATTEMPTS,
    delay_seconds: float = RETRY_DELAY_SECONDS,
) -> Any:
    last_error: Exception | None = None

    for attempt in range(1, attempts + 1):
        try:
            return await _maybe_await(func())
        except Exception as exc:
            last_error = exc
            logger.warning(
                "%s failed attempt=%s/%s error=%s",
                operation_name,
                attempt,
                attempts,
                exc,
            )
            if attempt < attempts:
                await asyncio.sleep(delay_seconds)

    raise last_error or IngestionServiceError(f"{operation_name} failed")


def _to_bytes(content: Any) -> bytes:
    if isinstance(content, bytes):
        return content
    if isinstance(content, bytearray):
        return bytes(content)
    if isinstance(content, str):
        return content.encode("utf-8")
    raise FetchError(f"Unsupported fetched content type: {type(content)}")


def _normalize_fetch_result(raw_result: Any) -> FetchResult:
    if isinstance(raw_result, FetchResult):
        return raw_result

    if isinstance(raw_result, bytes):
        return FetchResult(content=raw_result, status_code=200)

    if isinstance(raw_result, str):
        return FetchResult(content=raw_result.encode("utf-8"), status_code=200)

    if isinstance(raw_result, dict):
        headers = raw_result.get("headers") or {}
        content_type = raw_result.get("content_type")
        if not content_type and isinstance(headers, dict):
            content_type = headers.get("content-type") or headers.get("Content-Type")

        return FetchResult(
            content=_to_bytes(
                raw_result.get("content")
                or raw_result.get("body")
                or raw_result.get("data")
                or b""
            ),
            status_code=int(
                raw_result.get("status_code")
                or raw_result.get("status")
                or 200
            ),
            content_type=content_type,
            final_url=raw_result.get("final_url") or raw_result.get("url"),
        )

    headers = getattr(raw_result, "headers", {}) or {}
    content_type = None
    if isinstance(headers, dict):
        content_type = headers.get("content-type") or headers.get("Content-Type")

    content = getattr(raw_result, "content", None)
    if content is None:
        content = getattr(raw_result, "text", "")

    return FetchResult(
        content=_to_bytes(content),
        status_code=int(getattr(raw_result, "status_code", 200)),
        content_type=content_type,
        final_url=str(getattr(raw_result, "url", "") or "") or None,
    )


class IngestionService:
    def __init__(self, session: AsyncSession):
        self.session = session
        self.workflow_service = WorkflowService(session)

    async def ingest_url(
        self,
        regulation_id: int,
        source_url: str,
        scan_id: int | None = None,
    ) -> IngestionResult:
        logger.info(
            "Starting URL ingestion regulation_id=%s source_url=%s",
            regulation_id,
            source_url,
        )

        fetch_result = await self.fetch_regulation_content(source_url)
        return await self._ingest_fetch_result(
            regulation_id=regulation_id,
            fetch_result=fetch_result,
            source_name=source_url,
            scan_id=scan_id,
        )

    async def ingest_pdf(
        self,
        regulation_id: int,
        pdf_bytes: bytes,
        source_name: str = "uploaded_regulation.pdf",
    ) -> IngestionResult:
        logger.info(
            "Starting PDF ingestion regulation_id=%s source_name=%s",
            regulation_id,
            source_name,
        )

        fetch_result = FetchResult(
            content=pdf_bytes,
            status_code=200,
            content_type="application/pdf",
            final_url=source_name,
        )
        return await self._ingest_fetch_result(
            regulation_id=regulation_id,
            fetch_result=fetch_result,
            source_name=source_name,
            scan_id=None,
        )

    async def process_scan_job(self, scan_job: Any) -> IngestionResult:
        regulation_id = getattr(scan_job, "regulation_id", None)
        if regulation_id is None:
            raise IngestionServiceError("Scan job is missing regulation_id.")

        source_url = getattr(scan_job, "source_url", None)
        if not source_url:
            regulation = await queries.get_regulation(self.session, regulation_id)
            source_url = getattr(regulation, "url", None) if regulation else None

        if not source_url:
            raise IngestionServiceError(
                f"Unable to resolve source URL for scan_job_id={getattr(scan_job, 'id', None)}"
            )

        return await self.ingest_url(
            regulation_id,
            source_url,
            scan_id=getattr(scan_job, "id", None),
        )

    async def fetch_regulation_content(self, source_url: str) -> FetchResult:
        fetch_func = _callable_from(
            fetch_tool,
            (
                "run_fetch_tool",
                "fetch_url",
                "fetch",
                "fetch_content",
            ),
        )

        try:
            raw_result = await asyncio.wait_for(
                _retry_async(
                    "fetch_regulation_content",
                    lambda: fetch_func(source_url),
                ),
                timeout=DEFAULT_FETCH_TIMEOUT_SECONDS,
            )
        except asyncio.TimeoutError as exc:
            raise FetchError(
                f"Timed out fetching {source_url} after "
                f"{DEFAULT_FETCH_TIMEOUT_SECONDS} seconds."
            ) from exc
        except Exception as exc:
            raise FetchError(f"Failed to fetch {source_url}: {exc}") from exc

        fetch_result = _normalize_fetch_result(raw_result)
        if fetch_result.status_code < 200 or fetch_result.status_code >= 300:
            raise FetchError(
                f"Non-success response for {source_url}: "
                f"HTTP {fetch_result.status_code}"
            )
        if not fetch_result.content:
            raise FetchError(f"Empty response body for {source_url}")

        return fetch_result

    async def _ingest_fetch_result(
        self,
        regulation_id: int,
        fetch_result: FetchResult,
        source_name: str,
        scan_id: int | None = None,
    ) -> IngestionResult:
        regulation = await queries.get_regulation(self.session, regulation_id)
        if regulation is None:
            raise IngestionServiceError(
                f"Regulation not found: regulation_id={regulation_id}"
            )

        parsed_content = await self.parse_content(fetch_result, source_name)
        latest_snapshot = await self.get_latest_snapshot(regulation_id)
        previous_hash = (
            latest_snapshot.content_hash
            if latest_snapshot is not None
            else getattr(regulation, "last_hash", None)
        )

        if previous_hash == parsed_content.content_hash:
            logger.info(
                "No regulation change detected regulation_id=%s hash=%s",
                regulation_id,
                parsed_content.content_hash,
            )
            return IngestionResult(
                status=IngestionStatus.NO_CHANGE,
                regulation_id=regulation_id,
                content_hash=parsed_content.content_hash,
                previous_hash=previous_hash,
                content_kind=parsed_content.content_kind,
                message="No change detected.",
            )

        return await self._record_detected_change(
            regulation=regulation,
            parsed_content=parsed_content,
            previous_hash=previous_hash,
            scan_id=scan_id,
        )

    async def parse_content(
        self,
        fetch_result: FetchResult,
        source_name: str,
    ) -> ParsedRegulationContent:
        content_kind = self.detect_content_kind(fetch_result)

        if content_kind == ContentKind.PDF:
            return await self._parse_pdf(fetch_result.content, source_name)

        return await self._parse_html(fetch_result.content)

    def detect_content_kind(self, fetch_result: FetchResult) -> ContentKind:
        content_type = (fetch_result.content_type or "").lower()
        content = fetch_result.content.lstrip()

        if "application/pdf" in content_type or content.startswith(b"%PDF"):
            return ContentKind.PDF

        is_pdf = getattr(parse_tool, "is_pdf", None)
        if callable(is_pdf) and is_pdf(fetch_result.content):
            return ContentKind.PDF

        if (
            "text/html" in content_type
            or content.startswith(b"<!doctype html")
            or content.startswith(b"<html")
            or b"<body" in content[:2048].lower()
        ):
            return ContentKind.HTML

        raise ContentDetectionError("Content is neither PDF nor HTML.")

    async def _parse_html(self, html_bytes: bytes) -> ParsedRegulationContent:
        parse_func = _callable_from(
            parse_tool,
            (
                "run_parsing_tool",
                "parse_html_to_text",
            ),
        )

        try:
            content_text = await _retry_async(
                "parse_html",
                lambda: parse_func(html_bytes),
            )
        except Exception as exc:
            raise ParseError(f"Failed to parse HTML content: {exc}") from exc

        if not isinstance(content_text, str):
            raise ParseError(
                f"Parser returned unsupported type: {type(content_text)}"
            )

        return ParsedRegulationContent(
            content_kind=ContentKind.HTML,
            content_hash=self.generate_hash(content_text),
            content_text=content_text,
        )

    async def _parse_pdf(
        self,
        pdf_bytes: bytes,
        source_name: str,
    ) -> ParsedRegulationContent:
        upload_func = _callable_from(
            gemini_upload_tool,
            (
                "run_gemini_upload_tool",
                "upload_pdf_to_gemini",
            ),
        )

        content_hash = self.generate_hash(pdf_bytes)

        try:
            upload_result = await _retry_async(
                "upload_pdf_to_gemini",
                lambda: upload_func(pdf_bytes, self._pdf_filename(source_name, content_hash)),
            )
        except Exception as exc:
            raise ParseError(f"Failed to upload PDF for ingestion: {exc}") from exc

        gemini_file_id = self._gemini_file_id(upload_result)
        content_text = None
        if gemini_file_id:
            try:
                from integrations.gemini_client import GeminiClient

                client = GeminiClient()
                extraction_prompt = (
                    "Extract all text content from this PDF document. "
                    "Return only the extracted text, preserving structure. "
                    f"File: {gemini_file_id}"
                )
                content_text = await client.generate_text(extraction_prompt)
                logger.info(
                    "PDF text extracted via Gemini chars=%s source=%s",
                    len(content_text),
                    source_name,
                )
            except Exception as exc:
                logger.warning(
                    "PDF text extraction failed source=%s error=%s - proceeding without text",
                    source_name,
                    exc,
                )

        return ParsedRegulationContent(
            content_kind=ContentKind.PDF,
            content_hash=content_hash,
            content_text=content_text,
            gemini_file_id=gemini_file_id,
        )

    def generate_hash(self, content: bytes | str) -> str:
        hash_func = _callable_from(
            hash_tool,
            (
                "hash_regulation_data",
                "generate_sha256_hash",
                "compute_sha256",
            ),
        )
        return hash_func(content)

    async def get_latest_snapshot(
        self,
        regulation_id: int,
    ) -> RegulationSnapshot | None:
        stmt = (
            select(RegulationSnapshot)
            .where(RegulationSnapshot.regulation_id == regulation_id)
            .order_by(desc(RegulationSnapshot.created_at), desc(RegulationSnapshot.id))
            .limit(1)
        )
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def _record_detected_change(
        self,
        regulation: Regulation,
        parsed_content: ParsedRegulationContent,
        previous_hash: str | None,
        scan_id: int | None = None,
    ) -> IngestionResult:
        try:
            snapshot = await queries.create_regulation_snapshot(
                self.session,
                regulation.id,
                parsed_content.content_hash,
                parsed_content.content_text,
                parsed_content.gemini_file_id,
            )
            await queries.create_regulation_change(
                self.session,
                regulation.id,
                snapshot.id,
            )
            await queries.update_regulation_hash(
                self.session,
                regulation.id,
                parsed_content.content_hash,
            )

            workflow_run = await self.workflow_service.create_workflow_run(
                regulation_id=regulation.id,
                snapshot_id=snapshot.id,
                scan_id=scan_id,
                initial_stage=INITIAL_STAGE,
                emit_event=True,
            )

            logger.info(
                "Regulation change recorded regulation_id=%s snapshot_id=%s workflow_run_id=%s",
                regulation.id,
                snapshot.id,
                workflow_run.id,
            )

            return IngestionResult(
                status=IngestionStatus.CHANGED,
                regulation_id=regulation.id,
                content_hash=parsed_content.content_hash,
                previous_hash=previous_hash,
                snapshot_id=snapshot.id,
                workflow_run_id=workflow_run.id,
                content_kind=parsed_content.content_kind,
                message="Change detected and workflow triggered.",
            )
        except Exception:
            await self.session.rollback()
            logger.exception(
                "Failed to record detected change regulation_id=%s",
                regulation.id,
            )
            raise

    @staticmethod
    def _pdf_filename(source_name: str, content_hash: str) -> str:
        safe_name = source_name.rsplit("/", 1)[-1] or "regulation.pdf"
        if not safe_name.lower().endswith(".pdf"):
            safe_name = f"{safe_name}.pdf"
        return f"{content_hash[:12]}_{safe_name}"

    @staticmethod
    def _gemini_file_id(upload_result: Any) -> str | None:
        if upload_result is None:
            return None
        if isinstance(upload_result, str):
            return upload_result
        if isinstance(upload_result, dict):
            return (
                upload_result.get("file_name")
                or upload_result.get("name")
                or upload_result.get("file_uri")
                or upload_result.get("uri")
            )
        return getattr(upload_result, "name", None) or getattr(upload_result, "uri", None)


async def ingest_url(
    session: AsyncSession,
    regulation_id: int,
    source_url: str,
) -> IngestionResult:
    return await IngestionService(session).ingest_url(regulation_id, source_url)


async def ingest_pdf(
    session: AsyncSession,
    regulation_id: int,
    pdf_bytes: bytes,
    source_name: str = "uploaded_regulation.pdf",
) -> IngestionResult:
    return await IngestionService(session).ingest_pdf(
        regulation_id,
        pdf_bytes,
        source_name,
    )


async def process_scan_job(
    session: AsyncSession,
    scan_job: Any,
) -> IngestionResult:
    return await IngestionService(session).process_scan_job(scan_job)
