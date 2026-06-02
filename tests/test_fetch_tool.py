import asyncio
from types import SimpleNamespace

import pytest

from services.ingestion_service import (
    ContentDetectionError,
    ContentKind,
    FetchError,
    IngestionService,
)


pytestmark = pytest.mark.asyncio


async def test_fetch_successful_html(monkeypatch, db_session, sample_html_v1):
    async def fake_fetch(_url: str):
        return {
            "content": sample_html_v1,
            "status_code": 200,
            "content_type": "text/html; charset=utf-8",
            "final_url": "https://example.test/regulation",
        }

    monkeypatch.setattr("services.ingestion_service.fetch_tool.fetch_url", fake_fetch, raising=False)

    result = await IngestionService(db_session).fetch_regulation_content(
        "https://example.test/regulation"
    )

    assert result.status_code == 200
    assert result.content == sample_html_v1
    assert result.content_type.startswith("text/html")


async def test_fetch_successful_pdf(monkeypatch, db_session, sample_pdf):
    async def fake_fetch(_url: str):
        return {
            "content": sample_pdf,
            "status_code": 200,
            "content_type": "application/pdf",
        }

    monkeypatch.setattr("services.ingestion_service.fetch_tool.fetch_url", fake_fetch, raising=False)

    service = IngestionService(db_session)
    result = await service.fetch_regulation_content("https://example.test/regulation.pdf")

    assert result.content == sample_pdf
    assert service.detect_content_kind(result) == ContentKind.PDF


async def test_fetch_timeout_handling(monkeypatch, db_session):
    async def slow_fetch(_url: str):
        await asyncio.sleep(0.05)
        return b"never reached"

    monkeypatch.setattr("services.ingestion_service.DEFAULT_FETCH_TIMEOUT_SECONDS", 0.001)
    monkeypatch.setattr("services.ingestion_service.fetch_tool.fetch_url", slow_fetch, raising=False)

    with pytest.raises(FetchError, match="Timed out fetching"):
        await IngestionService(db_session).fetch_regulation_content(
            "https://example.test/slow"
        )


async def test_fetch_connection_failure_handling(monkeypatch, db_session):
    async def failing_fetch(_url: str):
        raise OSError("connection refused")

    monkeypatch.setattr("services.ingestion_service.fetch_tool.fetch_url", failing_fetch, raising=False)

    with pytest.raises(FetchError, match="Failed to fetch"):
        await IngestionService(db_session).fetch_regulation_content(
            "https://example.test/down"
        )


async def test_invalid_content_type_handling(db_session):
    service = IngestionService(db_session)
    fetch_result = SimpleNamespace(
        content=b"\x00\x01not-html-or-pdf",
        content_type="application/octet-stream",
        status_code=200,
    )

    with pytest.raises(ContentDetectionError):
        service.detect_content_kind(fetch_result)
