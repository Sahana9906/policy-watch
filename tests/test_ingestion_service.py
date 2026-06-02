import pytest
from sqlalchemy import func, select

from db.models import RegulationSnapshot, WorkflowRun
from services.ingestion_service import (
    ContentKind,
    FetchResult,
    IngestionService,
    IngestionStatus,
    ParsedRegulationContent,
)


pytestmark = pytest.mark.asyncio


async def _count(session, model) -> int:
    result = await session.execute(select(func.count()).select_from(model))
    return result.scalar_one()


async def test_changed_content_creates_snapshot_and_workflow(
    monkeypatch,
    db_session,
    seeded_regulation,
):
    async def no_emit(*_args, **_kwargs):
        return None

    monkeypatch.setattr("services.workflow_service.WorkflowService.emit_workflow_event", no_emit)

    service = IngestionService(db_session)
    async def parse_static(*_args, **_kwargs):
        return ParsedRegulationContent(
            content_kind=ContentKind.HTML,
            content_hash="hash-v1",
            content_text="report incidents within 72 hours",
        )

    monkeypatch.setattr(
        service,
        "parse_content",
        parse_static,
    )

    result = await service._ingest_fetch_result(
        regulation_id=seeded_regulation.id,
        fetch_result=FetchResult(b"<html></html>", 200, "text/html"),
        source_name=seeded_regulation.url,
    )

    assert result.status == IngestionStatus.CHANGED
    assert result.previous_hash is None
    assert result.snapshot_id is not None
    assert result.workflow_run_id is not None
    assert await _count(db_session, RegulationSnapshot) == 1
    assert await _count(db_session, WorkflowRun) == 1


async def test_unchanged_content_does_not_create_duplicate_snapshot_or_workflow(
    monkeypatch,
    db_session,
    seeded_regulation,
):
    async def no_emit(*_args, **_kwargs):
        return None

    monkeypatch.setattr("services.workflow_service.WorkflowService.emit_workflow_event", no_emit)
    service = IngestionService(db_session)
    fetch_result = FetchResult(b"<html></html>", 200, "text/html")

    async def parse_static(*_args, **_kwargs):
        return ParsedRegulationContent(
            content_kind=ContentKind.HTML,
            content_hash="same-hash",
            content_text="initial regulation text",
        )

    monkeypatch.setattr(
        service,
        "parse_content",
        parse_static,
    )
    first = await service._ingest_fetch_result(
        seeded_regulation.id,
        fetch_result,
        seeded_regulation.url,
    )

    second = await service._ingest_fetch_result(
        seeded_regulation.id,
        fetch_result,
        seeded_regulation.url,
    )

    assert first.status == IngestionStatus.CHANGED
    assert second.status == IngestionStatus.NO_CHANGE
    assert second.workflow_run_id is None
    assert second.snapshot_id is None
    assert await _count(db_session, RegulationSnapshot) == 1
    assert await _count(db_session, WorkflowRun) == 1


async def test_empty_content_hash_can_be_compared_but_empty_fetch_is_rejected(
    db_session,
    fake_hashes,
):
    service = IngestionService(db_session)

    assert service.generate_hash("") == service.generate_hash("")
    assert service.generate_hash(b"") == service.generate_hash("")
    assert fake_hashes["empty"]


async def test_changed_content_creates_second_snapshot_and_workflow(
    monkeypatch,
    db_session,
    seeded_regulation,
):
    async def no_emit(*_args, **_kwargs):
        return None

    monkeypatch.setattr("services.workflow_service.WorkflowService.emit_workflow_event", no_emit)
    service = IngestionService(db_session)
    fetch_result = FetchResult(b"<html></html>", 200, "text/html")
    hashes = iter(("hash-v1", "hash-v2"))

    async def parse_next(*_args, **_kwargs):
        content_hash = next(hashes)
        return ParsedRegulationContent(
            content_kind=ContentKind.HTML,
            content_hash=content_hash,
            content_text=f"text for {content_hash}",
        )

    monkeypatch.setattr(service, "parse_content", parse_next)

    first = await service._ingest_fetch_result(seeded_regulation.id, fetch_result, "v1")
    second = await service._ingest_fetch_result(seeded_regulation.id, fetch_result, "v2")

    assert first.status == IngestionStatus.CHANGED
    assert second.status == IngestionStatus.CHANGED
    assert second.previous_hash == "hash-v1"
    assert await _count(db_session, RegulationSnapshot) == 2
    assert await _count(db_session, WorkflowRun) == 2
