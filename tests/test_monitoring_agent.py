import pytest
from sqlalchemy import func, select

from agents import monitoring_agent
from db.models import Regulation, RegulationSnapshot, ScanJob, WorkflowRun
from services.ingestion_service import ContentKind, ParsedRegulationContent


pytestmark = pytest.mark.asyncio


async def _count(session, model) -> int:
    result = await session.execute(select(func.count()).select_from(model))
    return result.scalar_one()


async def _latest(session, model):
    result = await session.execute(select(model).order_by(model.id.desc()).limit(1))
    return result.scalar_one_or_none()


async def _add_pending_scan(session, regulation_id: int) -> ScanJob:
    scan_job = ScanJob(regulation_id=regulation_id, status="pending")
    session.add(scan_job)
    await session.commit()
    await session.refresh(scan_job)
    return scan_job


async def test_stage1_complete_flow_creates_snapshot_hash_and_workflow(
    monkeypatch,
    db_session,
    pending_scan_job,
    sample_html_v1,
):
    async def no_emit(*_args, **_kwargs):
        return None

    async def fake_fetch(_url: str):
        return {
            "content": sample_html_v1,
            "status_code": 200,
            "content_type": "text/html",
        }

    monkeypatch.setattr("services.workflow_service.WorkflowService.emit_workflow_event", no_emit)
    monkeypatch.setattr("services.ingestion_service.fetch_tool.fetch_url", fake_fetch, raising=False)
    monkeypatch.setattr(
        "services.ingestion_service.parse_tool.run_parsing_tool",
        lambda *_args, **_kwargs: "Covered entities must report incidents within 72 hours.",
        raising=False,
    )

    result = await monitoring_agent.process_scan_job(db_session, pending_scan_job)
    workflow = await _latest(db_session, WorkflowRun)
    snapshot = await _latest(db_session, RegulationSnapshot)
    regulation = await db_session.get(Regulation, pending_scan_job.regulation_id)
    await db_session.refresh(pending_scan_job)

    assert result.status.value == "changed"
    assert result.snapshot_id == snapshot.id
    assert result.workflow_run_id == workflow.id
    assert snapshot.content_hash == regulation.last_hash
    assert workflow.regulation_id == regulation.id
    assert getattr(workflow, "current_stage", "regulation_detected") == "regulation_detected"
    assert pending_scan_job.status == "completed"


async def test_second_run_with_unchanged_content_creates_no_new_workflow(
    monkeypatch,
    db_session,
    seeded_regulation,
    sample_html_v1,
):
    async def no_emit(*_args, **_kwargs):
        return None

    async def fake_fetch(_url: str):
        return {
            "content": sample_html_v1,
            "status_code": 200,
            "content_type": "text/html",
        }

    monkeypatch.setattr("services.workflow_service.WorkflowService.emit_workflow_event", no_emit)
    monkeypatch.setattr("services.ingestion_service.fetch_tool.fetch_url", fake_fetch, raising=False)
    monkeypatch.setattr(
        "services.ingestion_service.parse_tool.run_parsing_tool",
        lambda *_args, **_kwargs: "Covered entities must report incidents within 72 hours.",
        raising=False,
    )

    first_job = await _add_pending_scan(db_session, seeded_regulation.id)
    first = await monitoring_agent.process_scan_job(db_session, first_job)

    second_job = await _add_pending_scan(db_session, seeded_regulation.id)
    second = await monitoring_agent.process_scan_job(db_session, second_job)

    assert first.status.value == "changed"
    assert second.status.value == "no_change"
    assert second.workflow_run_id is None
    assert await _count(db_session, RegulationSnapshot) == 1
    assert await _count(db_session, WorkflowRun) == 1
    await db_session.refresh(second_job)
    assert second_job.status == "no_change"


async def test_changed_content_creates_new_snapshot_and_workflow(
    monkeypatch,
    db_session,
    seeded_regulation,
):
    async def no_emit(*_args, **_kwargs):
        return None

    monkeypatch.setattr("services.workflow_service.WorkflowService.emit_workflow_event", no_emit)

    parsed_versions = iter(
        [
            ParsedRegulationContent(
                content_kind=ContentKind.HTML,
                content_hash="hash-v1",
                content_text="Report incidents within 72 hours.",
            ),
            ParsedRegulationContent(
                content_kind=ContentKind.HTML,
                content_hash="hash-v2",
                content_text="Report incidents within 24 hours.",
            ),
        ]
    )

    async def fake_fetch(*_args, **_kwargs):
        return None

    async def parse_next(*_args, **_kwargs):
        return next(parsed_versions)

    monkeypatch.setattr(
        "services.ingestion_service.IngestionService.fetch_regulation_content",
        fake_fetch,
    )
    monkeypatch.setattr(
        "services.ingestion_service.IngestionService.parse_content",
        parse_next,
    )

    first_job = await _add_pending_scan(db_session, seeded_regulation.id)
    second_job = await _add_pending_scan(db_session, seeded_regulation.id)

    first = await monitoring_agent.process_scan_job(db_session, first_job)
    second = await monitoring_agent.process_scan_job(db_session, second_job)

    assert first.status.value == "changed"
    assert second.status.value == "changed"
    assert second.previous_hash == "hash-v1"
    assert await _count(db_session, RegulationSnapshot) == 2
    assert await _count(db_session, WorkflowRun) == 2


async def test_fetch_failure_marks_scan_failed_and_creates_no_workflow(
    monkeypatch,
    db_session,
    pending_scan_job,
):
    async def failing_fetch(*_args, **_kwargs):
        raise RuntimeError("network down")

    monkeypatch.setattr(
        "services.ingestion_service.IngestionService.fetch_regulation_content",
        failing_fetch,
    )

    with pytest.raises(RuntimeError, match="network down"):
        await monitoring_agent.process_scan_job(db_session, pending_scan_job)

    assert pending_scan_job.status == "failed"
    await db_session.refresh(pending_scan_job)
    assert pending_scan_job.status == "failed"
    assert await _count(db_session, RegulationSnapshot) == 0
    assert await _count(db_session, WorkflowRun) == 0
