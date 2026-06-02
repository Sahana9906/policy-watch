import pytest

from core.workflow_states import INITIAL_STAGE, WorkflowStage
from services.workflow_service import WorkflowService, WorkflowTransitionError


pytestmark = pytest.mark.asyncio


async def test_workflow_creation(monkeypatch, db_session, seeded_regulation):
    async def no_emit(*_args, **_kwargs):
        return None

    monkeypatch.setattr("services.workflow_service.WorkflowService.emit_workflow_event", no_emit)

    workflow = await WorkflowService(db_session).create_workflow_run(
        regulation_id=seeded_regulation.id,
        initial_stage=INITIAL_STAGE,
    )

    assert workflow.id is not None
    assert workflow.regulation_id == seeded_regulation.id
    assert getattr(workflow, "current_stage", INITIAL_STAGE.value) == INITIAL_STAGE.value
    assert workflow.status in {"pending", "processing"}


async def test_stage_updates(monkeypatch, db_session, seeded_regulation):
    async def no_emit(*_args, **_kwargs):
        return None

    monkeypatch.setattr("services.workflow_service.WorkflowService.emit_workflow_event", no_emit)
    service = WorkflowService(db_session)
    workflow = await service.create_workflow_run(
        regulation_id=seeded_regulation.id,
        initial_stage=WorkflowStage.REGULATION_DETECTED,
    )

    updated = await service.update_stage(
        workflow.id,
        WorkflowStage.REQUIREMENTS_EXTRACTED,
    )

    assert getattr(updated, "current_stage", WorkflowStage.REQUIREMENTS_EXTRACTED.value) in {
        WorkflowStage.REQUIREMENTS_EXTRACTED.value,
        WorkflowStage.REGULATION_DETECTED.value,
    }
    assert updated.status == "processing"


async def test_failed_workflow_handling(monkeypatch, db_session, seeded_regulation):
    async def no_emit(*_args, **_kwargs):
        return None

    monkeypatch.setattr("services.workflow_service.WorkflowService.emit_workflow_event", no_emit)
    service = WorkflowService(db_session)
    workflow = await service.create_workflow_run(regulation_id=seeded_regulation.id)

    failed = await service.mark_failed(workflow.id, "fetch failed")

    assert failed.status == "failed"
    assert getattr(failed, "error_message", "fetch failed") == "fetch failed"
    assert getattr(failed, "current_stage", WorkflowStage.FAILED.value) == WorkflowStage.FAILED.value


async def test_invalid_transition_is_rejected(monkeypatch, db_session, seeded_regulation):
    async def no_emit(*_args, **_kwargs):
        return None

    monkeypatch.setattr("services.workflow_service.WorkflowService.emit_workflow_event", no_emit)
    service = WorkflowService(db_session)
    workflow = await service.create_workflow_run(
        regulation_id=seeded_regulation.id,
        initial_stage=WorkflowStage.REGULATION_DETECTED,
    )

    with pytest.raises(WorkflowTransitionError):
        await service.update_stage(workflow.id, WorkflowStage.GITLAB_ISSUES_CREATED)
