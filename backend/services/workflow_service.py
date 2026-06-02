import asyncio
import json
import logging
from datetime import datetime
from typing import Any

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from core.workflow_states import (
    FAILED_STAGE,
    INITIAL_STAGE,
    WorkflowStage,
    can_transition,
    get_next_stage,
    is_valid_stage,
    is_terminal_stage,
    normalize_stage,
    require_valid_transition,
    stage_value,
)
from db import queries
from db.models import WorkflowRun

logger = logging.getLogger(__name__)

WORKFLOW_RUNS_CHANNEL = "workflow_runs"
NOTIFY_RETRY_ATTEMPTS = 2
NOTIFY_RETRY_DELAY_SECONDS = 0.25


class WorkflowServiceError(Exception):
    """Base exception for workflow service failures."""


class WorkflowNotFoundError(WorkflowServiceError):
    """Raised when a workflow run cannot be found."""


class WorkflowTransitionError(WorkflowServiceError):
    """Raised when a workflow transition is invalid."""


def _set_if_present(instance: Any, attr_name: str, value: Any) -> None:
    if hasattr(instance, attr_name):
        setattr(instance, attr_name, value)


def _get_stage(workflow_run: WorkflowRun) -> WorkflowStage:
    raw_stage = (
        getattr(workflow_run, "current_stage", None)
        or getattr(workflow_run, "stage", None)
    )
    if raw_stage and is_valid_stage(raw_stage):
        return normalize_stage(raw_stage)

    status = getattr(workflow_run, "status", None)
    if status == "completed":
        return WorkflowStage.COMPLETED
    if status == "failed":
        return WorkflowStage.FAILED

    return INITIAL_STAGE


def _workflow_status_for_stage(stage: WorkflowStage) -> str:
    if stage == WorkflowStage.COMPLETED:
        return "completed"
    if stage in {WorkflowStage.FAILED, WorkflowStage.ANALYSIS_FAILED}:
        return "failed"
    return "processing"


class WorkflowService:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def create_workflow_run(
        self,
        regulation_id: int,
        snapshot_id: int | None = None,
        scan_id: int | None = None,
        initial_stage: WorkflowStage | str = INITIAL_STAGE,
        emit_event: bool = True,
    ) -> WorkflowRun:
        stage = normalize_stage(initial_stage)

        try:
            workflow_run = await queries.create_workflow_run(
                self.session,
                regulation_id,
                snapshot_id=snapshot_id,
                scan_id=scan_id,
            )
            _set_if_present(workflow_run, "snapshot_id", snapshot_id)
            _set_if_present(workflow_run, "scan_id", scan_id)
            _set_if_present(workflow_run, "current_stage", stage.value)
            _set_if_present(workflow_run, "status", _workflow_status_for_stage(stage))
            _set_if_present(workflow_run, "updated_at", datetime.utcnow())

            await self.session.flush()
            await queries.create_workflow_event(
                self.session,
                workflow_run.id,
                stage.value,
                "Workflow created.",
            )
            await self.session.commit()

            logger.info(
                "Created workflow_run_id=%s regulation_id=%s stage=%s",
                workflow_run.id,
                regulation_id,
                stage.value,
            )

            if emit_event:
                await self.emit_workflow_event(workflow_run)

            return workflow_run
        except Exception:
            await self.session.rollback()
            logger.exception(
                "Failed to create workflow run for regulation_id=%s snapshot_id=%s",
                regulation_id,
                snapshot_id,
            )
            raise

    async def get_workflow_run(self, workflow_run_id: int) -> WorkflowRun:
        result = await self.session.execute(
            select(WorkflowRun).where(WorkflowRun.id == workflow_run_id)
        )
        workflow_run = result.scalar_one_or_none()
        if workflow_run is None:
            raise WorkflowNotFoundError(
                f"Workflow run not found: {workflow_run_id}"
            )
        return workflow_run

    async def update_stage(
        self,
        workflow_run_id: int,
        next_stage: WorkflowStage | str,
        emit_event: bool = True,
    ) -> WorkflowRun:
        workflow_run = await self.get_workflow_run(workflow_run_id)
        current_stage = _get_stage(workflow_run)
        target_stage = normalize_stage(next_stage)

        if not can_transition(current_stage, target_stage):
            raise WorkflowTransitionError(
                f"Invalid workflow transition: "
                f"{current_stage.value} -> {target_stage.value}"
            )

        try:
            _set_if_present(workflow_run, "current_stage", target_stage.value)
            _set_if_present(workflow_run, "status", _workflow_status_for_stage(target_stage))
            _set_if_present(workflow_run, "updated_at", datetime.utcnow())
            if is_terminal_stage(target_stage):
                _set_if_present(workflow_run, "completed_at", datetime.utcnow())

            await queries.create_workflow_event(
                self.session,
                workflow_run_id,
                target_stage.value,
                f"Workflow advanced from {current_stage.value} to {target_stage.value}.",
            )
            await self.session.commit()

            logger.info(
                "Workflow_run_id=%s advanced from %s to %s",
                workflow_run_id,
                current_stage.value,
                target_stage.value,
            )

            if emit_event and not is_terminal_stage(target_stage):
                await self.emit_workflow_event(workflow_run)

            return workflow_run
        except Exception:
            await self.session.rollback()
            logger.exception(
                "Failed to update workflow_run_id=%s to stage=%s",
                workflow_run_id,
                target_stage.value,
            )
            raise

    async def advance_to_next_stage(
        self,
        workflow_run_id: int,
        emit_event: bool = True,
    ) -> WorkflowRun:
        workflow_run = await self.get_workflow_run(workflow_run_id)
        current_stage = _get_stage(workflow_run)
        next_stage = get_next_stage(current_stage)

        if next_stage is None:
            raise WorkflowTransitionError(
                f"Workflow run {workflow_run_id} has no next stage from "
                f"{current_stage.value}"
            )

        return await self.update_stage(
            workflow_run_id,
            next_stage,
            emit_event=emit_event,
        )

    async def mark_completed(
        self,
        workflow_run_id: int,
        emit_event: bool = False,
    ) -> WorkflowRun:
        workflow_run = await self.get_workflow_run(workflow_run_id)
        current_stage = _get_stage(workflow_run)

        if current_stage != WorkflowStage.COMPLETED:
            require_valid_transition(current_stage, WorkflowStage.COMPLETED)

        try:
            _set_if_present(workflow_run, "current_stage", WorkflowStage.COMPLETED.value)
            _set_if_present(workflow_run, "status", "completed")
            _set_if_present(workflow_run, "updated_at", datetime.utcnow())
            _set_if_present(workflow_run, "completed_at", datetime.utcnow())

            await queries.create_workflow_event(
                self.session,
                workflow_run_id,
                WorkflowStage.COMPLETED.value,
                "Workflow completed.",
            )
            await self.session.commit()

            logger.info("Workflow_run_id=%s marked completed.", workflow_run_id)

            if emit_event:
                await self.emit_workflow_event(workflow_run)

            return workflow_run
        except Exception:
            await self.session.rollback()
            logger.exception(
                "Failed to mark workflow_run_id=%s completed.",
                workflow_run_id,
            )
            raise

    async def mark_failed(
        self,
        workflow_run_id: int,
        error_message: str,
        emit_event: bool = False,
    ) -> WorkflowRun:
        workflow_run = await self.get_workflow_run(workflow_run_id)

        try:
            _set_if_present(workflow_run, "current_stage", FAILED_STAGE.value)
            _set_if_present(workflow_run, "status", "failed")
            _set_if_present(workflow_run, "error_message", error_message)
            _set_if_present(workflow_run, "updated_at", datetime.utcnow())
            _set_if_present(workflow_run, "completed_at", datetime.utcnow())

            await queries.create_workflow_event(
                self.session,
                workflow_run_id,
                FAILED_STAGE.value,
                error_message,
            )
            await self.session.commit()

            logger.error(
                "Workflow_run_id=%s marked failed: %s",
                workflow_run_id,
                error_message,
            )

            if emit_event:
                await self.emit_workflow_event(workflow_run)

            return workflow_run
        except Exception:
            await self.session.rollback()
            logger.exception(
                "Failed to mark workflow_run_id=%s failed.",
                workflow_run_id,
            )
            raise

    async def emit_workflow_event(
        self,
        workflow_run: WorkflowRun,
        channel: str = WORKFLOW_RUNS_CHANNEL,
    ) -> None:
        payload = self.build_event_payload(workflow_run)

        for attempt in range(1, NOTIFY_RETRY_ATTEMPTS + 1):
            try:
                await self.session.execute(
                    text("SELECT pg_notify(:channel, :payload)"),
                    {
                        "channel": channel,
                        "payload": json.dumps(payload),
                    },
                )
                await self.session.commit()

                logger.info(
                    "Emitted workflow event channel=%s workflow_run_id=%s stage=%s",
                    channel,
                    workflow_run.id,
                    payload.get("current_stage"),
                )
                return
            except Exception:
                await self.session.rollback()
                logger.exception(
                    "Failed to emit workflow event attempt=%s workflow_run_id=%s",
                    attempt,
                    workflow_run.id,
                )
                if attempt < NOTIFY_RETRY_ATTEMPTS:
                    await asyncio.sleep(NOTIFY_RETRY_DELAY_SECONDS)

        raise WorkflowServiceError(
            f"Failed to emit workflow event for workflow_run_id={workflow_run.id}"
        )

    def build_event_payload(self, workflow_run: WorkflowRun) -> dict[str, Any]:
        stage = _get_stage(workflow_run)
        return {
            "event_type": "workflow_run_updated",
            "workflow_run_id": workflow_run.id,
            "regulation_id": getattr(workflow_run, "regulation_id", None),
            "snapshot_id": getattr(workflow_run, "snapshot_id", None),
            "scan_id": getattr(workflow_run, "scan_id", None),
            "status": getattr(workflow_run, "status", None),
            "current_stage": stage.value,
            "is_terminal": is_terminal_stage(stage),
            "created_at": self._datetime_value(getattr(workflow_run, "created_at", None)),
            "updated_at": self._datetime_value(getattr(workflow_run, "updated_at", None)),
        }

    async def get_status(self, workflow_run_id: int) -> dict[str, Any]:
        workflow_run = await self.get_workflow_run(workflow_run_id)
        stage = _get_stage(workflow_run)
        next_stage = get_next_stage(stage)

        return {
            "workflow_run_id": workflow_run.id,
            "regulation_id": getattr(workflow_run, "regulation_id", None),
            "snapshot_id": getattr(workflow_run, "snapshot_id", None),
            "scan_id": getattr(workflow_run, "scan_id", None),
            "status": getattr(workflow_run, "status", None),
            "current_stage": stage.value,
            "next_stage": stage_value(next_stage) if next_stage else None,
            "is_terminal": is_terminal_stage(stage),
            "created_at": self._datetime_value(getattr(workflow_run, "created_at", None)),
            "updated_at": self._datetime_value(getattr(workflow_run, "updated_at", None)),
            "completed_at": self._datetime_value(getattr(workflow_run, "completed_at", None)),
            "error_message": getattr(workflow_run, "error_message", None),
        }

    @staticmethod
    def _datetime_value(value: Any) -> str | None:
        if isinstance(value, datetime):
            return value.isoformat()
        return None


async def create_workflow_run(
    session: AsyncSession,
    regulation_id: int,
    snapshot_id: int | None = None,
    scan_id: int | None = None,
    initial_stage: WorkflowStage | str = INITIAL_STAGE,
) -> WorkflowRun:
    return await WorkflowService(session).create_workflow_run(
        regulation_id=regulation_id,
        snapshot_id=snapshot_id,
        scan_id=scan_id,
        initial_stage=initial_stage,
    )


async def emit_workflow_event(
    session: AsyncSession,
    workflow_run: WorkflowRun,
) -> None:
    await WorkflowService(session).emit_workflow_event(workflow_run)
