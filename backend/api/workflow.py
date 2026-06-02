import logging
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from db import models
from db.session import get_db

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/workflow", tags=["Workflow"])

Regulation = getattr(models, "Regulation")
RegulationSnapshot = getattr(models, "RegulationSnapshot")
RegulationChange = getattr(models, "RegulationChange")
WorkflowRun = getattr(models, "WorkflowRun")
WorkflowEvent = getattr(models, "WorkflowEvent")
SystemAlert = getattr(models, "SystemAlert", None)


def _serialize_datetime(value: Any) -> str | None:
    if isinstance(value, datetime):
        return value.isoformat()
    return None


def _get_attr(instance: Any, attr_name: str, default: Any = None) -> Any:
    return getattr(instance, attr_name, default)


def _workflow_stage(workflow_run: Any) -> str:
    return (
        _get_attr(workflow_run, "current_stage")
        or _get_attr(workflow_run, "stage")
        or "regulation_detected"
    )


def _workflow_status(workflow_run: Any) -> str:
    return _get_attr(workflow_run, "status", "pending")


def _serialize_workflow_run(workflow_run: Any) -> dict[str, Any]:
    regulation = _get_attr(workflow_run, "regulation")

    return {
        "id": workflow_run.id,
        "regulation_id": _get_attr(workflow_run, "regulation_id"),
        "regulation_name": _get_attr(regulation, "name") if regulation else None,
        "snapshot_id": _get_attr(workflow_run, "snapshot_id"),
        "scan_id": _get_attr(workflow_run, "scan_id"),
        "status": _workflow_status(workflow_run),
        "current_stage": _workflow_stage(workflow_run),
        "created_at": _serialize_datetime(_get_attr(workflow_run, "created_at")),
        "updated_at": _serialize_datetime(_get_attr(workflow_run, "updated_at")),
        "completed_at": _serialize_datetime(_get_attr(workflow_run, "completed_at")),
    }


def _serialize_change(change: Any) -> dict[str, Any]:
    snapshot = _get_attr(change, "snapshot")
    regulation = _get_attr(change, "regulation")

    return {
        "id": change.id,
        "regulation_id": _get_attr(change, "regulation_id"),
        "regulation_name": _get_attr(regulation, "name") if regulation else None,
        "snapshot_id": _get_attr(change, "snapshot_id"),
        "scan_id": _get_attr(change, "scan_id"),
        "source_url": _get_attr(change, "source_url")
        or (_get_attr(regulation, "url") if regulation else None),
        "content_hash": _get_attr(snapshot, "content_hash") if snapshot else None,
        "gemini_file_id": _get_attr(snapshot, "gemini_file_id") if snapshot else None,
        "created_at": _serialize_datetime(_get_attr(change, "created_at")),
    }


def _serialize_alert(alert: Any) -> dict[str, Any]:
    return {
        "id": _get_attr(alert, "id"),
        "regulation_id": _get_attr(alert, "regulation_id"),
        "scan_id": _get_attr(alert, "scan_id"),
        "source_url": _get_attr(alert, "source_url"),
        "severity": _get_attr(alert, "severity", "warning"),
        "status": _get_attr(alert, "status", "open"),
        "message": _get_attr(alert, "message"),
        "created_at": _serialize_datetime(_get_attr(alert, "created_at")),
        "resolved_at": _serialize_datetime(_get_attr(alert, "resolved_at")),
    }


def _serialize_event(event: Any) -> dict[str, Any]:
    return {
        "id": _get_attr(event, "id"),
        "workflow_run_id": _get_attr(event, "workflow_run_id"),
        "stage": _get_attr(event, "stage"),
        "message": _get_attr(event, "message"),
        "created_at": _serialize_datetime(_get_attr(event, "created_at")),
    }


async def _get_regulation_name(
    db: AsyncSession,
    regulation_id: int | None,
) -> str | None:
    if regulation_id is None:
        return None

    result = await db.execute(
        select(Regulation.name).where(Regulation.id == regulation_id)
    )
    return result.scalar_one_or_none()


@router.get("/runs")
async def get_workflow_runs(
    db: AsyncSession = Depends(get_db),
    status_filter: str | None = Query(default=None, alias="status"),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    try:
        stmt = (
            select(WorkflowRun)
            .options(selectinload(WorkflowRun.regulation))
            .order_by(desc(WorkflowRun.created_at))
            .offset(offset)
            .limit(limit)
        )
        if status_filter:
            stmt = stmt.where(WorkflowRun.status == status_filter)

        result = await db.execute(stmt)
        runs = result.scalars().all()

        return {
            "items": [_serialize_workflow_run(run) for run in runs],
            "limit": limit,
            "offset": offset,
        }
    except Exception as exc:
        logger.exception("Failed to fetch workflow runs.")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch workflow runs.",
        ) from exc


@router.get("/changes")
async def get_regulation_changes(
    db: AsyncSession = Depends(get_db),
    regulation_id: int | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    try:
        stmt = (
            select(RegulationChange)
            .options(selectinload(RegulationChange.snapshot))
            .order_by(desc(RegulationChange.created_at))
            .offset(offset)
            .limit(limit)
        )
        if regulation_id is not None:
            stmt = stmt.where(RegulationChange.regulation_id == regulation_id)

        result = await db.execute(stmt)
        changes = result.scalars().all()

        return {
            "items": [_serialize_change(change) for change in changes],
            "limit": limit,
            "offset": offset,
        }
    except Exception as exc:
        logger.exception("Failed to fetch regulation changes.")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch regulation changes.",
        ) from exc


@router.get("/alerts")
async def get_system_alerts(
    db: AsyncSession = Depends(get_db),
    status_filter: str | None = Query(default=None, alias="status"),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    if SystemAlert is None:
        return {
            "items": [],
            "limit": limit,
            "offset": offset,
            "message": "SystemAlert model is not available.",
        }

    try:
        stmt = select(SystemAlert).offset(offset).limit(limit)
        if hasattr(SystemAlert, "created_at"):
            stmt = stmt.order_by(desc(SystemAlert.created_at))
        if status_filter and hasattr(SystemAlert, "status"):
            stmt = stmt.where(SystemAlert.status == status_filter)

        result = await db.execute(stmt)
        alerts = result.scalars().all()

        return {
            "items": [_serialize_alert(alert) for alert in alerts],
            "limit": limit,
            "offset": offset,
        }
    except Exception as exc:
        logger.exception("Failed to fetch system alerts.")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch system alerts.",
        ) from exc


@router.get("/events")
async def get_workflow_events(
    db: AsyncSession = Depends(get_db),
    workflow_run_id: int | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=300),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    try:
        stmt = (
            select(WorkflowEvent)
            .order_by(desc(WorkflowEvent.created_at), desc(WorkflowEvent.id))
            .offset(offset)
            .limit(limit)
        )
        if workflow_run_id is not None:
            stmt = stmt.where(WorkflowEvent.workflow_run_id == workflow_run_id)

        result = await db.execute(stmt)
        events = result.scalars().all()

        return {
            "items": [_serialize_event(event) for event in events],
            "limit": limit,
            "offset": offset,
        }
    except Exception as exc:
        logger.exception("Failed to fetch workflow events.")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch workflow events.",
        ) from exc


@router.get("/status/{run_id}")
async def get_workflow_status(
    run_id: int,
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    try:
        result = await db.execute(
            select(WorkflowRun)
            .options(selectinload(WorkflowRun.regulation))
            .where(WorkflowRun.id == run_id)
        )
        workflow_run = result.scalar_one_or_none()

        if workflow_run is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Workflow run not found.",
            )

        response = _serialize_workflow_run(workflow_run)
        response["regulation_name"] = await _get_regulation_name(
            db,
            response.get("regulation_id"),
        )
        response["orchestration_state"] = {
            "pollable": True,
            "stage_2_ready": response["current_stage"] == "regulation_detected",
            "terminal": response["status"] in {"completed", "failed", "cancelled"},
        }
        return response
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Failed to fetch workflow status for run_id=%s.", run_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch workflow status.",
        ) from exc
