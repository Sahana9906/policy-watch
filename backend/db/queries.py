from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import desc, select, update, text
from .models import (
    ChangeAnalysis,
    ComplianceAction,
    ComplianceRequirement,
    Regulation,
    ScanJob,
    RegulationSnapshot,
    RegulationChange,
    WorkflowRun,
    WorkflowEvent,
)

async def get_active_regulations(session: AsyncSession):
    """Fetch all active regulations to be scheduled for scanning."""
    stmt = select(Regulation).where(Regulation.is_active == True)
    result = await session.execute(stmt)
    return result.scalars().all()

async def create_scan_job(session: AsyncSession, reg_id: int):
    """Create a new pending scan job for a regulation."""
    job = ScanJob(regulation_id=reg_id, status="pending")
    session.add(job)
    return job

async def get_pending_scan_jobs(session: AsyncSession):
    """Fetch all jobs that are ready to be processed by the ingestion agent."""
    stmt = select(ScanJob).where(ScanJob.status == "pending")
    result = await session.execute(stmt)
    return result.scalars().all()

async def update_scan_job_status(session: AsyncSession, job_id: int, status: str):
    """Update the status of a scan job (e.g., 'running', 'completed', 'failed')."""
    stmt = update(ScanJob).where(ScanJob.id == job_id).values(status=status)
    await session.execute(stmt)

async def get_regulation(session: AsyncSession, reg_id: int):
    """Fetch a specific regulation by ID."""
    stmt = select(Regulation).where(Regulation.id == reg_id)
    result = await session.execute(stmt)
    return result.scalar_one_or_none()

async def update_regulation_hash(session: AsyncSession, reg_id: int, new_hash: str):
    """Update the last known hash of a regulation's content."""
    stmt = update(Regulation).where(Regulation.id == reg_id).values(last_hash=new_hash)
    await session.execute(stmt)

async def create_regulation_snapshot(session: AsyncSession, reg_id: int, content_hash: str, content_text: str = None, gemini_file_id: str = None):
    """Create a new snapshot record for a regulation."""
    snapshot = RegulationSnapshot(
        regulation_id=reg_id,
        content_hash=content_hash,
        content_text=content_text,
        gemini_file_id=gemini_file_id
    )
    session.add(snapshot)
    await session.flush()  # Ensures snapshot.id is populated
    return snapshot

async def create_regulation_change(session: AsyncSession, reg_id: int, snapshot_id: int):
    """Record that a change was detected and link it to the snapshot."""
    change = RegulationChange(
        regulation_id=reg_id,
        snapshot_id=snapshot_id
    )
    session.add(change)
    return change

async def create_workflow_run(
    session: AsyncSession,
    reg_id: int,
    snapshot_id: int | None = None,
    scan_id: int | None = None,
):
    """Create a workflow run entry to track Stage 2 processing."""
    workflow = WorkflowRun(
        regulation_id=reg_id,
        snapshot_id=snapshot_id,
        scan_id=scan_id,
        status="pending",
    )
    session.add(workflow)
    await session.flush()  # Ensures workflow.id is populated
    return workflow

async def get_regulation_snapshot(session: AsyncSession, snapshot_id: int):
    """Fetch a regulation snapshot by ID."""
    stmt = select(RegulationSnapshot).where(RegulationSnapshot.id == snapshot_id)
    result = await session.execute(stmt)
    return result.scalar_one_or_none()

async def get_latest_snapshot(session: AsyncSession, reg_id: int):
    """Fetch the latest snapshot for a regulation."""
    stmt = (
        select(RegulationSnapshot)
        .where(RegulationSnapshot.regulation_id == reg_id)
        .order_by(desc(RegulationSnapshot.created_at), desc(RegulationSnapshot.id))
        .limit(1)
    )
    result = await session.execute(stmt)
    return result.scalar_one_or_none()

async def get_previous_snapshot(
    session: AsyncSession,
    reg_id: int,
    before_snapshot_id: int,
):
    """Fetch the latest snapshot before the provided new snapshot."""
    new_snapshot = await get_regulation_snapshot(session, before_snapshot_id)
    if new_snapshot is None:
        return None

    stmt = (
        select(RegulationSnapshot)
        .where(RegulationSnapshot.regulation_id == reg_id)
        .where(RegulationSnapshot.id != before_snapshot_id)
        .where(RegulationSnapshot.created_at <= new_snapshot.created_at)
        .order_by(desc(RegulationSnapshot.created_at), desc(RegulationSnapshot.id))
        .limit(1)
    )
    result = await session.execute(stmt)
    return result.scalar_one_or_none()

async def create_change_analysis(
    session: AsyncSession,
    workflow_run_id: int,
    regulation_id: int,
    old_snapshot_id: int | None,
    new_snapshot_id: int,
    analysis: dict,
    raw_response: str | None = None,
):
    """Persist structured AI change analysis for a workflow run."""
    row = ChangeAnalysis(
        workflow_run_id=workflow_run_id,
        regulation_id=regulation_id,
        old_snapshot_id=old_snapshot_id,
        new_snapshot_id=new_snapshot_id,
        summary=analysis["summary"],
        severity=analysis["severity"],
        impacted_domains=analysis["impacted_domains"],
        changed_clauses=analysis["changed_clauses"],
        recommended_next_action=analysis["recommended_next_action"],
        raw_response=raw_response,
    )
    session.add(row)
    await session.flush()
    return row

async def create_compliance_requirement(
    session: AsyncSession,
    regulation_id: int,
    workflow_run_id: int,
    requirement_text: str,
    impacted_domain: str | None,
    severity: str,
):
    row = ComplianceRequirement(
        regulation_id=regulation_id,
        workflow_run_id=workflow_run_id,
        requirement_text=requirement_text,
        impacted_domain=impacted_domain,
        severity=severity,
    )
    session.add(row)
    await session.flush()
    return row

async def create_compliance_action(
    session: AsyncSession,
    requirement_id: int,
    workflow_run_id: int,
    action_title: str,
    action_description: str,
):
    row = ComplianceAction(
        requirement_id=requirement_id,
        workflow_run_id=workflow_run_id,
        action_title=action_title,
        action_description=action_description,
        status="pending",
    )
    session.add(row)
    await session.flush()
    return row

async def create_workflow_event(
    session: AsyncSession,
    workflow_run_id: int,
    stage: str,
    message: str,
):
    row = WorkflowEvent(
        workflow_run_id=workflow_run_id,
        stage=stage,
        message=message,
    )
    session.add(row)
    await session.flush()
    return row

async def emit_workflow_event(session: AsyncSession, workflow_id: int):
    """Emit a PostgreSQL NOTIFY event for the orchestrator."""
    await session.execute(text(f"NOTIFY workflow_events, '{workflow_id}'"))

async def get_existing_active_job(session: AsyncSession, reg_id: int) -> ScanJob | None:
    """Return any pending or running job for this regulation, if one exists."""
    stmt = (
        select(ScanJob)
        .where(ScanJob.regulation_id == reg_id)
        .where(ScanJob.status.in_(["pending", "running"]))
        .order_by(desc(ScanJob.created_at))
        .limit(1)
    )
    result = await session.execute(stmt)
    return result.scalar_one_or_none()
