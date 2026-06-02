import asyncio
import logging
from dataclasses import asdict, dataclass
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from db import queries
from db.models import ScanJob
from db.session import AsyncSessionLocal
from services.ingestion_service import (
    IngestionResult,
    IngestionService,
    IngestionStatus,
)

logger = logging.getLogger(__name__)

INGESTION_RETRY_ATTEMPTS = 2
INGESTION_RETRY_DELAY_SECONDS = 0.5


@dataclass(slots=True)
class MonitoringSummary:
    pending: int = 0
    processed: int = 0
    changed: int = 0
    no_change: int = 0
    failed: int = 0

    def as_dict(self) -> dict[str, int]:
        return asdict(self)


class MonitoringAgent:
    name = "monitoring_agent"
    description = "Polls pending regulation scan jobs and delegates ingestion."

    async def run(self) -> dict[str, int]:
        summary = await run_monitoring_cycle()
        return summary.as_dict()


async def run_monitoring_cycle() -> MonitoringSummary:
    summary = MonitoringSummary()

    async with AsyncSessionLocal() as session:
        pending_jobs = await queries.get_pending_scan_jobs(session)
        summary.pending = len(pending_jobs)

    if not pending_jobs:
        logger.info("Monitoring agent found no pending scan jobs.")
        return summary

    logger.info(
        "Monitoring agent found %s pending scan job(s).",
        summary.pending,
    )

    for job in pending_jobs:
        job_id = getattr(job, "id", None)
        async with AsyncSessionLocal() as session:
            try:
                db_job = await _load_scan_job(session, job_id)
                if db_job is None:
                    logger.warning(
                        "Scan job disappeared before processing job_id=%s.",
                        job_id,
                    )
                    continue

                result = await process_scan_job(session, db_job)
                summary.processed += 1

                if result.status == IngestionStatus.CHANGED:
                    summary.changed += 1
                elif result.status == IngestionStatus.NO_CHANGE:
                    summary.no_change += 1

            except Exception:
                summary.failed += 1
                logger.exception(
                    "Monitoring agent failed scan job_id=%s.",
                    job_id,
                )

    logger.info("Monitoring agent cycle complete: %s", summary.as_dict())
    return summary


async def process_scan_job(
    session: AsyncSession,
    scan_job: Any,
) -> IngestionResult:
    job_id = getattr(scan_job, "id", None)
    regulation_id = getattr(scan_job, "regulation_id", None)

    logger.info(
        "Monitoring agent processing scan job_id=%s regulation_id=%s.",
        job_id,
        regulation_id,
    )

    await _mark_scan_status(session, scan_job, "running")

    try:
        result = await _run_ingestion_with_retry(session, scan_job)
        next_status = (
            "no_change"
            if result.status == IngestionStatus.NO_CHANGE
            else "completed"
        )
        await _mark_scan_status(session, scan_job, next_status)

        logger.info(
            "Monitoring agent completed scan job_id=%s status=%s workflow_run_id=%s snapshot_id=%s.",
            job_id,
            result.status.value,
            result.workflow_run_id,
            result.snapshot_id,
        )
        return result
    except Exception:
        await _mark_scan_status(session, job_id, "failed")
        try:
            scan_job.status = "failed"
        except Exception:
            logger.debug("Could not update in-memory scan job status.", exc_info=True)
        logger.exception(
            "Monitoring agent marked scan job_id=%s failed.",
            job_id,
        )
        raise


async def _run_ingestion_with_retry(
    session: AsyncSession,
    scan_job: Any,
) -> IngestionResult:
    last_error: Exception | None = None
    job_id = getattr(scan_job, "id", None)
    current_job = scan_job

    for attempt in range(1, INGESTION_RETRY_ATTEMPTS + 1):
        try:
            return await IngestionService(session).process_scan_job(current_job)
        except Exception as exc:
            last_error = exc
            await session.rollback()
            logger.warning(
                "Ingestion failed attempt=%s/%s scan_job_id=%s error=%s",
                attempt,
                INGESTION_RETRY_ATTEMPTS,
                job_id,
                exc,
            )
            if attempt < INGESTION_RETRY_ATTEMPTS:
                if job_id is not None:
                    current_job = await session.get(ScanJob, job_id)
                    if current_job is None:
                        raise RuntimeError(f"Scan job disappeared during retry: {job_id}") from exc
                await asyncio.sleep(INGESTION_RETRY_DELAY_SECONDS)

    raise last_error or RuntimeError("Ingestion failed without an exception.")


async def _load_scan_job(
    session: AsyncSession,
    job_id: int | None,
) -> Any | None:
    if job_id is None:
        return None

    return await session.get(ScanJob, job_id)


async def _mark_scan_status(
    session: AsyncSession,
    scan_job: Any,
    status: str,
) -> None:
    job_id = scan_job if isinstance(scan_job, int) else getattr(scan_job, "id", None)
    if job_id is None:
        raise ValueError("Scan job is missing id.")

    await queries.update_scan_job_status(session, job_id, status)
    await session.commit()


async def run() -> dict[str, int]:
    summary = await run_monitoring_cycle()
    return summary.as_dict()


monitoring_agent = MonitoringAgent()
