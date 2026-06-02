import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession

from db import models, queries
from db.session import AsyncSessionLocal

logger = logging.getLogger(__name__)

SCAN_INTERVAL_MINUTES = 15
SCHEDULER_JOB_ID = "policywatch_recurring_scan_insertion"

ScheduledScan = getattr(models, "ScheduledScan", getattr(models, "ScanJob"))
Regulation = getattr(models, "Regulation")
RegulationSource = getattr(models, "RegulationSource", None)
TenantRssFeed = getattr(
    models,
    "TenantRssFeed",
    getattr(models, "RSSFeed", getattr(models, "RssFeed", None)),
)


@dataclass(frozen=True, slots=True)
class ScanSource:
    regulation_id: int
    source_url: str
    source_type: str = "url"
    tenant_id: int | None = None


def _has_attr(model_or_instance: Any, attr_name: str) -> bool:
    return hasattr(model_or_instance, attr_name)


def _set_if_present(instance: Any, attr_name: str, value: Any) -> None:
    if hasattr(instance, attr_name):
        setattr(instance, attr_name, value)


def _first_present(instance: Any, attr_names: Iterable[str]) -> Any:
    for attr_name in attr_names:
        value = getattr(instance, attr_name, None)
        if value:
            return value
    return None


async def _get_active_regulation_sources(session: AsyncSession) -> list[ScanSource]:
    sources: list[ScanSource] = []

    active_regulations = await queries.get_active_regulations(session)
    for regulation in active_regulations:
        source_url = getattr(regulation, "url", None)
        if source_url:
            sources.append(
                ScanSource(
                    regulation_id=regulation.id,
                    source_url=source_url,
                    source_type="url",
                    tenant_id=getattr(regulation, "tenant_id", None),
                )
            )

    if RegulationSource is not None:
        stmt = select(RegulationSource)
        if _has_attr(RegulationSource, "is_active"):
            stmt = stmt.where(RegulationSource.is_active.is_(True))

        result = await session.execute(stmt)
        for source in result.scalars().all():
            regulation_id = getattr(source, "regulation_id", None)
            source_url = _first_present(
                source,
                ("source_url", "url", "feed_url", "rss_url"),
            )
            if regulation_id is None or not source_url:
                continue

            sources.append(
                ScanSource(
                    regulation_id=regulation_id,
                    source_url=source_url,
                    source_type=getattr(source, "source_type", None) or "url",
                    tenant_id=getattr(source, "tenant_id", None),
                )
            )

    return _dedupe_sources(sources)


async def _get_tenant_rss_sources(session: AsyncSession) -> list[ScanSource]:
    if TenantRssFeed is None:
        return []

    stmt = select(TenantRssFeed)
    if _has_attr(TenantRssFeed, "is_active"):
        stmt = stmt.where(TenantRssFeed.is_active.is_(True))
    elif _has_attr(TenantRssFeed, "enabled"):
        stmt = stmt.where(TenantRssFeed.enabled.is_(True))

    result = await session.execute(stmt)
    sources: list[ScanSource] = []

    for feed in result.scalars().all():
        regulation_id = getattr(feed, "regulation_id", None)
        source_url = _first_present(
            feed,
            ("rss_url", "feed_url", "source_url", "url"),
        )
        if regulation_id is None or not source_url:
            continue

        sources.append(
            ScanSource(
                regulation_id=regulation_id,
                source_url=source_url,
                source_type="rss",
                tenant_id=getattr(feed, "tenant_id", None),
            )
        )

    return _dedupe_sources(sources)


def _dedupe_sources(sources: Iterable[ScanSource]) -> list[ScanSource]:
    deduped: dict[tuple[int, str, str, int | None], ScanSource] = {}
    for source in sources:
        key = (
            source.regulation_id,
            source.source_url,
            source.source_type,
            source.tenant_id,
        )
        deduped[key] = source
    return list(deduped.values())


async def _pending_scan_exists(
    session: AsyncSession,
    source: ScanSource,
) -> bool:
    filters = [ScheduledScan.status == "pending"]

    if _has_attr(ScheduledScan, "regulation_id"):
        filters.append(ScheduledScan.regulation_id == source.regulation_id)
    if _has_attr(ScheduledScan, "source_url"):
        filters.append(ScheduledScan.source_url == source.source_url)
    if _has_attr(ScheduledScan, "source_type"):
        filters.append(ScheduledScan.source_type == source.source_type)
    if _has_attr(ScheduledScan, "tenant_id") and source.tenant_id is not None:
        filters.append(ScheduledScan.tenant_id == source.tenant_id)

    stmt = select(ScheduledScan.id).where(and_(*filters)).limit(1)
    result = await session.execute(stmt)
    return result.scalar_one_or_none() is not None


async def _create_scheduled_scan(
    session: AsyncSession,
    source: ScanSource,
) -> Any:
    if ScheduledScan.__name__ == "ScanJob":
        return await queries.create_scan_job(session, source.regulation_id)

    scan = ScheduledScan()
    _set_if_present(scan, "regulation_id", source.regulation_id)
    _set_if_present(scan, "source_url", source.source_url)
    _set_if_present(scan, "source_type", source.source_type)
    _set_if_present(scan, "tenant_id", source.tenant_id)
    _set_if_present(scan, "status", "pending")
    _set_if_present(scan, "scheduled_at", datetime.utcnow())
    _set_if_present(scan, "created_at", datetime.utcnow())
    _set_if_present(scan, "updated_at", datetime.utcnow())
    session.add(scan)
    await session.flush()
    return scan


async def discover_scan_sources(session: AsyncSession) -> list[ScanSource]:
    regulation_sources = await _get_active_regulation_sources(session)
    rss_sources = await _get_tenant_rss_sources(session)
    return _dedupe_sources([*regulation_sources, *rss_sources])


async def enqueue_pending_scans() -> dict[str, int]:
    summary = {
        "sources_discovered": 0,
        "scans_created": 0,
        "duplicates_skipped": 0,
        "failed": 0,
    }

    async with AsyncSessionLocal() as session:
        try:
            sources = await discover_scan_sources(session)
            summary["sources_discovered"] = len(sources)

            for source in sources:
                try:
                    if await _pending_scan_exists(session, source):
                        summary["duplicates_skipped"] += 1
                        continue

                    await _create_scheduled_scan(session, source)
                    summary["scans_created"] += 1
                except Exception:
                    summary["failed"] += 1
                    logger.exception(
                        "Failed to enqueue scan for regulation_id=%s source_url=%s",
                        source.regulation_id,
                        source.source_url,
                    )

            await session.commit()
            logger.info("Scheduler scan enqueue complete: %s", summary)
            return summary
        except Exception:
            await session.rollback()
            logger.exception("Scheduler scan enqueue job failed.")
            raise


async def recurring_scan_insertion_job() -> dict[str, int]:
    logger.info("Scheduler recurring scan insertion job started.")
    return await enqueue_pending_scans()


def create_scheduler() -> AsyncIOScheduler:
    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        recurring_scan_insertion_job,
        trigger=IntervalTrigger(minutes=SCAN_INTERVAL_MINUTES),
        id=SCHEDULER_JOB_ID,
        name="PolicyWatch recurring scan insertion",
        replace_existing=True,
        coalesce=True,
        max_instances=1,
        misfire_grace_time=300,
    )
    return scheduler


def init_scheduler(start: bool = True) -> AsyncIOScheduler:
    scheduler = create_scheduler()
    if start:
        scheduler.start()
        logger.info(
            "PolicyWatch scheduler started with %s minute interval.",
            SCAN_INTERVAL_MINUTES,
        )
    return scheduler


async def shutdown_scheduler(scheduler: AsyncIOScheduler) -> None:
    if scheduler.running:
        scheduler.shutdown(wait=False)
        logger.info("PolicyWatch scheduler stopped.")

