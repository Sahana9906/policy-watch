import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from db.session import AsyncSessionLocal
from db import queries

logger = logging.getLogger(__name__)

async def enqueue_regulation_scans():
    async with AsyncSessionLocal() as session:
        try:
            active_regulations = await queries.get_active_regulations(session)
            if not active_regulations:
                return

            count = 0
            for reg in active_regulations:
                existing = await queries.get_existing_active_job(session, reg.id)
                if existing:
                    logger.info(
                        "Skipping reg %s - job %s already %s",
                        reg.id,
                        existing.id,
                        existing.status,
                    )
                    continue
                await queries.create_scan_job(session, reg.id)
                count += 1

            await session.commit()
            logger.info("Scheduler: Enqueued %s scan jobs.", count)
        except Exception as e:
            logger.error("Scheduler error: %s", e)
            await session.rollback()

def init_scheduler():
    """
    Initializes the AsyncIOScheduler and adds periodic jobs.
    """
    scheduler = AsyncIOScheduler()
    
    # Schedule the enqueuing task
    # For hackathon purposes, we use a frequent interval (e.g., every 5 minutes)
    scheduler.add_job(enqueue_regulation_scans, 'interval', minutes=5)
    
    return scheduler
