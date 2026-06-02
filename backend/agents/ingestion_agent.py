import logging
from sqlalchemy.ext.asyncio import AsyncSession
from db.session import AsyncSessionLocal
from db import queries
from services import fetcher, parser, hashing, gemini_service

logger = logging.getLogger(__name__)

async def run_ingestion_cycle():
    """
    Main entry point for the ingestion agent cycle.
    Fetches all pending scan jobs and processes them one by one.
    """
    async with AsyncSessionLocal() as session:
        jobs = await queries.get_pending_scan_jobs(session)
        if not jobs:
            logger.info("No pending scan jobs found.")
            return

        for job in jobs:
            try:
                await process_scan_job(session, job)
            except Exception as e:
                logger.error(f"Error processing job {job.id}: {e}")
                # Attempt to mark as failed
                try:
                    await queries.update_scan_job_status(session, job.id, "failed")
                    await session.commit()
                except Exception:
                    logger.critical(f"Failed to update status for job {job.id} to 'failed'")

async def process_scan_job(session: AsyncSession, job):
    """
    Orchestrates the lifecycle of a single scan job.
    """
    # 1. Update job status to running
    await queries.update_scan_job_status(session, job.id, "running")
    await session.commit()

    # 2. Fetch regulation metadata
    regulation = await queries.get_regulation(session, job.regulation_id)
    if not regulation:
        logger.error(f"Regulation {job.regulation_id} not found for job {job.id}")
        await queries.update_scan_job_status(session, job.id, "failed")
        await session.commit()
        return

    logger.info(f"Processing regulation: {regulation.name} ({regulation.url})")

    try:
        # 3. Fetch content
        raw_content = await fetcher.fetch_url(regulation.url)
        
        # 4. Determine content type and parse/hash
        is_pdf_content = parser.is_pdf(raw_content)
        
        content_text = None
        gemini_file_id = None
        
        if is_pdf_content:
            # For PDFs, we hash the raw bytes
            current_hash = hashing.compute_sha256(raw_content)
            if current_hash != regulation.last_hash:
                # Only upload to Gemini if hash changed to save resources
                gemini_file_id = await gemini_service.upload_pdf_to_gemini(
                    raw_content, 
                    f"reg_{regulation.id}_{current_hash[:8]}"
                )
        else:
            # For HTML, we hash the normalized text
            content_text = parser.parse_html_to_text(raw_content)
            current_hash = hashing.compute_sha256(content_text)

        # 5. Check for changes
        if current_hash != regulation.last_hash:
            logger.info(f"Change detected for {regulation.name}. Previous hash: {regulation.last_hash}, New hash: {current_hash}")
            
            # Create Snapshot
            snapshot = await queries.create_regulation_snapshot(
                session, 
                regulation.id, 
                current_hash, 
                content_text, 
                gemini_file_id
            )
            
            # Create Change record
            await queries.create_regulation_change(session, regulation.id, snapshot.id)
            
            # Create Workflow Run for Stage 2
            workflow = await queries.create_workflow_run(
                session,
                regulation.id,
                snapshot_id=snapshot.id,
                scan_id=job.id,
            )
            
            # Update Regulation last_hash
            await queries.update_regulation_hash(session, regulation.id, current_hash)
            
            # Commit changes before NOTIFY to ensure data is visible to the listener
            await session.commit()
            
            # Emit NOTIFY event
            await queries.emit_workflow_event(session, workflow.id)
            await session.commit()
        else:
            logger.info(f"No changes detected for {regulation.name}.")

        # 6. Mark job as completed
        await queries.update_scan_job_status(session, job.id, "completed")
        await session.commit()

    except Exception as e:
        logger.error(f"Processing failed for regulation {regulation.id}: {e}")
        await queries.update_scan_job_status(session, job.id, "failed")
        await session.commit()
        raise
