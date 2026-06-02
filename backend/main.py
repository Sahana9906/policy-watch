import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

# Import components
from db.session import engine, Base
from api.workflow import router as workflow_router
from api.ingestion import router as ingestion_router
from api.regulations import router as regulations_router
from scheduler.scheduler import init_scheduler
from orchestrator.listener import start_listener
from agents.monitoring_agent import run_monitoring_cycle

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

logger = logging.getLogger(__name__)


async def ingestion_agent_loop():
    """
    Background loop for the ingestion agent
    to process pending scan jobs.
    """

    logger.info("Main: Ingestion Agent loop started.")

    while True:
        try:
            await run_monitoring_cycle()

        except Exception as e:
            logger.error(f"Main: Ingestion Agent cycle error: {e}")

        # Check for new work every 60 seconds
        await asyncio.sleep(60)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manages the lifecycle of background tasks
    and database initialization.
    """

    # 1. Initialize Database Tables
    logger.info("Main: Initializing database tables...")

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # 2. Start Scheduler
    scheduler = init_scheduler()
    scheduler.start()

    logger.info("Main: Scheduler started.")

    # 3. Start PostgreSQL Listener
    listener_task = asyncio.create_task(start_listener())

    # 4. Start Ingestion Agent Loop
    agent_task = asyncio.create_task(ingestion_agent_loop())

    yield

    # Shutdown logic
    logger.info("Main: Shutting down services...")

    scheduler.shutdown()

    listener_task.cancel()
    agent_task.cancel()

    # Wait for tasks to clean up
    await asyncio.gather(
        listener_task,
        agent_task,
        return_exceptions=True,
    )

    logger.info("Main: Shutdown complete.")


app = FastAPI(
    title="PolicyWatch - Regulation Monitoring Module",
    description="Automated system for monitoring regulation changes using Gemini and PostgreSQL events.",
    version="1.0.0",
    lifespan=lifespan,
)

# Register API routers
app.include_router(workflow_router)
app.include_router(ingestion_router)
app.include_router(regulations_router)


@app.get("/")
async def health_check():
    """
    Simple health check endpoint.
    """

    return {
        "status": "online",
        "module": "Regulation Monitoring",
        "version": "1.0.0",
    }


if __name__ == "__main__":

    import uvicorn

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
    )
    
    
