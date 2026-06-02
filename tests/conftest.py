import os
import sys
from pathlib import Path
from typing import AsyncIterator

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

PROJECT_ROOT = Path(__file__).resolve().parents[1]
BACKEND_ROOT = PROJECT_ROOT / "backend"
sys.path.insert(0, str(BACKEND_ROOT))

from db.models import Regulation, ScanJob
from db.session import Base


TEST_DATABASE_URL = os.getenv(
    "TEST_DATABASE_URL",
    "sqlite+aiosqlite:///:memory:",
)


@pytest.fixture
def sample_html_v1() -> bytes:
    return b"""
    <!doctype html>
    <html>
      <body>
        <h1>PolicyWatch Test Regulation</h1>
        <p>Covered entities must report incidents within 72 hours.</p>
      </body>
    </html>
    """


@pytest.fixture
def sample_html_v2() -> bytes:
    return b"""
    <!doctype html>
    <html>
      <body>
        <h1>PolicyWatch Test Regulation</h1>
        <p>Covered entities must report incidents within 24 hours.</p>
        <p>Audit evidence must be retained for three years.</p>
      </body>
    </html>
    """


@pytest.fixture
def sample_pdf() -> bytes:
    return b"%PDF-1.7\n1 0 obj\n<< /Type /Catalog >>\nendobj\n%%EOF"


@pytest.fixture
def mock_workflow_payload() -> dict[str, object]:
    return {
        "event_type": "workflow_run_updated",
        "workflow_run_id": 100,
        "regulation_id": 10,
        "snapshot_id": 50,
        "status": "processing",
        "current_stage": "regulation_detected",
    }


@pytest.fixture
def fake_hashes() -> dict[str, str]:
    return {
        "v1": "hash-regulation-v1",
        "v2": "hash-regulation-v2",
        "empty": "e3b0c44298fc1c149afbf4c8996fb924",
    }


@pytest_asyncio.fixture
async def test_engine():
    engine = create_async_engine(TEST_DATABASE_URL, echo=False)
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.drop_all)
        await connection.run_sync(Base.metadata.create_all)

    try:
        yield engine
    finally:
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.drop_all)
        await engine.dispose()


@pytest_asyncio.fixture
async def db_session(test_engine) -> AsyncIterator[AsyncSession]:
    session_factory = async_sessionmaker(
        bind=test_engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autoflush=False,
    )
    async with session_factory() as session:
        yield session


@pytest_asyncio.fixture
async def seeded_regulation(db_session: AsyncSession) -> Regulation:
    regulation = Regulation(
        name="PolicyWatch Test Regulation",
        url="https://example.test/regulation",
        is_active=True,
    )
    db_session.add(regulation)
    await db_session.commit()
    await db_session.refresh(regulation)
    return regulation


@pytest_asyncio.fixture
async def pending_scan_job(
    db_session: AsyncSession,
    seeded_regulation: Regulation,
) -> ScanJob:
    scan_job = ScanJob(
        regulation_id=seeded_regulation.id,
        status="pending",
    )
    db_session.add(scan_job)
    await db_session.commit()
    await db_session.refresh(scan_job)
    return scan_job
