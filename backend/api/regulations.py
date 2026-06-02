import logging

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db import queries
from db.models import Regulation
from db.session import get_db

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/regulations", tags=["Regulations"])


class RegulationCreate(BaseModel):
    name: str
    url: str


class RegulationResponse(BaseModel):
    id: int
    name: str
    url: str
    is_active: bool
    last_hash: str | None


@router.get("/", response_model=list[RegulationResponse])
async def list_regulations(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Regulation).order_by(Regulation.created_at.desc()))
    return result.scalars().all()


@router.post(
    "/",
    response_model=RegulationResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_regulation(
    body: RegulationCreate,
    db: AsyncSession = Depends(get_db),
):
    reg = Regulation(name=body.name, url=body.url, is_active=True)
    db.add(reg)
    await db.commit()
    await db.refresh(reg)
    logger.info("Created regulation id=%s name=%s", reg.id, reg.name)
    return reg


@router.delete("/{reg_id}", status_code=status.HTTP_204_NO_CONTENT)
async def deactivate_regulation(reg_id: int, db: AsyncSession = Depends(get_db)):
    reg = await db.get(Regulation, reg_id)
    if not reg:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Regulation not found",
        )
    reg.is_active = False
    await db.commit()


@router.post("/{reg_id}/scan", status_code=status.HTTP_202_ACCEPTED)
async def trigger_scan(reg_id: int, db: AsyncSession = Depends(get_db)):
    """Manually trigger a scan for a specific regulation immediately."""
    reg = await db.get(Regulation, reg_id)
    if not reg:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Regulation not found",
        )
    job = await queries.create_scan_job(db, reg_id)
    await db.commit()
    return {"scan_job_id": job.id, "regulation_id": reg_id, "status": "pending"}
