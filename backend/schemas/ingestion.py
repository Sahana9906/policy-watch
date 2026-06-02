from typing import Literal

from pydantic import BaseModel, Field


class UrlIngestionRequest(BaseModel):
    regulation_id: int = Field(..., gt=0)
    source_url: str = Field(..., min_length=8, max_length=2048)


class IngestionResponse(BaseModel):
    status: Literal["changed", "no_change", "failed"]
    regulation_id: int
    content_hash: str
    previous_hash: str | None = None
    snapshot_id: int | None = None
    workflow_run_id: int | None = None
    content_kind: Literal["html", "pdf"] | None = None
    message: str | None = None


class IngestionErrorResponse(BaseModel):
    detail: str

