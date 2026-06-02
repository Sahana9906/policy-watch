import logging
from typing import Annotated

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile, status
from sqlalchemy.ext.asyncio import AsyncSession

from db.session import get_db
from schemas.ingestion import IngestionResponse, UrlIngestionRequest
from services.ingestion_service import IngestionResult, IngestionService, IngestionServiceError
from validators.file_validator import FileValidationError, validate_pdf_upload
from validators.url_validator import UrlValidationError, validate_source_url

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/ingestion", tags=["Ingestion"])

try:
    import multipart  # noqa: F401

    MULTIPART_AVAILABLE = True
except ImportError:
    MULTIPART_AVAILABLE = False


def _to_response(result: IngestionResult) -> IngestionResponse:
    return IngestionResponse(
        status=result.status.value,
        regulation_id=result.regulation_id,
        content_hash=result.content_hash,
        previous_hash=result.previous_hash,
        snapshot_id=result.snapshot_id,
        workflow_run_id=result.workflow_run_id,
        content_kind=result.content_kind.value if result.content_kind else None,
        message=result.message,
    )


@router.post(
    "/url",
    response_model=IngestionResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def ingest_url(
    request: UrlIngestionRequest,
    db: AsyncSession = Depends(get_db),
) -> IngestionResponse:
    try:
        source_url = validate_source_url(request.source_url)
        result = await IngestionService(db).ingest_url(
            regulation_id=request.regulation_id,
            source_url=source_url,
        )
        return _to_response(result)
    except UrlValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        ) from exc
    except IngestionServiceError as exc:
        logger.exception(
            "URL ingestion failed regulation_id=%s source_url=%s",
            request.regulation_id,
            request.source_url,
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc
    except Exception as exc:
        logger.exception(
            "Unexpected URL ingestion failure regulation_id=%s source_url=%s",
            request.regulation_id,
            request.source_url,
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="URL ingestion failed.",
        ) from exc


if MULTIPART_AVAILABLE:
    @router.post(
        "/pdf",
        response_model=IngestionResponse,
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def ingest_pdf(
        regulation_id: Annotated[int, Form(gt=0)],
        file: Annotated[UploadFile, File()],
        db: AsyncSession = Depends(get_db),
    ) -> IngestionResponse:
        try:
            pdf_bytes = await validate_pdf_upload(file)
            result = await IngestionService(db).ingest_pdf(
                regulation_id=regulation_id,
                pdf_bytes=pdf_bytes,
                source_name=file.filename or "uploaded_regulation.pdf",
            )
            return _to_response(result)
        except FileValidationError as exc:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=str(exc),
            ) from exc
        except IngestionServiceError as exc:
            logger.exception(
                "PDF ingestion failed regulation_id=%s filename=%s",
                regulation_id,
                file.filename,
            )
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=str(exc),
            ) from exc
        except Exception as exc:
            logger.exception(
                "Unexpected PDF ingestion failure regulation_id=%s filename=%s",
                regulation_id,
                file.filename,
            )
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="PDF ingestion failed.",
            ) from exc
