import asyncio
import json
import logging
import re
from dataclasses import asdict, dataclass, field
from enum import StrEnum
from typing import Any

from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from core.workflow_states import WorkflowStage
from db import models
from db.models import RegulationSnapshot, WorkflowRun
from integrations.gemini_client import GeminiClient, GeminiClientError
from services.workflow_service import WorkflowService

logger = logging.getLogger(__name__)

EXTRACTION_RETRY_ATTEMPTS = 2
EXTRACTION_RETRY_DELAY_SECONDS = 0.75
MAX_TEXT_CHARS = 18000

RequirementModel = getattr(
    models,
    "ComplianceRequirement",
    getattr(models, "Requirement", None),
)


class RequirementPriority(StrEnum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class RequirementCategory(StrEnum):
    REPORTING = "reporting"
    SECURITY = "security"
    PRIVACY = "privacy"
    GOVERNANCE = "governance"
    OPERATIONAL = "operational"
    RECORDKEEPING = "recordkeeping"
    OTHER = "other"


class RequirementExtractionError(Exception):
    pass


@dataclass(slots=True)
class ComplianceRequirementDTO:
    title: str
    description: str
    category: str = RequirementCategory.OTHER.value
    priority: str = RequirementPriority.MEDIUM.value
    deadline: str | None = None
    evidence_required: list[str] = field(default_factory=list)
    source_excerpt: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class RequirementExtractionResult:
    workflow_run_id: int | None
    snapshot_id: int
    regulation_id: int
    requirements: list[ComplianceRequirementDTO]
    persisted_count: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "workflow_run_id": self.workflow_run_id,
            "snapshot_id": self.snapshot_id,
            "regulation_id": self.regulation_id,
            "requirements": [item.as_dict() for item in self.requirements],
            "persisted_count": self.persisted_count,
        }


class RequirementExtractionService:
    def __init__(
        self,
        session: AsyncSession,
        gemini_client: GeminiClient | None = None,
    ):
        self.session = session
        self.gemini_client = gemini_client or GeminiClient()
        self.workflow_service = WorkflowService(session)

    async def extract_for_workflow(
        self,
        workflow_run_id: int,
    ) -> RequirementExtractionResult:
        workflow_run = await self.workflow_service.get_workflow_run(workflow_run_id)
        snapshot_id = getattr(workflow_run, "snapshot_id", None)

        if snapshot_id is None:
            snapshot = await self._get_latest_snapshot(workflow_run.regulation_id)
        else:
            snapshot = await self._get_snapshot(snapshot_id)

        result = await self.extract_from_snapshot(
            snapshot_id=snapshot.id,
            workflow_run_id=workflow_run_id,
        )

        await self.workflow_service.update_stage(
            workflow_run_id,
            WorkflowStage.REQUIREMENTS_EXTRACTED,
            emit_event=True,
        )
        return result

    async def extract_from_snapshot(
        self,
        snapshot_id: int,
        workflow_run_id: int | None = None,
    ) -> RequirementExtractionResult:
        snapshot = await self._get_snapshot(snapshot_id)
        prompt = self._build_extraction_prompt(snapshot)
        raw_response = await self._generate_with_retry(prompt)
        requirements = self._normalize_requirements(raw_response)
        persisted_count = await self._persist_requirements(
            snapshot=snapshot,
            workflow_run_id=workflow_run_id,
            requirements=requirements,
        )

        logger.info(
            "Extracted requirements snapshot_id=%s regulation_id=%s count=%s persisted=%s",
            snapshot.id,
            snapshot.regulation_id,
            len(requirements),
            persisted_count,
        )

        return RequirementExtractionResult(
            workflow_run_id=workflow_run_id,
            snapshot_id=snapshot.id,
            regulation_id=snapshot.regulation_id,
            requirements=requirements,
            persisted_count=persisted_count,
        )

    async def _get_snapshot(self, snapshot_id: int) -> RegulationSnapshot:
        result = await self.session.execute(
            select(RegulationSnapshot).where(RegulationSnapshot.id == snapshot_id)
        )
        snapshot = result.scalar_one_or_none()
        if snapshot is None:
            raise RequirementExtractionError(f"Snapshot not found: {snapshot_id}")
        return snapshot

    async def _get_latest_snapshot(self, regulation_id: int) -> RegulationSnapshot:
        result = await self.session.execute(
            select(RegulationSnapshot)
            .where(RegulationSnapshot.regulation_id == regulation_id)
            .order_by(desc(RegulationSnapshot.created_at), desc(RegulationSnapshot.id))
            .limit(1)
        )
        snapshot = result.scalar_one_or_none()
        if snapshot is None:
            raise RequirementExtractionError(
                f"No snapshot found for regulation_id={regulation_id}"
            )
        return snapshot

    def _build_extraction_prompt(self, snapshot: RegulationSnapshot) -> str:
        if snapshot.content_text:
            source_text = snapshot.content_text[:MAX_TEXT_CHARS]
            source_hint = "Regulation text is provided below."
        elif snapshot.gemini_file_id:
            source_text = f"Gemini file reference: {snapshot.gemini_file_id}"
            source_hint = "Use the referenced uploaded regulation PDF."
        else:
            raise RequirementExtractionError(
                f"Snapshot {snapshot.id} has no text or Gemini file reference."
            )

        return f"""
You are PolicyWatch, an AI compliance analyst.

Extract structured compliance requirements from the regulation source.

Return ONLY valid JSON with this shape:
{{
  "requirements": [
    {{
      "title": "short obligation title",
      "description": "clear requirement in plain language",
      "category": "reporting | security | privacy | governance | operational | recordkeeping | other",
      "priority": "high | medium | low",
      "deadline": "deadline or null",
      "evidence_required": ["evidence artifact 1", "evidence artifact 2"],
      "source_excerpt": "short supporting excerpt"
    }}
  ]
}}

Guidelines:
- Extract obligations, controls, deadlines, reporting duties, evidence duties, and operational actions.
- Prefer 5 to 12 high-signal requirements.
- Do not invent obligations not supported by the source.
- Keep source_excerpt short.

{source_hint}

SOURCE:
{source_text}
""".strip()

    async def _generate_with_retry(self, prompt: str) -> str:
        last_error: Exception | None = None

        for attempt in range(1, EXTRACTION_RETRY_ATTEMPTS + 1):
            try:
                return await self.gemini_client.generate_text(prompt)
            except GeminiClientError as exc:
                last_error = exc
                logger.warning(
                    "Gemini extraction failed attempt=%s/%s error=%s",
                    attempt,
                    EXTRACTION_RETRY_ATTEMPTS,
                    exc,
                )
                if attempt < EXTRACTION_RETRY_ATTEMPTS:
                    await asyncio.sleep(EXTRACTION_RETRY_DELAY_SECONDS)

        raise RequirementExtractionError(
            f"Gemini requirement extraction failed: {last_error}"
        )

    def _normalize_requirements(
        self,
        raw_response: str,
    ) -> list[ComplianceRequirementDTO]:
        payload = self._parse_json_payload(raw_response)
        raw_requirements = payload.get("requirements", [])

        if not isinstance(raw_requirements, list):
            raise RequirementExtractionError(
                "Gemini response field 'requirements' must be a list."
            )

        requirements: list[ComplianceRequirementDTO] = []
        for item in raw_requirements:
            if not isinstance(item, dict):
                continue

            title = str(item.get("title") or "").strip()
            description = str(item.get("description") or "").strip()
            if not title or not description:
                continue

            requirements.append(
                ComplianceRequirementDTO(
                    title=title[:240],
                    description=description,
                    category=self._normalize_category(item.get("category")),
                    priority=self._normalize_priority(item.get("priority")),
                    deadline=item.get("deadline") or None,
                    evidence_required=self._normalize_evidence(
                        item.get("evidence_required")
                    ),
                    source_excerpt=(item.get("source_excerpt") or None),
                )
            )

        if not requirements:
            raise RequirementExtractionError(
                "No valid requirements were extracted from Gemini response."
            )

        return requirements

    @staticmethod
    def _parse_json_payload(raw_response: str) -> dict[str, Any]:
        cleaned = raw_response.strip()
        fenced_match = re.search(
            r"```(?:json)?\s*(.*?)```",
            cleaned,
            flags=re.DOTALL | re.IGNORECASE,
        )
        if fenced_match:
            cleaned = fenced_match.group(1).strip()

        try:
            payload = json.loads(cleaned)
        except json.JSONDecodeError as exc:
            raise RequirementExtractionError(
                "Gemini response was not valid JSON."
            ) from exc

        if not isinstance(payload, dict):
            raise RequirementExtractionError("Gemini response must be a JSON object.")

        return payload

    @staticmethod
    def _normalize_category(value: Any) -> str:
        normalized = str(value or RequirementCategory.OTHER.value).lower().strip()
        valid = {item.value for item in RequirementCategory}
        return normalized if normalized in valid else RequirementCategory.OTHER.value

    @staticmethod
    def _normalize_priority(value: Any) -> str:
        normalized = str(value or RequirementPriority.MEDIUM.value).lower().strip()
        valid = {item.value for item in RequirementPriority}
        return normalized if normalized in valid else RequirementPriority.MEDIUM.value

    @staticmethod
    def _normalize_evidence(value: Any) -> list[str]:
        if value is None:
            return []
        if isinstance(value, str):
            return [value]
        if isinstance(value, list):
            return [str(item) for item in value if str(item).strip()]
        return []

    async def _persist_requirements(
        self,
        snapshot: RegulationSnapshot,
        workflow_run_id: int | None,
        requirements: list[ComplianceRequirementDTO],
    ) -> int:
        if RequirementModel is None:
            logger.info(
                "Requirement model not available; returning extracted DTOs without persistence."
            )
            return 0

        persisted_count = 0
        try:
            for requirement in requirements:
                db_requirement = RequirementModel()
                self._set_if_present(db_requirement, "regulation_id", snapshot.regulation_id)
                self._set_if_present(db_requirement, "snapshot_id", snapshot.id)
                self._set_if_present(db_requirement, "workflow_run_id", workflow_run_id)
                self._set_if_present(db_requirement, "requirement_text", requirement.description)
                self._set_if_present(db_requirement, "impacted_domain", requirement.category)
                self._set_if_present(db_requirement, "severity", requirement.priority)
                self._set_if_present(db_requirement, "title", requirement.title)
                self._set_if_present(db_requirement, "description", requirement.description)
                self._set_if_present(db_requirement, "category", requirement.category)
                self._set_if_present(db_requirement, "priority", requirement.priority)
                self._set_if_present(db_requirement, "deadline", requirement.deadline)
                self._set_if_present(
                    db_requirement,
                    "evidence_required",
                    json.dumps(requirement.evidence_required),
                )
                self._set_if_present(
                    db_requirement,
                    "source_excerpt",
                    requirement.source_excerpt,
                )
                self.session.add(db_requirement)
                persisted_count += 1

            await self.session.commit()
            return persisted_count
        except Exception:
            await self.session.rollback()
            logger.exception(
                "Failed to persist extracted requirements snapshot_id=%s",
                snapshot.id,
            )
            raise

    @staticmethod
    def _set_if_present(instance: Any, attr_name: str, value: Any) -> None:
        if hasattr(instance, attr_name):
            setattr(instance, attr_name, value)


async def extract_requirements(
    workflow_run_id: int,
    session: AsyncSession | None = None,
) -> RequirementExtractionResult:
    if session is not None:
        return await RequirementExtractionService(session).extract_for_workflow(
            workflow_run_id
        )

    from db.session import AsyncSessionLocal

    async with AsyncSessionLocal() as db_session:
        return await RequirementExtractionService(db_session).extract_for_workflow(
            workflow_run_id
        )
