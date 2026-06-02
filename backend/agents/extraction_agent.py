"""
Extraction Agent
================
Stage transition: ANALYSIS_COMPLETE -> REQUIREMENTS_EXTRACTED

Loads the ChangeAnalysis produced by change_analyzer_agent, sends the
regulation snapshot text to Gemini with an extraction-focused prompt, and
persists each extracted obligation as a ComplianceRequirement row.
"""

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
from db import queries
from db.models import ChangeAnalysis, ComplianceRequirement, RegulationSnapshot, WorkflowRun
from db.session import AsyncSessionLocal
from integrations.gemini_client import GeminiClient, GeminiClientError
from services.workflow_service import WorkflowService

logger = logging.getLogger(__name__)

EXTRACTION_RETRY_ATTEMPTS = 2
EXTRACTION_RETRY_DELAY_SECONDS = 0.75
MAX_SNAPSHOT_CHARS = 16000


class RequirementCategory(StrEnum):
    REPORTING = "reporting"
    SECURITY = "security"
    PRIVACY = "privacy"
    GOVERNANCE = "governance"
    OPERATIONAL = "operational"
    RECORDKEEPING = "recordkeeping"
    OTHER = "other"


class RequirementPriority(StrEnum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class ExtractionAgentError(Exception):
    pass


@dataclass(slots=True)
class ExtractedRequirement:
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
class ExtractionResult:
    workflow_run_id: int
    regulation_id: int
    snapshot_id: int
    requirements: list[ExtractedRequirement]
    persisted_ids: list[int]

    def as_dict(self) -> dict[str, Any]:
        return {
            "workflow_run_id": self.workflow_run_id,
            "regulation_id": self.regulation_id,
            "snapshot_id": self.snapshot_id,
            "requirement_count": len(self.requirements),
            "persisted_ids": self.persisted_ids,
        }


class ExtractionAgent:
    name = "extraction_agent"
    description = (
        "Reads the ChangeAnalysis for a workflow run, re-prompts Gemini to extract "
        "discrete compliance obligations, and persists them as ComplianceRequirement records."
    )

    def __init__(
        self,
        session: AsyncSession,
        gemini_client: GeminiClient | None = None,
    ) -> None:
        self.session = session
        self.gemini_client = gemini_client or GeminiClient()
        self.workflow_service = WorkflowService(session)

    async def run(self, workflow_run_id: int) -> ExtractionResult:
        workflow_run = await self.workflow_service.get_workflow_run(workflow_run_id)
        regulation_id = workflow_run.regulation_id

        logger.info(
            "ExtractionAgent started workflow_run_id=%s regulation_id=%s",
            workflow_run_id,
            regulation_id,
        )

        try:
            change_analysis = await self._load_change_analysis(workflow_run_id)
            snapshot = await self._resolve_snapshot(workflow_run, regulation_id)
            prompt = self._build_prompt(snapshot=snapshot, change_analysis=change_analysis)
            raw_response = await self._generate_with_retry(prompt, workflow_run_id)
            requirements = self._parse_requirements(raw_response)
            persisted_ids = await self._persist_requirements(
                regulation_id=regulation_id,
                workflow_run_id=workflow_run_id,
                snapshot_id=snapshot.id,
                requirements=requirements,
            )

            await self.workflow_service.update_stage(
                workflow_run_id,
                WorkflowStage.REQUIREMENTS_EXTRACTED,
                emit_event=True,
            )

            logger.info(
                "ExtractionAgent completed workflow_run_id=%s requirements=%s persisted=%s",
                workflow_run_id,
                len(requirements),
                len(persisted_ids),
            )

            return ExtractionResult(
                workflow_run_id=workflow_run_id,
                regulation_id=regulation_id,
                snapshot_id=snapshot.id,
                requirements=requirements,
                persisted_ids=persisted_ids,
            )
        except Exception as exc:
            logger.exception(
                "ExtractionAgent failed workflow_run_id=%s error=%s",
                workflow_run_id,
                exc,
            )
            await self.workflow_service.mark_failed(workflow_run_id, str(exc))
            raise

    async def _load_change_analysis(self, workflow_run_id: int) -> ChangeAnalysis:
        result = await self.session.execute(
            select(ChangeAnalysis)
            .where(ChangeAnalysis.workflow_run_id == workflow_run_id)
            .order_by(desc(ChangeAnalysis.created_at))
            .limit(1)
        )
        analysis = result.scalar_one_or_none()
        if analysis is None:
            raise ExtractionAgentError(
                f"No ChangeAnalysis found for workflow_run_id={workflow_run_id}."
            )
        return analysis

    async def _resolve_snapshot(
        self,
        workflow_run: WorkflowRun,
        regulation_id: int,
    ) -> RegulationSnapshot:
        snapshot_id = getattr(workflow_run, "snapshot_id", None)
        if snapshot_id is not None:
            snapshot = await queries.get_regulation_snapshot(self.session, snapshot_id)
        else:
            snapshot = await queries.get_latest_snapshot(self.session, regulation_id)

        if snapshot is None:
            raise ExtractionAgentError(
                f"No snapshot found for workflow_run_id={workflow_run.id}."
            )
        if not snapshot.content_text:
            raise ExtractionAgentError(
                f"Snapshot {snapshot.id} has no text content; cannot extract requirements."
            )
        return snapshot

    def _build_prompt(
        self,
        snapshot: RegulationSnapshot,
        change_analysis: ChangeAnalysis,
    ) -> str:
        changed_clause_context = ""
        if change_analysis.changed_clauses:
            lines = []
            for clause in change_analysis.changed_clauses[:10]:
                if isinstance(clause, dict):
                    lines.append(
                        f"  - {clause.get('clause_title', '')}: "
                        f"{clause.get('compliance_impact', '')} "
                        f"(type: {clause.get('change_type', 'modified')})"
                    )
            changed_clause_context = "Changed clauses to prioritise:\n" + "\n".join(lines)

        return f"""
You are PolicyWatch's Extraction Agent.

A regulation has changed. The change analysis identified:
  Summary: {change_analysis.summary}
  Severity: {change_analysis.severity}
  Impacted domains: {', '.join(change_analysis.impacted_domains or [])}
  Recommended action: {change_analysis.recommended_next_action}

{changed_clause_context}

Your task: extract every discrete compliance obligation from the regulation text below.
An obligation is something a team MUST do, track, or evidence.

Return ONLY valid JSON; no markdown, no explanation:
{{
  "requirements": [
    {{
      "title": "short imperative title (max 80 chars)",
      "description": "plain-language 'you must...' statement",
      "category": "reporting | security | privacy | governance | operational | recordkeeping | other",
      "priority": "high | medium | low",
      "deadline": "ISO date string or null if unspecified",
      "evidence_required": ["list", "of", "audit", "artefacts"],
      "source_excerpt": "<=120 chars of verbatim source text"
    }}
  ]
}}

Aim for 5 to 15 requirements. Do not invent obligations not in the text.

REGULATION TEXT (snapshot_id={snapshot.id}):
{(snapshot.content_text or '')[:MAX_SNAPSHOT_CHARS]}
""".strip()

    async def _generate_with_retry(self, prompt: str, workflow_run_id: int) -> str:
        last_error: Exception | None = None
        for attempt in range(1, EXTRACTION_RETRY_ATTEMPTS + 1):
            try:
                response = await self.gemini_client.generate_text(prompt)
                logger.info(
                    "ExtractionAgent Gemini response received workflow_run_id=%s attempt=%s chars=%s",
                    workflow_run_id,
                    attempt,
                    len(response),
                )
                return response
            except GeminiClientError as exc:
                last_error = exc
                logger.warning(
                    "ExtractionAgent Gemini call failed workflow_run_id=%s attempt=%s/%s error=%s",
                    workflow_run_id,
                    attempt,
                    EXTRACTION_RETRY_ATTEMPTS,
                    exc,
                )
                if attempt < EXTRACTION_RETRY_ATTEMPTS:
                    await asyncio.sleep(EXTRACTION_RETRY_DELAY_SECONDS)
        raise ExtractionAgentError(
            f"Gemini requirement extraction failed after {EXTRACTION_RETRY_ATTEMPTS} attempts: {last_error}"
        )

    def _parse_requirements(self, raw_response: str) -> list[ExtractedRequirement]:
        payload = self._extract_json(raw_response)
        raw_list = payload.get("requirements", [])
        if not isinstance(raw_list, list):
            raise ExtractionAgentError("Gemini 'requirements' field must be a list.")

        requirements: list[ExtractedRequirement] = []
        for item in raw_list:
            if not isinstance(item, dict):
                continue
            title = str(item.get("title") or "").strip()
            description = str(item.get("description") or "").strip()
            if not title or not description:
                continue
            requirements.append(
                ExtractedRequirement(
                    title=title[:240],
                    description=description,
                    category=self._normalise_category(item.get("category")),
                    priority=self._normalise_priority(item.get("priority")),
                    deadline=item.get("deadline") or None,
                    evidence_required=self._normalise_list(item.get("evidence_required")),
                    source_excerpt=item.get("source_excerpt") or None,
                )
            )

        if not requirements:
            raise ExtractionAgentError("No valid requirements parsed from Gemini response.")
        return requirements

    @staticmethod
    def _extract_json(raw: str) -> dict[str, Any]:
        cleaned = raw.strip()
        match = re.search(r"```(?:json)?\s*(.*?)```", cleaned, flags=re.DOTALL | re.IGNORECASE)
        if match:
            cleaned = match.group(1).strip()
        try:
            payload = json.loads(cleaned)
        except json.JSONDecodeError as exc:
            raise ExtractionAgentError("Gemini response was not valid JSON.") from exc
        if not isinstance(payload, dict):
            raise ExtractionAgentError("Gemini response must be a JSON object.")
        return payload

    @staticmethod
    def _normalise_category(value: Any) -> str:
        v = str(value or "").lower().strip()
        return v if v in {c.value for c in RequirementCategory} else RequirementCategory.OTHER.value

    @staticmethod
    def _normalise_priority(value: Any) -> str:
        v = str(value or "").lower().strip()
        return v if v in {p.value for p in RequirementPriority} else RequirementPriority.MEDIUM.value

    @staticmethod
    def _normalise_list(value: Any) -> list[str]:
        if value is None:
            return []
        if isinstance(value, str):
            return [value.strip()] if value.strip() else []
        if isinstance(value, list):
            return [str(i).strip() for i in value if str(i).strip()]
        return []

    async def _persist_requirements(
        self,
        regulation_id: int,
        workflow_run_id: int,
        snapshot_id: int,
        requirements: list[ExtractedRequirement],
    ) -> list[int]:
        persisted_ids: list[int] = []
        try:
            for req in requirements:
                row = ComplianceRequirement(
                    regulation_id=regulation_id,
                    workflow_run_id=workflow_run_id,
                    requirement_text=req.description,
                    impacted_domain=req.category,
                    severity=req.priority,
                )
                for attr, val in [
                    ("title", req.title),
                    ("deadline", req.deadline),
                    ("evidence_required", json.dumps(req.evidence_required)),
                    ("source_excerpt", req.source_excerpt),
                    ("snapshot_id", snapshot_id),
                ]:
                    if hasattr(row, attr):
                        setattr(row, attr, val)
                self.session.add(row)
                await self.session.flush()
                persisted_ids.append(row.id)
            await self.session.commit()
            return persisted_ids
        except Exception:
            await self.session.rollback()
            logger.exception(
                "ExtractionAgent failed to persist requirements workflow_run_id=%s",
                workflow_run_id,
            )
            raise


async def run_extraction_agent(
    workflow_run_id: int,
    session: AsyncSession | None = None,
) -> ExtractionResult:
    if session is not None:
        return await ExtractionAgent(session).run(workflow_run_id)
    async with AsyncSessionLocal() as db:
        return await ExtractionAgent(db).run(workflow_run_id)


async def run(workflow_run_id: int) -> ExtractionResult:
    return await run_extraction_agent(workflow_run_id)
