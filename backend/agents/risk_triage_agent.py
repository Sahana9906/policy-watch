"""
Risk Triage Agent
=================
Stage transition: REQUIREMENTS_EXTRACTED -> RISK_TRIAGED

Loads all ComplianceRequirement rows for a workflow run, sends the full
list to Gemini for comparative risk scoring, and writes the scores back
onto each requirement row.
"""

import asyncio
import json
import logging
import re
from dataclasses import asdict, dataclass
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.workflow_states import WorkflowStage
from db.models import ComplianceRequirement
from db.session import AsyncSessionLocal
from integrations.gemini_client import GeminiClient, GeminiClientError
from services.workflow_service import WorkflowService

logger = logging.getLogger(__name__)

TRIAGE_RETRY_ATTEMPTS = 2
TRIAGE_RETRY_DELAY_SECONDS = 0.75
MAX_REQUIREMENTS_FOR_SCORING = 30


class RiskTriageAgentError(Exception):
    pass


@dataclass(slots=True)
class RequirementRiskScore:
    requirement_id: int
    risk_score: float
    rank: int
    revised_severity: str
    remediation_urgency: str
    risk_rationale: str

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class RiskTriageResult:
    workflow_run_id: int
    regulation_id: int
    scored_requirements: list[RequirementRiskScore]
    critical_count: int
    high_count: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "workflow_run_id": self.workflow_run_id,
            "regulation_id": self.regulation_id,
            "scored_count": len(self.scored_requirements),
            "critical_count": self.critical_count,
            "high_count": self.high_count,
            "top_risk": self.scored_requirements[0].as_dict()
            if self.scored_requirements
            else None,
        }


class RiskTriageAgent:
    name = "risk_triage_agent"
    description = (
        "Loads all ComplianceRequirement records for a workflow run, sends them "
        "to Gemini for relative risk scoring, writes scores back to the DB, and "
        "advances the workflow to RISK_TRIAGED."
    )

    def __init__(
        self,
        session: AsyncSession,
        gemini_client: GeminiClient | None = None,
    ) -> None:
        self.session = session
        self.gemini_client = gemini_client or GeminiClient()
        self.workflow_service = WorkflowService(session)

    async def run(self, workflow_run_id: int) -> RiskTriageResult:
        workflow_run = await self.workflow_service.get_workflow_run(workflow_run_id)
        regulation_id = workflow_run.regulation_id

        logger.info(
            "RiskTriageAgent started workflow_run_id=%s regulation_id=%s",
            workflow_run_id,
            regulation_id,
        )

        try:
            requirements = await self._load_requirements(workflow_run_id)
            if not requirements:
                raise RiskTriageAgentError(
                    f"No ComplianceRequirements found for workflow_run_id={workflow_run_id}."
                )

            prompt = self._build_prompt(requirements)
            raw_response = await self._generate_with_retry(prompt, workflow_run_id)
            scores = self._parse_scores(raw_response, requirements)
            await self._persist_scores(scores)

            await self.workflow_service.update_stage(
                workflow_run_id,
                WorkflowStage.RISK_TRIAGED,
                emit_event=True,
            )

            critical_count = sum(1 for score in scores if score.revised_severity == "critical")
            high_count = sum(1 for score in scores if score.revised_severity == "high")

            logger.info(
                "RiskTriageAgent completed workflow_run_id=%s scored=%s critical=%s high=%s",
                workflow_run_id,
                len(scores),
                critical_count,
                high_count,
            )

            return RiskTriageResult(
                workflow_run_id=workflow_run_id,
                regulation_id=regulation_id,
                scored_requirements=scores,
                critical_count=critical_count,
                high_count=high_count,
            )
        except Exception as exc:
            logger.exception(
                "RiskTriageAgent failed workflow_run_id=%s error=%s",
                workflow_run_id,
                exc,
            )
            await self.workflow_service.mark_failed(workflow_run_id, str(exc))
            raise

    async def _load_requirements(self, workflow_run_id: int) -> list[ComplianceRequirement]:
        result = await self.session.execute(
            select(ComplianceRequirement)
            .where(ComplianceRequirement.workflow_run_id == workflow_run_id)
            .order_by(ComplianceRequirement.id)
            .limit(MAX_REQUIREMENTS_FOR_SCORING)
        )
        return list(result.scalars().all())

    def _build_prompt(self, requirements: list[ComplianceRequirement]) -> str:
        requirement_lines = []
        for req in requirements:
            requirement_lines.append(
                f"requirement_id: {req.id}\n"
                f"severity: {req.severity}\n"
                f"domain: {req.impacted_domain or 'general'}\n"
                f"obligation: {(req.requirement_text or '')[:700]}"
            )

        return f"""
You are PolicyWatch's Risk Triage Agent.

Score these compliance requirements against each other. Consider legal exposure,
operational burden, deadlines, evidence burden, and enforcement risk.

Return ONLY valid JSON; no markdown, no explanation:
{{
  "scores": [
    {{
      "requirement_id": <int>,
      "risk_score": <number from 0 to 100>,
      "rank": <1 for highest risk>,
      "revised_severity": "critical | high | medium | low",
      "remediation_urgency": "immediate | short_term | medium_term | low",
      "risk_rationale": "brief reason"
    }}
  ]
}}

REQUIREMENTS:
{chr(10).join(requirement_lines)}
""".strip()

    async def _generate_with_retry(self, prompt: str, workflow_run_id: int) -> str:
        last_error: Exception | None = None
        for attempt in range(1, TRIAGE_RETRY_ATTEMPTS + 1):
            try:
                response = await self.gemini_client.generate_text(prompt)
                logger.info(
                    "RiskTriageAgent Gemini response received workflow_run_id=%s attempt=%s chars=%s",
                    workflow_run_id,
                    attempt,
                    len(response),
                )
                return response
            except GeminiClientError as exc:
                last_error = exc
                logger.warning(
                    "RiskTriageAgent Gemini call failed workflow_run_id=%s attempt=%s/%s error=%s",
                    workflow_run_id,
                    attempt,
                    TRIAGE_RETRY_ATTEMPTS,
                    exc,
                )
                if attempt < TRIAGE_RETRY_ATTEMPTS:
                    await asyncio.sleep(TRIAGE_RETRY_DELAY_SECONDS)
        raise RiskTriageAgentError(
            f"Gemini risk triage failed after {TRIAGE_RETRY_ATTEMPTS} attempts: {last_error}"
        )

    def _parse_scores(
        self,
        raw_response: str,
        requirements: list[ComplianceRequirement],
    ) -> list[RequirementRiskScore]:
        payload = self._extract_json(raw_response)
        raw_scores = payload.get("scores", [])
        if not isinstance(raw_scores, list):
            raise RiskTriageAgentError("Gemini 'scores' field must be a list.")

        known_ids = {req.id for req in requirements}
        scores: list[RequirementRiskScore] = []
        for item in raw_scores:
            if not isinstance(item, dict):
                continue
            req_id = item.get("requirement_id")
            if req_id not in known_ids:
                continue
            scores.append(
                RequirementRiskScore(
                    requirement_id=int(req_id),
                    risk_score=self._normalise_score(item.get("risk_score")),
                    rank=max(1, int(item.get("rank") or len(scores) + 1)),
                    revised_severity=self._normalise_severity(item.get("revised_severity")),
                    remediation_urgency=self._normalise_urgency(item.get("remediation_urgency")),
                    risk_rationale=str(item.get("risk_rationale") or "").strip(),
                )
            )

        scored_ids = {score.requirement_id for score in scores}
        for req in requirements:
            if req.id not in scored_ids:
                scores.append(
                    RequirementRiskScore(
                        requirement_id=req.id,
                        risk_score=50.0,
                        rank=len(scores) + 1,
                        revised_severity=self._normalise_severity(req.severity),
                        remediation_urgency="medium_term",
                        risk_rationale="Fallback score because Gemini did not return this requirement.",
                    )
                )

        scores.sort(key=lambda score: (score.rank, -score.risk_score))
        return scores

    @staticmethod
    def _extract_json(raw: str) -> dict[str, Any]:
        cleaned = raw.strip()
        match = re.search(r"```(?:json)?\s*(.*?)```", cleaned, flags=re.DOTALL | re.IGNORECASE)
        if match:
            cleaned = match.group(1).strip()
        try:
            payload = json.loads(cleaned)
        except json.JSONDecodeError as exc:
            raise RiskTriageAgentError("Gemini response was not valid JSON.") from exc
        if not isinstance(payload, dict):
            raise RiskTriageAgentError("Gemini response must be a JSON object.")
        return payload

    @staticmethod
    def _normalise_score(value: Any) -> float:
        try:
            return min(100.0, max(0.0, float(value)))
        except (TypeError, ValueError):
            return 50.0

    @staticmethod
    def _normalise_severity(value: Any) -> str:
        v = str(value or "").lower().strip()
        return v if v in {"critical", "high", "medium", "low"} else "medium"

    @staticmethod
    def _normalise_urgency(value: Any) -> str:
        v = str(value or "").lower().strip()
        return v if v in {"immediate", "short_term", "medium_term", "low"} else "medium_term"

    async def _persist_scores(self, scores: list[RequirementRiskScore]) -> None:
        try:
            for score in scores:
                row = await self.session.get(ComplianceRequirement, score.requirement_id)
                if row is None:
                    continue
                row.severity = score.revised_severity
                metadata = {
                    "risk_score": score.risk_score,
                    "risk_rank": score.rank,
                    "remediation_urgency": score.remediation_urgency,
                    "risk_rationale": score.risk_rationale,
                }
                if hasattr(row, "risk_metadata"):
                    setattr(row, "risk_metadata", metadata)
                if hasattr(row, "risk_score"):
                    setattr(row, "risk_score", score.risk_score)
                if hasattr(row, "risk_rationale"):
                    setattr(row, "risk_rationale", score.risk_rationale)
            await self.session.commit()
        except Exception:
            await self.session.rollback()
            logger.exception("RiskTriageAgent failed to persist risk scores.")
            raise


async def run_risk_triage_agent(
    workflow_run_id: int,
    session: AsyncSession | None = None,
) -> RiskTriageResult:
    if session is not None:
        return await RiskTriageAgent(session).run(workflow_run_id)
    async with AsyncSessionLocal() as db:
        return await RiskTriageAgent(db).run(workflow_run_id)


async def run(workflow_run_id: int) -> RiskTriageResult:
    return await run_risk_triage_agent(workflow_run_id)
