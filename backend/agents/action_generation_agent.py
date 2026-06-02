"""
Action Generation Agent
=======================
Stage transition: RISK_TRIAGED -> ACTIONS_GENERATED

Loads triaged ComplianceRequirement rows, sends them to Gemini, and
persists concrete ComplianceAction rows for downstream GitLab issue creation.
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
from db import queries
from db.models import ComplianceRequirement, WorkflowRun
from db.session import AsyncSessionLocal
from integrations.gemini_client import GeminiClient, GeminiClientError
from services.workflow_service import WorkflowService

logger = logging.getLogger(__name__)

ACTION_RETRY_ATTEMPTS = 2
ACTION_RETRY_DELAY_SECONDS = 0.75
MAX_REQS_PER_BATCH = 10
CRITICAL_RISK_THRESHOLD = 80.0


class ActionGenerationAgentError(Exception):
    pass


@dataclass(slots=True)
class GeneratedAction:
    requirement_id: int
    action_title: str
    action_description: str
    implementation_notes: str = ""
    estimated_effort: str = "medium"
    suggested_owner_role: str = "engineering"
    acceptance_criteria: str = ""
    is_monitoring_action: bool = False

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class ActionGenerationResult:
    workflow_run_id: int
    regulation_id: int
    actions_created: int
    requirement_ids_covered: list[int]
    action_ids: list[int]

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


class ActionGenerationAgent:
    name = "action_generation_agent"
    description = (
        "Loads triaged requirements, asks Gemini for implementation tasks, "
        "persists ComplianceAction rows, and advances the workflow."
    )

    def __init__(
        self,
        session: AsyncSession,
        gemini_client: GeminiClient | None = None,
    ) -> None:
        self.session = session
        self.gemini_client = gemini_client or GeminiClient()
        self.workflow_service = WorkflowService(session)

    async def run(self, workflow_run_id: int) -> ActionGenerationResult:
        workflow_run = await self.workflow_service.get_workflow_run(workflow_run_id)
        regulation_id = workflow_run.regulation_id

        logger.info(
            "ActionGenerationAgent started workflow_run_id=%s regulation_id=%s",
            workflow_run_id,
            regulation_id,
        )

        try:
            requirements = await self._load_ranked_requirements(workflow_run_id)
            if not requirements:
                raise ActionGenerationAgentError(
                    f"No ComplianceRequirements found for workflow_run_id={workflow_run_id}."
                )

            all_actions: list[GeneratedAction] = []
            for batch in self._batch(requirements, MAX_REQS_PER_BATCH):
                prompt = self._build_prompt(batch, workflow_run)
                raw = await self._generate_with_retry(prompt, workflow_run_id)
                all_actions.extend(self._parse_actions(raw, batch))

            action_ids, req_ids_covered = await self._persist_actions(
                workflow_run_id=workflow_run_id,
                actions=all_actions,
            )

            await self.workflow_service.update_stage(
                workflow_run_id,
                WorkflowStage.ACTIONS_GENERATED,
                emit_event=True,
            )

            logger.info(
                "ActionGenerationAgent completed workflow_run_id=%s actions=%s requirements_covered=%s",
                workflow_run_id,
                len(action_ids),
                len(req_ids_covered),
            )

            return ActionGenerationResult(
                workflow_run_id=workflow_run_id,
                regulation_id=regulation_id,
                actions_created=len(action_ids),
                requirement_ids_covered=req_ids_covered,
                action_ids=action_ids,
            )
        except Exception as exc:
            logger.exception(
                "ActionGenerationAgent failed workflow_run_id=%s error=%s",
                workflow_run_id,
                exc,
            )
            await self.workflow_service.mark_failed(workflow_run_id, str(exc))
            raise

    async def _load_ranked_requirements(
        self,
        workflow_run_id: int,
    ) -> list[ComplianceRequirement]:
        severity_order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
        result = await self.session.execute(
            select(ComplianceRequirement)
            .where(ComplianceRequirement.workflow_run_id == workflow_run_id)
            .order_by(ComplianceRequirement.id)
        )
        rows = list(result.scalars().all())
        rows.sort(key=lambda row: severity_order.get(row.severity or "medium", 2))
        return rows

    @staticmethod
    def _batch(
        items: list[ComplianceRequirement],
        size: int,
    ) -> list[list[ComplianceRequirement]]:
        return [items[i : i + size] for i in range(0, len(items), size)]

    def _build_prompt(
        self,
        requirements: list[ComplianceRequirement],
        workflow_run: WorkflowRun,
    ) -> str:
        req_lines = []
        for req in requirements:
            is_critical = req.severity in ("critical", "high")
            dual_action_hint = (
                " [GENERATE TWO ACTIONS: one remediation + one monitoring]"
                if is_critical
                else ""
            )
            req_lines.append(
                f"requirement_id: {req.id}\n"
                f"severity: {req.severity}\n"
                f"domain: {req.impacted_domain or 'general'}\n"
                f"obligation: {(req.requirement_text or '')[:700]}\n"
                f"{dual_action_hint}"
            )

        return f"""
You are PolicyWatch's Action Generation Agent.

Generate specific, actionable implementation tasks for each compliance requirement
(workflow_run_id={workflow_run.id}).

For critical/high severity requirements, generate TWO actions:
  1. Remediation action
  2. Monitoring action with is_monitoring_action=true

Return ONLY valid JSON; no markdown, no explanation:
{{
  "actions": [
    {{
      "requirement_id": <int>,
      "action_title": "imperative task title, max 90 chars",
      "action_description": "step-by-step implementation guide",
      "implementation_notes": "tooling suggestions, edge cases, gotchas",
      "estimated_effort": "small | medium | large | xlarge",
      "suggested_owner_role": "security | engineering | legal | hr | ops",
      "acceptance_criteria": "how a reviewer confirms this task is complete",
      "is_monitoring_action": false
    }}
  ]
}}

REQUIREMENTS:
{chr(10).join(req_lines)}
""".strip()

    async def _generate_with_retry(self, prompt: str, workflow_run_id: int) -> str:
        last_error: Exception | None = None
        for attempt in range(1, ACTION_RETRY_ATTEMPTS + 1):
            try:
                response = await self.gemini_client.generate_text(prompt)
                logger.info(
                    "ActionGenerationAgent Gemini response received workflow_run_id=%s attempt=%s chars=%s",
                    workflow_run_id,
                    attempt,
                    len(response),
                )
                return response
            except GeminiClientError as exc:
                last_error = exc
                logger.warning(
                    "ActionGenerationAgent Gemini call failed workflow_run_id=%s attempt=%s/%s error=%s",
                    workflow_run_id,
                    attempt,
                    ACTION_RETRY_ATTEMPTS,
                    exc,
                )
                if attempt < ACTION_RETRY_ATTEMPTS:
                    await asyncio.sleep(ACTION_RETRY_DELAY_SECONDS)
        raise ActionGenerationAgentError(
            f"Gemini action generation failed after {ACTION_RETRY_ATTEMPTS} attempts: {last_error}"
        )

    def _parse_actions(
        self,
        raw_response: str,
        requirements: list[ComplianceRequirement],
    ) -> list[GeneratedAction]:
        payload = self._extract_json(raw_response)
        raw_actions = payload.get("actions", [])
        if not isinstance(raw_actions, list):
            raise ActionGenerationAgentError("Gemini 'actions' field must be a list.")

        known_ids = {req.id for req in requirements}
        actions: list[GeneratedAction] = []
        for item in raw_actions:
            if not isinstance(item, dict):
                continue
            req_id = item.get("requirement_id")
            if req_id not in known_ids:
                logger.warning(
                    "ActionGenerationAgent received unknown requirement_id=%s; skipping",
                    req_id,
                )
                continue
            title = str(item.get("action_title") or "").strip()
            description = str(item.get("action_description") or "").strip()
            if not title or not description:
                continue
            actions.append(
                GeneratedAction(
                    requirement_id=int(req_id),
                    action_title=title[:90],
                    action_description=description,
                    implementation_notes=str(item.get("implementation_notes") or ""),
                    estimated_effort=self._normalise_effort(item.get("estimated_effort")),
                    suggested_owner_role=self._normalise_owner(item.get("suggested_owner_role")),
                    acceptance_criteria=str(item.get("acceptance_criteria") or ""),
                    is_monitoring_action=bool(item.get("is_monitoring_action", False)),
                )
            )

        covered_req_ids = {action.requirement_id for action in actions}
        for req in requirements:
            if req.id not in covered_req_ids:
                actions.append(
                    GeneratedAction(
                        requirement_id=req.id,
                        action_title=f"Implement compliance update: {(req.requirement_text or '')[:60]}",
                        action_description=(
                            "Review and implement the following compliance obligation:\n"
                            f"{req.requirement_text or ''}\n\n"
                            f"Domain: {req.impacted_domain or 'general'}\n"
                            f"Priority: {req.severity}"
                        ),
                        implementation_notes="Action was auto-generated as a fallback. Review and refine.",
                        estimated_effort="medium",
                        suggested_owner_role="engineering",
                        acceptance_criteria="Obligation satisfied and evidenced in audit log.",
                        is_monitoring_action=False,
                    )
                )
        return actions

    @staticmethod
    def _extract_json(raw: str) -> dict[str, Any]:
        cleaned = raw.strip()
        match = re.search(r"```(?:json)?\s*(.*?)```", cleaned, flags=re.DOTALL | re.IGNORECASE)
        if match:
            cleaned = match.group(1).strip()
        try:
            payload = json.loads(cleaned)
        except json.JSONDecodeError as exc:
            raise ActionGenerationAgentError("Gemini response was not valid JSON.") from exc
        if not isinstance(payload, dict):
            raise ActionGenerationAgentError("Gemini response must be a JSON object.")
        return payload

    @staticmethod
    def _normalise_effort(value: Any) -> str:
        v = str(value or "").lower().strip()
        return v if v in {"small", "medium", "large", "xlarge"} else "medium"

    @staticmethod
    def _normalise_owner(value: Any) -> str:
        v = str(value or "").lower().strip()
        return v if v in {"security", "engineering", "legal", "hr", "ops"} else "engineering"

    async def _persist_actions(
        self,
        workflow_run_id: int,
        actions: list[GeneratedAction],
    ) -> tuple[list[int], list[int]]:
        action_ids: list[int] = []
        req_ids_covered: set[int] = set()

        try:
            for action in actions:
                full_description = action.action_description
                if action.implementation_notes:
                    full_description += f"\n\nImplementation notes:\n{action.implementation_notes}"
                if action.acceptance_criteria:
                    full_description += f"\n\nAcceptance criteria:\n{action.acceptance_criteria}"
                if action.is_monitoring_action:
                    full_description += "\n\n[MONITORING ACTION: evidence collection for ongoing compliance]"

                action_title = action.action_title
                if action.is_monitoring_action:
                    action_title = f"[Monitor] {action_title}"

                row = await queries.create_compliance_action(
                    self.session,
                    requirement_id=action.requirement_id,
                    workflow_run_id=workflow_run_id,
                    action_title=action_title,
                    action_description=full_description,
                )
                for attr, val in [
                    ("estimated_effort", action.estimated_effort),
                    ("suggested_owner_role", action.suggested_owner_role),
                    ("is_monitoring_action", action.is_monitoring_action),
                ]:
                    if hasattr(row, attr):
                        setattr(row, attr, val)
                action_ids.append(row.id)
                req_ids_covered.add(action.requirement_id)

            await self.session.commit()
            return action_ids, sorted(req_ids_covered)
        except Exception:
            await self.session.rollback()
            logger.exception(
                "ActionGenerationAgent failed to persist actions workflow_run_id=%s",
                workflow_run_id,
            )
            raise


async def run_action_generation_agent(
    workflow_run_id: int,
    session: AsyncSession | None = None,
) -> ActionGenerationResult:
    if session is not None:
        return await ActionGenerationAgent(session).run(workflow_run_id)
    async with AsyncSessionLocal() as db:
        return await ActionGenerationAgent(db).run(workflow_run_id)


async def run(workflow_run_id: int) -> ActionGenerationResult:
    return await run_action_generation_agent(workflow_run_id)
