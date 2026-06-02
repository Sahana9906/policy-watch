
import asyncio
import json
import logging
import re
from dataclasses import asdict, dataclass
from typing import Any, Literal
 
from pydantic import BaseModel, Field, ValidationError, field_validator
from sqlalchemy.ext.asyncio import AsyncSession
 
from core.workflow_states import WorkflowStage
from db import queries
from db.models import RegulationSnapshot
from db.session import AsyncSessionLocal
from integrations.gemini_client import GeminiClient, GeminiClientError
from services.workflow_service import WorkflowService
 
logger = logging.getLogger(__name__)
 
MAX_SNAPSHOT_CHARS = 14000
GEMINI_RETRY_ATTEMPTS = 2
GEMINI_RETRY_DELAY_SECONDS = 0.75
 
 
class ChangedClause(BaseModel):
    clause_title: str = Field(..., min_length=1, max_length=240)
    change_type: Literal["added", "modified", "removed"]
    old_summary: str = Field(..., min_length=1)
    new_summary: str = Field(..., min_length=1)
    compliance_impact: str = Field(..., min_length=1)
 
 
class ChangeAnalysisPayload(BaseModel):
    summary: str = Field(..., min_length=1)
    severity: Literal["low", "medium", "high", "critical"]
    changed_clauses: list[ChangedClause] = Field(default_factory=list)
    impacted_domains: list[str] = Field(default_factory=list)
    recommended_next_action: str = Field(..., min_length=1)
 
    @field_validator("impacted_domains")
    @classmethod
    def impacted_domains_must_be_clean(cls, value: list[str]) -> list[str]:
        cleaned = [item.strip().lower() for item in value if item.strip()]
        return list(dict.fromkeys(cleaned))
 
 
@dataclass(slots=True)
class ChangeAnalysisResult:
    workflow_run_id: int
    regulation_id: int
    old_snapshot_id: int | None
    new_snapshot_id: int
    analysis_id: int
    severity: str
    summary: str
 
    def as_dict(self) -> dict[str, Any]:
        return asdict(self)
 
 
class ChangeAnalyzerError(Exception):
    pass
 
 
class GeminiResponseFormatError(ChangeAnalyzerError):
    pass
 
 
class ChangeAnalyzerAgent:
    name = "change_analyzer_agent"
    description = "Compares old/new regulation snapshots and produces structured AI impact analysis."
 
    def __init__(
        self,
        session: AsyncSession,
        gemini_client: GeminiClient | None = None,
    ):
        self.session = session
        self.workflow_service = WorkflowService(session)
        self.gemini_client = gemini_client or GeminiClient()
 
    async def run(self, workflow_run_id: int) -> ChangeAnalysisResult:
        workflow_run = await self.workflow_service.get_workflow_run(workflow_run_id)
        regulation_id = workflow_run.regulation_id
        new_snapshot = await self._resolve_new_snapshot(workflow_run)
        old_snapshot = await self._resolve_old_snapshot(regulation_id, new_snapshot.id)
 
        logger.info(
            "ChangeAnalyzer started workflow_run_id=%s regulation_id=%s old_snapshot_id=%s new_snapshot_id=%s",
            workflow_run_id,
            regulation_id,
            getattr(old_snapshot, "id", None),
            new_snapshot.id,
        )
 
        try:
            prompt = self.build_prompt(
                workflow_run=workflow_run,
                old_snapshot=old_snapshot,
                new_snapshot=new_snapshot,
            )
            analysis, raw_response = await self._analyze_with_retry(prompt, workflow_run_id)
            analysis_row = await queries.create_change_analysis(
                self.session,
                workflow_run_id=workflow_run.id,
                regulation_id=regulation_id,
                old_snapshot_id=getattr(old_snapshot, "id", None),
                new_snapshot_id=new_snapshot.id,
                analysis=analysis.model_dump(),
                raw_response=raw_response,
            )
            await self.workflow_service.update_stage(
                workflow_run.id,
                WorkflowStage.ANALYSIS_COMPLETE,
                emit_event=True,
            )
 
            logger.info(
                "ChangeAnalyzer completed workflow_run_id=%s regulation_id=%s analysis_id=%s severity=%s summary=%s",
                workflow_run.id,
                regulation_id,
                analysis_row.id,
                analysis.severity,
                analysis.summary[:220],
            )
 
            return ChangeAnalysisResult(
                workflow_run_id=workflow_run.id,
                regulation_id=regulation_id,
                old_snapshot_id=getattr(old_snapshot, "id", None),
                new_snapshot_id=new_snapshot.id,
                analysis_id=analysis_row.id,
                severity=analysis.severity,
                summary=analysis.summary,
            )
        except Exception as exc:
            await self._mark_analysis_failed(workflow_run.id, str(exc))
            logger.exception(
                "ChangeAnalyzer failed workflow_run_id=%s regulation_id=%s old_snapshot_id=%s new_snapshot_id=%s",
                workflow_run_id,
                regulation_id,
                getattr(old_snapshot, "id", None),
                new_snapshot.id,
            )
            raise
 
    async def _resolve_new_snapshot(self, workflow_run: Any) -> RegulationSnapshot:
        snapshot_id = getattr(workflow_run, "snapshot_id", None)
        if snapshot_id is not None:
            snapshot = await queries.get_regulation_snapshot(self.session, snapshot_id)
        else:
            snapshot = await queries.get_latest_snapshot(
                self.session,
                workflow_run.regulation_id,
            )
 
        if snapshot is None:
            raise ChangeAnalyzerError(
                f"No new snapshot found for workflow_run_id={workflow_run.id}."
            )
        if not snapshot.content_text:
            raise ChangeAnalyzerError(
                f"Snapshot {snapshot.id} has no text content for analysis."
            )
        return snapshot
 
    async def _resolve_old_snapshot(
        self,
        regulation_id: int,
        new_snapshot_id: int,
    ) -> RegulationSnapshot | None:
        return await queries.get_previous_snapshot(
            self.session,
            regulation_id,
            new_snapshot_id,
        )

    async def _create_compliance_work_items(
        self,
        workflow_run_id: int,
        regulation_id: int,
        analysis: ChangeAnalysisPayload,
    ) -> None:
        clauses = analysis.changed_clauses or [
            ChangedClause(
                clause_title="Regulation change",
                change_type="modified",
                old_summary="No clause-level summary was provided.",
                new_summary=analysis.summary,
                compliance_impact=analysis.recommended_next_action,
            )
        ]
        impacted_domain = analysis.impacted_domains[0] if analysis.impacted_domains else None

        for clause in clauses:
            requirement_text = (
                f"{clause.clause_title}: {clause.new_summary} "
                f"Compliance impact: {clause.compliance_impact}"
            )
            requirement = await queries.create_compliance_requirement(
                self.session,
                regulation_id=regulation_id,
                workflow_run_id=workflow_run_id,
                requirement_text=requirement_text,
                impacted_domain=impacted_domain,
                severity=analysis.severity,
            )
            await queries.create_compliance_action(
                self.session,
                requirement_id=requirement.id,
                workflow_run_id=workflow_run_id,
                action_title=f"Review {clause.clause_title}",
                action_description=analysis.recommended_next_action,
            )
 
    def build_prompt(
        self,
        workflow_run: Any,
        old_snapshot: RegulationSnapshot | None,
        new_snapshot: RegulationSnapshot,
    ) -> str:
        old_text = (
            old_snapshot.content_text
            if old_snapshot and old_snapshot.content_text
            else "No previous snapshot exists. Treat this as a newly detected regulation baseline."
        )
        new_text = new_snapshot.content_text or ""
 
        return f"""
You are PolicyWatch's Change Analyzer Agent.
 
Compare the OLD regulation text against the NEW regulation text.
Your job is to explain what changed legally and what compliance teams should do next.
 
Return STRICT JSON ONLY. Do not include markdown, comments, explanations, or code fences.
 
Required JSON shape:
{{
  "summary": "string",
  "severity": "low|medium|high|critical",
  "changed_clauses": [
    {{
      "clause_title": "string",
      "change_type": "added|modified|removed",
      "old_summary": "string",
      "new_summary": "string",
      "compliance_impact": "string"
    }}
  ],
  "impacted_domains": ["data retention", "access control", "incident response"],
  "recommended_next_action": "string"
}}
 
Severity guidance:
- low: wording or administrative change with minimal operational impact
- medium: meaningful process or evidence update
- high: new obligation, shorter deadline, expanded scope, or audit exposure
- critical: immediate legal/compliance risk, enforcement deadline, breach/reporting impact
 
Workflow context:
- workflow_run_id: {workflow_run.id}
- regulation_id: {workflow_run.regulation_id}
- old_snapshot_id: {getattr(old_snapshot, "id", None)}
- new_snapshot_id: {new_snapshot.id}
 
OLD REGULATION TEXT:
{old_text[:MAX_SNAPSHOT_CHARS]}
 
NEW REGULATION TEXT:
{new_text[:MAX_SNAPSHOT_CHARS]}
""".strip()
 
    async def _analyze_with_retry(
        self,
        prompt: str,
        workflow_run_id: int,
    ) -> tuple[ChangeAnalysisPayload, str]:
        last_error: Exception | None = None
 
        for attempt in range(1, GEMINI_RETRY_ATTEMPTS + 1):
            try:
                raw_response = await self.gemini_client.generate_text(prompt)
                logger.info(
                    "Gemini response received workflow_run_id=%s attempt=%s status=received chars=%s",
                    workflow_run_id,
                    attempt,
                    len(raw_response),
                )
                return self.parse_response(raw_response), raw_response
            except (GeminiClientError, GeminiResponseFormatError, ValidationError) as exc:
                last_error = exc
                logger.warning(
                    "Change analysis attempt failed workflow_run_id=%s attempt=%s/%s error=%s",
                    workflow_run_id,
                    attempt,
                    GEMINI_RETRY_ATTEMPTS,
                    exc,
                )
                if attempt < GEMINI_RETRY_ATTEMPTS:
                    await asyncio.sleep(GEMINI_RETRY_DELAY_SECONDS)
 
        raise ChangeAnalyzerError(
            f"Gemini change analysis failed after retries: {last_error}"
        )
 
    def parse_response(self, raw_response: str) -> ChangeAnalysisPayload:
        cleaned = self._extract_json(raw_response)
        try:
            payload = json.loads(cleaned)
        except json.JSONDecodeError as exc:
            logger.warning(
                "Gemini returned non-JSON change analysis response: %s",
                raw_response[:500],
            )
            raise GeminiResponseFormatError("Gemini response was not valid JSON.") from exc
 
        if not isinstance(payload, dict):
            raise GeminiResponseFormatError("Gemini response must be a JSON object.")
 
        return ChangeAnalysisPayload.model_validate(payload)
 
    @staticmethod
    def _extract_json(raw_response: str) -> str:
        cleaned = raw_response.strip()
        fenced_match = re.search(
            r"```(?:json)?\s*(.*?)```",
            cleaned,
            flags=re.DOTALL | re.IGNORECASE,
        )
        if fenced_match:
            cleaned = fenced_match.group(1).strip()
        return cleaned
 
    async def _mark_analysis_failed(
        self,
        workflow_run_id: int,
        error_message: str,
    ) -> None:
        try:
            await self.workflow_service.update_stage(
                workflow_run_id,
                WorkflowStage.ANALYSIS_FAILED,
                emit_event=True,
            )
        except Exception:
            logger.exception(
                "Could not transition workflow_run_id=%s to analysis_failed.",
                workflow_run_id,
            )
            await self.workflow_service.mark_failed(
                workflow_run_id,
                error_message,
                emit_event=True,
            )
 
 
async def run_change_analyzer_agent(
    workflow_run_id: int,
    session: AsyncSession | None = None,
) -> ChangeAnalysisResult:
    if session is not None:
        return await ChangeAnalyzerAgent(session).run(workflow_run_id)
 
    async with AsyncSessionLocal() as db_session:
        return await ChangeAnalyzerAgent(db_session).run(workflow_run_id)
 
 
async def run(workflow_run_id: int) -> ChangeAnalysisResult:
    return await run_change_analyzer_agent(workflow_run_id)
 
 
change_analyzer_agent = ChangeAnalyzerAgent
