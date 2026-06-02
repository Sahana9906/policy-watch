
import pytest
 
from agents.change_analyzer_agent import (
    ChangeAnalyzerAgent,
    ChangeAnalyzerError,
    ChangeAnalysisPayload,
)
from core.workflow_states import WorkflowStage
from db.models import ChangeAnalysis, RegulationSnapshot
from services.workflow_service import WorkflowService
 
 
pytestmark = pytest.mark.asyncio
 
 
class FakeGeminiClient:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = 0
 
    async def generate_text(self, _prompt: str) -> str:
        self.calls += 1
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response
 
 
SUCCESS_JSON = """
{
  "summary": "Incident reporting deadline changed from 72 hours to 24 hours.",
  "severity": "high",
  "changed_clauses": [
    {
      "clause_title": "Incident reporting",
      "change_type": "modified",
      "old_summary": "Covered entities reported incidents within 72 hours.",
      "new_summary": "Covered entities must now report incidents within 24 hours.",
      "compliance_impact": "Incident response playbooks and escalation SLAs need updates."
    }
  ],
  "impacted_domains": ["incident response", "audit evidence"],
  "recommended_next_action": "Update incident response procedures and notify compliance owners."
}
"""
 
 
async def _create_snapshot(session, regulation_id, text):
    snapshot = RegulationSnapshot(
        regulation_id=regulation_id,
        content_hash=f"hash-{len(text)}",
        content_text=text,
    )
    session.add(snapshot)
    await session.commit()
    await session.refresh(snapshot)
    return snapshot
 
 
async def _create_workflow(session, regulation_id, snapshot_id, monkeypatch):
    async def no_emit(*_args, **_kwargs):
        return None
 
    monkeypatch.setattr(
        "services.workflow_service.WorkflowService.emit_workflow_event",
        no_emit,
    )
    return await WorkflowService(session).create_workflow_run(
        regulation_id=regulation_id,
        snapshot_id=snapshot_id,
        initial_stage=WorkflowStage.REGULATION_DETECTED,
    )
 
 
async def test_change_analyzer_creates_analysis_and_stops_at_analysis_complete(
    db_session,
    seeded_regulation,
    monkeypatch,
):
    """
    After a successful Gemini analysis the analyzer must:
    - persist the ChangeAnalysis row
    - advance the workflow to ANALYSIS_COMPLETE (not beyond)
    - emit the event so the dispatcher can invoke the ExtractionAgent next
 
    The analyzer must NOT create ComplianceRequirement or ComplianceAction records;
    those are owned by the downstream agents.
    """
    old_snapshot = await _create_snapshot(
        db_session,
        seeded_regulation.id,
        "Covered entities must report incidents within 72 hours.",
    )
    new_snapshot = await _create_snapshot(
        db_session,
        seeded_regulation.id,
        "Covered entities must report incidents within 24 hours and retain audit evidence.",
    )
    workflow = await _create_workflow(
        db_session,
        seeded_regulation.id,
        new_snapshot.id,
        monkeypatch,
    )
 
    result = await ChangeAnalyzerAgent(
        db_session,
        gemini_client=FakeGeminiClient([SUCCESS_JSON]),
    ).run(workflow.id)
 
    # Returned result carries the correct identifiers and severity
    assert result.old_snapshot_id == old_snapshot.id
    assert result.new_snapshot_id == new_snapshot.id
    assert result.severity == "high"
 
    # ChangeAnalysis row must be persisted with full Gemini output
    analysis = await db_session.get(ChangeAnalysis, result.analysis_id)
    assert analysis.summary.startswith("Incident reporting deadline")
    assert analysis.impacted_domains == ["incident response", "audit evidence"]
 
    # Workflow must stop at ANALYSIS_COMPLETE — the dispatcher drives the next stage
    refreshed_workflow = await WorkflowService(db_session).get_workflow_run(workflow.id)
    assert refreshed_workflow.current_stage == WorkflowStage.ANALYSIS_COMPLETE.value
    assert refreshed_workflow.status == "processing"
 
 
async def test_change_analyzer_retries_malformed_json(
    db_session,
    seeded_regulation,
    monkeypatch,
):
    new_snapshot = await _create_snapshot(
        db_session,
        seeded_regulation.id,
        "New access reviews are required every quarter.",
    )
    workflow = await _create_workflow(
        db_session,
        seeded_regulation.id,
        new_snapshot.id,
        monkeypatch,
    )
    fake_gemini = FakeGeminiClient(["not json", SUCCESS_JSON])
 
    result = await ChangeAnalyzerAgent(
        db_session,
        gemini_client=fake_gemini,
    ).run(workflow.id)
 
    assert fake_gemini.calls == 2
    assert result.analysis_id is not None
 
 
async def test_change_analyzer_marks_analysis_failed_after_retry_exhaustion(
    db_session,
    seeded_regulation,
    monkeypatch,
):
    new_snapshot = await _create_snapshot(
        db_session,
        seeded_regulation.id,
        "New data retention obligation.",
    )
    workflow = await _create_workflow(
        db_session,
        seeded_regulation.id,
        new_snapshot.id,
        monkeypatch,
    )
 
    with pytest.raises(ChangeAnalyzerError):
        await ChangeAnalyzerAgent(
            db_session,
            gemini_client=FakeGeminiClient(["not json", "still not json"]),
        ).run(workflow.id)
 
    refreshed_workflow = await WorkflowService(db_session).get_workflow_run(workflow.id)
    assert refreshed_workflow.current_stage == WorkflowStage.ANALYSIS_FAILED.value
    assert refreshed_workflow.status == "failed"
 
 
def test_change_analysis_payload_normalizes_domains():
    payload = ChangeAnalysisPayload.model_validate(
        {
            "summary": "Access control scope expanded.",
            "severity": "medium",
            "changed_clauses": [],
            "impacted_domains": ["Access Control", " access control ", "Data Retention"],
            "recommended_next_action": "Review controls.",
        }
    )
 
    assert payload.impacted_domains == ["access control", "data retention"]
