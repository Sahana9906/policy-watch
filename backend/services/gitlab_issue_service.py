import json
import logging
import os
from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from typing import Any

from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from db import models
from db.models import Regulation, RegulationSnapshot, WorkflowRun
from integrations.gitlab_client import GitLabClient, GitLabIssue

logger = logging.getLogger(__name__)

ActionModel = getattr(models, "ComplianceAction", getattr(models, "Action", None))
RequirementModel = getattr(
    models,
    "ComplianceRequirement",
    getattr(models, "Requirement", None),
)

DEFAULT_LABELS = ("policywatch", "compliance")
PRIORITY_LABELS = {
    "critical": "priority::critical",
    "high": "priority::high",
    "medium": "priority::medium",
    "low": "priority::low",
}


class GitLabIssueServiceError(Exception):
    pass


@dataclass(slots=True)
class ComplianceActionIssueDraft:
    title: str
    description: str
    action_id: int | None = None
    requirement_id: int | None = None
    priority: str = "medium"
    due_date: str | None = None
    labels: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class GitLabIssueCreationResult:
    workflow_run_id: int
    regulation_id: int
    created_count: int
    skipped_count: int
    issues: list[GitLabIssue]

    def as_dict(self) -> dict[str, Any]:
        return {
            "workflow_run_id": self.workflow_run_id,
            "regulation_id": self.regulation_id,
            "created_count": self.created_count,
            "skipped_count": self.skipped_count,
            "issues": [
                {
                    "id": issue.id,
                    "iid": issue.iid,
                    "title": issue.title,
                    "web_url": issue.web_url,
                    "state": issue.state,
                }
                for issue in self.issues
            ],
        }


class GitLabIssueService:
    def __init__(
        self,
        session: AsyncSession,
        gitlab_client: GitLabClient | None = None,
        project_id: str | int | None = None,
    ):
        self.session = session
        self.gitlab_client = gitlab_client or GitLabClient()
        self.project_id = project_id or os.getenv("GITLAB_PROJECT_ID")

    async def create_issues_for_workflow(
        self,
        workflow_run_id: int,
    ) -> GitLabIssueCreationResult:
        workflow_run = await self._get_workflow_run(workflow_run_id)
        regulation = await self._get_regulation(workflow_run.regulation_id)
        actions = await self._load_actions(workflow_run)
        skipped_count = sum(1 for action in actions if self._already_linked(action))
        action_drafts = [
            self.build_issue_draft(action, workflow_run, regulation)
            for action in actions
            if not self._already_linked(action)
        ]

        if actions and not action_drafts:
            result = GitLabIssueCreationResult(
                workflow_run_id=workflow_run.id,
                regulation_id=workflow_run.regulation_id,
                created_count=0,
                skipped_count=skipped_count,
                issues=[],
            )
            logger.info("GitLab issues already exist for workflow: %s", result.as_dict())
            return result

        if not action_drafts:
            action_drafts = await self._fallback_drafts(workflow_run, regulation)

        created_issues: list[GitLabIssue] = []
        for draft in action_drafts:
            payload = self.build_issue_payload(draft, workflow_run, regulation)
            issue = await self._create_issue_with_retry(payload)
            created_issues.append(issue)
            await self._persist_issue_link(draft, issue)

            logger.info(
                "Created GitLab issue workflow_run_id=%s action_id=%s iid=%s url=%s",
                workflow_run_id,
                draft.action_id,
                issue.iid,
                issue.web_url,
            )

        await self.session.commit()

        result = GitLabIssueCreationResult(
            workflow_run_id=workflow_run.id,
            regulation_id=workflow_run.regulation_id,
            created_count=len(created_issues),
            skipped_count=skipped_count,
            issues=created_issues,
        )
        logger.info("GitLab issue creation complete: %s", result.as_dict())
        return result

    async def _create_issue_with_retry(self, payload: dict[str, Any]) -> GitLabIssue:
        last_error: Exception | None = None
        for attempt in range(1, 3):
            try:
                return await self.gitlab_client.create_issue(
                    payload,
                    project_id=self.project_id,
                )
            except Exception as exc:
                last_error = exc
                logger.warning(
                    "GitLab issue creation failed attempt=%s/2 title=%s error=%s",
                    attempt,
                    payload.get("title"),
                    exc,
                )
                if attempt == 2:
                    raise

        raise GitLabIssueServiceError(f"GitLab issue creation failed: {last_error}")

    async def _get_workflow_run(self, workflow_run_id: int) -> WorkflowRun:
        result = await self.session.execute(
            select(WorkflowRun).where(WorkflowRun.id == workflow_run_id)
        )
        workflow_run = result.scalar_one_or_none()
        if workflow_run is None:
            raise GitLabIssueServiceError(
                f"Workflow run not found: {workflow_run_id}"
            )
        return workflow_run

    async def _get_regulation(self, regulation_id: int) -> Regulation | None:
        result = await self.session.execute(
            select(Regulation).where(Regulation.id == regulation_id)
        )
        return result.scalar_one_or_none()

    async def _load_actions(self, workflow_run: WorkflowRun) -> list[Any]:
        if ActionModel is None:
            logger.info("Compliance action model is not available; using fallback issue.")
            return []

        candidates = []
        for attr_name, value in (
            ("workflow_run_id", workflow_run.id),
            ("regulation_id", workflow_run.regulation_id),
        ):
            if hasattr(ActionModel, attr_name):
                result = await self.session.execute(
                    select(ActionModel).where(getattr(ActionModel, attr_name) == value)
                )
                candidates.extend(result.scalars().all())

        unique: dict[Any, Any] = {}
        for action in candidates:
            unique[getattr(action, "id", id(action))] = action
        return list(unique.values())

    async def _fallback_drafts(
        self,
        workflow_run: WorkflowRun,
        regulation: Regulation | None,
    ) -> list[ComplianceActionIssueDraft]:
        requirement = await self._latest_requirement(workflow_run)
        if requirement is not None:
            return [
                ComplianceActionIssueDraft(
                    title=self._first_attr(
                        requirement,
                        ("title", "name", "requirement_text"),
                        default="Review compliance requirement",
                    ),
                    description=self._first_attr(
                        requirement,
                        ("description", "summary", "details", "requirement_text"),
                        default="Review and implement this extracted compliance requirement.",
                    ),
                    requirement_id=getattr(requirement, "id", None),
                    priority=self._normalize_priority(
                        self._first_attr(requirement, ("priority", "severity"))
                    ),
                    due_date=self._date_value(
                        self._first_attr(requirement, ("deadline", "due_date"))
                    ),
                )
            ]

        snapshot = await self._latest_snapshot(workflow_run.regulation_id)
        source_hint = ""
        if snapshot is not None:
            source_hint = f"\n\nSnapshot id: {snapshot.id}"

        regulation_name = getattr(regulation, "name", None) or "policy update"
        return [
            ComplianceActionIssueDraft(
                title=f"Review compliance actions for {regulation_name}",
                description=(
                    "PolicyWatch reached the action-to-issue stage, but no persisted "
                    "compliance action records were found. Review the workflow output "
                    "and create implementation tasks."
                    f"{source_hint}"
                ),
                priority="medium",
            )
        ]

    async def _latest_requirement(self, workflow_run: WorkflowRun) -> Any | None:
        if RequirementModel is None:
            return None

        filters = []
        if hasattr(RequirementModel, "workflow_run_id"):
            filters.append(RequirementModel.workflow_run_id == workflow_run.id)
        if hasattr(RequirementModel, "regulation_id"):
            filters.append(RequirementModel.regulation_id == workflow_run.regulation_id)

        for filter_expr in filters:
            query = select(RequirementModel).where(filter_expr)
            if hasattr(RequirementModel, "created_at"):
                query = query.order_by(desc(RequirementModel.created_at))
            result = await self.session.execute(query.limit(1))
            requirement = result.scalar_one_or_none()
            if requirement is not None:
                return requirement
        return None

    async def _latest_snapshot(self, regulation_id: int) -> RegulationSnapshot | None:
        result = await self.session.execute(
            select(RegulationSnapshot)
            .where(RegulationSnapshot.regulation_id == regulation_id)
            .order_by(desc(RegulationSnapshot.created_at), desc(RegulationSnapshot.id))
            .limit(1)
        )
        return result.scalar_one_or_none()

    def build_issue_draft(
        self,
        action: Any,
        workflow_run: WorkflowRun,
        regulation: Regulation | None,
    ) -> ComplianceActionIssueDraft:
        title = self._first_attr(
            action,
            ("action_title", "title", "name", "summary"),
            default="Implement compliance action",
        )
        description = self._first_attr(
            action,
            ("action_description", "description", "details", "rationale", "implementation_notes"),
            default="Implement this compliance action generated by PolicyWatch.",
        )
        priority = self._normalize_priority(
            self._first_attr(action, ("priority", "severity", "risk_level"))
        )
        labels = self._labels_for(action, priority)

        owner = self._first_attr(action, ("owner", "assignee", "team"))
        if owner:
            description = f"{description}\n\nSuggested owner: {owner}"

        return ComplianceActionIssueDraft(
            title=str(title)[:255],
            description=str(description),
            action_id=getattr(action, "id", None),
            requirement_id=self._first_attr(
                action,
                ("requirement_id", "compliance_requirement_id"),
            ),
            priority=priority,
            due_date=self._date_value(
                self._first_attr(action, ("due_date", "deadline", "target_date"))
            ),
            labels=labels,
        )

    def build_issue_payload(
        self,
        draft: ComplianceActionIssueDraft,
        workflow_run: WorkflowRun,
        regulation: Regulation | None,
    ) -> dict[str, Any]:
        labels = [*DEFAULT_LABELS, *draft.labels]
        priority_label = PRIORITY_LABELS.get(draft.priority)
        if priority_label:
            labels.append(priority_label)

        payload: dict[str, Any] = {
            "title": draft.title,
            "description": self._build_description(draft, workflow_run, regulation),
            "labels": ",".join(dict.fromkeys(labels)),
        }
        if draft.due_date:
            payload["due_date"] = draft.due_date
        return payload

    def _build_description(
        self,
        draft: ComplianceActionIssueDraft,
        workflow_run: WorkflowRun,
        regulation: Regulation | None,
    ) -> str:
        regulation_name = getattr(regulation, "name", None) or "Unknown regulation"
        regulation_url = getattr(regulation, "url", None)
        metadata = {
            "workflow_run_id": workflow_run.id,
            "regulation_id": workflow_run.regulation_id,
            "action_id": draft.action_id,
            "requirement_id": draft.requirement_id,
            "priority": draft.priority,
        }

        lines = [
            draft.description.strip(),
            "",
            "PolicyWatch context",
            f"- Regulation: {regulation_name}",
            f"- Workflow run: {workflow_run.id}",
            f"- Priority: {draft.priority}",
        ]
        if regulation_url:
            lines.append(f"- Source: {regulation_url}")
        if draft.due_date:
            lines.append(f"- Due date: {draft.due_date}")

        lines.extend(
            [
                "",
                "Trace metadata",
                "```json",
                json.dumps(metadata, indent=2, sort_keys=True),
                "```",
            ]
        )
        return "\n".join(lines)

    async def _persist_issue_link(
        self,
        draft: ComplianceActionIssueDraft,
        issue: GitLabIssue,
    ) -> None:
        if ActionModel is None or draft.action_id is None:
            return

        action = await self.session.get(ActionModel, draft.action_id)
        if action is None:
            return

        self._set_if_present(action, "gitlab_issue_id", issue.id)
        self._set_if_present(action, "gitlab_issue_iid", issue.iid)
        self._set_if_present(action, "gitlab_issue_url", issue.web_url)
        self._set_if_present(action, "external_issue_id", issue.id)
        self._set_if_present(action, "external_issue_url", issue.web_url)
        self._set_if_present(action, "status", "issue_created")
        self._set_if_present(action, "updated_at", datetime.utcnow())

    @staticmethod
    def _already_linked(action: Any) -> bool:
        return any(
            bool(getattr(action, attr_name, None))
            for attr_name in (
                "gitlab_issue_id",
                "gitlab_issue_iid",
                "gitlab_issue_url",
                "external_issue_id",
                "external_issue_url",
            )
        )

    @staticmethod
    def _first_attr(
        instance: Any,
        attr_names: tuple[str, ...],
        default: Any = None,
    ) -> Any:
        for attr_name in attr_names:
            value = getattr(instance, attr_name, None)
            if value not in (None, ""):
                return value
        return default

    @staticmethod
    def _labels_for(action: Any, priority: str) -> list[str]:
        raw_labels = getattr(action, "labels", None)
        labels: list[str] = []
        if isinstance(raw_labels, str):
            labels.extend(
                label.strip() for label in raw_labels.split(",") if label.strip()
            )
        elif isinstance(raw_labels, list):
            labels.extend(str(label).strip() for label in raw_labels if str(label).strip())

        category = getattr(action, "category", None) or getattr(action, "type", None)
        if category:
            labels.append(f"compliance::{str(category).lower().strip()}")
        if priority:
            labels.append(f"risk::{priority}")
        return labels

    @staticmethod
    def _normalize_priority(value: Any) -> str:
        normalized = str(value or "medium").lower().strip()
        if normalized in {"critical", "high", "medium", "low"}:
            return normalized
        return "medium"

    @staticmethod
    def _date_value(value: Any) -> str | None:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value.date().isoformat()
        if isinstance(value, date):
            return value.isoformat()
        text = str(value).strip()
        return text[:10] if text else None

    @staticmethod
    def _set_if_present(instance: Any, attr_name: str, value: Any) -> None:
        if hasattr(instance, attr_name):
            setattr(instance, attr_name, value)


async def create_issues_for_workflow(
    workflow_run_id: int,
    session: AsyncSession | None = None,
) -> GitLabIssueCreationResult:
    if session is not None:
        return await GitLabIssueService(session).create_issues_for_workflow(
            workflow_run_id
        )

    from db.session import AsyncSessionLocal

    async with AsyncSessionLocal() as db_session:
        service = GitLabIssueService(db_session)
        try:
            return await service.create_issues_for_workflow(workflow_run_id)
        finally:
            await service.gitlab_client.aclose()


async def create_gitlab_issues(
    workflow_run_id: int,
    session: AsyncSession | None = None,
) -> GitLabIssueCreationResult:
    return await create_issues_for_workflow(workflow_run_id, session=session)


def build_gitlab_issue_payload(
    draft: ComplianceActionIssueDraft,
    workflow_run: WorkflowRun,
    regulation: Regulation | None = None,
) -> dict[str, Any]:
    return GitLabIssueService(session=None).build_issue_payload(
        draft,
        workflow_run,
        regulation,
    )
