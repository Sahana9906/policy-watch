import asyncio
import importlib
import logging
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

from core.workflow_states import (
    INITIAL_STAGE,
    WorkflowStage,
    get_next_stage,
    is_terminal_stage,
    is_valid_stage,
    normalize_stage,
)
from db.models import WorkflowRun
from db.session import AsyncSessionLocal
from services.workflow_service import WorkflowNotFoundError, WorkflowService

logger = logging.getLogger(__name__)

AgentHandler = Callable[[int], Awaitable[Any]]


@dataclass(frozen=True, slots=True)
class DispatchRoute:
    stage: WorkflowStage
    handler_paths: tuple[str, ...]
    next_stage: WorkflowStage | None


STAGE_ROUTES: dict[WorkflowStage, DispatchRoute] = {
    WorkflowStage.REGULATION_DETECTED: DispatchRoute(
        stage=WorkflowStage.REGULATION_DETECTED,
        handler_paths=(
            "agents.change_analyzer_agent.run",
            "agents.change_analyzer_agent.run_change_analyzer_agent",
        ),
        next_stage=None,
    ),
    WorkflowStage.ANALYSIS_COMPLETE: DispatchRoute(
        stage=WorkflowStage.ANALYSIS_COMPLETE,
        handler_paths=(
            "agents.extraction_agent.run",
            "agents.extraction_agent.run_extraction_agent",
            "services.requirement_extraction_service.extract_requirements",
        ),
        next_stage=WorkflowStage.REQUIREMENTS_EXTRACTED,
    ),
    WorkflowStage.REQUIREMENTS_EXTRACTED: DispatchRoute(
        stage=WorkflowStage.REQUIREMENTS_EXTRACTED,
        handler_paths=(
            "agents.risk_triage_agent.run",
            "agents.risk_triage_agent.run_risk_triage_agent",
            "agents.risk_agent.run",
            "agents.risk_agent.run_risk_agent",
        ),
        next_stage=WorkflowStage.RISK_TRIAGED,
    ),
    WorkflowStage.RISK_TRIAGED: DispatchRoute(
        stage=WorkflowStage.RISK_TRIAGED,
        handler_paths=(
            "agents.action_generation_agent.run",
            "agents.action_generation_agent.run_action_generation_agent",
            "agents.action_agent.run",
            "agents.action_agent.run_action_agent",
        ),
        next_stage=WorkflowStage.ACTIONS_GENERATED,
    ),
    WorkflowStage.ACTIONS_GENERATED: DispatchRoute(
        stage=WorkflowStage.ACTIONS_GENERATED,
        handler_paths=(
            "services.gitlab_issue_service.create_issues_for_workflow",
            "services.gitlab_issue_service.create_gitlab_issues",
            "agents.gitlab_agent.run",
        ),
        next_stage=WorkflowStage.GITLAB_ISSUES_CREATED,
    ),
    WorkflowStage.GITLAB_ISSUES_CREATED: DispatchRoute(
        stage=WorkflowStage.GITLAB_ISSUES_CREATED,
        handler_paths=(),
        next_stage=WorkflowStage.COMPLETED,
    ),
}


def _workflow_stage(workflow_run: WorkflowRun) -> WorkflowStage:
    raw_stage = (
        getattr(workflow_run, "current_stage", None)
        or getattr(workflow_run, "stage", None)
    )
    if raw_stage and is_valid_stage(raw_stage):
        return normalize_stage(raw_stage)

    status = getattr(workflow_run, "status", None)
    if status == "completed":
        return WorkflowStage.COMPLETED
    if status == "failed":
        return WorkflowStage.FAILED

    return INITIAL_STAGE


def _extract_workflow_run_id(event: dict[str, Any] | str | int) -> int:
    if isinstance(event, int):
        return event
    if isinstance(event, str):
        return int(event)

    workflow_run_id = (
        event.get("workflow_run_id")
        or event.get("workflow_id")
        or event.get("id")
    )
    if workflow_run_id is None:
        raise ValueError(f"Workflow event is missing workflow_run_id: {event}")

    return int(workflow_run_id)


def _resolve_handler(handler_paths: tuple[str, ...]) -> AgentHandler | None:
    for handler_path in handler_paths:
        module_name, function_name = handler_path.rsplit(".", 1)
        try:
            module = importlib.import_module(module_name)
        except ImportError:
            logger.debug("Dispatcher handler module not available: %s", module_name)
            continue

        handler = getattr(module, function_name, None)
        if callable(handler):
            logger.info("Dispatcher resolved handler=%s", handler_path)
            return handler

    return None


async def _maybe_await(value: Any) -> Any:
    if asyncio.iscoroutine(value):
        return await value
    return value


async def dispatch_event(event: dict[str, Any] | str | int) -> dict[str, Any]:
    workflow_run_id = _extract_workflow_run_id(event)
    return await dispatch_workflow_run(workflow_run_id)


async def dispatch_workflow_run(workflow_run_id: int) -> dict[str, Any]:
    async with AsyncSessionLocal() as session:
        workflow_service = WorkflowService(session)

        try:
            workflow_run = await workflow_service.get_workflow_run(workflow_run_id)
            current_stage = _workflow_stage(workflow_run)

            logger.info(
                "Dispatcher received workflow_run_id=%s stage=%s status=%s",
                workflow_run_id,
                current_stage.value,
                getattr(workflow_run, "status", None),
            )

            if is_terminal_stage(current_stage):
                logger.info(
                    "Dispatcher ignoring terminal workflow_run_id=%s stage=%s",
                    workflow_run_id,
                    current_stage.value,
                )
                return {
                    "workflow_run_id": workflow_run_id,
                    "stage": current_stage.value,
                    "dispatched": False,
                    "reason": "terminal_stage",
                }

            route = STAGE_ROUTES.get(current_stage)
            if route is None:
                logger.warning(
                    "Dispatcher has no route for workflow_run_id=%s stage=%s",
                    workflow_run_id,
                    current_stage.value,
                )
                return {
                    "workflow_run_id": workflow_run_id,
                    "stage": current_stage.value,
                    "dispatched": False,
                    "reason": "missing_route",
                }

            handler = _resolve_handler(route.handler_paths)
            if handler is not None:
                await _maybe_await(handler(workflow_run_id))
                logger.info(
                    "Dispatcher handler completed workflow_run_id=%s stage=%s",
                    workflow_run_id,
                    current_stage.value,
                )

                refreshed_workflow = await workflow_service.get_workflow_run(
                    workflow_run_id
                )
                refreshed_stage = _workflow_stage(refreshed_workflow)
                if refreshed_stage != current_stage:
                    return {
                        "workflow_run_id": workflow_run_id,
                        "stage": current_stage.value,
                        "dispatched": True,
                        "next_stage": refreshed_stage.value,
                        "reason": "handler_managed_transition",
                    }
            else:
                logger.warning(
                    "Dispatcher found no handler for workflow_run_id=%s stage=%s. "
                    "Advancing only when this stage is terminal-ready.",
                    workflow_run_id,
                    current_stage.value,
                )
                if current_stage != WorkflowStage.GITLAB_ISSUES_CREATED:
                    return {
                        "workflow_run_id": workflow_run_id,
                        "stage": current_stage.value,
                        "dispatched": False,
                        "reason": "missing_handler",
                    }

            if route.next_stage is None:
                return {
                    "workflow_run_id": workflow_run_id,
                    "stage": current_stage.value,
                    "dispatched": True,
                    "next_stage": None,
                    "reason": "handler_managed_transition",
                }

            next_stage = route.next_stage or get_next_stage(current_stage)
            if next_stage is None:
                return {
                    "workflow_run_id": workflow_run_id,
                    "stage": current_stage.value,
                    "dispatched": True,
                    "next_stage": None,
                }

            if next_stage == WorkflowStage.COMPLETED:
                await workflow_service.mark_completed(workflow_run_id)
            else:
                await workflow_service.update_stage(workflow_run_id, next_stage)

            logger.info(
                "Dispatcher advanced workflow_run_id=%s from %s to %s",
                workflow_run_id,
                current_stage.value,
                next_stage.value,
            )
            return {
                "workflow_run_id": workflow_run_id,
                "stage": current_stage.value,
                "dispatched": True,
                "next_stage": next_stage.value,
            }
        except WorkflowNotFoundError:
            logger.warning(
                "Dispatcher could not find workflow_run_id=%s",
                workflow_run_id,
            )
            return {
                "workflow_run_id": workflow_run_id,
                "dispatched": False,
                "reason": "workflow_not_found",
            }
        except Exception as exc:
            logger.exception(
                "Dispatcher failed workflow_run_id=%s",
                workflow_run_id,
            )
            try:
                await workflow_service.mark_failed(workflow_run_id, str(exc))
            except Exception:
                logger.exception(
                    "Dispatcher could not mark workflow_run_id=%s failed.",
                    workflow_run_id,
                )
            return {
                "workflow_run_id": workflow_run_id,
                "dispatched": False,
                "reason": "dispatch_error",
                "error": str(exc),
            }


async def dispatch_workflow(workflow_id: str | int) -> dict[str, Any]:
    return await dispatch_workflow_run(int(workflow_id))
