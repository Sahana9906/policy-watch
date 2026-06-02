from enum import StrEnum


class WorkflowStage(StrEnum):
    REGULATION_DETECTED = "regulation_detected"
    ANALYSIS_COMPLETE = "analysis_complete"
    ANALYSIS_FAILED = "analysis_failed"
    REQUIREMENTS_EXTRACTED = "requirements_extracted"
    RISK_TRIAGED = "risk_triaged"
    ACTIONS_GENERATED = "actions_generated"
    GITLAB_ISSUES_CREATED = "gitlab_issues_created"
    COMPLETED = "completed"
    FAILED = "failed"


INITIAL_STAGE = WorkflowStage.REGULATION_DETECTED
COMPLETED_STAGE = WorkflowStage.COMPLETED
FAILED_STAGE = WorkflowStage.FAILED
TERMINAL_STAGES = {
    WorkflowStage.COMPLETED,
    WorkflowStage.FAILED,
    WorkflowStage.ANALYSIS_FAILED,
}

VALID_TRANSITIONS: dict[WorkflowStage, set[WorkflowStage]] = {
    WorkflowStage.REGULATION_DETECTED: {
        WorkflowStage.ANALYSIS_COMPLETE,
        WorkflowStage.ANALYSIS_FAILED,
        WorkflowStage.REQUIREMENTS_EXTRACTED,
        WorkflowStage.FAILED,
    },
    WorkflowStage.ANALYSIS_COMPLETE: {
        WorkflowStage.REQUIREMENTS_EXTRACTED,
        WorkflowStage.FAILED,
    },
    WorkflowStage.ANALYSIS_FAILED: set(),
    WorkflowStage.REQUIREMENTS_EXTRACTED: {
        WorkflowStage.RISK_TRIAGED,
        WorkflowStage.FAILED,
    },
    WorkflowStage.RISK_TRIAGED: {
        WorkflowStage.ACTIONS_GENERATED,
        WorkflowStage.FAILED,
    },
    WorkflowStage.ACTIONS_GENERATED: {
        WorkflowStage.GITLAB_ISSUES_CREATED,
        WorkflowStage.FAILED,
    },
    WorkflowStage.GITLAB_ISSUES_CREATED: {
        WorkflowStage.COMPLETED,
        WorkflowStage.FAILED,
    },
    WorkflowStage.COMPLETED: set(),
    WorkflowStage.FAILED: set(),
}

NEXT_STAGE: dict[WorkflowStage, WorkflowStage] = {
    WorkflowStage.REGULATION_DETECTED: WorkflowStage.ANALYSIS_COMPLETE,
    WorkflowStage.ANALYSIS_COMPLETE: WorkflowStage.REQUIREMENTS_EXTRACTED,
    WorkflowStage.REQUIREMENTS_EXTRACTED: WorkflowStage.RISK_TRIAGED,
    WorkflowStage.RISK_TRIAGED: WorkflowStage.ACTIONS_GENERATED,
    WorkflowStage.ACTIONS_GENERATED: WorkflowStage.GITLAB_ISSUES_CREATED,
    WorkflowStage.GITLAB_ISSUES_CREATED: WorkflowStage.COMPLETED,
}


def normalize_stage(stage: WorkflowStage | str) -> WorkflowStage:
    if isinstance(stage, WorkflowStage):
        return stage
    return WorkflowStage(stage)


def is_valid_stage(stage: WorkflowStage | str) -> bool:
    try:
        normalize_stage(stage)
        return True
    except ValueError:
        return False


def is_terminal_stage(stage: WorkflowStage | str) -> bool:
    return normalize_stage(stage) in TERMINAL_STAGES


def can_transition(
    current_stage: WorkflowStage | str,
    next_stage: WorkflowStage | str,
) -> bool:
    current = normalize_stage(current_stage)
    target = normalize_stage(next_stage)
    return target in VALID_TRANSITIONS[current]


def get_next_stage(stage: WorkflowStage | str) -> WorkflowStage | None:
    return NEXT_STAGE.get(normalize_stage(stage))


def require_valid_transition(
    current_stage: WorkflowStage | str,
    next_stage: WorkflowStage | str,
) -> None:
    if not can_transition(current_stage, next_stage):
        raise ValueError(
            f"Invalid workflow transition: "
            f"{normalize_stage(current_stage).value} -> "
            f"{normalize_stage(next_stage).value}"
        )


def stage_value(stage: WorkflowStage | str) -> str:
    return normalize_stage(stage).value


def all_stage_values() -> list[str]:
    return [stage.value for stage in WorkflowStage]


def active_stage_values() -> list[str]:
    return [
        stage.value
        for stage in WorkflowStage
        if stage not in TERMINAL_STAGES
    ]
