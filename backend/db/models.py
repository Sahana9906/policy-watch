from datetime import datetime
from sqlalchemy import Column, Integer, String, Text, DateTime, ForeignKey, Boolean, JSON
from sqlalchemy.orm import relationship
from .session import Base

class Regulation(Base):
    __tablename__ = "regulations"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False)
    url = Column(String, nullable=False)
    last_hash = Column(String, nullable=True)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    snapshots = relationship("RegulationSnapshot", back_populates="regulation")
    scan_jobs = relationship("ScanJob", back_populates="regulation")
    workflow_runs = relationship("WorkflowRun", back_populates="regulation")
    compliance_requirements = relationship("ComplianceRequirement", back_populates="regulation")

class ScanJob(Base):
    __tablename__ = "scan_jobs"
    
    id = Column(Integer, primary_key=True, index=True)
    regulation_id = Column(Integer, ForeignKey("regulations.id"), nullable=False)
    status = Column(String, default="pending") # pending, running, completed, failed
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    regulation = relationship("Regulation", back_populates="scan_jobs")

class RegulationSnapshot(Base):
    __tablename__ = "regulation_snapshots"
    
    id = Column(Integer, primary_key=True, index=True)
    regulation_id = Column(Integer, ForeignKey("regulations.id"), nullable=False)
    content_hash = Column(String, nullable=False)
    content_text = Column(Text, nullable=True)
    gemini_file_id = Column(String, nullable=True) # For Gemini File API
    created_at = Column(DateTime, default=datetime.utcnow)

    regulation = relationship("Regulation", back_populates="snapshots")
    changes = relationship("RegulationChange", back_populates="snapshot")
    analyses_as_new = relationship(
        "ChangeAnalysis",
        back_populates="new_snapshot",
        foreign_keys="ChangeAnalysis.new_snapshot_id",
    )

class RegulationChange(Base):
    __tablename__ = "regulation_changes"
    
    id = Column(Integer, primary_key=True, index=True)
    regulation_id = Column(Integer, ForeignKey("regulations.id"), nullable=False)
    snapshot_id = Column(Integer, ForeignKey("regulation_snapshots.id"), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

    snapshot = relationship("RegulationSnapshot", back_populates="changes")

class WorkflowRun(Base):
    __tablename__ = "workflow_runs"
    
    id = Column(Integer, primary_key=True, index=True)
    regulation_id = Column(Integer, ForeignKey("regulations.id"), nullable=False)
    snapshot_id = Column(Integer, ForeignKey("regulation_snapshots.id"), nullable=True)
    scan_id = Column(Integer, ForeignKey("scan_jobs.id"), nullable=True)
    current_stage = Column(String, default="regulation_detected")
    status = Column(String, default="pending") # pending, processing, completed, failed
    error_message = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    completed_at = Column(DateTime, nullable=True)

    regulation = relationship("Regulation", back_populates="workflow_runs")
    change_analyses = relationship("ChangeAnalysis", back_populates="workflow_run")
    compliance_requirements = relationship("ComplianceRequirement", back_populates="workflow_run")
    compliance_actions = relationship("ComplianceAction", back_populates="workflow_run")
    events = relationship("WorkflowEvent", back_populates="workflow_run")

class ChangeAnalysis(Base):
    __tablename__ = "change_analyses"

    id = Column(Integer, primary_key=True, index=True)
    workflow_run_id = Column(Integer, ForeignKey("workflow_runs.id"), nullable=False)
    regulation_id = Column(Integer, ForeignKey("regulations.id"), nullable=False)
    old_snapshot_id = Column(Integer, ForeignKey("regulation_snapshots.id"), nullable=True)
    new_snapshot_id = Column(Integer, ForeignKey("regulation_snapshots.id"), nullable=False)
    summary = Column(Text, nullable=False)
    severity = Column(String, nullable=False)
    impacted_domains = Column(JSON, nullable=False, default=list)
    changed_clauses = Column(JSON, nullable=False, default=list)
    recommended_next_action = Column(Text, nullable=False)
    raw_response = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    workflow_run = relationship("WorkflowRun", back_populates="change_analyses")
    regulation = relationship("Regulation")
    old_snapshot = relationship("RegulationSnapshot", foreign_keys=[old_snapshot_id])
    new_snapshot = relationship(
        "RegulationSnapshot",
        back_populates="analyses_as_new",
        foreign_keys=[new_snapshot_id],
    )


class ComplianceRequirement(Base):
    __tablename__ = "compliance_requirements"

    id = Column(Integer, primary_key=True, index=True)
    regulation_id = Column(Integer, ForeignKey("regulations.id"), nullable=False)
    workflow_run_id = Column(Integer, ForeignKey("workflow_runs.id"), nullable=False)
    requirement_text = Column(Text, nullable=False)
    impacted_domain = Column(String, nullable=True)
    severity = Column(String, nullable=False, default="medium")
    created_at = Column(DateTime, default=datetime.utcnow)

    regulation = relationship("Regulation", back_populates="compliance_requirements")
    workflow_run = relationship("WorkflowRun", back_populates="compliance_requirements")
    actions = relationship("ComplianceAction", back_populates="requirement")


class ComplianceAction(Base):
    __tablename__ = "compliance_actions"

    id = Column(Integer, primary_key=True, index=True)
    requirement_id = Column(Integer, ForeignKey("compliance_requirements.id"), nullable=False)
    workflow_run_id = Column(Integer, ForeignKey("workflow_runs.id"), nullable=False)
    action_title = Column(String, nullable=False)
    action_description = Column(Text, nullable=False)
    status = Column(String, nullable=False, default="pending")
    gitlab_issue_id = Column(Integer, nullable=True)
    gitlab_issue_iid = Column(Integer, nullable=True)
    gitlab_issue_url = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    requirement = relationship("ComplianceRequirement", back_populates="actions")
    workflow_run = relationship("WorkflowRun", back_populates="compliance_actions")


class WorkflowEvent(Base):
    __tablename__ = "workflow_events"

    id = Column(Integer, primary_key=True, index=True)
    workflow_run_id = Column(Integer, ForeignKey("workflow_runs.id"), nullable=False)
    stage = Column(String, nullable=False)
    message = Column(Text, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

    workflow_run = relationship("WorkflowRun", back_populates="events")
