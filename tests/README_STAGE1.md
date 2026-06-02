# Stage 1 Regulation Monitor Test Strategy

These tests focus only on the Stage 1 path:

`URL -> fetch -> hash -> snapshot -> workflow creation -> no duplicate workflow on unchanged content`

## What Must Be Tested

- Fetching regulation content from a URL as HTML and PDF.
- Safe fetch failures for timeouts, connection failures, empty bodies, and invalid content.
- Hash-based change detection.
- Snapshot creation only when content changes.
- Workflow run creation only when content changes.
- Scan job status changes: `pending -> running -> completed`, `pending -> running -> no_change`, and `pending -> running -> failed`.
- Initial workflow stage semantics: the Stage 1 workflow starts at `regulation_detected`.

## Critical Edge Cases

- First scan has no previous hash and must create a snapshot and workflow.
- Second scan has the same hash and must not create another snapshot or workflow.
- Later scan has a different hash and must create a new snapshot and workflow.
- Empty fetched body is rejected before parsing.
- Unknown binary content is rejected by content detection.
- A fetch exception marks the scan job failed and leaves no partial snapshot/workflow.

## Async Failure Cases

- Fetch coroutine times out.
- Fetch coroutine raises a transport-style exception.
- Ingestion retry rolls back failed attempts.
- Monitoring agent catches ingestion failure and persists scan status as `failed`.

## DB Consistency Checks

- `regulations.last_hash` matches the latest snapshot hash after a changed scan.
- `regulation_snapshots` count does not increase for unchanged content.
- `workflow_runs` count does not increase for unchanged content.
- Failed fetches do not leave orphaned snapshots or workflows.
- The current model is lightweight, so tests assert `current_stage` when the column exists and use the Stage 1 default otherwise.

## Workflow Transition Checks

- Workflow creation starts in `regulation_detected`.
- Valid transition to `requirements_extracted` succeeds.
- Invalid jump to `gitlab_issues_created` is rejected.
- Failed workflows are marked with `status = failed`.

## Mocking Strategy

- External HTTP requests: monkeypatch `services.ingestion_service.fetch_tool.fetch_url` or `IngestionService.fetch_regulation_content`.
- Gemini calls: Stage 1 PDF tests should monkeypatch `services.ingestion_service.gemini_upload_tool.run_gemini_upload_tool`; Stage 1 HTML tests avoid Gemini entirely.
- GitLab calls: Stage 1 tests do not call GitLab. If orchestration is expanded, monkeypatch `services.gitlab_issue_service.GitLabClient.create_issue`.
- PostgreSQL notifications: monkeypatch `WorkflowService.emit_workflow_event` to a no-op so Stage 1 tests do not require `pg_notify`.
- Async DB sessions: use the `db_session` fixture from `conftest.py`, backed by `TEST_DATABASE_URL` or in-memory SQLite.

## Example Test Data

- `sample_html_v1`: 72-hour incident reporting obligation.
- `sample_html_v2`: changed 24-hour incident reporting obligation plus evidence retention.
- `sample_pdf`: minimal `%PDF` byte stream for PDF content detection.
- `mock_workflow_payload`: representative `workflow_run_updated` event.
- `fake_hashes`: deterministic labels for changed/unchanged assertions.
- Sample DB rows are created by `seeded_regulation` and `pending_scan_job`.

## Local Testing Instructions

Install lightweight test dependencies if they are not present:

```bash
pip install pytest pytest-asyncio aiosqlite
```

Run all tests:

```bash
pytest
```

Run only Stage 1 tests:

```bash
pytest tests/test_fetch_tool.py tests/test_ingestion_service.py tests/test_workflow_service.py tests/test_monitoring_agent.py
```

Run one async integration scenario with logs:

```bash
pytest tests/test_monitoring_agent.py::test_stage1_complete_flow_creates_snapshot_hash_and_workflow -vv -s
```

Use PostgreSQL instead of SQLite:

```bash
set TEST_DATABASE_URL=postgresql+asyncpg://postgres:postgres@localhost:5432/policywatch_test
pytest tests/test_monitoring_agent.py -vv
```

Inspect DB state after a failing test by temporarily adding a `select(...)` inside the test and running with `-s`, or point `TEST_DATABASE_URL` at a disposable Postgres database and query `regulations`, `regulation_snapshots`, `scan_jobs`, and `workflow_runs` after pausing teardown.
