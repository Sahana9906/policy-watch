import asyncio
import logging
import os
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote

import httpx

logger = logging.getLogger(__name__)

DEFAULT_GITLAB_BASE_URL = "https://gitlab.com"
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


class GitLabClientError(Exception):
    def __init__(
        self,
        message: str,
        status_code: int | None = None,
        response_payload: Any | None = None,
    ):
        super().__init__(message)
        self.status_code = status_code
        self.response_payload = response_payload


@dataclass(frozen=True, slots=True)
class GitLabClientConfig:
    base_url: str = DEFAULT_GITLAB_BASE_URL
    private_token: str | None = None
    project_id: str | None = None
    timeout_seconds: float = 10.0
    retry_attempts: int = 3
    retry_backoff_seconds: float = 0.75

    @classmethod
    def from_env(cls) -> "GitLabClientConfig":
        return cls(
            base_url=os.getenv("GITLAB_BASE_URL", DEFAULT_GITLAB_BASE_URL).rstrip("/"),
            private_token=os.getenv("GITLAB_TOKEN")
            or os.getenv("GITLAB_PRIVATE_TOKEN"),
            project_id=os.getenv("GITLAB_PROJECT_ID"),
            timeout_seconds=float(os.getenv("GITLAB_TIMEOUT_SECONDS", "10")),
            retry_attempts=int(os.getenv("GITLAB_RETRY_ATTEMPTS", "3")),
            retry_backoff_seconds=float(
                os.getenv("GITLAB_RETRY_BACKOFF_SECONDS", "0.75")
            ),
        )


@dataclass(frozen=True, slots=True)
class GitLabIssue:
    id: int | None
    iid: int | None
    title: str
    web_url: str | None
    state: str | None
    raw: dict[str, Any]


class GitLabClient:
    def __init__(
        self,
        config: GitLabClientConfig | None = None,
        http_client: httpx.AsyncClient | None = None,
    ):
        self.config = config or GitLabClientConfig.from_env()
        self._http_client = http_client
        self._owns_client = http_client is None

    async def __aenter__(self) -> "GitLabClient":
        self._ensure_http_client()
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        if self._owns_client and self._http_client is not None:
            await self._http_client.aclose()
            self._http_client = None

    async def create_issue(
        self,
        payload: dict[str, Any],
        project_id: str | int | None = None,
    ) -> GitLabIssue:
        resolved_project_id = str(project_id or self.config.project_id or "").strip()
        if not resolved_project_id:
            raise GitLabClientError(
                "GitLab project id is required. Set GITLAB_PROJECT_ID or pass project_id."
            )

        response_payload = await self._request(
            "POST",
            f"/api/v4/projects/{quote(resolved_project_id, safe='')}/issues",
            json=payload,
        )
        return GitLabIssue(
            id=response_payload.get("id"),
            iid=response_payload.get("iid"),
            title=response_payload.get("title") or payload.get("title") or "",
            web_url=response_payload.get("web_url"),
            state=response_payload.get("state"),
            raw=response_payload,
        )

    async def _request(
        self,
        method: str,
        path: str,
        **kwargs: Any,
    ) -> dict[str, Any]:
        client = self._ensure_http_client()
        last_error: Exception | None = None

        for attempt in range(1, self.config.retry_attempts + 1):
            try:
                response = await client.request(method, path, **kwargs)
                if response.status_code in RETRYABLE_STATUS_CODES:
                    raise GitLabClientError(
                        "GitLab returned a retryable status.",
                        status_code=response.status_code,
                        response_payload={
                            "body": self._safe_response_payload(response),
                            "retry_after": response.headers.get("Retry-After"),
                        },
                    )

                response.raise_for_status()
                payload = self._safe_response_payload(response)
                if not isinstance(payload, dict):
                    raise GitLabClientError("GitLab response was not a JSON object.")
                return payload
            except (httpx.HTTPError, GitLabClientError) as exc:
                last_error = exc
                status_code = getattr(exc, "status_code", None)
                retryable = isinstance(exc, httpx.TransportError) or (
                    status_code in RETRYABLE_STATUS_CODES
                )

                if not retryable or attempt >= self.config.retry_attempts:
                    logger.exception(
                        "GitLab request failed method=%s path=%s attempt=%s/%s status=%s",
                        method,
                        path,
                        attempt,
                        self.config.retry_attempts,
                        status_code,
                    )
                    raise self._normalize_error(exc) from exc

                delay = self._retry_delay(attempt, exc)
                logger.warning(
                    "Retrying GitLab request method=%s path=%s attempt=%s/%s status=%s delay=%s",
                    method,
                    path,
                    attempt,
                    self.config.retry_attempts,
                    status_code,
                    delay,
                )
                await asyncio.sleep(delay)

        raise GitLabClientError(f"GitLab request failed: {last_error}")

    def _ensure_http_client(self) -> httpx.AsyncClient:
        if not self.config.private_token:
            raise GitLabClientError(
                "GitLab token is required. Set GITLAB_TOKEN or GITLAB_PRIVATE_TOKEN."
            )

        if self._http_client is None:
            self._http_client = httpx.AsyncClient(
                base_url=self.config.base_url,
                timeout=self.config.timeout_seconds,
                headers={
                    "PRIVATE-TOKEN": self.config.private_token,
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                    "User-Agent": "PolicyWatch GitLab integration",
                },
            )
        return self._http_client

    def _retry_delay(self, attempt: int, exc: Exception | None = None) -> float:
        retry_after = None
        if isinstance(exc, GitLabClientError) and isinstance(
            exc.response_payload,
            dict,
        ):
            retry_after = exc.response_payload.get("retry_after")

        if retry_after:
            try:
                return float(retry_after)
            except ValueError:
                logger.debug("Ignoring invalid GitLab Retry-After value: %s", retry_after)

        return self.config.retry_backoff_seconds * attempt

    @staticmethod
    def _safe_response_payload(response: httpx.Response) -> Any:
        try:
            return response.json()
        except ValueError:
            return {"text": response.text}

    @staticmethod
    def _normalize_error(exc: Exception) -> GitLabClientError:
        if isinstance(exc, GitLabClientError):
            return exc
        if isinstance(exc, httpx.HTTPStatusError):
            response = exc.response
            return GitLabClientError(
                f"GitLab request failed with status {response.status_code}.",
                status_code=response.status_code,
                response_payload=GitLabClient._safe_response_payload(response),
            )
        return GitLabClientError(f"GitLab request failed: {exc}")
