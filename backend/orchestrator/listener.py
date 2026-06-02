import asyncio
import json
import logging
from dataclasses import dataclass
from typing import Any

import asyncpg

from db.session import DATABASE_URL
from orchestrator.dispatcher import dispatch_event

logger = logging.getLogger(__name__)

LISTEN_CHANNELS = ("regulation_changes", "workflow_runs")
RECONNECT_DELAY_SECONDS = 5
KEEPALIVE_SLEEP_SECONDS = 3600


@dataclass(frozen=True, slots=True)
class OrchestrationEvent:
    channel: str
    payload: dict[str, Any]
    raw_payload: str
    pid: int


def _asyncpg_database_url() -> str:
    if DATABASE_URL.startswith("postgresql+asyncpg://"):
        return DATABASE_URL.replace("postgresql+asyncpg://", "postgresql://", 1)
    return DATABASE_URL


def _parse_payload(payload: str) -> dict[str, Any]:
    if not payload:
        return {}

    try:
        parsed = json.loads(payload)
        if isinstance(parsed, dict):
            return parsed
        return {"value": parsed}
    except json.JSONDecodeError:
        return {"id": payload}


async def dispatch_orchestration_event(event: OrchestrationEvent) -> None:
    logger.info(
        "Orchestration event received channel=%s pid=%s payload=%s",
        event.channel,
        event.pid,
        event.payload,
    )

    workflow_id = (
        event.payload.get("workflow_run_id")
        or event.payload.get("workflow_id")
        or event.payload.get("id")
    )
    regulation_change_id = (
        event.payload.get("regulation_change_id")
        or event.payload.get("change_id")
    )

    logger.info(
        "Stage-2 dispatch hook prepared channel=%s workflow_id=%s regulation_change_id=%s",
        event.channel,
        workflow_id,
        regulation_change_id,
    )

    if event.channel == "workflow_runs" and workflow_id is not None:
        try:
            result = await dispatch_event(event.payload)
            logger.info(
                "Workflow event dispatched workflow_id=%s result=%s",
                workflow_id,
                result,
            )
        except Exception:
            logger.exception(
                "Workflow dispatch failed workflow_id=%s payload=%s",
                workflow_id,
                event.payload,
            )


async def _notification_handler(
    connection: asyncpg.Connection,
    pid: int,
    channel: str,
    payload: str,
) -> None:
    event = OrchestrationEvent(
        channel=channel,
        payload=_parse_payload(payload),
        raw_payload=payload,
        pid=pid,
    )

    try:
        await dispatch_orchestration_event(event)
    except Exception:
        logger.exception(
            "Failed to handle orchestration event channel=%s payload=%s",
            channel,
            payload,
        )


async def _subscribe(connection: asyncpg.Connection) -> None:
    for channel in LISTEN_CHANNELS:
        await connection.add_listener(channel, _notification_handler)
        logger.info("Subscribed to PostgreSQL channel '%s'.", channel)


async def _unsubscribe(connection: asyncpg.Connection) -> None:
    for channel in LISTEN_CHANNELS:
        try:
            await connection.remove_listener(channel, _notification_handler)
        except Exception:
            logger.debug(
                "Listener cleanup skipped for channel '%s'.",
                channel,
                exc_info=True,
            )


async def listen_for_notifications() -> None:
    database_url = _asyncpg_database_url()
    logger.info(
        "Starting PostgreSQL orchestration listener for channels: %s",
        ", ".join(LISTEN_CHANNELS),
    )

    while True:
        connection: asyncpg.Connection | None = None
        try:
            connection = await asyncpg.connect(database_url)
            await _subscribe(connection)

            while True:
                await asyncio.sleep(KEEPALIVE_SLEEP_SECONDS)

        except asyncio.CancelledError:
            logger.info("PostgreSQL orchestration listener cancellation requested.")
            raise
        except (asyncpg.PostgresError, OSError):
            logger.exception(
                "PostgreSQL listener connection failed. Reconnecting in %s seconds.",
                RECONNECT_DELAY_SECONDS,
            )
            await asyncio.sleep(RECONNECT_DELAY_SECONDS)
        except Exception:
            logger.exception(
                "Unexpected listener failure. Reconnecting in %s seconds.",
                RECONNECT_DELAY_SECONDS,
            )
            await asyncio.sleep(RECONNECT_DELAY_SECONDS)
        finally:
            if connection is not None:
                try:
                    await _unsubscribe(connection)
                    await connection.close()
                except Exception:
                    logger.debug("Listener connection cleanup failed.", exc_info=True)


async def start_listener() -> None:
    await listen_for_notifications()
