from collections.abc import Callable
import time
from typing import Any

from app.core.errors import BadRequestError

JobHandler = Callable[[dict[str, Any]], dict[str, Any]]


class TransientJobError(Exception):
    """A handler failure that can be retried by the worker."""


def echo_handler(payload: dict[str, Any]) -> dict[str, Any]:
    return payload


def always_fail_handler(payload: dict[str, Any]) -> dict[str, Any]:
    raise TransientJobError(str(payload.get("message", "handler failed")))


def fail_once_handler(payload: dict[str, Any]) -> dict[str, Any]:
    if payload.get("fail") is True:
        raise TransientJobError("configured first-attempt failure")
    return payload


def sleep_handler(payload: dict[str, Any]) -> dict[str, Any]:
    time.sleep(float(payload.get("seconds", 1)))
    return payload


HANDLERS: dict[str, JobHandler] = {
    "echo": echo_handler,
    "always_fail": always_fail_handler,
    "fail_once": fail_once_handler,
    "sleep": sleep_handler,
}


def get_handler(name: str) -> JobHandler:
    try:
        return HANDLERS[name]
    except KeyError as exc:
        raise BadRequestError("Unsupported job handler", {"handler": name}) from exc


def ensure_supported_handler(name: str) -> None:
    get_handler(name)
