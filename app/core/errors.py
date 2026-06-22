from dataclasses import dataclass, field
from typing import Any


@dataclass
class ServiceError(Exception):
    code: str
    message: str
    status_code: int
    details: dict[str, Any] = field(default_factory=dict)


class NotFoundError(ServiceError):
    def __init__(self, message: str = "Resource not found") -> None:
        super().__init__("not_found", message, 404)


class ConflictError(ServiceError):
    def __init__(self, message: str, details: dict[str, Any] | None = None) -> None:
        super().__init__("conflict", message, 409, details or {})


class BadRequestError(ServiceError):
    def __init__(self, message: str, details: dict[str, Any] | None = None) -> None:
        super().__init__("bad_request", message, 400, details or {})


class DependencyUnavailableError(ServiceError):
    def __init__(self, message: str, details: dict[str, Any] | None = None) -> None:
        super().__init__("dependency_unavailable", message, 503, details or {})
