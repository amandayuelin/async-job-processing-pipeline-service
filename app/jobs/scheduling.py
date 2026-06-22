from datetime import datetime, timedelta, timezone

from app.core.errors import BadRequestError


def validate_cron_expression(expression: str) -> str:
    parts = expression.strip().split()
    if len(parts) != 5:
        raise BadRequestError("Cron expression must have five fields", {"cron": expression})
    ranges = [(0, 59), (0, 23), (1, 31), (1, 12), (0, 6)]
    for part, (minimum, maximum) in zip(parts, ranges, strict=True):
        _parse_cron_part(part, minimum, maximum)
    return " ".join(parts)


def next_cron_run(expression: str, after: datetime | None = None) -> datetime:
    expression = validate_cron_expression(expression)
    after = after or datetime.now(timezone.utc)
    if after.tzinfo is None:
        after = after.replace(tzinfo=timezone.utc)
    candidate = after.replace(second=0, microsecond=0) + timedelta(minutes=1)
    minute, hour, day, month, weekday = expression.split()

    # One year of minute checks is enough for MVP cron expressions and prevents
    # accidentally looping forever on impossible schedules.
    for _ in range(366 * 24 * 60):
        if (
            candidate.minute in _parse_cron_part(minute, 0, 59)
            and candidate.hour in _parse_cron_part(hour, 0, 23)
            and candidate.day in _parse_cron_part(day, 1, 31)
            and candidate.month in _parse_cron_part(month, 1, 12)
            and candidate.weekday() in _parse_cron_part(weekday, 0, 6)
        ):
            return candidate
        candidate += timedelta(minutes=1)
    raise BadRequestError("Cron expression has no run in the next year", {"cron": expression})


def _parse_cron_part(part: str, minimum: int, maximum: int) -> set[int]:
    if part == "*":
        return set(range(minimum, maximum + 1))
    if part.startswith("*/"):
        try:
            step = int(part[2:])
        except ValueError as exc:
            raise BadRequestError("Cron step must be an integer", {"part": part}) from exc
        if step <= 0:
            raise BadRequestError("Cron step must be positive", {"part": part})
        return set(range(minimum, maximum + 1, step))
    try:
        value = int(part)
    except ValueError as exc:
        raise BadRequestError("Cron field must be '*', '*/n', or an integer", {"part": part}) from exc
    if value < minimum or value > maximum:
        raise BadRequestError("Cron value out of range", {"part": part, "minimum": minimum, "maximum": maximum})
    return {value}
