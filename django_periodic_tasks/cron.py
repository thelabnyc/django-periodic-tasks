from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from croniter import CroniterBadCronError, croniter


def validate_cron_expression(expression: str) -> bool:
    """Validate a cron expression string."""
    result: bool = croniter.is_valid(expression)
    return result


def compute_next_run_at(
    cron_expression: str,
    timezone_name: str = "UTC",
    base_time: datetime | None = None,
) -> datetime:
    """Compute the next run time from a cron expression.

    The base_time is interpreted in the given timezone for cron matching,
    but the result is always returned in UTC.
    """
    try:
        tz = ZoneInfo(timezone_name)
    except (KeyError, ValueError) as e:
        raise ValueError(f"Invalid timezone: {timezone_name}") from e

    if base_time is None:
        base_time = datetime.now(tz=timezone.utc)

    # Convert base_time to the target timezone for correct cron matching
    base_in_tz = base_time.astimezone(tz)

    try:
        cron = croniter(cron_expression, base_in_tz)
    except (CroniterBadCronError, KeyError, ValueError) as e:
        raise ValueError(f"Invalid cron expression: {cron_expression}") from e

    next_time: datetime = cron.get_next(datetime)
    # croniter returns tz-aware datetime in the same tz as input
    return next_time.astimezone(timezone.utc)
