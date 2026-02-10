"""Type-checking assertions for django_periodic_tasks.

This file is analyzed by mypy (via ``mypy sandbox/``) but never imported at
runtime.  It verifies that our generic type annotations actually work:

* **Positive cases** — valid code that must type-check without errors.
* **Negative cases** — invalid code with ``# type: ignore[error-code]``.
  If a ``type: ignore`` becomes unused (the error disappears), mypy's
  ``warn_unused_ignores`` setting will flag the regression.
"""

from django_periodic_tasks.compat import task
from django_periodic_tasks.decorators import exactly_once, is_exactly_once
from django_periodic_tasks.registry import ScheduleRegistry, scheduled_task

# ---------------------------------------------------------------------------
# Positive: valid usage (must type-check cleanly)
# ---------------------------------------------------------------------------


@scheduled_task(cron="* * * * *")
@task()
def my_task(foo: str, bar: int = 0) -> None:
    pass


@task()
@exactly_once
def my_once_task() -> str:
    return "done"


# Correct param types — must not error
my_task.enqueue(foo="hello", bar=1)
my_task.enqueue(foo="hello")  # bar is optional

# is_exactly_once returns bool
check: bool = is_exactly_once(lambda: None)

# Valid registry with explicit params
_registry = ScheduleRegistry()

# scheduled_task with all explicit options
scheduled_task(
    cron="0 * * * *",
    name="my-schedule",
    timezone="America/New_York",
    args=[1, "two"],
    kwargs={"key": "value"},
    queue_name="low",
    priority=5,
    backend="default",
)

# ---------------------------------------------------------------------------
# Negative: wrong types (each MUST trigger the marked mypy error)
# ---------------------------------------------------------------------------

# cron must be str, not int
scheduled_task(cron=123)  # type: ignore[arg-type]

# priority must be int, not str
scheduled_task(cron="* * * * *", priority="high")  # type: ignore[arg-type]

# timezone must be str, not int
scheduled_task(cron="* * * * *", timezone=0)  # type: ignore[arg-type]

# exactly_once requires a callable, not a string
exactly_once("not a function")  # type: ignore[arg-type]

# NOTE: The following negative cases are commented out because @task() from
# django_tasks is currently untyped, so mypy cannot check argument types.
# Uncomment if/when django_tasks gains proper decorator typing.
# my_task.enqueue(foo=123)  # type: ignore[arg-type]
# my_task.enqueue(foo="ok", bar="wrong")  # type: ignore[arg-type]
