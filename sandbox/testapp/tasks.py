from django_tasks import task

from django_periodic_tasks.decorators import exactly_once


@task()
def example_task() -> None:
    pass


@task()
def example_task_with_args(name: str, count: int = 1) -> str:
    return f"{name}: {count}"


@task()
@exactly_once
def exactly_once_task() -> str:
    return "exactly-once-done"
