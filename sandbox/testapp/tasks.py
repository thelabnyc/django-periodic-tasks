from django_tasks import task


@task()
def example_task() -> None:
    pass


@task()
def example_task_with_args(name: str, count: int = 1) -> str:
    return f"{name}: {count}"
