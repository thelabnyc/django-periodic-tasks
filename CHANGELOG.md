# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## v0.1.0a3 (2026-02-05)

## v0.1.0a2 (2026-02-05)

### Fix

- gitignored uv.lock

## v0.1.0a1 (2026-02-05)

## v0.1.0a0 (2026-02-05)

### Feat

- auto-discover tasks.py modules in installed apps
- add exactly-once docs, fluentcron examples, remove scheduler_db_worker
- add bug fixes and exactly-once decorator for scheduled tasks
- add MkDocs documentation with auto-generated API reference
- add integration tests
- add Django admin interface
- add management commands
- add PeriodicTaskScheduler with Docker test infrastructure
- add code-to-DB schedule sync
- add task path resolver
- add schedule registry and @scheduled_task decorator
- add ScheduledTask model
- add cron expression utilities
- initial project scaffolding

### Fix

- race condition in scheduler re-queuing
- uv commands
- remove dead code around TaskExec SKIPPED status
- hold row locks for entire tick to prevent duplicate task enqueue

### Refactor

- consolidate mocked tests and remove redundant test cases
- cleanup imports
- tweak how scheduler start works
