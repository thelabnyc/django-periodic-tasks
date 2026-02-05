from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from django.test import SimpleTestCase

from django_periodic_tasks.cron import compute_next_run_at, validate_cron_expression


class TestValidateCronExpression(SimpleTestCase):
    def test_valid_every_minute(self) -> None:
        self.assertTrue(validate_cron_expression("* * * * *"))

    def test_valid_specific_time(self) -> None:
        self.assertTrue(validate_cron_expression("0 5 * * *"))

    def test_valid_every_30_minutes(self) -> None:
        self.assertTrue(validate_cron_expression("*/30 * * * *"))

    def test_valid_complex_expression(self) -> None:
        self.assertTrue(validate_cron_expression("0 0 1,15 * *"))

    def test_valid_day_of_week(self) -> None:
        self.assertTrue(validate_cron_expression("0 0 * * 0"))

    def test_valid_range(self) -> None:
        self.assertTrue(validate_cron_expression("0 9-17 * * 1-5"))

    def test_invalid_empty(self) -> None:
        self.assertFalse(validate_cron_expression(""))

    def test_invalid_garbage(self) -> None:
        self.assertFalse(validate_cron_expression("not a cron"))

    def test_invalid_too_few_fields(self) -> None:
        self.assertFalse(validate_cron_expression("* * *"))

    def test_invalid_out_of_range(self) -> None:
        self.assertFalse(validate_cron_expression("99 * * * *"))


class TestComputeNextRunAt(SimpleTestCase):
    def test_every_minute(self) -> None:
        base = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        result = compute_next_run_at("* * * * *", base_time=base)
        expected = datetime(2025, 1, 1, 12, 1, 0, tzinfo=timezone.utc)
        self.assertEqual(result, expected)

    def test_daily_at_5am(self) -> None:
        base = datetime(2025, 1, 1, 6, 0, 0, tzinfo=timezone.utc)
        result = compute_next_run_at("0 5 * * *", base_time=base)
        expected = datetime(2025, 1, 2, 5, 0, 0, tzinfo=timezone.utc)
        self.assertEqual(result, expected)

    def test_daily_at_5am_before_5am(self) -> None:
        base = datetime(2025, 1, 1, 4, 0, 0, tzinfo=timezone.utc)
        result = compute_next_run_at("0 5 * * *", base_time=base)
        expected = datetime(2025, 1, 1, 5, 0, 0, tzinfo=timezone.utc)
        self.assertEqual(result, expected)

    def test_every_30_minutes(self) -> None:
        base = datetime(2025, 1, 1, 12, 10, 0, tzinfo=timezone.utc)
        result = compute_next_run_at("*/30 * * * *", base_time=base)
        expected = datetime(2025, 1, 1, 12, 30, 0, tzinfo=timezone.utc)
        self.assertEqual(result, expected)

    def test_with_timezone(self) -> None:
        eastern = ZoneInfo("America/New_York")
        base = datetime(2025, 1, 1, 4, 0, 0, tzinfo=eastern)
        result = compute_next_run_at("0 5 * * *", timezone_name="America/New_York", base_time=base)
        expected = datetime(2025, 1, 1, 5, 0, 0, tzinfo=eastern)
        self.assertEqual(result, expected)

    def test_result_is_utc(self) -> None:
        base = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        result = compute_next_run_at("* * * * *", base_time=base)
        self.assertEqual(result.tzinfo, timezone.utc)

    def test_timezone_result_is_utc(self) -> None:
        eastern = ZoneInfo("America/New_York")
        base = datetime(2025, 1, 1, 4, 0, 0, tzinfo=eastern)
        result = compute_next_run_at("0 5 * * *", timezone_name="America/New_York", base_time=base)
        self.assertEqual(result.tzinfo, timezone.utc)

    def test_default_base_time_is_now(self) -> None:
        result = compute_next_run_at("* * * * *")
        self.assertIsNotNone(result)
        self.assertEqual(result.tzinfo, timezone.utc)

    def test_weekly_sunday(self) -> None:
        # 2025-01-01 is a Wednesday
        base = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        result = compute_next_run_at("0 0 * * 0", base_time=base)
        # Next Sunday is Jan 5
        expected = datetime(2025, 1, 5, 0, 0, 0, tzinfo=timezone.utc)
        self.assertEqual(result, expected)

    def test_invalid_timezone_raises_valueerror(self) -> None:
        base = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        with self.assertRaises(ValueError) as cm:
            compute_next_run_at("* * * * *", timezone_name="Not/A/Timezone", base_time=base)
        self.assertIn("Invalid timezone", str(cm.exception))
