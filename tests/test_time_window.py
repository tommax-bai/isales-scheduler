"""Pure tests for time-window judgment + holiday + next-window-start.

Covers the 4 Scenarios in time-window spec § 窗外 lead 推迟.
"""

from __future__ import annotations

from datetime import date, datetime
from zoneinfo import ZoneInfo

from isales_scheduler.time_window import (
    is_holiday_for_campaign,
    is_in_window,
    next_window_start,
)

TZ = ZoneInfo("Asia/Shanghai")


WORKDAY_WINDOWS = [
    {"days": ["mon", "tue", "wed", "thu", "fri"], "start": "09:00", "end": "12:00"},
    {"days": ["mon", "tue", "wed", "thu", "fri"], "start": "14:00", "end": "18:00"},
    {"days": ["sat"], "start": "10:00", "end": "16:00"},
]


def test_is_in_window_inside_morning() -> None:
    # 2026-05-04 is a Monday
    assert is_in_window(WORKDAY_WINDOWS, datetime(2026, 5, 4, 9, 30, tzinfo=TZ)) is True


def test_is_in_window_lunch_break() -> None:
    assert is_in_window(WORKDAY_WINDOWS, datetime(2026, 5, 4, 12, 30, tzinfo=TZ)) is False


def test_is_in_window_evening() -> None:
    assert is_in_window(WORKDAY_WINDOWS, datetime(2026, 5, 4, 19, 0, tzinfo=TZ)) is False


def test_is_in_window_saturday() -> None:
    assert is_in_window(WORKDAY_WINDOWS, datetime(2026, 5, 9, 11, 0, tzinfo=TZ)) is True


def test_is_in_window_sunday_no_window() -> None:
    assert is_in_window(WORKDAY_WINDOWS, datetime(2026, 5, 10, 11, 0, tzinfo=TZ)) is False


def test_is_in_window_empty_array() -> None:
    assert is_in_window([], datetime(2026, 5, 4, 11, 0, tzinfo=TZ)) is False


def test_is_in_window_boundary_inclusive_start_exclusive_end() -> None:
    # 09:00 included, 12:00 excluded
    assert is_in_window(WORKDAY_WINDOWS, datetime(2026, 5, 4, 9, 0, tzinfo=TZ)) is True
    assert is_in_window(WORKDAY_WINDOWS, datetime(2026, 5, 4, 12, 0, tzinfo=TZ)) is False


def test_holiday_check() -> None:
    holidays = {date(2026, 5, 1), date(2026, 5, 2)}
    assert is_holiday_for_campaign(True, date(2026, 5, 1), holidays) is True
    assert is_holiday_for_campaign(False, date(2026, 5, 1), holidays) is False
    assert is_holiday_for_campaign(True, date(2026, 5, 4), holidays) is False


def test_next_window_start_same_day_evening() -> None:
    # spec § Scenario 当日工作时段已过: Mon 19:00 → next is Tue 09:00
    now = datetime(2026, 5, 4, 19, 0, tzinfo=TZ)  # Mon
    nxt = next_window_start(WORKDAY_WINDOWS, now, respect_holidays=False, holiday_dates=set())
    assert nxt == datetime(2026, 5, 5, 9, 0, tzinfo=TZ)  # Tue 09:00


def test_next_window_start_lunch_break_jumps_to_afternoon() -> None:
    now = datetime(2026, 5, 4, 12, 30, tzinfo=TZ)  # Mon lunch
    nxt = next_window_start(WORKDAY_WINDOWS, now, respect_holidays=False, holiday_dates=set())
    assert nxt == datetime(2026, 5, 4, 14, 0, tzinfo=TZ)


def test_next_window_start_skip_holiday_to_post() -> None:
    # spec § Scenario 节假日推迟到节后: 5/1 holiday → next is 5/4 09:00 (Mon)
    holidays = {date(2026, 5, 1), date(2026, 5, 2), date(2026, 5, 3)}
    now = datetime(2026, 5, 1, 8, 0, tzinfo=TZ)  # Fri but holiday
    nxt = next_window_start(WORKDAY_WINDOWS, now, respect_holidays=True, holiday_dates=holidays)
    assert nxt == datetime(2026, 5, 4, 9, 0, tzinfo=TZ)


def test_next_window_start_cross_week_sunday() -> None:
    # spec § Scenario 跨周边界: Sun 10:00 (no Sun window) → next Mon 09:00
    now = datetime(2026, 5, 10, 10, 0, tzinfo=TZ)  # Sun
    nxt = next_window_start(WORKDAY_WINDOWS, now, respect_holidays=False, holiday_dates=set())
    assert nxt == datetime(2026, 5, 11, 9, 0, tzinfo=TZ)  # Mon


def test_next_window_start_empty_windows_returns_none() -> None:
    nxt = next_window_start([], datetime(2026, 5, 4, 10, 0, tzinfo=TZ),
                            respect_holidays=False, holiday_dates=set())
    assert nxt is None


def test_next_window_start_no_window_within_horizon() -> None:
    # All days are holidays; with respect_holidays=True, no window found
    now = datetime(2026, 5, 4, 10, 0, tzinfo=TZ)
    holidays = {date(2026, 5, 4) + __import__("datetime").timedelta(days=i) for i in range(80)}
    nxt = next_window_start(WORKDAY_WINDOWS, now, respect_holidays=True, holiday_dates=holidays,
                            max_lookahead_days=30)
    assert nxt is None
