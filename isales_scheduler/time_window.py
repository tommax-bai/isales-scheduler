"""Pure functions for time-window + holiday judgment.

Spec: time-window § Campaign 级多窗口配置 / 统一服务器时区 / 全局节假日表 /
      窗外 lead 推迟到下个窗口开始时刻 / 跨窗口边界保护.

The ``time_windows`` JSONB array shape (per time-window spec) is::

    [{"days": ["mon", "tue", ...], "start": "09:00", "end": "12:00"}, ...]

All datetimes here are timezone-aware; callers SHALL pass ``datetime.now(tz)``
in the deployment timezone (``Asia/Shanghai`` by default — see TZ env var).
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from typing import Any

WEEKDAY_NAMES = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")


def _parse_hhmm(s: str) -> time:
    h, m = s.split(":")
    return time(hour=int(h), minute=int(m))


def _normalize_day(day: str) -> str:
    return day.strip().lower()[:3]


def _windows_on_weekday(
    time_windows: list[dict[str, Any]],
    weekday_idx: int,
) -> list[tuple[time, time]]:
    """Return [(start, end)] tuples active on a given weekday (0=mon..6=sun).

    End equal to start is treated as zero-length and ignored.
    """

    name = WEEKDAY_NAMES[weekday_idx]
    out: list[tuple[time, time]] = []
    for w in time_windows:
        days = [_normalize_day(d) for d in w.get("days", [])]
        if name not in days:
            continue
        start = _parse_hhmm(w["start"])
        end = _parse_hhmm(w["end"])
        if end <= start:
            continue
        out.append((start, end))
    return sorted(out)


def is_in_window(time_windows: list[dict[str, Any]], now: datetime) -> bool:
    """True iff ``now`` falls in any configured window (start ≤ t < end)."""

    if not time_windows:
        return False
    today = now.date()
    weekday = today.weekday()
    cur = now.time().replace(microsecond=0)
    return any(start <= cur < end for start, end in _windows_on_weekday(time_windows, weekday))


def is_holiday_for_campaign(
    respect_holidays: bool,
    day: date,
    holiday_dates: set[date],
) -> bool:
    """True iff the campaign respects holidays AND ``day`` is one."""

    return bool(respect_holidays) and day in holiday_dates


def next_window_start(
    time_windows: list[dict[str, Any]],
    now: datetime,
    *,
    respect_holidays: bool,
    holiday_dates: set[date],
    max_lookahead_days: int = 60,
) -> datetime | None:
    """Return the earliest future window-start at or after ``now``.

    Walks forward day-by-day up to ``max_lookahead_days``; skips holidays when
    ``respect_holidays`` is true. Returns ``None`` if no window found within
    horizon (e.g. ``time_windows=[]`` or all-holiday horizon).
    """

    if not time_windows:
        return None

    tzinfo = now.tzinfo
    cur_date = now.date()
    cur_time = now.time().replace(microsecond=0)

    for offset in range(max_lookahead_days + 1):
        day = cur_date + timedelta(days=offset)
        if respect_holidays and day in holiday_dates:
            continue
        weekday = day.weekday()
        for start, _end in _windows_on_weekday(time_windows, weekday):
            candidate_dt = datetime.combine(day, start, tzinfo=tzinfo)
            if offset == 0 and start <= cur_time:
                # Same-day window already started or passed — skip; the
                # in-window case is handled by is_in_window upstream.
                continue
            return candidate_dt
    return None
