"""NY session / daily_smart calendar helpers (no Celery ingest; P9 S3)."""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from typing import Any, Dict, Optional, Tuple
from zoneinfo import ZoneInfo

from bifrost_worker.data.massive._retired import retired_payload

NY = ZoneInfo("America/New_York")
DAILY_FINAL_CLOSE_GRACE_MINUTES = 20


def ny_calendar_today() -> date:
    return datetime.now(NY).date()


def is_ny_session_safely_closed(now_et: Optional[datetime] = None) -> bool:
    """True when the regular NY session should be considered final for day-level overwrite."""
    et_now = now_et.astimezone(NY) if now_et is not None else datetime.now(NY)
    final_cutoff = datetime.combine(
        et_now.date(),
        time(16, 0),
        tzinfo=NY,
    ) + timedelta(minutes=DAILY_FINAL_CLOSE_GRACE_MINUTES)
    return et_now >= final_cutoff


def compute_daily_smart_range(*_args: Any, **_kwargs: Any) -> Dict[str, Any]:
    return retired_payload()


def resolve_daily_smart_end_date(*_args: Any, **_kwargs: Any) -> Tuple[Any, bool, str]:
    return None, False, "massive_retired"
