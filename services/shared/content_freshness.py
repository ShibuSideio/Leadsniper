"""
V27.9.1 — Content freshness SSOT (90-day rolling product rule).

Every path that admits a URL into the lead funnel (produce Serper loop,
dispatch promote, harvest, inbound) must call these helpers. UI can mirror
via the same Reddit post-id rules.

Product rule: no social/forum content older than CONTENT_MAX_AGE_DAYS may
appear as an actionable lead.
"""
from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any, Optional
from urllib.parse import urlparse

# Unified product window
CONTENT_MAX_AGE_DAYS = 90

_SOCIAL_HOST_HINTS = (
    "reddit.com",
    "redd.it",
    "quora.com",
    "linkedin.com",
    "facebook.com",
    "twitter.com",
    "x.com",
    "youtube.com",
    "instagram.com",
    "tiktok.com",
    "team-bhp.com",
    "stackexchange.com",
    "stackoverflow.com",
    "news.ycombinator.com",
    "forum.",
    "discourse.",
)

# Reddit base36 floor: IDs strictly below this are older than ~90d (mid-2026 cal).
# Prefer over-rejecting undated borderline posts vs admitting multi-year threads.
_REDDIT_ID_MIN_RECENT = "1s00000"

# Any Reddit id with base36 length ≤5 is pre-2021 era noise
_REDDIT_ID_MAX_LEN_LEGACY = 5


def content_max_age_days() -> int:
    return CONTENT_MAX_AGE_DAYS


def is_social_forum_url(url: str) -> bool:
    u = (url or "").lower()
    return any(h in u for h in _SOCIAL_HOST_HINTS)


def is_reddit_non_thread_url(url: str) -> bool:
    u = (url or "").lower().split("?", 1)[0].rstrip("/")
    if "reddit.com" not in u and "redd.it" not in u:
        return False
    if "/user/" in u or "/u/" in u:
        return True
    if re.search(r"/r/[^/]+/(rising|hot|new|top|best)(/|$)", u):
        return True
    if re.search(r"/r/[^/]+/?$", u) and "/comments/" not in u:
        return True
    return False


def reddit_post_id_from_url(url: str) -> Optional[str]:
    """Extract Reddit base36 post id from common URL shapes."""
    u = (url or "").strip()
    if not u:
        return None
    low = u.lower()
    # /r/sub/comments/{id}/...
    m = re.search(r"reddit\.com/r/[^/]+/comments/([a-z0-9]+)", low)
    if m:
        return m.group(1)
    # /comments/{id}/ (no subreddit)
    m = re.search(r"reddit\.com/comments/([a-z0-9]+)", low)
    if m:
        return m.group(1)
    # redd.it/{id}
    m = re.search(r"(?:^|//)redd\.it/([a-z0-9]+)", low)
    if m:
        return m.group(1)
    # gallery / old variants
    m = re.search(r"reddit\.com/gallery/([a-z0-9]+)", low)
    if m:
        return m.group(1)
    return None


def reddit_id_is_older_than_window(
    post_id: str,
    *,
    min_recent: str = _REDDIT_ID_MIN_RECENT,
    max_age_days: int = CONTENT_MAX_AGE_DAYS,
) -> bool:
    """True if post_id is almost certainly older than the rolling window."""
    pid = (post_id or "").strip().lower()
    if not pid:
        return False
    if len(pid) <= _REDDIT_ID_MAX_LEN_LEGACY:
        return True
    try:
        return int(pid, 36) < int(min_recent, 36)
    except (ValueError, TypeError):
        return True  # unparseable id → treat as stale (fail-closed)


def age_days_from_date_string(raw_date: str) -> Optional[int]:
    """Parse Serper-style or ISO-ish date strings to age in days."""
    raw_date = (raw_date or "").strip()
    if not raw_date:
        return None
    _rel = re.match(
        r"(\d+)\s+(second|minute|hour|day|week|month|year)s?\s+ago",
        raw_date,
        re.IGNORECASE,
    )
    if _rel:
        count = int(_rel.group(1))
        unit = _rel.group(2).lower()
        mult = {
            "second": 0,
            "minute": 0,
            "hour": 0,
            "day": 1,
            "week": 7,
            "month": 30,
            "year": 365,
        }
        return count * mult.get(unit, 0)

    for fmt, slen in (
        ("%Y-%m-%dT%H:%M:%S", 19),
        ("%Y-%m-%d", 10),
        ("%b %d, %Y", 12),
        ("%B %d, %Y", 18),
    ):
        try:
            parsed = datetime.strptime(raw_date[:slen], fmt)
            return max(
                0,
                (datetime.now(timezone.utc) - parsed.replace(tzinfo=timezone.utc)).days,
            )
        except (ValueError, TypeError):
            continue
    ym = re.search(r"\b(20\d{2})\b", raw_date)
    if ym:
        year = int(ym.group(1))
        try:
            mid = datetime(year, 7, 1, tzinfo=timezone.utc)
            return max(0, (datetime.now(timezone.utc) - mid).days)
        except ValueError:
            pass
    return None


def is_stale_url(
    url: str,
    *,
    date_hint: str = "",
    title: str = "",
    snippet: str = "",
    max_age_days: int = CONTENT_MAX_AGE_DAYS,
    is_consumer: bool = False,
) -> tuple[bool, str]:
    """
    Return (is_stale, reason).

    Social/forum: hard 90d; undated fail-closed except recent Reddit ids.
    Non-social: fail-open when undated (company sites); reject when date > max.
    """
    url = url or ""
    if not url:
        return True, "empty_url"

    if is_reddit_non_thread_url(url):
        return True, "reddit_non_thread"

    is_social = is_social_forum_url(url)
    age = age_days_from_date_string(date_hint)
    if age is not None:
        if age > max_age_days:
            return True, f"date_age_{age}d"
        return False, f"date_ok_{age}d"

    rid = reddit_post_id_from_url(url)
    if rid:
        if reddit_id_is_older_than_window(rid, max_age_days=max_age_days):
            return True, f"reddit_id_old_{rid}"
        # Recent-looking id without date → allow
        if is_social:
            return False, f"reddit_id_recent_{rid}"

    if is_social:
        blob = f"{title or ''} {snippet or ''}"
        years = [int(y) for y in re.findall(r"\b(20\d{2})\b", blob)]
        if years:
            oldest = min(years)
            try:
                mid = datetime(oldest, 7, 1, tzinfo=timezone.utc)
                age_y = (datetime.now(timezone.utc) - mid).days
                if age_y > max_age_days:
                    return True, f"year_marker_{oldest}"
            except ValueError:
                return True, "year_marker_invalid"
        # Undated social without recent reddit id → fail-closed
        return True, "social_undated_fail_closed"

    # Non-social undated: allow (company site evergreen)
    return False, "nonsocial_undated_ok"


def is_stale_serper_result(result: dict, *, is_consumer: bool = False) -> tuple[bool, str]:
    """Adapter for Serper organic dicts (link/date/title/snippet)."""
    if not isinstance(result, dict):
        return True, "invalid_result"
    return is_stale_url(
        result.get("link") or result.get("url") or "",
        date_hint=str(result.get("date") or ""),
        title=str(result.get("title") or ""),
        snippet=str(result.get("snippet") or ""),
        is_consumer=is_consumer,
    )


def freshness_fields_for_url(url: str, *, date_hint: str = "") -> dict[str, Any]:
    """Fields to persist on lead docs for feed/export filtering."""
    age = age_days_from_date_string(date_hint)
    rid = reddit_post_id_from_url(url)
    stale, reason = is_stale_url(url, date_hint=date_hint)
    out: dict[str, Any] = {
        "content_max_age_days": CONTENT_MAX_AGE_DAYS,
        "content_stale": bool(stale),
        "content_stale_reason": reason,
    }
    if age is not None:
        out["content_age_days"] = age
    if rid:
        out["reddit_post_id"] = rid
    return out
