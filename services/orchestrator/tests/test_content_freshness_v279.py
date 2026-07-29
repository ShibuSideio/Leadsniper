"""V27.9.1 content freshness — 90-day hard rule forensic tests."""
from __future__ import annotations

import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_ORCH = os.path.dirname(_HERE)
_SERVICES = os.path.dirname(_ORCH)
for p in (_SERVICES, _ORCH):
    if p not in sys.path:
        sys.path.insert(0, p)

from shared.content_freshness import (  # noqa: E402
    CONTENT_MAX_AGE_DAYS,
    is_stale_url,
    is_stale_serper_result,
    reddit_post_id_from_url,
    reddit_id_is_older_than_window,
)


def test_max_age_is_90():
    assert CONTENT_MAX_AGE_DAYS == 90


def test_eight_year_old_reddit_id_stale():
    # 8fj352 ~2018 era
    assert reddit_id_is_older_than_window("8fj352") is True
    stale, reason = is_stale_url(
        "https://www.reddit.com/r/Indian_Academia/comments/8fj352/views_regarding_double_major/"
    )
    assert stale is True
    assert "reddit_id" in reason or "old" in reason


def test_reddit_id_extract_variants():
    assert reddit_post_id_from_url(
        "https://www.reddit.com/r/x/comments/8fj352/title/"
    ) == "8fj352"
    assert reddit_post_id_from_url("https://www.reddit.com/comments/8fj352/") == "8fj352"
    assert reddit_post_id_from_url("https://redd.it/8fj352") == "8fj352"


def test_serper_one_year_ago():
    r = {
        "link": "https://www.reddit.com/r/startups/comments/abc123/old",
        "date": "1 year ago",
        "title": "Old",
        "snippet": "x",
    }
    stale, reason = is_stale_serper_result(r)
    assert stale is True


def test_two_months_ok():
    r = {
        "link": "https://www.reddit.com/r/startups/comments/1v6uldv/new",
        "date": "2 months ago",
        "title": "Recent",
        "snippet": "fresh",
    }
    stale, _ = is_stale_serper_result(r)
    assert stale is False


def test_undated_social_fail_closed_without_recent_id():
    stale, reason = is_stale_url("https://www.quora.com/Some-old-question")
    assert stale is True
    assert "undated" in reason or "fail_closed" in reason


def test_company_site_undated_ok():
    stale, reason = is_stale_url("https://www.example-startup.com/about")
    assert stale is False
