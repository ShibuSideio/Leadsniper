#!/usr/bin/env python3
"""
Delete leads older than the product rolling window (default: 90 days / 3 months).

Auth: Application Default Credentials
  gcloud auth application-default login
  gcloud config set project lead-sniper-prod

Modes:
  content  — social source age (Reddit post via public API when possible) OR
             non-thread Reddit URLs always deleted; undated social treated stale
  created  — lead createdAt / created_at older than max-age-days
  both     — delete if either content OR created rule matches (default)

Safety:
  --dry-run is DEFAULT. Pass --execute to actually delete.
  Prefer --campaign <id> over full tenant wipe.

Examples:
  # Preview (campaign Georgia MBBS)
  python scripts/delete_stale_leads.py --campaign QzqnAG4fiKYOmP7UOOLG --max-age-days 90

  # Execute delete
  python scripts/delete_stale_leads.py --campaign QzqnAG4fiKYOmP7UOOLG --max-age-days 90 --execute

  # All leads for a tenant (careful)
  python scripts/delete_stale_leads.py --tenant-id <UID> --max-age-days 90 --execute
"""
from __future__ import annotations

import argparse
import re
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

try:
    from google.cloud import firestore
except ImportError:
    print("Install: pip install google-cloud-firestore", file=sys.stderr)
    sys.exit(1)

PROJECT = "lead-sniper-prod"
DEFAULT_MAX_AGE_DAYS = 90
BATCH_LIMIT = 400
_REDDIT_ID_MIN_RECENT = "1s00000"  # align with produce V27.8 floor

_SOCIAL_HOSTS = (
    "reddit.com",
    "quora.com",
    "linkedin.com",
    "facebook.com",
    "twitter.com",
    "x.com",
    "youtube.com",
    "instagram.com",
    "tiktok.com",
    "news.ycombinator.com",
    "stackoverflow.com",
)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _as_dt(val: Any) -> Optional[datetime]:
    if val is None:
        return None
    if isinstance(val, datetime):
        return val if val.tzinfo else val.replace(tzinfo=timezone.utc)
    if hasattr(val, "timestamp"):
        try:
            return datetime.fromtimestamp(val.timestamp(), tz=timezone.utc)
        except Exception:
            pass
    if isinstance(val, str) and val.strip():
        try:
            return datetime.fromisoformat(val.replace("Z", "+00:00"))
        except Exception:
            return None
    return None


def _url(data: dict) -> str:
    return str(data.get("url") or data.get("source_url") or data.get("link") or "")


def _is_social(url: str) -> bool:
    u = (url or "").lower()
    return any(h in u for h in _SOCIAL_HOSTS)


def _is_reddit_non_thread(url: str) -> bool:
    u = (url or "").lower().split("?", 1)[0].rstrip("/")
    if "reddit.com" not in u:
        return False
    if "/user/" in u or "/u/" in u:
        return True
    if re.search(r"/r/[^/]+/(rising|hot|new|top|best)(/|$)", u):
        return True
    if re.search(r"/r/[^/]+/?$", u) and "/comments/" not in u:
        return True
    return False


def _reddit_post_id(url: str) -> Optional[str]:
    m = re.search(r"reddit\.com/r/[^/]+/comments/([a-z0-9]+)/", (url or "").lower())
    return m.group(1) if m else None


def _reddit_id_stale(post_id: str) -> bool:
    try:
        return int(post_id, 36) < int(_REDDIT_ID_MIN_RECENT, 36)
    except (ValueError, TypeError):
        return False


def _fetch_reddit_age_days(post_id: str, cache: dict) -> Optional[int]:
    if post_id in cache:
        return cache[post_id]
    age: Optional[int] = None
    try:
        req = urllib.request.Request(
            f"https://api.reddit.com/api/info.json?id=t3_{post_id}",
            headers={"User-Agent": "SideioStaleLeadCleanup/1.0"},
        )
        with urllib.request.urlopen(req, timeout=8) as resp:
            import json as _json

            body = _json.loads(resp.read().decode())
        children = (
            ((body.get("data") or {}).get("children") or [])
        )
        if children:
            created = (children[0].get("data") or {}).get("created_utc")
            if created:
                age = max(0, int((_now().timestamp() - float(created)) / 86400))
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, ValueError, KeyError):
        age = None
    cache[post_id] = age
    time.sleep(0.35)  # be polite to Reddit
    return age


def content_is_stale(
    data: dict,
    max_age_days: int,
    reddit_cache: dict,
    *,
    use_reddit_api: bool,
) -> tuple[bool, str]:
    url = _url(data)
    if _is_reddit_non_thread(url):
        return True, "reddit_non_thread"

    rid = _reddit_post_id(url)
    if rid:
        if use_reddit_api:
            age = _fetch_reddit_age_days(rid, reddit_cache)
            if age is not None:
                if age > max_age_days:
                    return True, f"reddit_api_age_{age}d"
                return False, f"reddit_api_ok_{age}d"
        if _reddit_id_stale(rid):
            return True, f"reddit_id_floor_{rid}"

    # Stored fields if present
    for key in ("content_date", "post_date", "published_at", "source_date", "date"):
        dt = _as_dt(data.get(key))
        if dt:
            age = (_now() - dt).days
            if age > max_age_days:
                return True, f"field_{key}_{age}d"
            return False, f"field_{key}_ok"

    # Social without proven freshness:
    # - Reddit with post id above floor: keep (same as produce)
    # - Other undated social: delete (fail-closed)
    if _is_social(url):
        if rid and not _reddit_id_stale(rid):
            return False, f"reddit_id_recent_{rid}"
        return True, "social_undated_fail_closed"

    return False, "non_social_keep"


def created_is_stale(data: dict, max_age_days: int) -> tuple[bool, str]:
    dt = _as_dt(data.get("createdAt") or data.get("created_at"))
    if not dt:
        return False, "no_createdAt"
    age = (_now() - dt).days
    if age > max_age_days:
        return True, f"createdAt_{age}d"
    return False, f"createdAt_ok_{age}d"


def _match_campaign(data: dict, campaign_id: str) -> bool:
    if not campaign_id:
        return True
    if str(data.get("campaign_id") or "") == campaign_id:
        return True
    if str(data.get("highest_campaign_id") or "") == campaign_id:
        return True
    matched = data.get("matched_campaigns") or data.get("matched_campaign_ids") or []
    if isinstance(matched, list) and campaign_id in [str(x) for x in matched]:
        return True
    return False


def main() -> int:
    ap = argparse.ArgumentParser(description="Delete leads outside 3-month rolling window")
    ap.add_argument("--project", default=PROJECT)
    ap.add_argument("--campaign", default="", help="Campaign ID filter (recommended)")
    ap.add_argument("--tenant-id", default="", help="Optional tenant_id filter")
    ap.add_argument("--max-age-days", type=int, default=DEFAULT_MAX_AGE_DAYS)
    ap.add_argument(
        "--mode",
        choices=("content", "created", "both"),
        default="both",
        help="both=content OR created (default)",
    )
    ap.add_argument(
        "--execute",
        action="store_true",
        help="Actually delete. Default is dry-run.",
    )
    ap.add_argument(
        "--no-reddit-api",
        action="store_true",
        help="Skip Reddit API age lookup (faster; uses id floor + fail-closed)",
    )
    ap.add_argument("--limit", type=int, default=5000, help="Max docs to scan")
    args = ap.parse_args()

    if not args.campaign and not args.tenant_id:
        print(
            "ERROR: Pass --campaign and/or --tenant-id to avoid scanning the entire leads collection.",
            file=sys.stderr,
        )
        return 2

    db = firestore.Client(project=args.project)
    cutoff = _now() - timedelta(days=args.max_age_days)
    print(
        f"project={args.project} campaign={args.campaign or '-'} "
        f"tenant={args.tenant_id or '-'} max_age_days={args.max_age_days} "
        f"mode={args.mode} execute={args.execute} cutoff_utc={cutoff.isoformat()}"
    )

    # Dual-path campaign fetch
    by_id: dict[str, Any] = {}
    if args.campaign:
        for doc in (
            db.collection("leads")
            .where("campaign_id", "==", args.campaign)
            .limit(args.limit)
            .stream()
        ):
            by_id[doc.id] = doc
        try:
            for doc in (
                db.collection("leads")
                .where("matched_campaigns", "array_contains", args.campaign)
                .limit(args.limit)
                .stream()
            ):
                by_id[doc.id] = doc
        except Exception as exc:
            print(f"WARN matched_campaigns query: {exc}", file=sys.stderr)
    elif args.tenant_id:
        for doc in (
            db.collection("leads")
            .where("tenant_id", "==", args.tenant_id)
            .limit(args.limit)
            .stream()
        ):
            by_id[doc.id] = doc

    reddit_cache: dict = {}
    to_delete: list[tuple[str, str, str]] = []  # id, reason, url
    scanned = 0
    for doc_id, doc in by_id.items():
        scanned += 1
        data = doc.to_dict() or {}
        if args.tenant_id and str(data.get("tenant_id") or "") != args.tenant_id:
            continue
        if args.campaign and not _match_campaign(data, args.campaign):
            continue

        reasons = []
        if args.mode in ("content", "both"):
            stale, why = content_is_stale(
                data,
                args.max_age_days,
                reddit_cache,
                use_reddit_api=not args.no_reddit_api,
            )
            if stale:
                reasons.append(why)
        if args.mode in ("created", "both"):
            stale, why = created_is_stale(data, args.max_age_days)
            if stale:
                reasons.append(why)

        if reasons:
            to_delete.append((doc_id, "+".join(reasons), _url(data)[:120]))

    print(f"scanned={scanned} would_delete={len(to_delete)}")
    for i, (doc_id, reason, url) in enumerate(to_delete[:50]):
        print(f"  [{i+1}] {doc_id[:16]}… {reason} {url}")
    if len(to_delete) > 50:
        print(f"  … +{len(to_delete) - 50} more")

    if not args.execute:
        print("\nDRY-RUN only. Re-run with --execute to delete.")
        return 0

    if not to_delete:
        print("Nothing to delete.")
        return 0

    confirm = input(f"Type DELETE to permanently remove {len(to_delete)} leads: ").strip()
    if confirm != "DELETE":
        print("Aborted.")
        return 1

    deleted = 0
    batch = db.batch()
    batch_n = 0
    for doc_id, _, _ in to_delete:
        batch.delete(db.collection("leads").document(doc_id))
        batch_n += 1
        deleted += 1
        if batch_n >= 400:
            batch.commit()
            batch = db.batch()
            batch_n = 0
            print(f"  committed batch, deleted={deleted}")
    if batch_n:
        batch.commit()
    print(f"DONE deleted={deleted}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
