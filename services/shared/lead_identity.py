"""
Lead identity & status SSOT helpers (V27.2.0 scale).

Normalizes dual fields so produce dedup, feed, CRM, and settle share one contract:
  - url + source_url always both set when either is known
  - campaign_id + matched_campaigns (+ matched_campaign_ids) aligned
  - status normalization for reject dual (rejected ↔ ignored policy)
"""
from __future__ import annotations

from typing import Any, Mapping


# Pipeline statuses that must not block re-queue (terminal non-leads)
TERMINAL_NON_LEAD_STATUSES: frozenset[str] = frozenset({
    "scored_out",
    "rlhf_filtered",
    "failed",
    "failed_scrape",
    "failed_eval",
    "failed_vertex_timeout",
    "duplicate",  # V27.9: same portal/domain sibling — do not re-queue as fresh
})

# Statuses that count toward tenant velocity (live inventory)
VELOCITY_STATUSES: frozenset[str] = frozenset({
    "new",
    "enrichment_pending",
})

# Reject synonyms — writers may use either; readers should accept both
REJECT_STATUSES: frozenset[str] = frozenset({
    "ignored",
    "rejected",
})


def resolve_lead_url(data: Mapping[str, Any] | None) -> str:
    """Prefer source_url then url (promotion path vs stub)."""
    if not data:
        return ""
    return str(data.get("source_url") or data.get("url") or "").strip()


def normalize_lead_urls(
    *,
    url: str = "",
    source_url: str = "",
) -> dict[str, str]:
    """Return both url and source_url set to the same canonical value when possible."""
    primary = (source_url or url or "").strip()
    secondary = (url or source_url or "").strip()
    canon = primary or secondary
    return {"url": canon, "source_url": canon}


def normalize_campaign_refs(
    campaign_id: str,
    matched: list | None = None,
) -> dict[str, Any]:
    """Align campaign_id, matched_campaigns, matched_campaign_ids."""
    cid = (campaign_id or "").strip()
    arr: list[str] = []
    seen: set[str] = set()
    for raw in list(matched or []) + ([cid] if cid else []):
        s = str(raw).strip()
        if not s or s in seen:
            continue
        seen.add(s)
        arr.append(s)
    primary = cid or (arr[0] if arr else "")
    return {
        "campaign_id": primary,
        "matched_campaigns": arr,
        "matched_campaign_ids": list(arr),
        "highest_campaign_id": primary,
    }


def is_terminal_non_lead(status: str | None) -> bool:
    return str(status or "").strip().lower() in TERMINAL_NON_LEAD_STATUSES


def is_reject_status(status: str | None) -> bool:
    return str(status or "").strip().lower() in REJECT_STATUSES


def normalize_user_status(status: str | None) -> str:
    """Map UI/API status aliases to a stable pipeline status."""
    s = str(status or "").strip().lower()
    if s == "rejected":
        return "ignored"
    if s == "approved":
        return "converted"
    return s


def apply_lead_identity_fields(
    payload: dict[str, Any],
    *,
    url: str = "",
    source_url: str = "",
    campaign_id: str = "",
    matched_campaigns: list | None = None,
) -> dict[str, Any]:
    """Mutate+return payload with identity fields normalized (copy-safe)."""
    out = dict(payload)
    urls = normalize_lead_urls(url=url or out.get("url", ""), source_url=source_url or out.get("source_url", ""))
    out.update(urls)
    camps = normalize_campaign_refs(
        campaign_id or out.get("campaign_id", ""),
        matched=matched_campaigns if matched_campaigns is not None else out.get("matched_campaigns"),
    )
    out.update(camps)
    # Timestamp dual: prefer createdAt (Firestore convention)
    if "created_at" in out and "createdAt" not in out:
        out["createdAt"] = out["created_at"]
    return out
