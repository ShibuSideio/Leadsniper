"""
V27.7 — Serper enrichment waste controls (shared SSOT).

Used by pipeline-main deep_context + intelligence mesh to:
  - Skip non-company / aggregator / directory hosts
  - Cache domain enrichment results (multi-instance Firestore)
"""
from __future__ import annotations

import hashlib
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

# Domain-level enrichment cache (deep_context + mesh)
ENRICHMENT_CACHE_COLLECTION = "serper_domain_enrichment_cache"
ENRICHMENT_CACHE_TTL_HOURS = 48

# Hosts that must never burn company-level Serper enrichment credits.
# Lead URLs *on* these platforms still flow via snippet/PRISM paths.
ENRICHMENT_SKIP_HOSTS: frozenset[str] = frozenset({
    # Social
    "reddit.com", "facebook.com", "instagram.com", "youtube.com",
    "linkedin.com", "quora.com", "twitter.com", "x.com", "medium.com",
    "tiktok.com", "pinterest.com", "tumblr.com", "snapchat.com",
    # Forums / community
    "stackexchange.com", "stackoverflow.com", "serverfault.com",
    "news.ycombinator.com", "hackernews.com", "slashdot.org",
    "community.hubspot.com", "community.g2.com", "indiehackers.com",
    "team-bhp.com", "skyscrapercity.com", "discourse.org",
    # Search / news aggregators (observed waste: news.google.com places+reviews)
    "news.google.com", "google.com", "google.co.in", "google.co.uk",
    "news.yahoo.com", "news.msn.com", "msn.com", "bing.com",
    "duckduckgo.com", "apple.news", "flipboard.com", "feedly.com",
    # Wiki / research hosting (not the lead company)
    "wikipedia.org", "fandom.com", "archive.org", "academia.edu",
    "researchgate.net", "slideshare.net", "arxiv.org",
    # Classifieds / reviews containers
    "yelp.com", "yellowpages.com", "bbb.org", "trustpilot.com",
    "glassdoor.com", "indeed.com", "monster.com", "craigslist.org",
    "gumtree.com",
    # Directories / education marketplaces (enriching the portal is waste)
    "justdial.com", "sulekha.com", "indiaeducation.net", "indiamike.com",
    "collegedunia.com", "yocket.com", "shiksha.com", "collegeconfidential.com",
    # SaaS marketing platforms (root domain enrichment is meaningless)
    "alignable.com", "constantcontact.com", "mailchimp.com",
})

# High-liquidity Google indexes — do not force dual geo+global for non-platform.
HIGH_LIQUIDITY_GL: frozenset[str] = frozenset({
    "us", "in", "gb", "uk", "ae", "ca", "au", "de", "fr", "sg", "nl", "ie",
})


def normalize_host(domain: str) -> str:
    d = (domain or "").strip().lower()
    d = d.replace("https://", "").replace("http://", "")
    d = d.split("/")[0].split("?")[0]
    if d.startswith("www."):
        d = d[4:]
    return d


def is_enrichment_skip_domain(domain: str) -> bool:
    """True if company-level Serper enrichment should not run for this host."""
    cleaned = normalize_host(domain)
    if not cleaned or cleaned in ("n/a", "none", "null", "localhost", "unknown"):
        return True
    for host in ENRICHMENT_SKIP_HOSTS:
        if cleaned == host or cleaned.endswith("." + host):
            return True
    return False


def enrichment_cache_doc_id(domain: str, kind: str) -> str:
    """Stable Firestore doc id for domain enrichment cache."""
    key = f"{normalize_host(domain)}|{kind}"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:40]


def read_enrichment_cache(
    db: Any,
    domain: str,
    kind: str,
    *,
    log: Any = None,
) -> Optional[dict]:
    """Return cached payload if present and not expired, else None."""
    try:
        doc_id = enrichment_cache_doc_id(domain, kind)
        snap = db.collection(ENRICHMENT_CACHE_COLLECTION).document(doc_id).get()
        if not snap.exists:
            return None
        data = snap.to_dict() or {}
        exp = data.get("expires_at")
        now = datetime.now(timezone.utc)
        if exp is not None:
            if hasattr(exp, "timestamp"):
                exp_dt = datetime.fromtimestamp(exp.timestamp(), tz=timezone.utc)
            elif isinstance(exp, datetime):
                exp_dt = exp if exp.tzinfo else exp.replace(tzinfo=timezone.utc)
            else:
                exp_dt = None
            if exp_dt is not None and exp_dt < now:
                if log:
                    log("enrichment_cache_expired", domain=normalize_host(domain), kind=kind)
                return None
        if log:
            log("enrichment_cache_hit", domain=normalize_host(domain), kind=kind)
        return data
    except Exception as exc:
        if log:
            log("enrichment_cache_read_error", error=str(exc), domain=domain, kind=kind)
        return None


def write_enrichment_cache(
    db: Any,
    domain: str,
    kind: str,
    payload: dict,
    *,
    ttl_hours: int = ENRICHMENT_CACHE_TTL_HOURS,
    log: Any = None,
) -> None:
    """Write domain enrichment cache with TTL."""
    try:
        from google.cloud import firestore  # type: ignore[import]

        now = datetime.now(timezone.utc)
        doc_id = enrichment_cache_doc_id(domain, kind)
        body = {
            **payload,
            "domain": normalize_host(domain),
            "kind": kind,
            "updated_at": now,
            "expires_at": now + timedelta(hours=max(1, int(ttl_hours))),
        }
        db.collection(ENRICHMENT_CACHE_COLLECTION).document(doc_id).set(body, merge=True)
        if log:
            log("enrichment_cache_write", domain=normalize_host(domain), kind=kind)
    except Exception as exc:
        if log:
            log("enrichment_cache_write_error", error=str(exc), domain=domain, kind=kind)


def query_dedup_fingerprint(query: str) -> str:
    """Normalize Serper query for near-duplicate detection.

    Strips trailing negative operators so two queries that differ only in
    negation tails map to the same fingerprint.
    """
    q = (query or "").strip().lower()
    if not q:
        return ""
    body = re.split(
        r'\s+-(?:site:|wiki\b|jobs\b|careers\b|investors\b|directory\b|listicle\b|")',
        q,
        maxsplit=1,
    )[0].strip()
    body = re.sub(r"\s+", " ", body)
    return body
