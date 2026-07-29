"""Multi-entity host identity rules (path-level lock + dedup).

Portal / aggregator hosts list thousands of independent agents, listings, and
companies under one root domain. Domain-level global locks or B2B domain-level
dedup incorrectly collapse that inventory to a single slot and can block an
entire portal for all tenants for the exclusivity window.

This module is the SSOT for:
  - known multi-entity host suffixes
  - whether identity (lock / dedup / cache key) must be path-level

Used by pipeline-main produce + dispatch (and tests).
"""
from __future__ import annotations

import os
import re
from typing import Any, Iterable
from urllib.parse import urlparse

# ---------------------------------------------------------------------------
# Host catalogue
# ---------------------------------------------------------------------------
# Suffix match on extract_root_domain() output (no leading www.).
# Prefer registrable-style suffixes; country variants listed explicitly where
# the public suffix differs (propertyfinder.ae vs .com).
_DEFAULT_MULTI_ENTITY_HOST_SUFFIXES: tuple[str, ...] = (
    # Real estate portals / classifieds
    "bayut.com",
    "bayut.sa",
    "bayut.eg",
    "propertyfinder.ae",
    "propertyfinder.com",
    "propertyfinder.eg",
    "propertyfinder.sa",
    "propertyfinder.qa",
    "propertyfinder.bh",
    "dubizzle.com",
    "dubizzle.ae",
    "dubizzle.eg",
    "olx.com",
    "olx.in",
    "olx.ph",
    "olx.pt",
    "zillow.com",
    "realtor.com",
    "rightmove.co.uk",
    "zoopla.co.uk",
    "99acres.com",
    "magicbricks.com",
    "housing.com",
    "craigslist.org",
    "gumtree.com",
    "kijiji.ca",
    # Directories / review aggregators (many entities per host)
    "yelp.com",
    "yelp.ca",
    "justdial.com",
    "indiamart.com",
    "thomasnet.com",
    "clutch.co",
    "goodfirms.co",
    "g2.com",
    "capterra.com",
    "trustpilot.com",
    "glassdoor.com",
    "yellowpages.com",
    "bbb.org",
    "houzz.com",
    "angi.com",
    "sulekha.com",
    "practo.com",
    "tripadvisor.com",
    # Marketplaces / freelance (profile-level entities)
    "upwork.com",
    "fiverr.com",
)

# Optional env extension: comma-separated extra suffixes
# e.g. MULTI_ENTITY_HOST_SUFFIXES=example-portal.com,foo.bar
def _load_suffixes() -> tuple[str, ...]:
    extra_raw = os.environ.get("MULTI_ENTITY_HOST_SUFFIXES", "") or ""
    extra = [
        s.strip().lower().lstrip(".")
        for s in extra_raw.split(",")
        if s.strip()
    ]
    base = list(_DEFAULT_MULTI_ENTITY_HOST_SUFFIXES)
    for s in extra:
        if s and s not in base:
            base.append(s)
    return tuple(base)


MULTI_ENTITY_HOST_SUFFIXES: tuple[str, ...] = _load_suffixes()

# Precompiled for slightly faster matching on hot paths
_SUFFIX_RE = re.compile(
    r"(?:^|\.)("
    + "|".join(re.escape(s) for s in sorted(MULTI_ENTITY_HOST_SUFFIXES, key=len, reverse=True))
    + r")$",
    re.IGNORECASE,
)


def is_multi_entity_host(domain_or_host: str | None) -> bool:
    """True if *domain_or_host* is a known multi-entity portal/aggregator.

    Accepts root domains (``bayut.com``), hosts (``www.bayut.com``), or bare
    suffixes. Matching is suffix-based so ``en.bayut.com`` still hits.
    """
    raw = (domain_or_host or "").strip().lower()
    if not raw:
        return False
    raw = raw.replace("https://", "").replace("http://", "")
    raw = raw.split("/")[0].split("?")[0].split(":")[0]
    raw = raw[4:] if raw.startswith("www.") else raw
    if not raw:
        return False
    if raw in MULTI_ENTITY_HOST_SUFFIXES:
        return True
    return any(raw == s or raw.endswith("." + s) for s in MULTI_ENTITY_HOST_SUFFIXES)


def path_identity_key(url: str, *, include_fragment: bool = False) -> str:
    """Stable path-level identity: ``netloc+path`` (+ optional fragment), no www."""
    try:
        parsed = urlparse(url if "://" in (url or "") else f"https://{url}")
    except Exception:
        return (url or "").lower().replace("www.", "")
    frag = ""
    if include_fragment and parsed.fragment:
        frag = f"#{parsed.fragment}"
    return f"{parsed.netloc}{parsed.path}{frag}".lower().replace("www.", "")


def should_use_path_level_identity(
    domain: str,
    *,
    is_social: bool = False,
    is_shared: bool = False,
    is_consumer: bool = False,
    url: str | None = None,
) -> bool:
    """Whether lock/dedup/cache identity must be path-level.

    Path-level when any of:
      - social platform
      - shared publishing platform
      - consumer archetype (B2C/D2C/B2B2C) — legacy behaviour
      - multi-entity portal host (forced, **regardless of sourcing_vector**)
    """
    if is_social or is_shared or is_consumer:
        return True
    host = domain
    if (not host) and url:
        try:
            host = urlparse(url if "://" in url else f"https://{url}").netloc
        except Exception:
            host = ""
    return is_multi_entity_host(host or domain)


def resolve_identity_key(
    url: str,
    domain: str,
    *,
    is_social: bool = False,
    is_shared: bool = False,
    is_consumer: bool = False,
    include_fragment: bool = False,
) -> tuple[str, dict[str, Any]]:
    """Return ``(identity_key, meta)`` for lock / lead / cache document IDs.

    *identity_key* is either a path key or the root domain string.
    *meta* is safe for structured logs (no PII beyond host/path shape).
    """
    multi = is_multi_entity_host(domain) or is_multi_entity_host(url)
    use_path = should_use_path_level_identity(
        domain,
        is_social=is_social,
        is_shared=is_shared,
        is_consumer=is_consumer,
        url=url,
    )
    if use_path:
        key = path_identity_key(url, include_fragment=include_fragment)
        reason = (
            "multi_entity_host" if multi
            else "social" if is_social
            else "shared_platform" if is_shared
            else "consumer_vector"
        )
    else:
        key = (domain or "").lower().replace("www.", "")
        reason = "domain_default"

    meta = {
        "identity_mode": "path" if use_path else "domain",
        "identity_reason": reason,
        "multi_entity_host": multi,
        "domain": (domain or "")[:120],
    }
    return key, meta


# V27.9: Thin / policy-noise paths on company & portal sites (not unique buyers).
# Applied only when host is NOT social/shared multi-thread identity.
_JUNK_PORTAL_PATH_RE = re.compile(
    r"(?:"
    r"/(?:privacy|privacy-policy|terms|terms-of-service|tos|cookie|cookies|"
    r"login|log-in|signin|sign-in|signup|sign-up|register|wp-admin|wp-login|"
    r"cart|checkout|basket|wishlist|account|dashboard|"
    r"tag|tags|category|categories|author|authors|feed|rss|sitemap|"
    r"search|faq|faqs|help-center|support-center)(?:/|$)|"
    r"/page/\d+(?:/|$)"
    r")",
    re.IGNORECASE,
)


def is_junk_portal_path(url: str | None) -> bool:
    """True for privacy/login/tag/pagination paths that rarely hold buyer intent."""
    if not url:
        return False
    try:
        path = urlparse(url if "://" in url else f"https://{url}").path or ""
    except Exception:
        path = str(url)
    return bool(_JUNK_PORTAL_PATH_RE.search(path))


def multi_entity_host_list() -> tuple[str, ...]:
    """Public read-only view of configured suffixes (for tests / admin)."""
    return MULTI_ENTITY_HOST_SUFFIXES


def filter_known_hosts(hosts: Iterable[str]) -> list[str]:
    """Return hosts from *hosts* that match the multi-entity catalogue."""
    return [h for h in hosts if is_multi_entity_host(h)]
