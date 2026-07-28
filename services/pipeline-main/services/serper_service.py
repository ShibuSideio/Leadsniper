"""
Pipeline-main — Serper search service.

Extracted verbatim from ``main.py`` (V22 monolith) with the following changes:

V23 changes:
  - All gRPC clients obtained via lazy accessors (``get_sm_client()``) — never
    at module scope, safe under Gunicorn pre-fork.
  - ``search_serper()`` emits structured JSON logs via ``core.logging`` instead
    of ``print()``.
  - ``_update_circuit_telemetry()`` calls are preserved (imported from telemetry).

V23.4 changes (Serper Audit Telemetry):
  - ``search_serper()`` now accepts optional ``campaign_id`` and ``tenant_id``
    kwargs.  After each successful call it fires a background audit row to the
    orchestrator broker (POST /api/internal/telemetry/serper-audit) via a daemon
    thread — **zero latency** added to the active scraping loop.
  - The audit payload captures: raw_query, serper_parameters, result_count,
    credit_cost, and engine (always 'search' for this function).
"""
from __future__ import annotations

import json
import os
import re
import threading
import concurrent.futures as _cf
from urllib.parse import urlparse
from typing import Optional

import httpx
from tenacity import (
    retry, wait_exponential, stop_after_attempt,
    retry_if_exception,
)

from core.logging import get_logger   # type: ignore[import]
from core.config import SERPER_API_KEY_NAME  # type: ignore[import]
from core.clients import get_sm_client, get_serper_key  # type: ignore[import]

log = get_logger("pipeline.serper")

# ---------------------------------------------------------------------------
# Domain constants
# ---------------------------------------------------------------------------

SOCIAL_DOMAINS: set[str] = {
    "reddit.com", "linkedin.com", "facebook.com",
    "instagram.com", "x.com", "twitter.com",
    "quora.com", "youtube.com", "team-bhp.com",
}

_ENRICHMENT_SOCIAL_BLACKLIST = [
    # Social platforms
    "reddit.com", "facebook.com", "instagram.com", "youtube.com",
    "linkedin.com", "quora.com", "twitter.com", "x.com", "medium.com",
    "tiktok.com", "pinterest.com", "tumblr.com", "snapchat.com",
    # Community forums — not B2B leads
    "team-bhp.com", "skyscrapercity.com", "airliners.net",
    "stackexchange.com", "stackoverflow.com", "serverfault.com",
    "hackernews.com", "news.ycombinator.com", "slashdot.org",
    "discourse.org", "proboards.com", "phpbb.com", "vbulletin.com",
    # Content/wiki/education — not B2B leads
    "wikipedia.org", "wikia.com", "fandom.com", "archive.org",
    "academia.edu", "researchgate.net", "slideshare.net",
    # V24.1.1: Hobby/consumer domains observed in Serper logs
    "collegeconfidential.com", "nameberry.com", "garden.org",
    "contra.com",       # freelancer marketplace, not a B2B lead
    "gasoccerforum.com", "realcavsfans.com",
    "dukebasketballreport.com", "thecardboard.org",
    "volleytalk.com",   # catches volleytalk.proboards.com too
    # Classifieds / directories / reviews — not B2B leads
    "yelp.com", "yellowpages.com", "bbb.org", "trustpilot.com",
    "glassdoor.com", "indeed.com", "monster.com",
    # V27.7: News aggregators / search portals (Serper log waste)
    "news.google.com", "google.com", "google.co.in", "google.co.uk",
    "news.yahoo.com", "msn.com", "bing.com", "duckduckgo.com",
    "flipboard.com", "feedly.com",
    # V27.7: Directories / education portals (enriching portal ≠ lead company)
    "justdial.com", "sulekha.com", "indiaeducation.net", "indiamike.com",
    "collegedunia.com", "yocket.com", "shiksha.com",
    # V24.5.1: B2B networking / SaaS platform root domains.
    # These are search CONTAINERS — individual posts and profiles found
    # within them are valid leads and flow through the snippet path
    # (Tier 1 social short-circuit). But enriching the platform's OWN
    # root domain (company profile, funding news, careers page for the
    # platform itself) is meaningless and wastes 3-4 Serper credits.
    # Example: alignable.com/business/acme-co → valid lead (snippet path).
    #          alignable.com as a root domain → enrichment skipped here.
    "alignable.com",
    "constantcontact.com",
    "mailchimp.com",
    "hootsuite.com",
    "sproutsocial.com",
    "buffer.com",
    "typeform.com",
    "surveymonkey.com",
    "zoho.com",
    "freshworks.com",
    "intercom.com",
    "zendesk.com",
    "sendgrid.com",
    "klaviyo.com",
    "activecampaign.com",
    "pipedrive.com",
    "monday.com",
    "asana.com",
    "notion.so",
    "canva.com",
]

# V24.1.1: Forum subdomain prefixes — if domain starts with any of these,
# it's almost certainly a community forum, not a business website.
_FORUM_PREFIXES = (
    "forum.", "forums.", "talk.", "community.", "discuss.",
    "board.", "boards.", "bbs.", "chat.",
)

# V24.1.1: Keywords in domain name that strongly indicate non-business.
# Matched as substring: "gasoccerforum.com" contains "forum".
_FORUM_DOMAIN_KEYWORDS = (
    "forum", "fans", "proboards", "phpbb", "vbulletin",
    "boards", "fansite", "fandom",
)

# V24.0: Domain suffixes that indicate non-business entities.
# V24.1.1: .org now skips ALL enrichment (not just Places).
# V24.5.2: Added personal-publishing TLDs (.blog, .dev, .page, .app).
#           - .blog  : WordPress/personal blogs — never a B2B lead
#           - .dev   : Personal developer portfolios (not SaaS companies)
#           - .page  : Google Sites personal pages
#           - .app   : Usually app store redirect pages, not company sites
#           NOTE: .io is intentionally NOT blocked — many legitimate B2B SaaS
#           companies (Linear, Miro, Pitch) use .io as their primary domain.
_NON_BUSINESS_SUFFIXES = (
    ".edu", ".ac.in", ".ernet.in", ".gov", ".gov.in", ".mil",
    ".org",   # most .org are non-profits/foundations, not B2B leads
    ".blog",  # personal/corporate blogs on blog-hosting TLD
    ".dev",   # personal developer portfolios
    ".page",  # Google Sites personal pages
    ".app",   # app store redirect pages
)

# FIX (2026-06-21): Replaced dead platform-specific B2C list with archetype-based
# detection. The old list contained labels ("Reddit B2C", etc.) that could never
# V24.3 (L2-2): Imported from shared core.constants module.
# Previously defined inline with a "MUST stay in sync with query_brain" warning.
from core.constants import CONSUMER_ARCHETYPES as _CONSUMER_ARCHETYPES  # type: ignore[import]

def _is_consumer_archetype(vector: str) -> bool:
    """Return True if *vector* is a consumer-facing business archetype."""
    return (vector or "").upper().strip() in _CONSUMER_ARCHETYPES


# V27.2.0: Public review/lead channels are NEVER hard-blocked as domains.
# g2.com / capterra.com removed from enterprise hard-block list — they are
# PLATFORM_MINING / COMPETITOR_TOUCHPOINT sources. Path/author noise still
# filtered below. zoominfo remains enterprise data-broker noise.
_PUBLIC_LEAD_CHANNELS = frozenset({
    "g2.com", "capterra.com", "trustpilot.com", "reddit.com", "quora.com",
    "yelp.com", "trustradius.com", "sitejabber.com",
})

_ENTERPRISE_DOMAINS = [
    "ibm.com", "amazon.com", "microsoft.com",
    "zoominfo.com",
    # V24.5.8: Academic preprint and research repositories — never a business lead.
    # Gemini pre-filter can misclassify these as buyers when campaign domain overlaps
    # with the paper's subject (e.g., MediMorph AI campaign → medical AI papers).
    "ssrn.com",        # Social Science Research Network
    "researchgate.net", # Academic social network (researchers, not buyers)
    "semanticscholar.org",
    "pubmed.ncbi.nlm.nih.gov",
    # V24.6.3: Login-walled social platforms — organic Serper results from these
    # domains always produce snippets with "sign in" / "log in" / "create account".
    # They are never scrapeable leads. Block at domain level to fail-fast and
    # avoid wasting the snippet noise-check cycle on known-bad results.
    # V26.0.4: REMOVED linkedin.com — LinkedIn snippets from Serper contain
    # enough context for Gemini scoring (company names, titles, activity) even
    # without full page scrape. Same reasoning as Quora un-block in V25.2.3.
    # B2B campaigns lost their #1 lead source when this was blocked in V24.6.3.
    # "linkedin.com",    # UNBLOCKED V26.0.4 — B2B regression fix
    # V25.2.3: Removed quora.com — query_brain generates site:quora.com queries,
    # so blocking results here creates a produce-then-discard loop that burns
    # Serper credits with zero yield. Quora snippets from Serper contain enough
    # context for Gemini scoring even if PRISM can't scrape the full page.
]

_NOISE_PATHS    = ["/legal", "/pricing", "/docs", "/author/", "/login"]
_NOISE_SNIPPETS = ["sign in", "access denied", "forgot password", "please enable cookies"]

# V25.7.4 Phase 1A: Reddit news-subreddit blocklist.
# Organic Reddit results from these subreddits are never business leads — they
# are news commentary, entertainment, or opinion threads that waste Gemini
# scoring cycles and pollute the pipeline with zero-conversion noise.
_REDDIT_NEWS_SUBREDDITS = {
    "politics", "worldnews", "economics", "news", "videos",
    "credibledefense", "irstudies", "askpolitics", "geopolitics",
    "worldpolitics", "conservative", "liberal", "technology",
    "science", "todayilearned", "askreddit", "explainlikeimfive",
    "outoftheloop", "nottheonion", "upliftingnews", "pics",
    "funny", "gaming", "movies", "television", "music",
    "sports", "nfl", "nba", "soccer", "formula1",
    "memes", "dankmemes", "aww", "cats", "dogs",
    "showerthoughts", "lifeprotips", "unpopularopinion",
    "amitheasshole", "tifu", "relationships",
}

# V25.7.4 Phase 1A: Megathread / aggregation title patterns.
# Results with these title fragments are aggregation pages (megathreads, weekly
# roundups, daily discussions) that contain no single-entity lead signal.
_MEGATHREAD_PATTERNS = [
    "megathread", "mega thread", "daily discussion",
    "weekly roundup", "weekly thread", "monthly roundup",
    "monthly thread", "daily thread", "match thread",
    "game thread", "general discussion", "open thread",
    "free talk", "rant thread", "unpopular opinion thread",
]

# V25.7.4 Phase 1A: Content farm / news outlet domains.
# These domains produce high-traffic, low-signal pages that Gemini may score
# highly due to keyword overlap but never convert to actionable leads.
_CONTENT_FARM_DOMAINS = {
    "buzzfeed.com", "wikihow.com", "reuters.com", "bloomberg.com",
    "bbc.com", "bbc.co.uk", "cnn.com", "ndtv.com",
    "timesofindia.indiatimes.com", "gulfnews.com", "khaleejtimes.com",
    "huffpost.com", "huffingtonpost.com", "foxnews.com",
    "dailymail.co.uk", "nypost.com", "washingtonpost.com",
    "nytimes.com", "theguardian.com", "aljazeera.com",
    "cnbc.com", "abcnews.go.com", "nbcnews.com", "cbsnews.com",
    "usatoday.com", "apnews.com", "businessinsider.com",
    "insider.com", "vice.com", "vox.com", "theverge.com",
    "mashable.com", "boredpanda.com", "ranker.com",
    "screenrant.com", "gamerant.com", "cbr.com",
    "hindustantimes.com", "indiatoday.in", "firstpost.com",
    "news18.com", "thehindu.com", "livemint.com",
}

# V24.5.7: CDN/static subdomain prefixes. These are asset-delivery servers, not
# business websites. They return empty PRISM scrapes and waste 3-5 Serper credits.
# The root domain may be a legitimate company (cdngetgo.com), but the 'assets.'
# subdomain prefix unambiguously identifies a content delivery node, not a page.
# V24.5.8: Extended with academic repository subdomain prefixes. Repos like
# lirias.kuleuven.be (KU Leuven), papers.ssrn.com, eprints.university.edu
# are never buyers even if their content overlaps with the campaign's domain.
_CDN_SUBDOMAIN_PREFIXES = (
    # CDN / asset delivery
    "assets.", "cdn.", "static.", "img.", "images.",
    "media.", "s3.", "storage.", "files.", "dl.",
    "download.", "content.",
    # Academic repository subdomains
    "papers.", "repository.", "eprints.", "dspace.",
    "lirias.", "preprint.", "preprints.", "scholar.",
    # V26.0.4: REMOVED "research." — too broad, catches legitimate company pages
    # like research.google.com, research.facebook.com. Academic repos are already
    # blocked by _ENTERPRISE_DOMAINS (ssrn, researchgate, semanticscholar).
    "publications.", "pub.",
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def extract_root_domain(url: str) -> str:
    """Extract root domain from a URL, stripping www."""
    try:
        netloc = urlparse(url).netloc.lower()
        if not netloc:
            netloc = urlparse("http://" + url).netloc.lower()
        return netloc.replace("www.", "")
    except Exception:
        return ""


def filter_serper_noise(
    serper_results: list,
    intent_profile: dict | object | None = None,
) -> list:
    """Remove enterprise, noise-path, CDN, and bot-page results from Serper output.

    V24.1.1 FIX: Uses proper domain extraction and path-segment matching
    instead of substring. Prevents false positives like 'caribm.com' matching
    'ibm.com' or '/doctrine' matching '/docs'.
    V24.5.7 FIX: Added CDN subdomain detection (_CDN_SUBDOMAIN_PREFIXES).
    V24.6.3 FIX: Added per-reason telemetry counter (noise_filter_summary)
    so operators can diagnose which filter is killing results without
    expanding individual log entries.

    V27.0: When ``intent_profile.orchestrator_active`` is true, public lead
    channels (G2, Capterra, Trustpilot, Reddit, Quora, …) are **never**
    hard-blocked as domains. Exclusion is path/snippet/intent-aware only.
    Backward compatible: omit intent_profile → legacy hard filters.
    """
    clean = []
    # V24.6.3: per-reason counters for observability
    _rejected_enterprise    = 0
    _rejected_cdn          = 0
    _rejected_path         = 0
    _rejected_snippet      = 0
    _rejected_subreddit    = 0
    _rejected_megathread   = 0
    _rejected_content_farm = 0
    _channel_admitted      = 0
    _intent_soft_drop      = 0

    # Resolve V27 intent profile (fail-open: inactive if import/shape fails)
    _v27_active = False
    _profile_obj = None
    try:
        if intent_profile is not None:
            try:
                from shared.intent_orchestrator import (  # type: ignore[import]
                    IntentProfile,
                    should_hard_drop_result,
                    channel_is_admissible,
                )
            except Exception:
                from intelligence.orchestrator import (  # type: ignore[import]
                    IntentProfile,
                    should_hard_drop_result,
                    channel_is_admissible,
                )
            if isinstance(intent_profile, IntentProfile):
                _profile_obj = intent_profile
            elif isinstance(intent_profile, dict):
                _profile_obj = IntentProfile.from_dict(intent_profile)
            else:
                _profile_obj = intent_profile
            _v27_active = bool(getattr(_profile_obj, "orchestrator_active", False))
            if isinstance(_profile_obj, dict):
                _v27_active = bool(_profile_obj.get("orchestrator_active"))
    except Exception as _intent_err:
        log.warning(
            "noise_filter_intent_profile_failed",
            error=str(_intent_err),
            note="Fail-open to legacy noise filter.",
        )
        _v27_active = False
        _profile_obj = None

    for r in serper_results:
        link    = r.get("link", "").lower()
        snippet = r.get("snippet", "").lower()
        # V24.1.1: Use root domain extraction instead of substring match
        link_domain = extract_root_domain(link)

        # ── V27 intent-aware admission (no hard channel domain bans) ──────
        if _v27_active and _profile_obj is not None:
            try:
                try:
                    from shared.intent_orchestrator import (  # type: ignore[import]
                        should_hard_drop_result,
                        channel_is_admissible,
                    )
                except Exception:
                    from intelligence.orchestrator import (  # type: ignore[import]
                        should_hard_drop_result,
                        channel_is_admissible,
                    )
                # CDN still always dropped (asset hosts, not pages)
                try:
                    raw_netloc = urlparse(link).netloc.lower()
                except Exception:
                    raw_netloc = link_domain
                if any(raw_netloc.startswith(pfx) for pfx in _CDN_SUBDOMAIN_PREFIXES):
                    _rejected_cdn += 1
                    continue

                drop, reason = should_hard_drop_result(
                    r,
                    _profile_obj,
                    legacy_enterprise_domains=_ENTERPRISE_DOMAINS,
                )
                if drop:
                    _intent_soft_drop += 1
                    if reason.startswith("path_exclude"):
                        _rejected_path += 1
                    elif reason.startswith("snippet_exclude"):
                        _rejected_snippet += 1
                    elif "content_farm" in reason:
                        _rejected_content_farm += 1
                    elif "enterprise" in reason or "infrastructure" in reason:
                        _rejected_enterprise += 1
                    elif "megathread" in reason:
                        _rejected_megathread += 1
                    continue

                if channel_is_admissible(link_domain, _profile_obj):
                    _channel_admitted += 1

                # Reddit news-subreddit still filtered (not a domain ban)
                if "reddit.com" in link_domain:
                    try:
                        path_parts = urlparse(link).path.strip("/").lower().split("/")
                        if len(path_parts) >= 2 and path_parts[0] == "r":
                            if path_parts[1] in _REDDIT_NEWS_SUBREDDITS:
                                _rejected_subreddit += 1
                                continue
                    except Exception:
                        pass

                clean.append(r)
                continue
            except Exception as _v27_loop_err:
                log.warning(
                    "noise_filter_v27_item_failed",
                    error=str(_v27_loop_err),
                    link=link[:80],
                    note="Fail-open: item falls through to legacy checks.",
                )

        # ── Legacy path (V26 and earlier / orchestrator off) ──────────────
        # V27.2.0: never hard-drop public lead channels even when V27 flag off
        if link_domain in _ENTERPRISE_DOMAINS and link_domain not in _PUBLIC_LEAD_CHANNELS:
            _rejected_enterprise += 1
            continue
        if link_domain in _PUBLIC_LEAD_CHANNELS:
            _channel_admitted += 1
        # V24.5.7: Block CDN/static subdomains — these are asset servers, not business pages.
        # extract_root_domain strips 'www.' but keeps other subdomains.
        # Check if the netloc (before root domain stripping) starts with a CDN prefix.
        try:
            raw_netloc = urlparse(link).netloc.lower()
        except Exception:
            raw_netloc = link_domain
        if any(raw_netloc.startswith(pfx) for pfx in _CDN_SUBDOMAIN_PREFIXES):
            _rejected_cdn += 1
            continue
        # V24.1.1: Path-segment matching — check that the path segment starts with noise prefix
        try:
            link_path = urlparse(link).path.lower()
        except Exception:
            link_path = ""
        if any(link_path.startswith(p) or f"{p}/" in link_path for p in _NOISE_PATHS):
            _rejected_path += 1
            continue
        if any(s in snippet for s in _NOISE_SNIPPETS):
            _rejected_snippet += 1
            continue
        # V25.7.4 Phase 1A Layer 1: Reddit news-subreddit blocklist.
        # Extract subreddit name from reddit.com/r/<subreddit>/... URLs.
        if "reddit.com" in link_domain:
            try:
                path_parts = urlparse(link).path.strip("/").lower().split("/")
                if len(path_parts) >= 2 and path_parts[0] == "r":
                    subreddit_name = path_parts[1]
                    if subreddit_name in _REDDIT_NEWS_SUBREDDITS:
                        _rejected_subreddit += 1
                        continue
            except Exception:
                pass  # Malformed URL — fall through to remaining checks
        # V25.7.4 Phase 1A Layer 2: Megathread / aggregation title detection.
        title_lower = r.get("title", "").lower()
        if any(pat in title_lower for pat in _MEGATHREAD_PATTERNS):
            _rejected_megathread += 1
            continue
        # V25.7.4 Phase 1A Layer 3: Content farm / news domain blocking.
        # V26.0.4: B2B exception — business news sources (Bloomberg, BusinessInsider,
        # Reuters, CNBC, LiveMint) contain event-trigger leads for B2B campaigns
        # (funding rounds, expansions, rebranding, M&A). Allow these through —
        # Gemini scoring will handle relevance.
        if link_domain in _CONTENT_FARM_DOMAINS:
            _B2B_NEWS_EXCEPTIONS = {
                "bloomberg.com", "businessinsider.com", "insider.com",
                "reuters.com", "cnbc.com", "livemint.com",
                "washingtonpost.com", "nytimes.com",
            }
            if link_domain not in _B2B_NEWS_EXCEPTIONS:
                _rejected_content_farm += 1
                continue
        clean.append(r)
    # V24.6.3: emit summary only when results were rejected — avoids log spam on clean batches
    total_in = len(serper_results)
    total_rejected = (
        _rejected_enterprise + _rejected_cdn + _rejected_path + _rejected_snippet
        + _rejected_subreddit + _rejected_megathread + _rejected_content_farm
        + _intent_soft_drop
    )
    # Deduplicate intent soft-drop from category counters for log clarity
    if total_rejected > 0 or _channel_admitted > 0 or _v27_active:
        log.info(
            "noise_filter_summary",
            total_in=total_in,
            total_passed=len(clean),
            total_rejected=total_rejected,
            rejected_enterprise=_rejected_enterprise,
            rejected_cdn=_rejected_cdn,
            rejected_path=_rejected_path,
            rejected_snippet=_rejected_snippet,
            rejected_subreddit=_rejected_subreddit,
            rejected_megathread=_rejected_megathread,
            rejected_content_farm=_rejected_content_farm,
            v27_orchestrator=_v27_active,
            channel_admitted=_channel_admitted,
            intent_soft_drop=_intent_soft_drop,
        )
    return clean

# V24.1.1: Check env var for Serper paid tier (skips social domain stripping)
_SERPER_PAID_TIER = os.getenv("SERPER_PAID_TIER", "").lower() in ("true", "1", "yes")


def sanitize_query(query: str) -> str:
    """Sanitize the search query to remove patterns blocked by Serper free tier.

    V24.1.1 FIX: When SERPER_PAID_TIER=true, social domain stripping is skipped
    entirely. Paid tier supports site:linkedin.com and similar queries. On free
    tier, stripping is still active but now logs when positive site: operators
    are removed (to diagnose query degradation).

    V27.5: Strip numbered list labels (``1. Seed Investment``), fix orphan
    quotes after domain tokens (``linkedin.com"``), drop empty quote pairs.
    """
    if not query:
        return ""

    # Sanitize wildcard domain operators (site:*.org -> site:.org)
    query = re.sub(r'(?<!\w)site:\*\.', 'site:.', query)

    # V27.5: "1. Seed Investment" / "2) Foo" list labels — not search intent
    query = re.sub(r'(?i)(?:^|["\s])\d{1,2}[.)]\s+[A-Za-z][A-Za-z0-9 /&-]{2,40}', ' ', query)

    # V27.5: orphan trailing quote glued to domain: linkedin.com" → linkedin.com
    query = re.sub(r'([a-z0-9.-]+\.[a-z]{2,})"+(\s|$)', r'\1\2', query, flags=re.I)
    # leading orphan quote before domain
    query = re.sub(r'(^|\s)"+([a-z0-9.-]+\.[a-z]{2,})', r'\1\2', query, flags=re.I)

    # Insert missing space between quotes and opening parenthesis: "abc"(xyz) -> "abc" (xyz)
    query = re.sub(r'(?<=\")\(', ' (', query)

    # Insert missing space between alphanumeric/dots/hyphens and opening parenthesis: net(xyz) -> net (xyz)
    query = re.sub(r'([a-zA-Z0-9\.\-_])\(', r'\1 (', query)
    
    # Insert missing space between closing and opening parenthesis: )( -> ) (
    query = re.sub(r'\)(?=\()', ') ', query)
    query = re.sub(r'\s+', ' ', query).strip()

    # V24.1.1: Skip sanitization entirely on paid tier
    if _SERPER_PAID_TIER:
        return query

    # V27.2.0 SCALE: Always preserve positive site: for public lead channels
    # on free tier. Stripping them was a pay-then-zero-yield bug at scale.
    # Only strip non-site consumer tokens that Serper free-tier rejects.
    forbidden = ["twitter", "instagram", "reddit", "quora", "youtube", "x.com"]
    _SITE_KEEP = (
        "reddit", "quora", "youtube", "twitter", "x.com", "instagram",
        "g2.com", "capterra", "trustpilot", "linkedin", "facebook",
        "yelp", "trustradius", "sitejabber",
    )

    # Matches quoted strings (possibly with prefix like -site: or -intitle:) or parentheses or words
    token_re = re.compile(r'([^\s()"]*"[^"]*"|[()]|[^\s()"]+)')

    tokens = []
    for match in token_re.finditer(query):
        tokens.append(match.group(0))

    clean_tokens = []
    for token in tokens:
        token_lower = token.lower()
        is_forbidden = False
        for f in forbidden:
            if f in token_lower:
                is_forbidden = True
                break
        if is_forbidden:
            # Always keep positive site: for public lead channels (V27.2.0)
            if token_lower.startswith("site:") and any(k in token_lower for k in _SITE_KEEP):
                log.info(
                    "sanitize_query_positive_site_preserved",
                    token=token[:80],
                    note="V27.2.0: public channel site: always kept on free tier.",
                )
                clean_tokens.append(token)
                continue
            if token_lower.startswith("site:"):
                log.warning(
                    "sanitize_query_positive_site_stripped",
                    token=token[:80],
                    query=query[:100],
                    note="Non-keep-list site: stripped on free tier.",
                )
            continue
        clean_tokens.append(token)

    # Clean up consecutive logical operators
    final_tokens = []
    for token in clean_tokens:
        if token in ("AND", "OR", "NOT") and final_tokens and final_tokens[-1] in ("AND", "OR", "NOT"):
            continue
        final_tokens.append(token)

    # Remove leading/trailing AND/OR/NOT
    while final_tokens and final_tokens[0] in ("AND", "OR"):
        final_tokens.pop(0)
    while final_tokens and final_tokens[-1] in ("AND", "OR", "NOT"):
        final_tokens.pop()

    # Reassemble tokens - preserving proper spacing before opening parentheses
    result = ""
    for token in final_tokens:
        if token == "(":
            if result and not result.endswith("(") and not result.endswith(" "):
                result += " ("
            else:
                result += "("
        elif token == ")":
            result += ")"
        else:
            if result and not result.endswith("("):
                result += " " + token
            else:
                result += token

    # Clean up empty parentheses and trailing operators inside parentheses
    while True:
        new_result = re.sub(r'\(\s*\)', '', result)
        new_result = re.sub(r'\(\s*(AND|OR|NOT)\s*\)', '', new_result)
        new_result = re.sub(r'\(\s*(AND|OR|NOT)\s+', '(', new_result)
        new_result = re.sub(r'\s+(AND|OR|NOT)\s*\)', ')', new_result)
        new_result = re.sub(r'\s+', ' ', new_result).strip()
        if new_result == result:
            break
        result = new_result

    return result


# EXT-04: 5-minute TTL on the Serper key cache.
# core.clients caches indefinitely (container-lifetime). This wrapper adds a
# 5-minute TTL so that a key rotation in Secret Manager takes effect within
# 5 minutes without redeploying. The (key, timestamp) tuple is stored module-
# level; on expiry the cached value in core.clients is cleared and re-fetched.
_serper_key_cache_local: tuple[str, float] | None = None
_SERPER_KEY_TTL_S: float = 300.0  # 5 minutes
_serper_key_cache_lock = threading.Lock()  # P2-RACE-1: guard concurrent check-and-set


def _get_serper_api_key() -> str:
    """Fetch Serper API key with a 5-minute TTL cache layer.

    SF-004 fix: previously called Secret Manager on every invocation.
    get_serper_key() caches the result for the lifetime of the container.
    EXT-04: This wrapper adds a 5-min TTL so key rotations take effect
    without container restarts.
    """
    import time as _time
    import core.clients as _cc  # type: ignore[import]

    global _serper_key_cache_local
    now = _time.monotonic()
    # Fast path — no lock needed if cache is warm
    if _serper_key_cache_local is not None:
        cached_key, cached_ts = _serper_key_cache_local
        if now - cached_ts < _SERPER_KEY_TTL_S:
            return cached_key

    # P2-RACE-1: Lock to prevent concurrent threads from racing on
    # cache invalidation + Secret Manager re-fetch.
    with _serper_key_cache_lock:
        # Double-checked locking: re-test after acquiring lock
        now = _time.monotonic()
        if _serper_key_cache_local is not None:
            cached_key, cached_ts = _serper_key_cache_local
            if now - cached_ts < _SERPER_KEY_TTL_S:
                return cached_key
            # TTL expired — clear the upstream process-lifetime cache so
            # get_serper_key() re-fetches from Secret Manager.
            _cc._serper_key_cache = None

        key = get_serper_key(SERPER_API_KEY_NAME)
        _serper_key_cache_local = (key, now)
        return key


# ---------------------------------------------------------------------------
# Serper Audit Telemetry — async fire-and-forget (V23.4.1 — Zero-Trust OIDC)
# ---------------------------------------------------------------------------
_ORCHESTRATOR_URL: str = os.environ.get("ORCHESTRATOR_URL", "").rstrip("/")

# ── OIDC token cache (thread-safe, 55-min TTL) ─────────────────────────────
# GCE metadata server tokens expire after 60 minutes.  55-min TTL absorbs
# clock-skew and gives the refresh a 5-minute safety margin.
import threading as _th_oidc
_OIDC_CACHE_LOCK    = _th_oidc.Lock()
_oidc_token_value:  str   = ""
_oidc_token_expiry: float = 0.0  # monotonic timestamp


def _get_oidc_token(audience: str) -> str:
    """Return a valid Google OIDC identity token for *audience*.

    Fetches from the GCE instance metadata server — always available on
    Cloud Run, GKE, and GCE.  Result is cached for 55 minutes.

    Falls back to google-auth library (ADC / key file) if the metadata
    server is unavailable (e.g. local dev with GOOGLE_APPLICATION_CREDENTIALS).
    Returns "" on all failures so callers degrade gracefully.

    Args:
        audience: Base URL of the target service, e.g.
                  "https://orchestrator-xyz-uc.a.run.app".  Must exactly
                  match the audience the orchestrator validates against.
    """
    import time as _time

    global _oidc_token_value, _oidc_token_expiry

    # Fast path — no lock needed if cache is warm
    _now = _time.monotonic()
    if _oidc_token_value and _now < _oidc_token_expiry:
        return _oidc_token_value

    with _OIDC_CACHE_LOCK:
        # Double-checked locking: re-test after acquiring
        _now = _time.monotonic()
        if _oidc_token_value and _now < _oidc_token_expiry:
            return _oidc_token_value

        try:
            # Primary: GCE metadata server (always present on Cloud Run)
            import urllib.request as _url_req
            _meta_url = (
                "http://metadata.google.internal/computeMetadata/v1/instance"
                f"/service-accounts/default/identity"
                f"?audience={audience}&format=full"
            )
            _req = _url_req.Request(_meta_url, headers={"Metadata-Flavor": "Google"})
            with _url_req.urlopen(_req, timeout=3) as _resp:
                _token = _resp.read().decode("utf-8").strip()
            _oidc_token_value  = _token
            _oidc_token_expiry = _time.monotonic() + (55 * 60)
            log.debug("oidc_token_refreshed_metadata", audience=audience[:50])
            return _oidc_token_value

        except Exception as _meta_err:
            # Fallback: google-auth ADC (key file / workload identity)
            try:
                import google.auth as _gauth
                from google.auth.transport import requests as _gauth_req
                _creds, _ = _gauth.default(scopes=["openid"])
                _creds.refresh(_gauth_req.Request())
                _token = getattr(_creds, "id_token", None) or getattr(_creds, "token", "")
                if _token:
                    _oidc_token_value  = _token
                    _oidc_token_expiry = _time.monotonic() + (55 * 60)
                    log.debug("oidc_token_refreshed_adc", audience=audience[:50])
                    return _oidc_token_value
            except Exception as _adc_err:
                log.warning(
                    "oidc_token_fetch_failed",
                    metadata_err=str(_meta_err)[:120],
                    adc_err=str(_adc_err)[:120],
                    action="Telemetry will be dropped — check lead-pipeline-sa IAM."
                )
            return ""


def _push_serper_audit(
    campaign_id: str,
    tenant_id: str,
    raw_query: str,
    serper_parameters: dict,
    result_count: int,
    status_code: int = 200,
    error_message: str = "",
    engine: str = "search",
) -> None:
    """Push one audit row to the orchestrator BQ broker via Zero-Trust OIDC.

    Runs in a daemon thread — never blocks the calling scrape loop.
    Silently swallowed on any exception (non-critical telemetry path).

    V23.4 bug (fixed in V23.4.1):
        Previously called httpx.post() with no Authorization header.
        The orchestrator's _verify_oidc() silently returned HTTP 200 with
        {"ok": false, "reason": "auth_failed"}, so zero rows reached BigQuery.

    V23.4.1 fix:
        Fetches a Google OIDC identity token from the GCE metadata server
        (audience = ORCHESTRATOR_URL), caches it for 55 minutes, and attaches
        it as "Authorization: Bearer <token>".  _verify_oidc() validates the
        token signature via Google public certs and admits the request.
    """
    if not _ORCHESTRATOR_URL:
        return  # env var not set — skip silently (local/dev environments)
    try:
        import datetime as _dt
        payload = {
            "table":              "serper_audit_logs",  # broker routing key
            "campaign_id":        campaign_id or "",
            "tenant_id":          tenant_id   or "",
            "raw_query":          raw_query,
            "serper_parameters":  serper_parameters,
            "result_count":       result_count,
            "credit_cost":        1,
            "engine":             engine,
            "serper_status_code": status_code,
            "error_message":      error_message or None,
            "timestamp":          _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }

        # ── Zero-Trust OIDC — attach identity token ──────────────────────────
        _headers: dict = {"Content-Type": "application/json"}
        _token = _get_oidc_token(_ORCHESTRATOR_URL)
        if _token:
            _headers["Authorization"] = f"Bearer {_token}"
        else:
            log.warning(
                "serper_audit_no_oidc_token",
                orchestrator=_ORCHESTRATOR_URL[:50],
                action="Row will be rejected by orchestrator auth — check SA.",
            )

        # ── Non-fatal HTTP POST — telemetry must never crash the scraping loop ────
        # We capture the response and log at WARNING on failure, but we do NOT
        # call raise_for_status(). A 500 from the orchestrator means BQ rejected
        # the row; we log it so the issue is visible in Cloud Logging, then exit
        # cleanly. The scraping loop continues regardless.
        _resp = httpx.post(
            f"{_ORCHESTRATOR_URL}/api/internal/telemetry/serper-audit",
            json=payload,
            headers=_headers,
            timeout=5,
        )
        if _resp.status_code != 200:
            log.warning(
                "serper_audit_broker_non_200",
                status=_resp.status_code,
                body=_resp.text[:200],
                action="Telemetry row dropped. BQ schema mismatch or orchestrator error."
            )
    except Exception as _te:
        # Network-level failure (timeout, DNS, connection refused).
        # Log at WARNING so failures appear in production Cloud Logging.
        # Never raise — telemetry is non-critical fire-and-forget.
        log.warning("serper_audit_push_failed", error=str(_te)[:200])


def _async_serper_audit(**kwargs) -> None:
    """Fire-and-forget wrapper: launches _push_serper_audit in a daemon thread."""
    try:
        t = threading.Thread(target=_push_serper_audit, kwargs=kwargs, daemon=True)
        t.start()
    except Exception as _audit_err:
        log.debug("serper_audit_thread_error", error=str(_audit_err))


# ---------------------------------------------------------------------------
# Primary search function
# ---------------------------------------------------------------------------

def search_serper(
    query: str,
    location: Optional[str] = None,
    gl: Optional[str] = None,
    hl: Optional[str] = None,
    *,
    campaign_id: str = "",
    tenant_id: str = "",
    sourcing_vector: str = "",
    residual: bool = False,
) -> list:
    """Execute a Serper Google Search query with tenacity 429-retry.

    Uses a 4-attempt exponential backoff targeting only HTTP 429 responses.
    Auth failures (401/403) and server errors (5xx) are not retried.

    On all-retries-exhausted: emits circuit telemetry, fires a failed audit
    row, and returns [].

    Args:
        query:           Full search query string (may include site: operators).
        location:        Serper ``location`` field (optional, e.g. ``"India"``).
        gl:              Serper ``gl`` field / ISO country code (optional).
        hl:              Serper ``hl`` field / language code (optional).
        campaign_id:     Campaign context for BQ audit telemetry (optional).
        tenant_id:       Tenant context for BQ audit telemetry (optional).
        sourcing_vector: Campaign sourcing vector label (optional).
        residual:        V27.3.0 — True for non-produce paths (mesh, inbound,
                         PRISM). Counts against SERPER_RESIDUAL_DAILY_LIMIT.

    Returns:
        List of organic result dicts from Serper.  Empty on any failure.
    """
    # Import telemetry locally to avoid circular imports
    from services.telemetry import update_circuit_telemetry  # type: ignore[import]

    # Pre-emptively sanitize query for Serper free-tier compliance
    sanitized = sanitize_query(query)
    if not sanitized:
        log.warning("serper_query_sanitized_to_empty", original_query=query)
        return []

    if sanitized != query:
        log.info("serper_query_sanitized", original=query, sanitized=sanitized)
        query = sanitized

    # V27.3.0: project-wide Serper budget (multi-instance Firestore)
    try:
        from shared.serper_budget import record_serper_spend  # type: ignore[import]
        from core.clients import get_db as _get_db_budget  # type: ignore[import]
        if not record_serper_spend(
            _get_db_budget(),
            amount=1,
            residual=bool(residual),
            log=lambda msg, **kw: log.info(msg, **kw),
        ):
            log.warning(
                "serper_budget_blocked",
                query=query[:120],
                residual=bool(residual),
                campaign_id=campaign_id,
            )
            return []
    except Exception as _bg_err:
        log.warning("serper_budget_check_error", error=str(_bg_err), note="Fail-open")

    api_key = _get_serper_api_key()
    url     = "https://google.serper.dev/search"

    payload_dict: dict = {"q": query, "num": 20}
    if location:
        payload_dict["location"] = location
    if gl:
        payload_dict["gl"] = gl
    if hl:
        payload_dict["hl"] = hl
    elif gl:
        _HL_BY_GL = {
            "in": "en",
            "ae": "en",
            "om": "en",
            "sa": "en",
            "us": "en",
            "uk": "en",
            "de": "de",
            "fr": "fr",
            "es": "es",
        }
        payload_dict["hl"] = _HL_BY_GL.get(str(gl).lower(), "en")

    # V26.0.5 / V27.8: Smart time filter based on query structure.
    # Decision matrix (product rule: **3-month rolling** = qdr:3m):
    #   site: social/forum (reddit, quora, linkedin, …) → qdr:3m
    #   site: evergreen platforms (directories, listings) → NO time filter
    #   B2C / B2B non-site colloquial                     → qdr:3m
    import re as _re_tbs
    _query_body_tbs = _re_tbs.split(
        r'\s+-(?:site:|wiki\b|jobs\b|")', query, maxsplit=1
    )[0].strip()
    _has_positive_site = bool(_re_tbs.search(r'(?<!\-)site:', _query_body_tbs))
    _q_lower = query.lower()
    _SOCIAL_SITE_HINTS = (
        "site:reddit.", "site:quora.", "site:linkedin.", "site:facebook.",
        "site:twitter.", "site:x.com", "site:youtube.", "site:instagram.",
        "site:tiktok.", "site:news.ycombinator", "site:team-bhp.",
        "/r/", "comments/",
    )
    _is_social_forum_query = any(h in _q_lower for h in _SOCIAL_SITE_HINTS) or any(
        d.replace("www.", "") in _q_lower
        for d in (
            "reddit.com", "quora.com", "linkedin.com", "facebook.com",
            "twitter.com", "youtube.com",
        )
        if f"site:{d}" in _q_lower or f'site:{d.split(".")[0]}' in _q_lower
    )

    if _has_positive_site and _is_social_forum_query:
        # V27.8: social/forum site: dorks — 3-month freshness
        payload_dict["tbs"] = "qdr:3m"
        log.info(
            "serper_tbs_social_forum",
            tbs="qdr:3m",
            query=query[:100],
            note="Social/forum site: query — 3-month window (V27.8).",
        )
    elif _has_positive_site:
        # Evergreen platform mining — no time restriction
        pass
    elif sourcing_vector and _is_consumer_archetype(sourcing_vector):
        payload_dict["tbs"] = "qdr:3m"
    else:
        # V27.8: B2B colloquial also 3 months
        payload_dict["tbs"] = "qdr:3m"

    payload = json.dumps(payload_dict)
    headers = {"X-API-KEY": api_key, "Content-Type": "application/json"}

    # EXT-03: Retry on transient network errors (timeout, connection reset)
    # AND transient server errors (500/502/503/504) in addition to 429.
    _RETRYABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504})

    def _is_retryable(exc: BaseException) -> bool:
        if isinstance(exc, (httpx.TimeoutException, httpx.ConnectError)):
            return True
        if isinstance(exc, httpx.HTTPStatusError):
            return exc.response.status_code in _RETRYABLE_STATUS_CODES
        return False

    @retry(
        wait=wait_exponential(multiplier=2, min=4, max=32),
        stop=stop_after_attempt(4),
        retry=retry_if_exception(_is_retryable),
        reraise=True,  # V24.1.1 FIX: reraise=False returned None on exhaustion → TypeError on len()
    )
    def _do_post():
        r = httpx.post(url, headers=headers, data=payload, timeout=30)
        if r.status_code in _RETRYABLE_STATUS_CODES:
            r.raise_for_status()
        if r.status_code == 200:
            # EXT-02: Log remaining Serper credits if header is present.
            _credits_remaining = r.headers.get("X-Credits-Remaining")
            if _credits_remaining is not None:
                log.info(
                    "serper_credits_remaining",
                    credits=_credits_remaining,
                    query=query[:60],
                )
            return r.json().get("organic", [])
        log.warning("serper_non_retryable", status=r.status_code, body=r.text[:200])
        return []

    try:
        results = _do_post()
        if results is None:
            results = []  # V24.1.1: guard against tenacity edge cases
        # ── V23.4: Async audit telemetry — zero-latency fire-and-forget ──────
        _async_serper_audit(
            campaign_id       = campaign_id,
            tenant_id         = tenant_id,
            raw_query         = query,
            serper_parameters = payload_dict,
            result_count      = len(results),
            status_code       = 200,
            engine            = "search",
        )
        return results
    except Exception as exc:
        # V24.1.1 FIX: Determine actual failure status instead of hardcoding 429
        _fail_code = 429
        if hasattr(exc, 'response') and hasattr(exc.response, 'status_code'):
            _fail_code = exc.response.status_code
        log.error("serper_all_retries_exhausted", query=query[:60],
                  error=str(exc), status_code=_fail_code)
        if _fail_code == 429:
            update_circuit_telemetry("serper_429")
        # ── Audit row for failed call ─────────────────────────────────────────
        _async_serper_audit(
            campaign_id       = campaign_id,
            tenant_id         = tenant_id,
            raw_query         = query,
            serper_parameters = payload_dict,
            result_count      = 0,
            status_code       = _fail_code,
            error_message     = str(exc)[:500],
            engine            = "search",
        )
        return []


# ---------------------------------------------------------------------------
# Enrichment Gatekeeper
# ---------------------------------------------------------------------------

def deep_context_serper_dork(
    domain: str,
    tenant_id: str,
    sourcing_vector: str = "B2B",
    source_url: str = "",
) -> tuple[str, bool]:
    """Fetch contextual enrichment signals for a domain via Serper.

    V27.7: B2B domains receive Places + hiring only (2 credits). Company-profile
    search was removed — low SNR and duplicated mesh/social paths. Consumer
    vectors still get Places + reviews (2 credits). 48h Firestore cache prevents
    double-dispatch waste (same domain, multiple leads). Social/aggregator/
    directory hosts are gated (incl. news.google.com).

    Args:
        domain:          Root domain string.
        tenant_id:       Tenant UID for usage metering.
        sourcing_vector: Campaign sourcing vector label.
        source_url:      Full original URL (used for domain extraction).

    Returns:
        ``(context_data_string, hiring_intent_found)``
        ``("", False)`` if gatekeeper blocks enrichment.
    """
    from core.clients import get_db  # type: ignore[import]

    if not domain:
        return "", False

    # Ensure we are working with the root domain.
    # If a full URL is passed to domain (like in dispatch.py line 773), extract the root domain.
    if "://" in domain or "/" in domain or "." not in domain:
        domain = extract_root_domain(domain)
        if not domain:
            return "", False

    # Check against the social domains blacklist.
    # Social domains and B2C vectors bypass enrichment since they don't contain B2B lead info.
    cleaned = domain.lower().replace("www.", "")
    for blocked in _ENRICHMENT_SOCIAL_BLACKLIST:
        if cleaned == blocked or cleaned.endswith("." + blocked) or blocked in cleaned:
            log.info("enrichment_gated_social", domain=domain)
            return "", False

    # V27.7 shared SSOT (news.google, directories, etc.)
    try:
        from shared.serper_enrichment_policy import (  # type: ignore[import]
            is_enrichment_skip_domain,
            read_enrichment_cache,
            write_enrichment_cache,
        )
        if is_enrichment_skip_domain(cleaned):
            log.info("enrichment_gated_policy_skip", domain=domain)
            return "", False
    except Exception as _pol_err:
        log.warning("enrichment_policy_import_error", error=str(_pol_err))
        is_enrichment_skip_domain = None  # type: ignore[assignment]
        read_enrichment_cache = None  # type: ignore[assignment]
        write_enrichment_cache = None  # type: ignore[assignment]

    # V24.1.1: Forum prefix detection — "forum.example.com", "talk.site.com", etc.
    if any(cleaned.startswith(p) for p in _FORUM_PREFIXES):
        log.info("enrichment_gated_forum_prefix", domain=domain, prefix=cleaned.split(".")[0])
        return "", False

    # V24.1.1: Forum keyword detection — "gasoccerforum.com" contains "forum"
    if any(kw in cleaned for kw in _FORUM_DOMAIN_KEYWORDS):
        log.info("enrichment_gated_forum_keyword", domain=domain)
        return "", False

    # V24.1.1: Non-business suffixes (.edu, .gov, .org) skip ALL enrichment.
    # Previously only skipped Places; now skips social + hiring queries too.
    # These domains never produce B2B leads and waste 2-3 credits each.
    if any(cleaned.endswith(sfx) for sfx in _NON_BUSINESS_SUFFIXES):
        log.info("enrichment_gated_non_business_suffix", domain=domain)
        return "", False

    _is_consumer = _is_consumer_archetype(sourcing_vector)
    _cache_kind = "deep_context_consumer" if _is_consumer else "deep_context_b2b"

    # V27.7: 48h domain cache — zero Serper on repeat dispatch for same host
    if read_enrichment_cache is not None:
        try:
            cached = read_enrichment_cache(
                get_db(),
                cleaned,
                _cache_kind,
                log=lambda msg, **kw: log.info(msg, **kw),
            )
            if cached is not None and "context_str" in cached:
                return str(cached.get("context_str") or ""), bool(cached.get("hiring_intent"))
        except Exception as _ce:
            log.warning("deep_context_cache_read_failed", error=str(_ce))

    # V27.7: B2B and consumer both 2 residual credits (profile query removed)
    _n_calls = 2
    try:
        from shared.serper_budget import record_serper_spend  # type: ignore[import]
        from core.clients import get_db as _gdb  # type: ignore[import]
        if not record_serper_spend(
            _gdb(),
            amount=_n_calls,
            residual=True,
            log=lambda msg, **kw: log.info(msg, **kw),
        ):
            log.warning(
                "deep_context_serper_budget_blocked",
                domain=domain,
                amount=_n_calls,
            )
            return "", False
    except Exception as _bge:
        log.warning("deep_context_budget_error", error=str(_bge), note="Fail-open")

    api_key = _get_serper_api_key()
    headers = {"X-API-KEY": api_key, "Content-Type": "application/json"}
    context_data: list[str] = []

    # BUG-S1 FIX: Previously 3 sequential Serper calls with timeout=15 each = 45s max.
    # Fix: Run all calls in parallel via ThreadPoolExecutor. Max wall time ~15s.
    # BUG-S2 FIX: Usage metrics Firestore write was inside _fetch() per call = 3 RPCs.
    # Fix: Single batched write at the end (1 RPC total).
    def _fetch_parallel(serper_url: str, body: dict) -> dict:
        """Fire a Serper request. No Firestore write here (batched externally)."""
        try:
            resp = httpx.post(serper_url, headers=headers, json=body, timeout=12)
            if resp.status_code == 200:
                return resp.json()
        except Exception as exc:
            log.debug("enrichment_fetch_failed", error=str(exc))
        return {}

    # V24.1.1 FIX (W3): Consumer vectors get lightweight enrichment instead of
    # being blanket-blocked. B2C leads now receive GMB + review context instead of
    # zero context, reducing the scoring bias against consumer leads.
    tasks = []
    if _is_consumer:
        # Consumer enrichment: GMB (local presence) + review signals (consumer sentiment)
        # Skip company profile and hiring queries — they produce B2B noise for B2C leads.
        tasks.append(("https://google.serper.dev/places",  {"q": domain, "num": 3}))
        tasks.append(("https://google.serper.dev/search",  {
            "q": f'reviews OR complaints OR "customer experience" "{domain}"', "num": 3,
        }))
        log.info("enrichment_consumer_path", domain=domain, vector=sourcing_vector)
    else:
        # V27.7 B2B: Places + hiring only (2 credits). Dropped low-SNR
        # "company profile OR social media" query; mesh handles funding/reviews.
        tasks.append(("https://google.serper.dev/places",  {"q": domain, "num": 3}))
        tasks.append(("https://google.serper.dev/search",  {
            "q": f'job openings OR careers "{domain}"',
            "num": 3,
        }))
        log.info("enrichment_b2b_path_v27_7", domain=domain, calls=2)

    with _cf.ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(_fetch_parallel, url, body) for url, body in tasks]
        results = []
        for fut in futures:
            try:
                results.append(fut.result(timeout=13))
            except Exception:
                results.append({})

    # Parse results based on enrichment path
    gmb_data = results[0] if results else {}
    hiring_intent = False

    for place in gmb_data.get("places", []):
        context_data.append(
            f"[GMB] Rating: {place.get('rating', 'N/A')}, "
            f"Reviews: {place.get('ratingCount', 'N/A')}, "
            f"Address: {place.get('address', 'N/A')}"
        )

    if _is_consumer:
        # Consumer path: results[1] = review/complaint search
        review_data = results[1] if len(results) > 1 else {}
        for org in review_data.get("organic", []):
            context_data.append(f"[REVIEW] {org.get('snippet', '')}")
    else:
        # V27.7 B2B path: results[1] = hiring only
        hiring_data = results[1] if len(results) > 1 else {}
        hiring_sigs = [
            "we are hiring", "job description", "apply today",
            "openings", "careers", "looking for", "lakh", "lpa", "fresher",
        ]
        for job in hiring_data.get("organic", []):
            snippet_lc = job.get("snippet", "").lower()
            context_data.append(f"[HIRING] {snippet_lc}")
            if any(sig in snippet_lc for sig in hiring_sigs):
                hiring_intent = True

    # BUG-S2 FIX: Single batched Firestore write for all Serper calls.
    try:
        from google.cloud import firestore as _fs  # type: ignore[import]
        get_db().collection("usage_metrics").document(tenant_id).set(
            {"serper_searches": _fs.Increment(len(tasks))}, merge=True
        )
    except Exception:
        pass

    context_str = " | ".join(context_data)[:3000]

    if write_enrichment_cache is not None:
        try:
            write_enrichment_cache(
                get_db(),
                cleaned,
                _cache_kind,
                {"context_str": context_str, "hiring_intent": hiring_intent},
                log=lambda msg, **kw: log.info(msg, **kw),
            )
        except Exception as _cw:
            log.warning("deep_context_cache_write_failed", error=str(_cw))

    return context_str, hiring_intent
