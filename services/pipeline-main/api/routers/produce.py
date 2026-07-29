"""
Pipeline-Main V23 — /produce Blueprint (FULL IMPLEMENTATION).

THE PRODUCER — 24-Hour Serper Fetch Job.
=========================================
Runs Intent Translation (Query Brain) + Serper Execution.
Deduplicates against global leads collection.
Writes fresh URLs to campaigns/{id}.unprocessed_queue.
Does NOT call the Gemini Gate — only the Consumer does.

Raw GCS firehose dump deliberately removed per EA directive (2026-04-18).
Intelligence is sourced exclusively from BigQuery swarm_analytics via
the shadow_track hook — no parallel GCS write path exists.

Auth:
  - Zero-Trust OIDC: Google-signed JWT verified by @require_tasks_oidc.
  - Defense-in-depth: X-CloudTasks-QueueName header also enforced.
  - Cloud Run IAM (--no-allow-unauthenticated) is the outermost gate.
"""
from __future__ import annotations

import hashlib
import os
import re
from datetime import datetime, timezone, timedelta

from google.cloud import firestore  # type: ignore[import]
from google.cloud.firestore_v1.base_query import FieldFilter  # type: ignore[import]
from flask import Blueprint, jsonify, request

from core.logging import get_logger    # type: ignore[import]
from core.clients import get_db        # type: ignore[import]
from middleware.oidc import require_tasks_oidc  # type: ignore[import]


def should_attempt_geo_fallback(
    *,
    gl: str,
    has_results: bool,
    is_platform_query: bool,
    low_liquidity: bool,
) -> tuple[bool, str]:
    """Decide whether to retry a geo-zero Serper query on the global index.

    Returns:
        (should_retry, reason) where reason is one of:
          no_need | low_liquidity | platform | high_liquidity_skip
    """
    if has_results or not (gl or "").strip():
        return False, "no_need"
    if low_liquidity:
        return True, "low_liquidity"
    if is_platform_query:
        return True, "platform"
    return False, "high_liquidity_skip"


# ---------------------------------------------------------------------------
# V25.5.0 / V27.8: Content age filter — reject stale social/forum posts
# ---------------------------------------------------------------------------
# Product rule (V27.8): **3-month rolling window (90 days)** for ALL content
# including Reddit/Quora/LinkedIn. Undated social fails closed. Non-thread
# Reddit listings (/rising, /hot, subreddit roots, /user/) are always rejected.

_STALE_DAYS_B2C = 90
_STALE_DAYS_B2B = 90
_STALE_DAYS_SOCIAL = 90  # V27.8: unified 3-month rolling validity

_SOCIAL_STALE_HOST_HINTS = (
    "reddit.com", "quora.com", "linkedin.com", "facebook.com",
    "twitter.com", "x.com", "youtube.com", "instagram.com",
    "tiktok.com", "team-bhp.com", "forum.", "discourse.",
    "stackexchange.com", "stackoverflow.com", "news.ycombinator.com",
)

# Reddit base36 post-id floor for ~90d lookback (updated with product window).
# IDs strictly below this are treated as older than the rolling window when
# Serper omits date. Calibrated for mid-2026 traffic; fail-closed still applies.
_REDDIT_ID_MIN_RECENT = "1s00000"  # ~early 2026 — older base36 → reject


def _result_is_social_forum(result: dict) -> bool:
    link = (result.get("link") or result.get("url") or "").lower()
    return any(h in link for h in _SOCIAL_STALE_HOST_HINTS)


def _is_reddit_non_thread_url(url: str) -> bool:
    """True for subreddit hubs, sort views, and user profiles — not a post."""
    u = (url or "").lower().split("?", 1)[0].rstrip("/")
    if "reddit.com" not in u:
        return False
    if "/user/" in u or "/u/" in u:
        return True
    if re.search(r"/r/[^/]+/(rising|hot|new|top|best)(/|$)", u):
        return True
    # Subreddit root: /r/name or /r/name/ with no /comments/
    if re.search(r"/r/[^/]+/?$", u) and "/comments/" not in u:
        return True
    return False


def _reddit_post_id_from_url(url: str) -> str | None:
    m = re.search(r"reddit\.com/r/[^/]+/comments/([a-z0-9]+)/", (url or "").lower())
    return m.group(1) if m else None


def _reddit_id_is_older_than_window(post_id: str, min_recent: str = _REDDIT_ID_MIN_RECENT) -> bool:
    """Compare base36 post ids — lower id ⇒ older post."""
    try:
        return int(post_id, 36) < int(min_recent, 36)
    except (ValueError, TypeError):
        return False


def _age_days_from_serper_date(raw_date: str) -> int | None:
    """Parse Serper date string to age in days, or None if unparseable."""
    raw_date = (raw_date or "").strip()
    if not raw_date:
        return None
    _rel_match = re.match(
        r"(\d+)\s+(second|minute|hour|day|week|month|year)s?\s+ago",
        raw_date, re.IGNORECASE,
    )
    if _rel_match:
        _count = int(_rel_match.group(1))
        _unit = _rel_match.group(2).lower()
        _multipliers = {
            "second": 0, "minute": 0, "hour": 0,
            "day": 1, "week": 7, "month": 30, "year": 365,
        }
        return _count * _multipliers.get(_unit, 0)

    for fmt, slen in (
        ("%Y-%m-%dT%H:%M:%S", 19),
        ("%Y-%m-%d", 10),
        ("%b %d, %Y", 12),
        ("%B %d, %Y", 18),
    ):
        try:
            parsed = datetime.strptime(raw_date[:slen], fmt)
            return (datetime.now(timezone.utc) - parsed.replace(tzinfo=timezone.utc)).days
        except (ValueError, TypeError):
            continue
    _ym = re.search(r"\b(20\d{2})\b", raw_date)
    if _ym:
        year = int(_ym.group(1))
        try:
            mid = datetime(year, 7, 1, tzinfo=timezone.utc)
            return max(0, (datetime.now(timezone.utc) - mid).days)
        except ValueError:
            pass
    return None


def _is_stale_content(result: dict, is_consumer: bool) -> bool:
    """Return True if Serper result is too old to be actionable.

    V27.8: Unified **90-day (3-month)** rolling window for social and non-social.
    Social undated → fail-closed. Reddit non-thread URLs always stale/invalid.
    """
    link = (result.get("link") or result.get("url") or "")
    is_social = _result_is_social_forum(result)
    max_days = _STALE_DAYS_SOCIAL if is_social else (
        _STALE_DAYS_B2C if is_consumer else _STALE_DAYS_B2B
    )

    # Always drop Reddit listings / profiles (not a person-intent thread)
    if _is_reddit_non_thread_url(link):
        return True

    raw_date = (result.get("date") or "").strip()
    age = _age_days_from_serper_date(raw_date)
    if age is not None:
        return age > max_days

    # Reddit post-id floor when Serper omits date
    rid = _reddit_post_id_from_url(link)
    if rid and _reddit_id_is_older_than_window(rid):
        return True

    # Social/forum without date: year markers in title/snippet
    if is_social:
        blob = f"{result.get('title') or ''} {result.get('snippet') or ''}"
        years = [int(y) for y in re.findall(r"\b(20\d{2})\b", blob)]
        if years:
            oldest = min(years)
            try:
                mid = datetime(oldest, 7, 1, tzinfo=timezone.utc)
                age_y = (datetime.now(timezone.utc) - mid).days
                if age_y > max_days:
                    return True
            except ValueError:
                return True
        # Reddit with post id at/above floor and no contrary year → allow
        if rid and not _reddit_id_is_older_than_window(rid):
            return False
        # V27.8: undated social without a recent Reddit id — fail-closed
        # (was fail-open — admitted multi-year threads with empty Serper date)
        return True

    return False  # Non-social, unparseable — fail-open
from services.query_brain import generate_smart_query  # type: ignore[import]
from services.query_brain import _is_consumer_archetype  # type: ignore[import]
from services.query_governance import (  # type: ignore[import]
    govern_query_portfolio,
    filter_queries_against_memory,
    build_exhaustion_escalation_queries,
    query_signature,
)
from services.domain_intelligence import (  # type: ignore[import]
    apply_domain_query_profile,
    build_domain_impact_summary,
    resolve_campaign_domain_profile,
)
from services.serper_service import (  # type: ignore[import]
    search_serper,
    filter_serper_noise,
    extract_root_domain,
    SOCIAL_DOMAINS,
)

# V27 IntentDomainOrchestrator — SSOT is shared.intent_orchestrator (always in image).
# Optional intelligence.* package is a BC re-export only.
_V27_ORCH_AVAILABLE = False
_V27_IMPORT_ERROR: str | None = None
try:
    from shared.intent_orchestrator import (  # type: ignore[import]
        build_intent_profile,
        is_v27_orchestrator_enabled,
        funnel_snapshot,
        merge_intent_into_campaign,
        v27_flag_diagnostics,
        env_v27_flag,
    )
    _V27_ORCH_AVAILABLE = True
except Exception as _v27_imp_err:  # pragma: no cover
    _V27_IMPORT_ERROR = f"shared.intent_orchestrator: {_v27_imp_err}"
    try:
        # Fallback: top-level intelligence package (monorepo / alt Docker layout)
        from intelligence.orchestrator import (  # type: ignore[import]
            build_intent_profile,
            is_v27_orchestrator_enabled,
            funnel_snapshot,
            merge_intent_into_campaign,
            v27_flag_diagnostics,
            env_v27_flag,
        )
        _V27_ORCH_AVAILABLE = True
        _V27_IMPORT_ERROR = None
    except Exception as _v27_imp_err2:  # pragma: no cover
        _V27_IMPORT_ERROR = f"{_V27_IMPORT_ERROR}; intelligence: {_v27_imp_err2}"

        def build_intent_profile(*_a, **_k):  # type: ignore[misc]
            return None

        def is_v27_orchestrator_enabled(*_a, **_k):  # type: ignore[misc]
            return False

        def funnel_snapshot(**kwargs):  # type: ignore[misc]
            return dict(kwargs)

        def merge_intent_into_campaign(campaign, _profile):  # type: ignore[misc]
            return campaign

        def v27_flag_diagnostics(*_a, **_k):  # type: ignore[misc]
            return {"enabled": False, "env_raw": "", "env_enabled": False}

        def env_v27_flag(*_a, **_k):  # type: ignore[misc]
            return False, ""
from services.telemetry import update_circuit_telemetry  # type: ignore[import]
from shared.multi_entity_hosts import resolve_identity_key  # type: ignore[import]

bp  = Blueprint("produce", __name__)
log = get_logger("pipeline.produce")

_SOCIAL_DOMAINS_PRODUCER = SOCIAL_DOMAINS

# ---------------------------------------------------------------------------
# FIX (2026-06-21): System error string ingestion filter.
# Firestore campaign documents occasionally contain error messages, fallback
# sentinels, or log fragments that were accidentally persisted as keyword or
# bio values. When ingested, these produce searches like:
#   "fallback intent processing required" -wiki -jobs ...
# which return zero useful results and waste Serper credits.
# ---------------------------------------------------------------------------
_SYSTEM_JUNK_PATTERNS: frozenset[str] = frozenset({
    "fallback intent processing required",
    "error",
    "exception",
    "traceback",
    "internal server error",
    "timeout",
    "failed to",
    "null",
    "undefined",
    "none",
    "n/a",
    "child_campaign_override",
    "shadow_learner",
    "[shadow_learner",
    "placeholder",
    "test_keyword",
    "sample_data",
})


def _produce_identity_key(
    url: str,
    *,
    sourcing_vector: str,
    social_domains: set | frozenset,
    shared_platforms: set | frozenset,
) -> tuple[str, dict]:
    """Path- or domain-level identity key for produce cache + dedup (V26.7.0).

    Multi-entity portal hosts always use path-level keys regardless of vector.
    """
    domain = extract_root_domain(url)
    is_social = any(domain.endswith(s) for s in social_domains)
    is_shared = any(domain.endswith(s) for s in shared_platforms)
    is_consumer = _is_consumer_archetype(sourcing_vector)
    key, meta = resolve_identity_key(
        url,
        domain,
        is_social=is_social,
        is_shared=is_shared,
        is_consumer=is_consumer,
        include_fragment=False,
    )
    return key, meta


def _is_recent_for_dedup(raw_created_at: object, cutoff: datetime) -> bool:
    if raw_created_at is None:
        return True
    if isinstance(raw_created_at, datetime):
        value = raw_created_at if raw_created_at.tzinfo else raw_created_at.replace(tzinfo=timezone.utc)
        return value >= cutoff
    if isinstance(raw_created_at, str):
        text = raw_created_at.strip()
        if not text:
            return True
        try:
            if text.endswith("Z"):
                parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
            else:
                parsed = datetime.fromisoformat(text)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed >= cutoff
        except ValueError:
            return True
    return True


@bp.route("/produce", methods=["POST"])
@require_tasks_oidc
def produce():
    """V23 Producer — Intent Translation + Serper Execution.

    TRACE log convention (matches Cloud Run log filter):
      ``jsonPayload.message =~ "TRACE-[0-9]+"``
    """
    # ------------------------------------------------------------------
    # TRACE-1: Payload parsing
    # ------------------------------------------------------------------
    log.info("TRACE-1: produce() entered. Parsing payload.", path=request.path)
    lead_data   = request.json or {}
    tenant_id   = lead_data.get("tenant_id")
    campaign_id = lead_data.get("campaign_id")
    log.info("TRACE-2: payload parsed.", tenant_id=tenant_id, campaign_id=campaign_id)

    if not tenant_id or not campaign_id:
        log.critical(
            "produce_missing_ids",
            tenant_id=tenant_id,
            campaign_id=campaign_id,
            note="ABORT: Cloud Task payload must include tenant_id and campaign_id.",
        )
        return jsonify({"error": "Missing campaign_id or tenant_id"}), 400

    # ------------------------------------------------------------------
    # TRACE-3/4: Campaign document fetch
    # ------------------------------------------------------------------
    log.info("TRACE-3: Acquiring Firestore handle (lazy init).")
    campaign_ref = get_db().collection("campaigns").document(campaign_id)
    log.info("TRACE-4: Firestore handle ready. Fetching campaign document.")

    try:
        campaign = campaign_ref.get().to_dict() or {}
    except Exception as exc:
        log.critical(
            "produce_campaign_fetch_failed",
            campaign_id=campaign_id,
            error=str(exc),
            exc_info=True,
        )
        return jsonify({"error": "Firestore error fetching campaign"}), 500

    if campaign.get("tenant_id") != tenant_id:
        log.warning("produce_unauthorized_tenant_context", campaign_id=campaign_id, tenant_id=tenant_id)
        return jsonify({"error": "Unauthorized tenant context"}), 403

    log.info(
        "TRACE-5: Campaign fetched.",
        sourcing_vector=campaign.get("sourcing_vector"),
    )

    sourcing_vector = campaign.get("sourcing_vector", "B2B")
    location        = campaign.get("location", "").strip()
    gl              = campaign.get("gl", "").strip()
    # Domain profile: manual domain_override wins over auto-inference.
    domain_profile, _domain_meta = resolve_campaign_domain_profile(campaign)
    if _domain_meta.get("should_persist"):
        try:
            campaign_ref.update({"system_domain_profile": domain_profile})
        except Exception as _profile_write_err:
            log.warning(
                "produce_domain_profile_write_failed",
                campaign_id=campaign_id,
                error=str(_profile_write_err),
            )
    campaign["system_domain_profile"] = domain_profile
    if _domain_meta.get("override_active"):
        log.info(
            "produce_domain_override_active",
            campaign_id=campaign_id,
            domain_family=domain_profile.get("domain_family"),
            source=_domain_meta.get("source"),
            strictness_bias=domain_profile.get("strictness_bias"),
            note="Manual domain_override is active; auto-inference skipped.",
        )
    elif _domain_meta.get("error"):
        log.warning(
            "produce_domain_override_invalid",
            campaign_id=campaign_id,
            error=_domain_meta.get("error"),
            note="Invalid domain_override ignored; fell back to auto-inference.",
        )
    if domain_profile.get("thin_campaign") or str(
        domain_profile.get("profile_confidence") or ""
    ).lower() == "low":
        log.info(
            "produce_domain_thin_profile",
            campaign_id=campaign_id,
            domain_family=domain_profile.get("domain_family"),
            confidence=domain_profile.get("confidence"),
            profile_confidence=domain_profile.get("profile_confidence"),
            input_richness=domain_profile.get("input_richness"),
            soft_domain_adjustments=bool(domain_profile.get("soft_domain_adjustments")),
            strictness_bias=domain_profile.get("strictness_bias"),
            note="Thin/low-confidence domain profile — milder domain adjustments applied.",
        )
    log.info(
        "produce_domain_profile_loaded",
        campaign_id=campaign_id,
        domain_family=domain_profile.get("domain_family"),
        confidence=domain_profile.get("confidence"),
        profile_confidence=domain_profile.get("profile_confidence"),
        thin_campaign=bool(domain_profile.get("thin_campaign")),
        input_richness=domain_profile.get("input_richness"),
        liquidity_level=domain_profile.get("liquidity_level"),
        low_liquidity=bool(domain_profile.get("low_liquidity_market")),
        strictness_bias=domain_profile.get("strictness_bias"),
        preferred_sources=domain_profile.get("preferred_sources"),
        override_active=bool(_domain_meta.get("override_active")),
        domain_source=_domain_meta.get("source"),
    )

    # ------------------------------------------------------------------
    # V27 IntentDomainOrchestrator — single brain (flag-gated, fail-open)
    # ------------------------------------------------------------------
    _intent_profile = None
    _intent_profile_dict: dict = {}
    _v27_active = False
    try:
        _flag_diag = {}
        try:
            _flag_diag = v27_flag_diagnostics(campaign) if _V27_ORCH_AVAILABLE else {
                "enabled": False,
                "env_raw": (os.environ.get("V27_INTELLIGENCE_ORCHESTRATOR") or ""),
                "env_enabled": str(os.environ.get("V27_INTELLIGENCE_ORCHESTRATOR") or "").strip().lower()
                in ("1", "true", "yes", "on"),
            }
        except Exception:
            _flag_diag = {
                "enabled": False,
                "env_raw": (os.environ.get("V27_INTELLIGENCE_ORCHESTRATOR") or ""),
            }

        if not _V27_ORCH_AVAILABLE:
            log.warning(
                "produce_intent_orchestrator_skipped",
                campaign_id=campaign_id,
                available=False,
                import_error=_V27_IMPORT_ERROR,
                env_raw=_flag_diag.get("env_raw"),
                env_enabled=_flag_diag.get("env_enabled"),
                skip_reason="package_unavailable",
                note="V27 package import failed — env flag cannot activate without shared.intent_orchestrator. "
                     "Check Docker COPY of services/shared.",
            )
        elif is_v27_orchestrator_enabled(campaign=campaign):
            _intent_profile = build_intent_profile(campaign, domain_profile)
            if _intent_profile is not None:
                _intent_profile_dict = (
                    _intent_profile.to_dict()
                    if hasattr(_intent_profile, "to_dict")
                    else dict(_intent_profile)
                )
                _v27_active = bool(_intent_profile_dict.get("orchestrator_active"))
                merge_intent_into_campaign(campaign, _intent_profile)
                # Persist for dispatch / later cycles (additive BC field)
                try:
                    campaign_ref.update({
                        "intent_profile": _intent_profile_dict,
                        "intent_profile_updated_at": firestore.SERVER_TIMESTAMP,
                    })
                except Exception as _ip_write_err:
                    log.warning(
                        "produce_intent_profile_write_failed",
                        campaign_id=campaign_id,
                        error=str(_ip_write_err),
                    )
                log.info(
                    "produce_intent_profile_built",
                    campaign_id=campaign_id,
                    use_case=_intent_profile_dict.get("use_case"),
                    primary_strategy=_intent_profile_dict.get("primary_strategy"),
                    platform_mining_level=_intent_profile_dict.get("platform_mining_level"),
                    buyer_intent=_intent_profile_dict.get("buyer_intent"),
                    nourish_depth=_intent_profile_dict.get("nourish_depth"),
                    force_geo_global_fallback=_intent_profile_dict.get("force_geo_global_fallback"),
                    force_platform_mining=_intent_profile_dict.get("force_platform_mining"),
                    channel_priority=(_intent_profile_dict.get("channel_priority") or [])[:6],
                    decision_reasons=(_intent_profile_dict.get("decision_reasons") or [])[:8],
                    domain_family=_intent_profile_dict.get("domain_family"),
                    env_raw=_flag_diag.get("env_raw"),
                    note="V27 IntentDomainOrchestrator active for this produce cycle.",
                )
            else:
                log.warning(
                    "produce_intent_orchestrator_skipped",
                    campaign_id=campaign_id,
                    available=True,
                    skip_reason="build_returned_none",
                    **{k: _flag_diag.get(k) for k in ("env_raw", "env_enabled", "campaign_flag_source")},
                )
        else:
            log.info(
                "produce_intent_orchestrator_skipped",
                campaign_id=campaign_id,
                available=True,
                skip_reason="flag_disabled",
                env_raw=_flag_diag.get("env_raw"),
                env_enabled=_flag_diag.get("env_enabled"),
                campaign_flag_source=_flag_diag.get("campaign_flag_source"),
                campaign_flag_raw=_flag_diag.get("campaign_flag_raw"),
                note="V27 flag off (env and/or campaign) — legacy domain/strategy path.",
            )
    except Exception as _orch_err:
        log.warning(
            "produce_intent_orchestrator_failed",
            campaign_id=campaign_id,
            error=str(_orch_err),
            available=_V27_ORCH_AVAILABLE,
            import_error=_V27_IMPORT_ERROR,
            note="Fail-open: continuing with legacy produce path.",
        )
        _intent_profile = None
        _intent_profile_dict = {}
        _v27_active = False

    # ------------------------------------------------------------------
    # Persona Vault field extraction (V23 Persona Vault precedence fix)
    # ------------------------------------------------------------------
    _persona_id   = campaign.get("persona_id", "")
    _persona_bio  = campaign.get("persona_bio", "").strip()
    _persona_keys = campaign.get("persona_keywords", "").strip()

    bio = _persona_bio or campaign.get("bio", "")
    if _persona_id and _persona_bio:
        log.info(
            "persona_injected",
            persona_name=campaign.get("persona_name", _persona_id),
            bio_preview=bio[:60],
            campaign_id=campaign_id,
        )

    raw_keywords = _persona_keys or campaign.get("keywords", "")
    if isinstance(raw_keywords, str):
        keywords = [k.strip() for k in raw_keywords.split(",") if k.strip()]
    else:
        keywords = list(raw_keywords) if raw_keywords else []

    # CHILD_CAMPAIGN_OVERRIDE sentinel guard
    if bio == "CHILD_CAMPAIGN_OVERRIDE":
        bio = (
            campaign.get("effective_bio")
            or campaign.get("campaign_focus")
            or ", ".join(keywords)
        )
        log.info("child_campaign_override_resolved", bio_preview=bio[:80])

    # V25.3.1: Preserve raw bio BEFORE enrichment for keyword synthesis.
    # build_enriched_context() adds structural labels ("PRODUCT/SERVICE:",
    # "BUYER TYPE:") that must NOT leak into Serper search queries.
    _raw_bio = (campaign.get("bio") or campaign.get("effective_bio") or
                campaign.get("persona_bio") or campaign.get("name") or "").strip()

    # V24.6.1: Replace thin bio assembly with build_enriched_context().
    # Previously: picked ONE field (persona_bio OR bio) and ignored all others.
    # Now: aggregates ALL 15+ campaign fields (effective_bio, pain_point,
    # target_angle_hook, unfair_advantage, persona_name, geo_hierarchy, etc.)
    # into a structured ICP context. Handles sparse campaigns (user filled only
    # campaign name + location) and rich campaigns (all fields filled) equally.
    # Overrides the above `bio` variable entirely.
    try:
        from services.context_builder import build_enriched_context  # type: ignore[import]
        bio = build_enriched_context(campaign)
    except Exception as _ctx_err:
        log.warning(
            "context_builder_failed",
            campaign_id=campaign_id,
            error=str(_ctx_err),
            note="Falling back to raw bio field. Check context_builder.py.",
        )
        # bio stays as-is from the persona vault logic above

    # ------------------------------------------------------------------
    # FIX (2026-06-21): Bio field sanitizer.
    # Scrub the bio if it contains system error strings or sentinels
    # that should never reach the Gemini prompt (they cause intent
    # hallucination and system-error-string searches).
    # Uses a stricter set than keywords — generic words like "error"
    # could appear legitimately in a campaign bio.
    # ------------------------------------------------------------------
    _BIO_JUNK_PATTERNS: set[str] = {
        "fallback intent processing required",
        "internal server error",
        "traceback",
        "child_campaign_override",
        "shadow_learner",
        "[shadow_learner",
        "test_keyword",
        "sample_data",
        "placeholder bio",
        "undefined",
    }
    if bio and any(junk in bio.lower() for junk in _BIO_JUNK_PATTERNS):
        log.warning(
            "produce_bio_sanitized",
            campaign_id=campaign_id,
            original_bio_preview=bio[:120],
            note="Bio field contains system junk. Cleared to prevent prompt pollution.",
        )
        bio = ""

    # Synthesise keywords from bio if empty
    # V27.1.0: Do NOT word-split bio into weak singles ("customer", "reduce",
    # "Target", "Persona"). That pattern produced cartesian Serper fan-out and
    # literal "Target Persona" searches. Prefer one short phrase instead.
    if not keywords:
        _synth_source = (
            (campaign.get("campaign_focus") or "").strip()
            or (campaign.get("pain_point") or "").strip()
            or _raw_bio
        )
        if _synth_source:
            _stop = {
                "that", "this", "with", "from", "they", "their", "have", "been",
                "will", "about", "what", "when", "which", "your", "the", "and",
                "for", "are", "was", "product", "service", "target", "persona",
            }
            _junk_phrases = {
                "target persona", "general business", "n/a", "placeholder",
                "product/service",
            }
            _text = re.sub(r"\s+", " ", _synth_source).strip()
            if _text.lower().startswith("product/service:"):
                _text = _text.split(":", 1)[1].strip()
            # Question-shaped bios must not become keywords.
            if "?" in _text or _text.lower().startswith(
                ("what ", "how ", "why ", "who ", "when ", "where ")
            ):
                log.warning(
                    "keywords_synth_skipped_question_bio",
                    campaign_id=campaign_id,
                    preview=_text[:80],
                )
            elif _text.lower() in _junk_phrases or "target persona" in _text.lower():
                log.warning(
                    "keywords_synth_skipped_junk_bio",
                    campaign_id=campaign_id,
                    preview=_text[:80],
                )
            else:
                _words = [
                    w for w in re.findall(r"[A-Za-z0-9][A-Za-z0-9\-'/]*", _text)
                    if len(w) > 2 and w.lower() not in _stop
                ][:6]
                if _words:
                    # One phrase keyword, not five singles.
                    keywords = [" ".join(_words)]
                    log.info(
                        "keywords_synthesised_from_bio",
                        count=len(keywords),
                        campaign_id=campaign_id,
                        source="phrase",
                        keyword=keywords[0][:80],
                    )

    # ------------------------------------------------------------------
    # FIX (2026-06-21): Keyword ingestion sanitizer.
    # Drop any keywords that match known system error strings, log
    # fragments, or fallback sentinels before they reach Query Brain.
    # ------------------------------------------------------------------
    _raw_count = len(keywords)
    keywords = [
        kw for kw in keywords
        if kw.strip()
        and len(kw.strip()) > 2
        and not any(junk in kw.lower() for junk in _SYSTEM_JUNK_PATTERNS)
    ]
    _dropped = _raw_count - len(keywords)
    if _dropped > 0:
        log.warning(
            "produce_keywords_sanitized",
            campaign_id=campaign_id,
            dropped=_dropped,
            remaining=len(keywords),
            note="System error strings or sentinel values removed from keywords.",
        )

    if not keywords:
        log.critical(
            "produce_empty_keywords",
            campaign_id=campaign_id,
            persona_id=_persona_id,
            persona_keywords=campaign.get("persona_keywords"),
            keywords=campaign.get("keywords"),
            bio=campaign.get("bio"),
            note="ABORT: No Serper query can be constructed (post-sanitization).",
        )
        return jsonify({
            "error":       "Empty keywords matrix",
            "campaign_id": campaign_id,
            "debug": {
                "persona_id":        _persona_id,
                "persona_keywords":  campaign.get("persona_keywords"),
                "keywords":          campaign.get("keywords"),
                "bio":               campaign.get("bio"),
            },
        }), 400

    # ------------------------------------------------------------------
    # FIX (2026-06-21): Location field validation guard.
    # Reject location values that are obviously not geographic (audience
    # descriptions, error messages, or strings > 100 chars).
    # ------------------------------------------------------------------
    _LOCATION_JUNK_TOKENS = {
        "interested", "customers", "vehicle", "users", "audience",
        "persona", "error", "exception", "fallback", "null",
    }
    if location and (
        len(location) > 100
        or any(tok in location.lower() for tok in _LOCATION_JUNK_TOKENS)
    ):
        log.warning(
            "produce_location_rejected",
            campaign_id=campaign_id,
            original_location=location[:120],
            note="Location field contains non-geographic data. Reset to empty.",
        )
        location = ""

    log.info(
        "TRACE-6: Keywords resolved.",
        keyword_count=len(keywords),
        bio_len=len(bio),
        sourcing_vector=sourcing_vector,
    )

    # Persona negative targeting signals ("NOT <phrase>" → Serper exclusion operators)
    _targeting_signals: list[str] = campaign.get("persona_targeting_signals") or []
    if _targeting_signals:
        neg_count = sum(1 for s in _targeting_signals if s.upper().startswith("NOT "))
        log.info(
            "persona_targeting_signals_loaded",
            total=len(_targeting_signals),
            negative=neg_count,
            campaign_id=campaign_id,
        )

    # ------------------------------------------------------------------
    # TRACE-7: Query Brain (Intent Translation)
    # ------------------------------------------------------------------
    log.info("TRACE-7: Calling generate_smart_query() (Vertex AI).")
    _persona_cat = (
        campaign.get("persona_name") or campaign.get("name") or "general"
    ).strip()

    # V26: Extract intelligence_strategy fields for query_brain
    _intel_strategy = campaign.get("intelligence_strategy") or {}
    _vocab_notes = ""
    if isinstance(_intel_strategy, dict):
        _vocab_notes = (_intel_strategy.get("vocabulary_notes") or "").strip()

    try:
        smart_keywords = generate_smart_query(
            keywords, tenant_id, bio, sourcing_vector,
            persona_category=_persona_cat,
            targeting_signals=_targeting_signals,
            campaign_id=campaign_id,
            force_query_refresh=bool(campaign.get("_force_query_refresh")),
            vocabulary_notes=_vocab_notes,
            intelligence_strategy=_intel_strategy if _intel_strategy else None,
            campaign_name=(campaign.get("name") or ""),
            location=location,
            pain_point=(campaign.get("pain_point") or ""),
            domain_profile=domain_profile if isinstance(domain_profile, dict) else None,
        )
    except Exception as exc:
        log.critical(
            "produce_query_brain_failed",
            campaign_id=campaign_id,
            error=str(exc),
            exc_info=True,
        )
        return jsonify({"error": "Query Brain failed", "details": str(exc)}), 500

    log.info("TRACE-8: generate_smart_query() complete.",
             smart_keyword_count=len(smart_keywords))
    _query_memory_cap = 80
    _prior_query_memory = campaign.get("_query_novelty_memory_signatures") or []
    _prior_query_memory = [str(sig).strip() for sig in _prior_query_memory if str(sig).strip()]
    _executed_query_signatures: list[str] = []

    def _persist_query_memory() -> None:
        if not _executed_query_signatures:
            return
        merged: list[str] = []
        seen: set[str] = set()
        for sig in _executed_query_signatures + _prior_query_memory:
            if not sig or sig in seen:
                continue
            seen.add(sig)
            merged.append(sig)
            if len(merged) >= _query_memory_cap:
                break
        try:
            campaign_ref.update({
                "_query_novelty_memory_signatures": merged,
                "_query_novelty_memory_updated_at": firestore.SERVER_TIMESTAMP,
            })
        except Exception as _memory_exc:
            log.warning(
                "produce_query_memory_update_failed",
                campaign_id=campaign_id,
                error=str(_memory_exc),
            )

    def _run_signal_harvest_pathway(campaign_snapshot: dict) -> dict:
        """Run multi-source signal harvest with a bounded wait for metrics."""
        import os as _os
        import threading as _threading

        harvest_metrics: dict = {}
        _harvest_enabled = _os.environ.get("HARVEST_ENABLED", "true").lower() != "false"
        if not _harvest_enabled:
            return harvest_metrics

        # Produce-gated path: Serper-backed harvest sources are allowed.
        # Contrast with /harvest which always sets allow_serper=False.
        _serper_key_for_harvest = ""
        try:
            from core.clients import get_serper_key  # type: ignore[import]
            _serper_key_for_harvest = get_serper_key() or ""
        except Exception:
            pass  # SerperDiscoverySource will be skipped without a key

        _campaign_with_id = {
            **campaign_snapshot,
            "id": campaign_id,
            "tenant_id": tenant_id,
        }
        harvest_result_holder: list[dict] = []

        def _run_harvest() -> None:
            try:
                from services.signal_harvest import harvest_signals  # type: ignore[import]
                result = harvest_signals(
                    campaign=_campaign_with_id,
                    db=get_db(),
                    serper_api_key=_serper_key_for_harvest,
                    allow_serper=True,
                )
                harvest_result_holder.append(result)
            except Exception as _h_exc:
                log.warning(
                    "signal_harvest_thread_failed",
                    campaign_id=campaign_id,
                    error=str(_h_exc),
                )

        harvest_thread = _threading.Thread(target=_run_harvest, daemon=True)
        harvest_thread.start()
        # 5-minute wall-clock budget: Google Reviews (5 competitors × 10 reviews
        # each) + PRISM enrichment + Gemini inline scoring can exceed 3 minutes.
        # 300s accommodates worst-case Serper + Gemini latency chains.
        harvest_thread.join(timeout=300)

        if harvest_result_holder:
            harvest_metrics = harvest_result_holder[0]
            log.info(
                "signal_harvest_pathway_complete",
                campaign_id=campaign_id,
                **harvest_metrics,
            )
        elif harvest_thread.is_alive():
            log.warning(
                "signal_harvest_thread_timeout",
                campaign_id=campaign_id,
                note="Harvest exceeded 300s wait budget. Continuing without harvest metrics for this response.",
            )

        return harvest_metrics

    # ------------------------------------------------------------------
    # FIX (2026-06-21): Post-generation query sanitizer.
    # Drop any generated Serper queries that contain system error strings
    # or internal pipeline terms that should never reach Serper.
    # ------------------------------------------------------------------
    _pre_sanitize_count = len(smart_keywords)
    smart_keywords = [
        sq for sq in smart_keywords
        if not any(junk in sq.lower() for junk in _SYSTEM_JUNK_PATTERNS)
    ]
    _sq_dropped = _pre_sanitize_count - len(smart_keywords)
    if _sq_dropped > 0:
        log.warning(
            "produce_smart_queries_sanitized",
            campaign_id=campaign_id,
            dropped=_sq_dropped,
            remaining=len(smart_keywords),
            note="System junk detected in generated Serper queries. Dropped.",
        )

    if not smart_keywords:
        harvest_metrics = _run_signal_harvest_pathway(campaign)
        log.warning(
            "produce_all_queries_sanitized_empty",
            campaign_id=campaign_id,
            note="All generated queries were system junk. Running signal_harvest fallback.",
        )
        try:
            from shared.campaign_queue import queue_depth as _qd  # type: ignore[import]
            _qd0 = _qd(get_db(), campaign_id, campaign)
        except Exception:
            _qd0 = len(campaign.get("unprocessed_queue") or [])
        return jsonify({
            "status": "produced",
            "fetched": 0,
            "deduplicated": 0,
            "queued": 0,
            "queue_depth": _qd0,
            "warning": "All queries sanitized as system junk.",
            "harvest": harvest_metrics,
        }), 200

    # Telemetry: bill the expected Serper calls
    try:
        get_db().collection("usage_metrics").document(tenant_id).set(
            {"serper_searches": firestore.Increment(len(smart_keywords))}, merge=True
        )
    except Exception:
        pass  # non-fatal

    # ------------------------------------------------------------------
    # TRACE-9: Serper Execution loop
    # ------------------------------------------------------------------
    raw_urls:   list[str] = []
    snippet_db: dict[str, dict] = {}
    # Observability counters for domain_impact / produce summary (V26.8.1)
    _geo_fallbacks_attempted = 0
    _geo_fallbacks_succeeded = 0
    _platform_queries_executed = 0
    _negative_filters_trimmed = 0
    _funnel_raw_hits = 0
    _funnel_after_noise = 0
    _funnel_after_stale = 0
    _funnel_channel_admitted = 0
    _is_low_liquidity = bool(
        isinstance(domain_profile, dict)
        and (
            domain_profile.get("low_liquidity_market")
            or str(domain_profile.get("liquidity_level") or "").lower() == "low"
        )
    )
    # V27: intent may force low-liquidity / geo fallback behaviour
    if _v27_active and _intent_profile_dict.get("low_liquidity_market"):
        _is_low_liquidity = True
    if _v27_active and _intent_profile_dict.get("force_geo_global_fallback"):
        _is_low_liquidity = True  # force geo fallback path via should_attempt_geo_fallback
    _liquidity_level = (
        str(domain_profile.get("liquidity_level") or "unknown").lower()
        if isinstance(domain_profile, dict)
        else "unknown"
    )
    if _v27_active and _intent_profile_dict.get("liquidity_level"):
        _liquidity_level = str(_intent_profile_dict.get("liquidity_level")).lower()

    # V27.7: High-liquidity country codes must not dual-fire via forced
    # low_liquidity (intent force_geo_global_fallback was burning 2× on IN/US).
    _gl_norm = (gl or "").strip().lower()
    try:
        from shared.serper_enrichment_policy import HIGH_LIQUIDITY_GL  # type: ignore[import]
        _high_gl = _gl_norm in HIGH_LIQUIDITY_GL
    except Exception:
        _high_gl = _gl_norm in {
            "us", "in", "gb", "uk", "ae", "ca", "au", "de", "fr", "sg", "nl", "ie",
        }
    if _high_gl and _is_low_liquidity and _liquidity_level != "low":
        log.info(
            "produce_low_liquidity_cleared_high_gl",
            campaign_id=campaign_id,
            gl=_gl_norm,
            liquidity_level=_liquidity_level,
            note="V27.7: cleared forced low_liquidity for high-liquidity gl — "
                 "stops dual geo+global on empty non-platform queries.",
        )
        _is_low_liquidity = False

    # ------------------------------------------------------------------
    # V26 (Task 2.4) / V27.7: Dedup smart_keywords.
    # Exact lowercase + fingerprint (strip negation tails) so near-duplicates
    # that only differ in -wiki/-site: tails do not burn extra Serper credits.
    # ------------------------------------------------------------------
    try:
        from shared.serper_enrichment_policy import query_dedup_fingerprint  # type: ignore[import]
    except Exception:
        def query_dedup_fingerprint(q: str) -> str:  # type: ignore[misc]
            return (q or "").strip().lower()

    _seen_queries: set[str] = set()
    _seen_fps: set[str] = set()
    _deduped_keywords: list[str] = []
    for _kw in smart_keywords:
        _dedup_key = _kw.strip().lower()
        _fp = query_dedup_fingerprint(_kw)
        if _dedup_key in _seen_queries:
            continue
        if _fp and _fp in _seen_fps:
            continue
        _seen_queries.add(_dedup_key)
        if _fp:
            _seen_fps.add(_fp)
        _deduped_keywords.append(_kw)
    _dedup_dropped = len(smart_keywords) - len(_deduped_keywords)
    if _dedup_dropped > 0:
        log.info(
            "produce_query_dedup",
            campaign_id=campaign_id,
            original_count=len(smart_keywords),
            deduped_count=len(_deduped_keywords),
            dropped=_dedup_dropped,
            note="V27.7 fingerprint+case dedup removed duplicate queries before Serper loop.",
        )
    smart_keywords = _deduped_keywords
    _governed = govern_query_portfolio(
        smart_keywords,
        campaign=campaign,
        sourcing_vector=sourcing_vector,
        location=location,
        domain_profile=domain_profile if isinstance(domain_profile, dict) else None,
        intent_profile=_intent_profile_dict if _v27_active else None,
    )
    smart_keywords = _governed.get("queries", []) or []
    _govern_stats = _governed.get("stats", {}) or {}
    _negative_filters_trimmed = int(
        _govern_stats.get("blacklist_sites_trimmed")
        or _govern_stats.get("negatives_trimmed")
        or 0
    )
    if _negative_filters_trimmed or int(_govern_stats.get("platform_injected") or 0):
        log.info(
            "produce_query_governance_trimmed",
            campaign_id=campaign_id,
            original_count=int(_govern_stats.get("original_count") or 0),
            final_count=int(_govern_stats.get("final_count") or 0),
            dropped_negatives=int(_govern_stats.get("negative_dropped") or 0),
            blacklist_sites_trimmed=_negative_filters_trimmed,
            platform_injected=int(_govern_stats.get("platform_injected") or 0),
            max_site_exclusions=int(_govern_stats.get("max_site_exclusions") or 0),
            reason=_govern_stats.get("trim_reason") or "governance_cap",
            low_liquidity=bool(
                isinstance(domain_profile, dict)
                and (
                    domain_profile.get("low_liquidity_market")
                    or str(domain_profile.get("liquidity_level") or "").lower() == "low"
                )
            ),
        )
    _platform_qs = [
        q for q in smart_keywords
        if re.search(r"(?<!-)site:", q or "")
    ]
    if int(_govern_stats.get("platform_injected") or 0) > 0 or _platform_qs:
        log.info(
            "produce_platform_mining_forced",
            campaign_id=campaign_id,
            platform_count=int(_govern_stats.get("platform_count") or len(_platform_qs)),
            platform_injected=int(_govern_stats.get("platform_injected") or 0),
            primary_strategy=_govern_stats.get("primary_strategy"),
            platform_queries=[q[:80] for q in _platform_qs[:6]],
        )
        log.info(
            "produce_platform_mining_execution_order",
            campaign_id=campaign_id,
            order=[
                ("platform" if re.search(r"(?<!-)site:", q or "") else "other")
                for q in smart_keywords[:12]
            ],
            queries=[q[:70] for q in smart_keywords[:8]],
        )
    log.info(
        "produce_query_governance_applied",
        campaign_id=campaign_id,
        **_govern_stats,
    )
    # Domain portfolio shaping runs AFTER governance and BEFORE Serper /
    # exhaustion escalation so preferred platforms and blocked subreddits
    # win over generic query mix without fighting governance caps.
    _kw_for_domain = ""
    if isinstance(keywords, list):
        _kw_for_domain = ", ".join(str(k) for k in keywords if k)
    else:
        _kw_for_domain = str(
            campaign.get("persona_keywords") or campaign.get("keywords") or ""
        )
    _domain_profiled = apply_domain_query_profile(
        smart_keywords,
        domain_profile if isinstance(domain_profile, dict) else None,
        location=location or "",
        keywords=_kw_for_domain,
    )
    smart_keywords = _domain_profiled.get("queries", []) or []
    _dom_dropped = int(_domain_profiled.get("dropped") or 0)
    _dom_injected = int(_domain_profiled.get("injected") or 0)
    _dom_boosted = int(_domain_profiled.get("boosted") or 0)
    _dom_reordered = bool(_domain_profiled.get("reordered"))
    if _dom_dropped or _dom_injected or _dom_boosted or _dom_reordered:
        log.info(
            "produce_domain_query_profile_applied",
            campaign_id=campaign_id,
            domain_family=_domain_profiled.get("domain_family")
            or (domain_profile.get("domain_family") if isinstance(domain_profile, dict) else None),
            dropped=_dom_dropped,
            injected=_dom_injected,
            boosted=_dom_boosted,
            reordered=_dom_reordered,
            preferred_hints=(
                (domain_profile.get("preferred_query_hints") or [])[:5]
                if isinstance(domain_profile, dict)
                else []
            ),
            preferred_sources=(
                (domain_profile.get("preferred_sources") or [])[:5]
                if isinstance(domain_profile, dict)
                else []
            ),
            remaining=len(smart_keywords),
            note="Domain profile shaped governed queries before Serper execution.",
        )
    else:
        log.info(
            "produce_domain_query_profile_noop",
            campaign_id=campaign_id,
            domain_family=(
                domain_profile.get("domain_family")
                if isinstance(domain_profile, dict)
                else None
            ),
            remaining=len(smart_keywords),
            note="No domain query adjustments needed (or no domain profile signals).",
        )
    _escalation_level = int(campaign.get("_query_exhaustion_escalation_level") or 0)
    if _escalation_level > 0:
        _escalation_queries = build_exhaustion_escalation_queries(
            campaign=campaign,
            location=location,
            level=_escalation_level,
        )
        if _escalation_queries:
            smart_keywords = _escalation_queries + smart_keywords
            log.info(
                "produce_query_exhaustion_escalation_applied",
                campaign_id=campaign_id,
                escalation_level=_escalation_level,
                injected=len(_escalation_queries),
            )

    _memory_filtered = filter_queries_against_memory(
        smart_keywords,
        prior_signatures=_prior_query_memory,
        keep_minimum=2,
    )
    smart_keywords = _memory_filtered.get("queries", []) or []
    if int(_memory_filtered.get("dropped") or 0) > 0:
        log.info(
            "produce_query_memory_filter_applied",
            campaign_id=campaign_id,
            dropped=int(_memory_filtered.get("dropped") or 0),
            kept=int(_memory_filtered.get("kept") or 0),
        )
    # Final front-load of platform site: queries so low-liquidity markets
    # execute Bayut/PropertyFinder/etc. before colloquial noise queries.
    if smart_keywords:
        _pf = [q for q in smart_keywords if re.search(r"(?<!-)site:", q or "")]
        _ot = [q for q in smart_keywords if not re.search(r"(?<!-)site:", q or "")]
        if _pf:
            smart_keywords = _pf + _ot

    if not smart_keywords:
        log.warning(
            "produce_query_governance_empty",
            campaign_id=campaign_id,
            note="Governance removed/trimmed all candidate queries. Triggering harvest fallback.",
        )
        harvest_metrics = _run_signal_harvest_pathway(campaign)
        _empty_domain_impact = build_domain_impact_summary(
            domain_profile if isinstance(domain_profile, dict) else None,
            query_stats={
                "dropped": _dom_dropped,
                "injected": _dom_injected,
                "boosted": _dom_boosted,
                "reordered": _dom_reordered,
                "domain_family": (
                    domain_profile.get("domain_family")
                    if isinstance(domain_profile, dict)
                    else None
                ),
            },
            cycle="produce",
            extra={"fetched": 0, "queued": 0, "query_count": 0, "empty_portfolio": True},
        )
        log.info(
            "produce_domain_impact_summary",
            campaign_id=campaign_id,
            domain_family=_empty_domain_impact.get("domain_family"),
            confidence=_empty_domain_impact.get("confidence"),
            strictness_bias=_empty_domain_impact.get("strictness_bias"),
            queries_dropped=_empty_domain_impact.get("queries_dropped"),
            queries_injected=_empty_domain_impact.get("queries_injected"),
            queries_boosted=_empty_domain_impact.get("queries_boosted"),
            queries_reordered=_empty_domain_impact.get("queries_reordered"),
            note="End-of-produce domain impact (empty query portfolio after governance).",
        )
        try:
            from shared.campaign_queue import queue_depth as _qd  # type: ignore[import]
            _qd1 = _qd(get_db(), campaign_id, campaign)
        except Exception:
            _qd1 = len(campaign.get("unprocessed_queue") or [])
        return jsonify({
            "status": "produced",
            "fetched": 0,
            "deduplicated": 0,
            "queued": 0,
            "queue_depth": _qd1,
            "warning": "No governed queries available.",
            "harvest": harvest_metrics,
            "domain_impact_summary": _empty_domain_impact,
        }), 200

    for kw in smart_keywords:
        clean_location = location if location and location.lower() != "all" else ""
        search_query   = kw

        # F2 (V25.6.1): Query quality gate — reject known garbage patterns
        # before they consume Serper credits. query_brain occasionally generates
        # queries that echo back social URLs, N/A literals, or numbered list
        # fragments from scraped content (e.g. "quora.com 1. Oman Reality user").
        _q_lower = search_query.lower().strip()
        _GARBAGE_PATTERNS = (
            "n/a", "none", "null", "undefined", "unknown",
            "1. ", "2. ", "3. ",  # numbered list fragments from scraped content
        )
        _ECHO_DOMAINS = (
            "quora.com", "reddit.com", "facebook.com", "youtube.com",
            "linkedin.com", "twitter.com", "x.com", "instagram.com",
        )
        _is_garbage = (
            len(_q_lower) < 10
            or _q_lower in _GARBAGE_PATTERNS
            or any(_q_lower.startswith(p) for p in _GARBAGE_PATTERNS)
            # Detect echo queries: "quora.com <scraped title>" or
            # "\"quora.com\" <snippet>" that just re-search the source platform
            or any(
                _q_lower.startswith(f'"{d}"') or _q_lower.startswith(d)
                for d in _ECHO_DOMAINS
            )
        )
        if _is_garbage:
            log.info(
                "produce_query_quality_gate",
                query=search_query[:80],
                campaign_id=campaign_id,
                note="Garbage query blocked before Serper call — saves 1 credit.",
            )
            continue

        _executed_query_signatures.append(query_signature(search_query))

        # V25.3.0 / V26.8.1: Split Serper strategy by sourcing vector + liquidity.
        # Consumer archetypes use geo-restricted indexes first (local ranking).
        # B2B defaults to global-only (geo terms already in query text).
        # Low-liquidity markets (e.g. gl=om) force one global fallback when
        # geo returns 0 — even for non-platform colloquial queries — so sparse
        # markets are not starved by the high-liquidity credit-protection skip.
        _is_consumer_vector = _is_consumer_archetype(sourcing_vector)
        import re as _re_produce
        _query_body = _re_produce.split(
            r'\s+-(?:site:|wiki\b|jobs\b|careers\b|investors\b|directory\b|listicle\b|")',
            search_query,
            maxsplit=1,
        )[0].strip()
        _is_platform_query = bool(_re_produce.search(r'(?<!\-)site:', _query_body))
        if _is_platform_query:
            _platform_queries_executed += 1

        if _is_consumer_vector:
            # V27.7: site: platform queries (reddit/quora/linkedin/…) always global.
            # Geo-restricted social indexes almost always return 0, then dual-fired
            # global — burning 2 credits per platform query. Global-only = 1 credit.
            if _is_platform_query:
                log.info(
                    "produce_platform_global_only",
                    query=search_query[:80],
                    campaign_id=campaign_id,
                    gl=gl or None,
                    note="V27.7: platform site: query skips geo — single global Serper call.",
                )
                raw_results = search_serper(
                    search_query,
                    location=None,
                    gl=None,
                    campaign_id=campaign_id,
                    tenant_id=tenant_id,
                    sourcing_vector=sourcing_vector,
                )
            else:
                # Consumer colloquial: geo-restricted first, then conditional global
                raw_results = search_serper(
                    search_query,
                    location=clean_location or None,
                    gl=gl or None,
                    campaign_id=campaign_id,
                    tenant_id=tenant_id,
                    sourcing_vector=sourcing_vector,
                )
                if not raw_results and gl:
                    _do_fallback, _fb_reason = should_attempt_geo_fallback(
                        gl=gl,
                        has_results=False,
                        is_platform_query=False,
                        low_liquidity=_is_low_liquidity,
                    )
                    if _do_fallback:
                        _geo_fallbacks_attempted += 1
                        log.info(
                            "produce_geo_fallback_low_liquidity"
                            if _fb_reason == "low_liquidity"
                            else "produce_geo_fallback",
                            query=search_query[:80],
                            original_gl=gl,
                            sourcing_vector=sourcing_vector,
                            is_platform_query=False,
                            low_liquidity=_is_low_liquidity,
                            liquidity_level=_liquidity_level,
                            fallback_reason=_fb_reason,
                            campaign_id=campaign_id,
                            note=(
                                "Geo returned 0; retrying once on global index."
                                + (
                                    " Forced for low-liquidity market."
                                    if _fb_reason == "low_liquidity"
                                    else ""
                                )
                            ),
                        )
                        raw_results = search_serper(
                            search_query,
                            location=None,
                            gl=None,
                            campaign_id=campaign_id,
                            tenant_id=tenant_id,
                            sourcing_vector=sourcing_vector,
                        )
                        if raw_results:
                            _geo_fallbacks_succeeded += 1
                    else:
                        log.info(
                            "produce_geo_fallback_skipped",
                            query=search_query[:80],
                            original_gl=gl,
                            sourcing_vector=sourcing_vector,
                            low_liquidity=_is_low_liquidity,
                            liquidity_level=_liquidity_level,
                            fallback_reason=_fb_reason,
                            campaign_id=campaign_id,
                            note="Non-platform query returned 0 on geo in high/medium "
                                 "liquidity market — skipping global retry to save credits.",
                        )
        else:
            # B2B: global-only (geo terms already in query text from query_brain).
            raw_results = search_serper(
                search_query,
                location=None,
                gl=None,
                campaign_id=campaign_id,
                tenant_id=tenant_id,
                sourcing_vector=sourcing_vector,
            )

        update_circuit_telemetry("serper_call")

        _raw_count = len(raw_results) if raw_results else 0
        _funnel_raw_hits += _raw_count
        filtered = filter_serper_noise(
            raw_results,
            intent_profile=_intent_profile_dict if _v27_active else None,
        )
        _filtered_count = len(filtered)
        _funnel_after_noise += _filtered_count
        _new_count = 0
        _rejected_stale = 0
        for r in filtered:
            link = r.get("link")
            if not link or link in raw_urls:
                continue
            # V25.5.0: Content age filter — reject stale Reddit/forum posts
            if _is_stale_content(r, _is_consumer_vector):
                _rejected_stale += 1
                continue
            raw_urls.append(link)
            _new_count += 1
            _funnel_after_stale += 1
            snippet_db[link] = {
                "title":   r.get("title", ""),
                "snippet": r.get("snippet", ""),
                "query":   search_query,
            }
        log.info("produce_serper_query_result",
                 query=search_query[:120],
                 campaign_id=campaign_id,
                 raw=_raw_count,
                 after_noise_filter=_filtered_count,
                 rejected_stale=_rejected_stale,
                 new_urls=_new_count,
                 is_platform_query=_is_platform_query,
                 v27_orchestrator=_v27_active,
                 cumulative=len(raw_urls))

    fetched_count = len(raw_urls)
    log.info(
        "TRACE-10: Serper loop complete.",
        fetched_count=fetched_count,
        geo_fallbacks_attempted=_geo_fallbacks_attempted,
        geo_fallbacks_succeeded=_geo_fallbacks_succeeded,
        platform_queries_executed=_platform_queries_executed,
        negative_filters_trimmed=_negative_filters_trimmed,
    )

    # ------------------------------------------------------------------
    # Snippet cache: persist snippets universally for two-stage funnel
    # ------------------------------------------------------------------
    # V24.5.4 FIX: Added buyer-forum platforms to shared_platforms.
    # Without this, B2B campaigns deduplicate reddit.com to ONE slot — meaning
    # 19 out of 20 Reddit buyer pain posts are silently dropped as domain-level
    # duplicates. Each Reddit/Quora/HN thread is a UNIQUE lead, not a domain.
    shared_platforms = {
        "linkedin.com", "medium.com", "substack.com", "wordpress.com", "github.io",
        # Buyer forum platforms — each thread/post is a unique lead (URL-path dedup)
        "reddit.com", "quora.com", "stackexchange.com", "stackoverflow.com",
        "news.ycombinator.com",   # Hacker News
        "community.hubspot.com", "community.g2.com",  # vendor community boards
        "forum.growthackers.com", "indiehackers.com",
    }
    _multi_entity_cache_hits = 0
    for surl, meta in snippet_db.items():
        # V26.7.0: align cache key with dispatch lead/lock identity (incl. multi-entity portals)
        dedup_key, _id_meta = _produce_identity_key(
            surl,
            sourcing_vector=sourcing_vector,
            social_domains=_SOCIAL_DOMAINS_PRODUCER,
            shared_platforms=shared_platforms,
        )
        if _id_meta.get("multi_entity_host"):
            _multi_entity_cache_hits += 1

        cache_key = hashlib.sha256(f"{tenant_id}_{dedup_key}".encode()).hexdigest()
        combined  = f"Query: {meta.get('query', '')}\nTitle: {meta['title']}\nSnippet: {meta['snippet']}".strip()
        if combined:
            try:
                get_db().collection("scraped_cache").document(cache_key).set({
                    "url":        surl,
                    "text":       combined,
                    "source":     "serper_snippet",
                    "tech_stack": [],
                    "emails":     [],
                    "phones":     [],
                    "cached_at":  firestore.SERVER_TIMESTAMP,
                }, merge=True)
            except Exception as exc:
                log.warning("snippet_persist_failed", url=surl, error=str(exc))
    if _multi_entity_cache_hits:
        log.info(
            "produce_multi_entity_path_identity",
            campaign_id=campaign_id,
            count=_multi_entity_cache_hits,
            context="snippet_cache",
            note="Path-level cache keys forced for multi-entity portal hosts.",
        )

    # ------------------------------------------------------------------
    # Social-aware global deduplication
    # ------------------------------------------------------------------
    existing_ids: set[str] = set()
    known_docs: list = []
    _dedup_recrawl_days = max(1, min(120, int(os.environ.get("DEDUP_RECRAWL_DAYS", "30"))))
    _dedup_cutoff = datetime.now(timezone.utc) - timedelta(days=_dedup_recrawl_days)
    # defaults if import fails inside try
    def is_terminal_non_lead(s):  # type: ignore
        return str(s or "").lower() in {
            "scored_out", "rlhf_filtered", "failed", "failed_scrape",
            "failed_eval", "failed_vertex_timeout", "duplicate",
        }
    def resolve_lead_url(d):  # type: ignore
        return str((d or {}).get("source_url") or (d or {}).get("url") or "")
    try:
        # V27.2.0 scale: paginated dedup scan (was hard 500 — re-queue risk at 1k+ users).
        # Pages of DEDUP_SCAN_PAGE_SIZE up to DEDUP_SCAN_LIMIT; reads url + source_url.
        try:
            from shared.scale_limits import (  # type: ignore[import]
                DEDUP_SCAN_LIMIT as _DEDUP_SCAN_LIMIT,
                DEDUP_SCAN_PAGE_SIZE as _DEDUP_PAGE,
            )
        except Exception:
            _DEDUP_SCAN_LIMIT = 2500
            _DEDUP_PAGE = 500
        try:
            from shared.lead_identity import (  # type: ignore[import]
                is_terminal_non_lead as _itnl,
                resolve_lead_url as _rlu,
            )
            is_terminal_non_lead = _itnl  # type: ignore
            resolve_lead_url = _rlu  # type: ignore
        except Exception:
            pass

        known_docs = []
        _q = (
            get_db().collection("leads")
            .where(filter=FieldFilter("tenant_id", "==", tenant_id))
            .select(["url", "source_url", "createdAt", "status"])
            .order_by("createdAt", direction=firestore.Query.DESCENDING)
            .limit(_DEDUP_PAGE)
        )
        _pages = 0
        _max_pages = max(1, (_DEDUP_SCAN_LIMIT + _DEDUP_PAGE - 1) // _DEDUP_PAGE)
        try:
            while _pages < _max_pages and len(known_docs) < _DEDUP_SCAN_LIMIT:
                batch = list(_q.stream())
                if not batch:
                    break
                known_docs.extend(batch)
                _pages += 1
                if len(batch) < _DEDUP_PAGE:
                    break
                # Cursor pagination
                _q = (
                    get_db().collection("leads")
                    .where(filter=FieldFilter("tenant_id", "==", tenant_id))
                    .select(["url", "source_url", "createdAt", "status"])
                    .order_by("createdAt", direction=firestore.Query.DESCENDING)
                    .start_after(batch[-1])
                    .limit(_DEDUP_PAGE)
                )
        except Exception as _page_err:
            # Fallback: unordered limit if composite index missing
            log.warning(
                "produce_dedup_pagination_fallback",
                error=str(_page_err),
                note="Using unordered limit scan — deploy firestore index tenant_id+createdAt DESC.",
            )
            known_docs = list(
                get_db().collection("leads")
                .where(filter=FieldFilter("tenant_id", "==", tenant_id))
                .select(["url", "source_url", "createdAt", "status"])
                .limit(_DEDUP_SCAN_LIMIT)
                .stream()
            )
        if len(known_docs) >= _DEDUP_SCAN_LIMIT:
            log.warning(
                "produce_dedup_scan_cap_hit",
                tenant_id=tenant_id,
                limit=_DEDUP_SCAN_LIMIT,
                note="Dedup scan hit scale cap. Increase DEDUP_SCAN_LIMIT or archive old leads.",
            )
        for doc in known_docs:
            lead_data = doc.to_dict() or {}
            u = resolve_lead_url(lead_data)
            _created_at = lead_data.get("createdAt")
            _status = str(lead_data.get("status") or "").strip().lower()
            if u:
                if not _is_recent_for_dedup(_created_at, _dedup_cutoff):
                    continue
                if is_terminal_non_lead(_status):
                    continue
                # V26.7.0: multi-entity portals always path-keyed (even for B2B
                # producers) so portal inventory is not domain-collapsed.
                dedup_key, _exist_meta = _produce_identity_key(
                    u,
                    sourcing_vector=sourcing_vector,
                    social_domains=_SOCIAL_DOMAINS_PRODUCER,
                    shared_platforms=shared_platforms,
                )
                existing_ids.add(
                    hashlib.sha256(f"{tenant_id}_{dedup_key}".encode()).hexdigest()
                )
                existing_ids.add(u)
    except Exception as exc:
        log.warning("produce_dedup_query_failed", error=str(exc))

    # V27.9: Also track existing root domains so same portal ≠ many page URLs.
    # Historical leads may only have url/source_url (no domain field).
    existing_domains: set[str] = set()
    try:
        for doc in known_docs:
            lead_data = doc.to_dict() or {}
            _st = str(lead_data.get("status") or "").strip().lower()
            if is_terminal_non_lead(_st):
                continue
            _u = resolve_lead_url(lead_data)
            if not _u:
                continue
            if not _is_recent_for_dedup(lead_data.get("createdAt"), _dedup_cutoff):
                continue
            _d = extract_root_domain(_u)
            if _d:
                existing_domains.add(_d.lower().replace("www.", ""))
    except Exception as _dom_err:
        log.warning("produce_domain_set_build_failed", error=str(_dom_err))

    try:
        from shared.multi_entity_hosts import (  # type: ignore[import]
            is_junk_portal_path,
            is_multi_entity_host,
        )
    except Exception:
        def is_junk_portal_path(_u):  # type: ignore
            return False
        def is_multi_entity_host(_d):  # type: ignore
            return False

    fresh_urls: list[str] = []
    _multi_entity_fresh = 0
    _batch_domains: set[str] = set()
    _skipped_junk = 0
    _skipped_domain_dup = 0
    for url in raw_urls:
        _root = (extract_root_domain(url) or "").lower().replace("www.", "")
        _is_social_u = any(_root.endswith(s) for s in _SOCIAL_DOMAINS_PRODUCER) if _root else False
        _is_shared_u = any(_root.endswith(s) for s in shared_platforms) if _root else False
        _is_multi_u = bool(_root and is_multi_entity_host(_root))

        # Junk portal paths (privacy/login/tag) — never queue
        if _root and not _is_social_u and not _is_shared_u and is_junk_portal_path(url):
            _skipped_junk += 1
            continue

        dedup_key, _fresh_meta = _produce_identity_key(
            url,
            sourcing_vector=sourcing_vector,
            social_domains=_SOCIAL_DOMAINS_PRODUCER,
            shared_platforms=shared_platforms,
        )
        if _fresh_meta.get("multi_entity_host"):
            _multi_entity_fresh += 1
        lead_hash = hashlib.sha256(f"{tenant_id}_{dedup_key}".encode()).hexdigest()
        if lead_hash in existing_ids or url in existing_ids:
            continue

        # V27.9: Same-domain collapse for single-entity company/portal sites.
        # Different pages of one site (About / Team / Portfolio) are one lead.
        # Social + multi-entity portals keep path-level uniqueness.
        if (
            _root
            and not _is_social_u
            and not _is_shared_u
            and not _is_multi_u
            and (_root in existing_domains or _root in _batch_domains)
        ):
            _skipped_domain_dup += 1
            log.info(
                "produce_skip_dup_domain",
                domain=_root,
                url=url[:100],
                campaign_id=campaign_id,
                note="V27.9: same root domain already queued/known — skip sibling page.",
            )
            continue

        fresh_urls.append(url)
        existing_ids.add(lead_hash)
        existing_ids.add(url)
        if _root and not _is_social_u and not _is_shared_u and not _is_multi_u:
            _batch_domains.add(_root)

    duped_count  = fetched_count - len(fresh_urls)
    queued_count = len(fresh_urls)
    if _multi_entity_fresh:
        log.info(
            "produce_multi_entity_path_identity",
            campaign_id=campaign_id,
            count=_multi_entity_fresh,
            context="fresh_dedup",
            note="Path-level dedup forced for multi-entity portal hosts (vector-independent).",
        )
    log.info(
        "produce_dedup_complete",
        campaign_id=campaign_id,
        fetched=fetched_count,
        deduplicated=duped_count,
        queued=queued_count,
        multi_entity_urls=_multi_entity_fresh,
        skipped_junk_path=_skipped_junk,
        skipped_domain_dup=_skipped_domain_dup,
    )

    # ------------------------------------------------------------------
    # V25.5.0: Query exhaustion detection
    # If 0 new URLs after dedup for 3+ consecutive cycles, the market is
    # saturated or queries are stale. Log a warning and set a flag for
    # query_brain to generate fresh query angles next cycle.
    # ------------------------------------------------------------------
    _exhaustion_counter_field = "_query_exhaustion_consecutive_zeros"
    _exhaustion_level_field = "_query_exhaustion_escalation_level"
    if queued_count == 0:
        _prev_zeros = campaign.get(_exhaustion_counter_field, 0)
        _new_zeros = _prev_zeros + 1
        _prev_level = int(campaign.get(_exhaustion_level_field) or 0)
        _next_level = _prev_level
        _update_payload: dict[str, object] = {_exhaustion_counter_field: _new_zeros}
        if _new_zeros >= 2:
            _next_level = min(_prev_level + 1, 3)
            _update_payload[_exhaustion_level_field] = _next_level
            _update_payload["_force_query_refresh"] = True
        try:
            campaign_ref.update(_update_payload)
        except Exception:
            pass  # non-fatal metadata
        if _new_zeros >= 3:
            log.warning(
                "produce_query_exhaustion_detected",
                campaign_id=campaign_id,
                consecutive_zero_cycles=_new_zeros,
                note="Market may be saturated or queries are stale. "
                     "query_brain should generate fresh query angles.",
            )
            log.warning(
                "produce_query_exhaustion_escalation",
                campaign_id=campaign_id,
                consecutive_zero_cycles=_new_zeros,
                escalation_level=_next_level,
            )
    else:
        # Reset counter on successful produce
        if campaign.get(_exhaustion_counter_field, 0) > 0 or int(campaign.get(_exhaustion_level_field) or 0) > 0:
            try:
                campaign_ref.update({
                    _exhaustion_counter_field: 0,
                    _exhaustion_level_field: 0,
                    "_force_query_refresh": firestore.DELETE_FIELD,
                })
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Write to unprocessed_queue (atomic ArrayUnion, cap at 200)
    # RACE-01/02 FIX: Use firestore.ArrayUnion for atomic, race-safe
    # append instead of destructive overwrite that loses concurrent writes.
    # ------------------------------------------------------------------
    # V27.4.0: dual-path queue SSOT (array + queue_items)
    try:
        from shared.campaign_queue import (  # type: ignore[import]
            append_urls as _queue_append,
            load_queued_urls as _load_q,
            queue_depth as _q_depth_fn,
        )
        current_queue = _load_q(get_db(), campaign_id, campaign, log=log)
        _queue_depth = _q_depth_fn(get_db(), campaign_id, campaign, log=log)
    except Exception as _qload_err:
        log.warning("produce_queue_load_fallback", error=str(_qload_err))
        current_queue = list(campaign.get("unprocessed_queue") or [])
        _queue_depth = len(current_queue)
        _queue_append = None  # type: ignore

    if _queue_depth > 150:
        _persist_query_memory()
        log.info(
            "produce_skipped_queue_full",
            campaign_id=campaign_id,
            queue_depth=_queue_depth,
            threshold=150,
            note="Queue saturated. Skipping produce run — consumer must drain queue first.",
        )
        return jsonify({"status": "skipped_queue_full", "queue_depth": _queue_depth}), 200

    _remaining_capacity = max(200 - _queue_depth, 0)
    _capped_fresh = fresh_urls[:_remaining_capacity] if fresh_urls else []

    if not _capped_fresh:
        _persist_query_memory()
        log.info(
            "produce_no_fresh_after_cap",
            campaign_id=campaign_id,
            queue_depth=_queue_depth,
            fresh_count=len(fresh_urls),
            note="No fresh URLs fit within 200-URL cap.",
        )
        return jsonify({"status": "skipped_no_fresh", "queue_depth": _queue_depth}), 200

    _append_res: dict = {}
    try:
        if _queue_append is not None:
            _append_res = _queue_append(
                get_db(),
                campaign_ref,
                campaign_id,
                _capped_fresh,
                source="produce",
                campaign_doc=campaign,
                log=log,
            ) or {}
            if _append_res.get("skipped") and not _append_res.get("appended"):
                _persist_query_memory()
                return jsonify({
                    "status": "skipped_queue_full",
                    "queue_depth": _append_res.get("depth_before", _queue_depth),
                    "reason": _append_res.get("skipped"),
                }), 200
            queued_count = int(_append_res.get("appended") or 0)
            _capped_fresh = _capped_fresh[:queued_count] if queued_count else []
        else:
            campaign_ref.update({
                "unprocessed_queue": firestore.ArrayUnion(_capped_fresh),
            })
            queued_count = len(_capped_fresh)

        update_meta = {"last_produced_at": firestore.SERVER_TIMESTAMP}
        if _capped_fresh:
            update_meta["next_drip_due"] = datetime.now(timezone.utc).isoformat()
        campaign_ref.update(update_meta)

        log.info(
            "produce_queue_size_telemetry",
            campaign_id=campaign_id,
            queue_depth=_queue_depth + len(_capped_fresh),
            appended=len(_capped_fresh),
            mode=_append_res.get("mode") or "legacy",
        )
    except Exception as exc:
        log.critical(
            "produce_queue_write_failed",
            campaign_id=campaign_id,
            error=str(exc),
            exc_info=True,
        )
        return jsonify({"error": "Queue write failed", "details": str(exc)}), 500

    _persist_query_memory()
    combined_queue = list(current_queue) + list(_capped_fresh)

    # ------------------------------------------------------------------
    # V25.1.0: Signal Harvest — multi-source intent discovery pathway.
    # Runs after Serper queue write so it cannot block query production.
    # ------------------------------------------------------------------
    harvest_metrics = _run_signal_harvest_pathway(campaign)

    # Domain impact summary for this produce cycle (query shaping focus).
    _produce_domain_impact = build_domain_impact_summary(
        domain_profile if isinstance(domain_profile, dict) else None,
        query_stats={
            "dropped": _dom_dropped,
            "injected": _dom_injected,
            "boosted": _dom_boosted,
            "reordered": _dom_reordered,
            "domain_family": (
                domain_profile.get("domain_family")
                if isinstance(domain_profile, dict)
                else None
            ),
        },
        cycle="produce",
        extra={
            "fetched": fetched_count,
            "deduplicated": duped_count,
            "queued": len(_capped_fresh),
            "query_count": len(smart_keywords) if isinstance(smart_keywords, list) else 0,
            "geo_fallbacks_attempted": _geo_fallbacks_attempted,
            "geo_fallbacks_succeeded": _geo_fallbacks_succeeded,
            "negative_filters_trimmed": _negative_filters_trimmed,
            "platform_queries_executed": _platform_queries_executed,
            "low_liquidity_market": _is_low_liquidity,
        },
    )
    log.info(
        "produce_domain_impact_summary",
        campaign_id=campaign_id,
        domain_family=_produce_domain_impact.get("domain_family"),
        confidence=_produce_domain_impact.get("confidence"),
        strictness_bias=_produce_domain_impact.get("strictness_bias"),
        queries_dropped=_produce_domain_impact.get("queries_dropped"),
        queries_injected=_produce_domain_impact.get("queries_injected"),
        queries_boosted=_produce_domain_impact.get("queries_boosted"),
        queries_reordered=_produce_domain_impact.get("queries_reordered"),
        liquidity_level=_produce_domain_impact.get("liquidity_level"),
        fetched=fetched_count,
        queued=len(_capped_fresh),
        geo_fallbacks_attempted=_geo_fallbacks_attempted,
        geo_fallbacks_succeeded=_geo_fallbacks_succeeded,
        negative_filters_trimmed=_negative_filters_trimmed,
        platform_queries_executed=_platform_queries_executed,
        note="End-of-produce domain intelligence impact for this campaign run.",
    )

    # V27 funnel telemetry — additive campaign field for observability
    _cycle_funnel = {}
    try:
        _cycle_funnel = funnel_snapshot(
            intent_profile=_intent_profile_dict if _v27_active else None,
            queries_executed=len(smart_keywords) if isinstance(smart_keywords, list) else 0,
            raw_hits=_funnel_raw_hits,
            after_noise=_funnel_after_noise,
            after_stale=_funnel_after_stale,
            queued=len(_capped_fresh),
            geo_fallbacks_attempted=_geo_fallbacks_attempted,
            geo_fallbacks_succeeded=_geo_fallbacks_succeeded,
            platform_queries_executed=_platform_queries_executed,
            noise_dropped=max(0, _funnel_raw_hits - _funnel_after_noise),
            channel_admitted=_funnel_channel_admitted,
            extra={
                "deduplicated": duped_count,
                "use_case": (_intent_profile_dict or {}).get("use_case"),
            },
        )
        campaign_ref.update({"last_cycle_funnel": _cycle_funnel})
        log.info(
            "produce_funnel_telemetry",
            campaign_id=campaign_id,
            **{k: v for k, v in _cycle_funnel.items() if k != "recorded_at"},
        )
    except Exception as _funnel_err:
        log.warning(
            "produce_funnel_telemetry_failed",
            campaign_id=campaign_id,
            error=str(_funnel_err),
            note="Fail-open: funnel write skipped.",
        )

    log.info("TRACE-DONE: produce() complete.",
             campaign_id=campaign_id, queue_depth=len(_capped_fresh))

    return jsonify({
        "status":        "produced",
        "fetched":       fetched_count,
        "deduplicated":  duped_count,
        "queued":        len(_capped_fresh),
        "queue_depth":   len(combined_queue),
        # V25.1.0: Signal harvest pathway metrics
        "harvest": harvest_metrics,
        "domain_impact_summary": _produce_domain_impact,
        "geo_fallbacks_attempted": _geo_fallbacks_attempted,
        "geo_fallbacks_succeeded": _geo_fallbacks_succeeded,
        "negative_filters_trimmed": _negative_filters_trimmed,
        "platform_queries_executed": _platform_queries_executed,
        "intent_profile": {
            "use_case": (_intent_profile_dict or {}).get("use_case"),
            "primary_strategy": (_intent_profile_dict or {}).get("primary_strategy"),
            "platform_mining_level": (_intent_profile_dict or {}).get("platform_mining_level"),
            "orchestrator_active": _v27_active,
        } if _v27_active else None,
        "last_cycle_funnel": _cycle_funnel or None,
    }), 200
