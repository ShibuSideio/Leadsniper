"""
Pipeline-main — Gemini/Vertex AI service.

Extracted from ``main.py`` with V23 safety amendments:
  - ``init_vertex()`` is called lazily via ``core.clients`` — never at module scope.
  - ``call_gemini_2_5()`` uses a ``concurrent.futures.ThreadPoolExecutor`` to
    enforce the 45-second wall-clock ceiling (unchanged from monolith).
  - All callers block synchronously — no fire-and-forget threads.
"""
from __future__ import annotations

import json
import os
import concurrent.futures
from typing import Any, Optional

from tenacity import (
    retry, wait_exponential, stop_after_attempt, retry_if_exception_type,
)

from core.logging import get_logger  # type: ignore[import]
from core.clients import init_vertex  # type: ignore[import]
from core.config import GEMINI_TIMEOUT_S  # type: ignore[import]

log = get_logger("pipeline.gemini")


# ---------------------------------------------------------------------------
# Core Gemini caller
# ---------------------------------------------------------------------------

_GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")
_FALLBACK_MODEL = "gemini-2.0-flash"


def call_gemini_2_5(
    prompt: str,
    expect_json: bool = True,
    response_schema: Optional[dict] = None,
    system_instruction: Optional[str] = None,
) -> Any:
    """Invoke Gemini 2.5 Flash with a wall-clock timeout and tenacity retry.

    Initialises Vertex AI lazily on first call (thread-safe, no import-time gRPC).

    Args:
        prompt:             Full prompt string.
        expect_json:        If True, sets ``response_mime_type="application/json"``.
        response_schema:    Gemini JSON schema dict (optional).
        system_instruction: Gemini system instruction string (optional).

    Returns:
        Parsed dict or list if ``expect_json`` is True, otherwise raw text string.

    Raises:
        TimeoutError: If Vertex AI exceeds the 45-second wall-clock ceiling.
        Exception:    Propagates on all-retries-exhausted quota errors.
    """
    # Lazy init — safe under Gunicorn pre-fork (threading.Lock in clients.py)
    init_vertex()

    from vertexai.generative_models import GenerativeModel, GenerationConfig  # type: ignore[import]
    from google.api_core.exceptions import (  # type: ignore[import]
        ResourceExhausted,
        ServiceUnavailable,
        DeadlineExceeded,
        NotFound,
    )

    def _build_and_invoke(model_name: str):
        """Build a GenerativeModel for *model_name* and invoke it with retries."""
        _model = GenerativeModel(
            model_name,
            system_instruction=system_instruction,
        )
        _config = (
            GenerationConfig(
                response_mime_type="application/json",
                response_schema=response_schema,
            )
            if expect_json
            else None
        )

        @retry(
            wait=wait_exponential(multiplier=1, min=2, max=10),
            stop=stop_after_attempt(5),
            retry=retry_if_exception_type(
                (ResourceExhausted, ServiceUnavailable, DeadlineExceeded, ConnectionError)
            ),
        )
        def _invoke():
            return _model.generate_content(prompt, generation_config=_config)

        return _invoke()

    def _safe_extract(response):
        """P0-5: Guard response.text — return safe fallback on empty candidates."""
        if not response.candidates:
            log.warning(
                "gemini_empty_candidates",
                note="Gemini returned no candidates (possible safety block).",
            )
            if expect_json:
                return {}
            return ""
        if expect_json:
            return json.loads(response.text)
        return response.text

    # P2-EXT-2: Model fallback chain — primary → fallback on model-specific errors
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future   = executor.submit(_build_and_invoke, _GEMINI_MODEL)
            response = future.result(timeout=GEMINI_TIMEOUT_S)
        return _safe_extract(response)
    except concurrent.futures.TimeoutError:
        log.error("gemini_timeout", timeout_s=GEMINI_TIMEOUT_S)
        raise TimeoutError("Vertex AI timeout")
    except (NotFound, ResourceExhausted) as fallback_exc:
        if _GEMINI_MODEL == _FALLBACK_MODEL:
            raise  # Already on fallback; don't loop
        log.warning(
            "gemini_model_fallback",
            primary_model=_GEMINI_MODEL,
            fallback_model=_FALLBACK_MODEL,
            error=str(fallback_exc),
        )
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future   = executor.submit(_build_and_invoke, _FALLBACK_MODEL)
                response = future.result(timeout=GEMINI_TIMEOUT_S)
            return _safe_extract(response)
        except concurrent.futures.TimeoutError:
            log.error("gemini_fallback_timeout", timeout_s=GEMINI_TIMEOUT_S)
            raise TimeoutError("Vertex AI fallback timeout")


# ---------------------------------------------------------------------------
# Topic coherence gate (Phase 1B)
# ---------------------------------------------------------------------------


def check_topic_coherence(
    title: str,
    snippet: str,
    campaign_topic: str,
) -> bool:
    """Cheap Gemini Flash gate: is this content *primarily* about the campaign topic?

    Uses temperature=0.1 for near-deterministic YES/NO classification.
    Fail-open on any exception (returns True) so the pipeline never blocks
    on a transient Gemini error.

    Cost: ~$0.0001 per call (minimal prompt, no schema enforcement).

    Args:
        title:          Content title (headline, post title, etc.).
        snippet:        First ~300 chars of the content body.
        campaign_topic: The campaign's core topic string.

    Returns:
        True if the content is primarily about the campaign topic (or on
        any error), False if the content merely mentions it in passing.
    """
    if not campaign_topic or not campaign_topic.strip():
        log.debug("topic_coherence_skip", reason="empty_campaign_topic")
        return True

    coherence_prompt = (
        f"Is this content PRIMARILY about {campaign_topic}? "
        f"Title: {title}. "
        f"Snippet: {snippet[:300]}. "
        "Answer only YES or NO."
    )

    try:
        # Lazy init — safe under Gunicorn pre-fork
        init_vertex()

        from vertexai.generative_models import (  # type: ignore[import]
            GenerativeModel,
            GenerationConfig,
        )

        model = GenerativeModel(_GEMINI_MODEL)
        config = GenerationConfig(
            temperature=0.1,
            max_output_tokens=8,
        )
        response = model.generate_content(coherence_prompt, generation_config=config)
        # P0-5: Guard response.text access — fail-open on empty candidates
        if not response.candidates:
            log.warning(
                "topic_coherence_empty_candidates",
                campaign_topic=campaign_topic,
                title=title[:80],
                note="Gemini returned no candidates. Failing open.",
            )
            return True
        answer = response.text.strip().upper()
        is_coherent = answer.startswith("YES")

        log.info(
            "topic_coherence_result",
            campaign_topic=campaign_topic,
            title=title[:80],
            answer=answer,
            is_coherent=is_coherent,
        )
        return is_coherent

    except Exception as exc:
        # Fail-open: never block the pipeline on a coherence check failure
        log.warning(
            "topic_coherence_error_failopen",
            campaign_topic=campaign_topic,
            title=title[:80],
            error=str(exc),
        )
        return True


# ---------------------------------------------------------------------------
# Pre-filter tiering gate
# ---------------------------------------------------------------------------

_TIERING_SCHEMA = {
    "type": "ARRAY",
    "items": {
        "type": "OBJECT",
        "properties": {
            "url":             {"type": "STRING"},
            "confidence_tier": {"type": "STRING", "enum": ["High", "Medium", "Low"]},
            "reason":          {"type": "STRING"},
        },
        "required": ["url", "confidence_tier", "reason"],
    },
}

# Verticals where directories / review portals / aggregators are often the
# primary OSINT surface (entity mining, listing intent, local demand).
_PLATFORM_TOLERANT_FAMILIES = frozenset({
    "real_estate",
    "manufacturing",
    "professional_services",
    "construction",
    "healthcare",
    "hospitality",
})

# preferred_sources that strongly imply directory/classified platform mining.
# (google_reviews alone is too common to use as a softening trigger.)
_PLATFORM_SOURCE_HINTS = frozenset({
    "classified_listings",
})

# URL signatures treated as directory / review / aggregator surfaces.
# Softened (not auto-Low) when domain mode is platform-tolerant or low-liquidity.
_DIRECTORY_REVIEW_AGG_SIGNATURES = (
    "yelp.com",
    "g2.com",
    "capterra.com",
    "clutch.co",
    "trustpilot.com",
    "expertise.com",
    "justdial.com",
    "indiamart.com",
    "thomasnet.com",
    "propertyfinder",
    "bayut.com",
    "dubizzle.com",
    "zillow.com",
    "rightmove",
    "99acres.com",
    "magicbricks.com",
    "housing.com",
    "houzz.com",
    "angi.com",
    "homestars.com",
    "sulekha.com",
    "practo.com",
    "tripadvisor.com",
    "bbb.org",
    "yellowpages",
    "zoominfo.com",
    "crunchbase.com",
    "glassdoor.com",
)

# Always-noise patterns — never rescued by domain softening.
_HARD_NOISE_SIGNATURES = (
    "expertise.com",
    "wikipedia.org",
    "amazon.com",
    "/best-",
    "/top-",
    "/vs/",
    "/compare/",
    "linkedin.com/jobs",
)

# Baseline fallback noise set (exact legacy list when no domain profile).
_LEGACY_FALLBACK_NOISE = {
    "yelp.com", "expertise.com", "g2.com", "capterra.com", "upwork.com",
    "glassdoor.com", "indeed.com", "linkedin.com/jobs", "quora.com",
    "wikipedia.org", "amazon.com", "zoominfo.com", "crunchbase.com",
    "/blog/", "/article/", "/post/", "/best-", "/top-", "/vs/", "/compare/",
}


def _snippet_url(snippet: dict[str, Any]) -> str:
    return str(snippet.get("link") or snippet.get("url") or "").strip()


def _url_matches_any(url: str, signatures: tuple[str, ...] | set[str]) -> bool:
    lowered = (url or "").lower()
    return bool(lowered) and any(sig in lowered for sig in signatures)


# ---------------------------------------------------------------------------
# V26.6.0 — Domain / strategy / vector context for scoring & pre-filter
# ---------------------------------------------------------------------------

_CONSUMER_VECTORS = frozenset({"B2C", "D2C", "B2B2C"})
_PLATFORM_STRATEGIES = frozenset({"PLATFORM_MINING", "COMPETITOR_TOUCHPOINT"})
_MEANINGFUL_UNKNOWN = frozenset({"", "unknown", "none", "n/a", "null", "undefined"})


def _normalize_sourcing_vector(raw: Any) -> str:
    value = str(raw or "").strip().upper()
    return value if value else "B2B"


def _normalize_primary_strategy(raw: Any) -> str:
    if isinstance(raw, dict):
        raw = raw.get("primary") or raw.get("primary_strategy") or ""
    value = str(raw or "").strip().upper()
    return value or "COLLOQUIAL_DISCOVERY"


def _domain_family_from_profile(domain_profile: dict[str, Any] | None) -> str:
    if not isinstance(domain_profile, dict):
        return ""
    return str(domain_profile.get("domain_family") or "").strip().lower()


def _is_meaningful_field(value: Any) -> bool:
    text = str(value or "").strip()
    return bool(text) and text.lower() not in _MEANINGFUL_UNKNOWN


def _extract_primary_strategy(
    primary_strategy: Any = None,
    campaign: dict[str, Any] | None = None,
) -> str:
    if primary_strategy:
        return _normalize_primary_strategy(primary_strategy)
    if isinstance(campaign, dict):
        intel = campaign.get("intelligence_strategy") or {}
        if isinstance(intel, dict) and intel.get("primary"):
            return _normalize_primary_strategy(intel.get("primary"))
    return "COLLOQUIAL_DISCOVERY"


def _campaign_context_block(
    *,
    domain_profile: dict[str, Any] | None = None,
    sourcing_vector: str = "",
    primary_strategy: str = "",
    enriched_context: str = "",
    max_enriched_chars: int = 1200,
) -> str:
    """Compact labeled context header shared by scorer and pre-filter prompts."""
    profile = domain_profile if isinstance(domain_profile, dict) else {}
    family = str(profile.get("domain_family") or "").strip() or "unknown"
    conf = str(profile.get("profile_confidence") or "").strip() or "unknown"
    liquidity = str(profile.get("liquidity_level") or "").strip()
    if not liquidity and profile.get("low_liquidity_market"):
        liquidity = "low"
    liquidity = liquidity or "unknown"
    bias = profile.get("strictness_bias")
    vector = _normalize_sourcing_vector(sourcing_vector)
    strategy = _normalize_primary_strategy(primary_strategy)
    thin = bool(profile.get("thin_campaign"))

    lines = [
        "# CAMPAIGN RUNTIME CONTEXT",
        f"sourcing_vector: {vector}",
        f"primary_strategy: {strategy}",
        f"domain_family: {family}",
        f"profile_confidence: {conf}",
        f"liquidity_level: {liquidity}",
        f"thin_campaign: {thin}",
    ]
    if bias is not None:
        try:
            lines.append(f"strictness_bias: {float(bias):.3f}")
        except (TypeError, ValueError):
            pass

    enriched = str(enriched_context or "").strip()
    if enriched:
        if len(enriched) > max_enriched_chars:
            enriched = enriched[:max_enriched_chars] + "\n... [truncated]"
        lines.append("enriched_icp_context:")
        lines.append(enriched)

    return "\n".join(lines)


def _scoring_intent_rules(sourcing_vector: str, primary_strategy: str, domain_family: str) -> str:
    """Branch GENERIC B2B / fit rules by vector + strategy (keeps seller exclusion)."""
    vector = _normalize_sourcing_vector(sourcing_vector)
    strategy = _normalize_primary_strategy(primary_strategy)
    family = (domain_family or "").strip().lower()
    is_consumer = vector in _CONSUMER_VECTORS

    try:
        from services.peer_seller_gate import peer_seller_prompt_block  # type: ignore[import]
        peer_block = peer_seller_prompt_block()
    except Exception:
        peer_block = (
            "\nPEER SELLER EXCLUSION: Businesses that sell the same service as USER BIO "
            "are competitors — score 0–2. Buyers of that service score normally.\n"
        )

    # Tool/lead-gen competitor exclusion + universal peer-seller rule.
    seller = (
        "LEAD-GEN TOOL EXCLUSION: If the target sells B2B lead generation, cold email "
        "marketing, B2B contact databases/data scraping, or outbound sales-agency "
        "tools as their core product, score 0 (or <4).\n"
        f"{peer_block}"
    )

    if strategy == "PLATFORM_MINING":
        return (
            f"{seller}\n\n"
            "PLATFORM_MINING FIT RULE (overrides generic B2B service-page penalties):\n"
            "This campaign mines listing/directory/aggregator platforms for ICP entities "
            "that would BUY from the campaign owner (or match the stated buyer/channel ICP). "
            "Public profile pages and listings WITH identity/contact footprint can score 6–9 "
            "when the entity is a buyer/channel target — NOT when they are peer sellers of "
            "the owner's own offering (e.g. other consultancies when we ARE a consultancy).\n"
            "Directory category SERPs ('Popular X in City') listing same-service peers → score ≤2.\n"
            "Do NOT apply the old 'public services page = GENERAL_FIT ≤3' penalty to true "
            "ICP channel targets under PLATFORM_MINING."
        )

    if strategy == "COMPETITOR_TOUCHPOINT":
        return (
            f"{seller}\n\n"
            "COMPETITOR_TOUCHPOINT FIT RULE:\n"
            "Reviewers and commenters expressing experience with a competitor are leads. "
            "Prioritise reviewer intent and use-case over polished company homepage fit. "
            "A detailed review that reveals ICP match can score 6–9 even without hiring signals."
        )

    if is_consumer:
        return (
            f"{seller}\n\n"
            f"CONSUMER FIT RULE ({vector}):\n"
            "Targets are individuals or local service providers, not enterprise buyers. "
            "Valid high scores include: active listing/profile pages, local business sites "
            "matching the ICP, community posts with purchase/need language, and review "
            "threads with real buyer context. Do NOT require B2B hiring intent or SaaS "
            "pain jargon. A relevant local agent/clinic/shop page can score 5–8 on ICP+geo "
            "fit alone. Still score ≤3 for listicles, pure SEO guides, wrong geography, "
            "or vendors selling the same service as the campaign owner."
        )

    # B2B default — softened: allow strong ICP company footprint matches.
    domain_note = ""
    if family in {"marketing_agency", "professional_services", "saas"}:
        domain_note = (
            f"\nDOMAIN NOTE ({family}): Prefer practitioner pain, agency/client friction, "
            "or tool-switch signals over generic corporate brochure pages."
        )
    elif family in {"real_estate", "manufacturing", "healthcare", "construction"}:
        domain_note = (
            f"\nDOMAIN NOTE ({family}): Local operators and facility pages matching ICP "
            "may score medium when geo+role align; still prioritise active intent."
        )

    return (
        f"{seller}\n\n"
        "B2B INTENT FIT RULE:\n"
        "Pure brochure pages that only list standard services with no pain, hiring, "
        "or change signal should score ≤3 (GENERAL_FIT). HOWEVER: if the page clearly "
        "matches the campaign ICP (role, industry, stack, or operational footprint) and "
        "shows a usable contact surface, you MAY score 4–6 as a qualified ICP company "
        "target — do not zero out every service website. Score 7+ only with explicit "
        "intent (hiring for relevant roles, public complaints, competitor churn, "
        "active project language, or strong multi-signal evidence)."
        f"{domain_note}"
    )


def _domain_scoring_guidance(domain_family: str, primary_strategy: str) -> str:
    """Lightweight per-family scoring hints (compact — avoids prompt bloat)."""
    family = (domain_family or "").strip().lower()
    strategy = _normalize_primary_strategy(primary_strategy)
    hints: dict[str, str] = {
        "real_estate": (
            "REAL ESTATE: Prefer agents/brokers with listings or contact pages; "
            "local property portals; buyer/renter complaint threads. Listicles of "
            "'top agencies' without an entity stay Low."
        ),
        "saas": (
            "SAAS: Prefer tool-switch, pricing-pain, integration-break, and "
            "alternatives threads. Ignore pure product marketing blogs."
        ),
        "marketing_agency": (
            "MARKETING / BRAND: Prefer practitioners venting about messaging, "
            "attribution, agency churn, or inconsistent brand narrative — not "
            "'how to improve your brand' guides."
        ),
        "professional_services": (
            "PROFESSIONAL SERVICES: Prefer firms showing growth friction, hiring, "
            "or operational pain matching the ICP specialty."
        ),
        "manufacturing": (
            "MANUFACTURING: Prefer suppliers/plants with RFQ, capacity, or equipment "
            "signals; IndiaMART/ThomasNet-style profiles can be valid."
        ),
        "healthcare": (
            "HEALTHCARE: Prefer clinics/practitioners with service+geo match; "
            "patient complaint threads only when they reveal buyer context."
        ),
        "education": (
            "EDUCATION: Prefer consultants/institutions with clear program+geo; "
            "student complaint threads with decision intent."
        ),
        "ecommerce": (
            "ECOMMERCE: Prefer operators discussing ops/fulfillment/ads pain or "
            "storefronts matching ICP; ignore affiliate listicles."
        ),
        "finance": (
            "FINANCE: Be strict on compliance noise; prefer clear commercial intent "
            "and avoid government/regulatory brochure pages."
        ),
    }
    body = hints.get(family)
    if not body and strategy == "PLATFORM_MINING":
        body = (
            "PLATFORM MINING (general): Entity identity + contact + geo match beats "
            "abstract pain language on listing pages."
        )
    if not body:
        return ""
    return f"\n# DOMAIN-SPECIFIC SCORING GUIDANCE\n{body}\n"


def _prefilter_strategy_guidance(primary_strategy: str) -> str:
    strategy = _normalize_primary_strategy(primary_strategy)
    if strategy == "PLATFORM_MINING":
        return (
            "\n# STRATEGY OVERRIDE — PLATFORM_MINING\n"
            "Directories, classified portals, review aggregators, and listing/profile pages "
            "are PRIMARY sources, not noise. Default relevant listing/profile pages to Medium; "
            "High when snippet shows ICP entity + geo or clear contact/listing intent. "
            "Still Low for SEO listicles, wrong geography, or pure vendor homepages selling "
            "the same service as USER BIO.\n"
        )
    if strategy == "COMPETITOR_TOUCHPOINT":
        return (
            "\n# STRATEGY OVERRIDE — COMPETITOR_TOUCHPOINT\n"
            "Review and engagement pages are valuable. Reviewer/commenter context → Medium/High. "
            "Do not Low solely because the host is G2/Capterra/Trustpilot/Yelp.\n"
        )
    if strategy == "EVENT_TRIGGER_MINING":
        return (
            "\n# STRATEGY OVERRIDE — EVENT_TRIGGER_MINING\n"
            "News/press about funding, expansion, rebranding, or hiring can be Medium/High "
            "when the company matches the ICP. Pure mega-publishers with no company entity stay Low.\n"
        )
    return ""


def _prefilter_step4_consumer(sourcing_vector: str) -> str:
    """STEP 4 only when the campaign is actually consumer-facing."""
    if _normalize_sourcing_vector(sourcing_vector) not in _CONSUMER_VECTORS:
        return ""
    return (
        "\n# STEP 4 — CONSUMER CONTEXT-AWARE INFERENCE "
        f"({_normalize_sourcing_vector(sourcing_vector)})\n"
        "Snippet fields may include 'Query: <triggering search query>'. Use that context "
        "to reverse-engineer thread state. Dialog dorks (\"pm me\", \"still available\") "
        "plus forum/social replies indicating purchase intent → High/Medium. Do not "
        "auto-Low forum/social snippets when query context shows active consumer discussion.\n"
    )


def _prefilter_few_shot_guidance(domain_family: str, sourcing_vector: str) -> str:
    """Compact High/Low anchors by domain (replaces marketing-only examples)."""
    family = (domain_family or "").strip().lower()
    vector = _normalize_sourcing_vector(sourcing_vector)
    is_consumer = vector in _CONSUMER_VECTORS

    if family == "real_estate" or (is_consumer and family in {"", "general_services", "hospitality"}):
        return (
            "\n# CALIBRATION EXAMPLES (REAL ESTATE / LOCAL SERVICES)\n"
            "HIGH: agent profile with listings in target city; forum post "
            "\"agent ghosted me on Muscat villa deposit\".\n"
            "MEDIUM: property portal search results page for target geo; local brokerage about page.\n"
            "LOW: \"Top 10 real estate agencies in 2024\" listicle; national news with no entity.\n"
        )
    if family in {"marketing_agency", "saas", "professional_services"}:
        return (
            "\n# CALIBRATION EXAMPLES (B2B SERVICES / SAAS / MARKETING)\n"
            "HIGH: practitioner post \"3 brand agencies and still no consistent messaging\"; "
            "\"attribution broken since iOS update\".\n"
            "MEDIUM: job post implying capability gap; news about company rebrand struggles.\n"
            "LOW: \"5 ways to improve your brand narrative\"; HubSpot vs Marketo comparison article.\n"
        )
    if family == "manufacturing":
        return (
            "\n# CALIBRATION EXAMPLES (MANUFACTURING)\n"
            "HIGH: RFQ/supplier profile matching ICP equipment; plant manager complaint thread.\n"
            "MEDIUM: IndiaMART/ThomasNet company listing with geo+category match.\n"
            "LOW: generic \"best CNC machines 2024\" listicle.\n"
        )
    if family == "healthcare":
        return (
            "\n# CALIBRATION EXAMPLES (HEALTHCARE)\n"
            "HIGH: clinic profile in target geo with services matching ICP; patient thread with care-seeking intent.\n"
            "MEDIUM: directory listing for relevant specialty + city.\n"
            "LOW: wellness listicle; government health brochure.\n"
        )
    # Default mixed anchors (backward-compatible flavour)
    return (
        "\n# CALIBRATION EXAMPLES (GENERAL)\n"
        "HIGH: raw forum/community complaint matching USER BIO; identifiable local business with intent.\n"
        "MEDIUM: ambiguous but industry+location relevant page; hiring signal implying gap.\n"
        "LOW: SEO listicles, Top-N posts, how-to guides, wrong geography, pure competitor sales pages.\n"
    )


def _apply_strategy_to_prefilter_mode(
    mode: dict[str, Any] | None,
    domain_profile: dict[str, Any] | None,
    primary_strategy: str,
) -> dict[str, Any] | None:
    """Ensure PLATFORM_MINING / COMPETITOR_TOUCHPOINT enable directory softening."""
    strategy = _normalize_primary_strategy(primary_strategy)
    if strategy not in _PLATFORM_STRATEGIES:
        return mode

    profile = domain_profile if isinstance(domain_profile, dict) else {}
    family = _domain_family_from_profile(profile) or "general_services"
    base = dict(mode) if isinstance(mode, dict) else {
        "active": False,
        "domain_family": family,
        "liquidity_level": profile.get("liquidity_level"),
        "low_liquidity": bool(profile.get("low_liquidity_market")),
        "platform_tolerant": True,
        "strictness_bias": profile.get("strictness_bias"),
        "soften_directories": False,
        "permissive_ambiguous": False,
        "preferred_sources": list(profile.get("preferred_sources") or []),
        "profile_confidence": profile.get("profile_confidence"),
        "thin_campaign": bool(profile.get("thin_campaign")),
    }
    base["active"] = True
    base["soften_directories"] = True
    base["platform_tolerant"] = True
    base["strategy_directory_softening"] = True
    base["primary_strategy"] = strategy
    # Mild permissive ambiguous for platform mining in low-liquidity markets.
    if base.get("low_liquidity") or str(base.get("liquidity_level") or "").lower() == "low":
        base["permissive_ambiguous"] = True
    return base


def _build_campaign_scoring_card(
    campaign: dict[str, Any],
    *,
    enriched_context: str = "",
) -> dict[str, Any]:
    """Structured campaign card for final_score_and_dm (richer than bio+keywords)."""

    def _resolve_bio(c: dict) -> str:
        if c.get("persona_id") and c.get("persona_bio"):
            return str(c.get("persona_bio") or "")
        raw = c.get("bio", "")
        if raw == "CHILD_CAMPAIGN_OVERRIDE":
            return str(
                c.get("effective_bio") or c.get("campaign_focus") or c.get("pain_point") or ""
            )
        return str(raw or "")

    c = campaign if isinstance(campaign, dict) else {}
    card: dict[str, Any] = {
        "campaign_id": c.get("id", c.get("name")),
        "bio": _resolve_bio(c),
        "keywords": c.get("persona_keywords") or c.get("keywords", "") or "",
    }
    if _is_meaningful_field(c.get("pain_point")):
        card["pain_point"] = str(c.get("pain_point")).strip()[:400]
    hook = c.get("target_angle_hook") or ""
    if not hook and isinstance(c.get("system_enrichment"), dict):
        hook = c["system_enrichment"].get("derived_target_angle_hook") or ""
    if _is_meaningful_field(hook):
        card["target_angle_hook"] = str(hook).strip()[:300]
    ua = c.get("unfair_advantage") or c.get("target_angle_adv") or ""
    if not ua and isinstance(c.get("system_enrichment"), dict):
        ua = c["system_enrichment"].get("derived_unfair_advantage") or ""
    if _is_meaningful_field(ua):
        card["unfair_advantage"] = str(ua).strip()[:300]
    if _is_meaningful_field(c.get("sourcing_vector")):
        card["sourcing_vector"] = str(c.get("sourcing_vector")).strip()
    if _is_meaningful_field(c.get("effective_bio")) and c.get("effective_bio") != card.get("bio"):
        card["effective_bio"] = str(c.get("effective_bio")).strip()[:500]
    # Prefer per-campaign enriched context if already on the dict; else shared.
    local_enriched = str(c.get("_enriched_context") or enriched_context or "").strip()
    if local_enriched:
        card["enriched_icp_context"] = local_enriched[:800]
    return card


def is_prefilter_domain_softening_active(
    domain_profile: dict[str, Any] | None,
) -> bool:
    """True when domain profile will soften directory/review/aggregator rejections.

    Lightweight observability helper for domain impact summaries. Safe when
    *domain_profile* is missing (returns False).
    """
    mode = _resolve_prefilter_domain_mode(domain_profile)
    return bool(mode and mode.get("active") and mode.get("soften_directories"))


def _resolve_prefilter_domain_mode(
    domain_profile: dict[str, Any] | None,
) -> dict[str, Any] | None:
    """Derive pre-filter strictness knobs from system_domain_profile.

    Returns None when no usable profile is present → callers MUST keep the
    legacy (strict directory rejection) path bit-for-bit.
    """
    if not isinstance(domain_profile, dict) or not domain_profile:
        return None

    family = str(domain_profile.get("domain_family") or "").strip().lower()
    if not family and domain_profile.get("strictness_bias") is None:
        # Empty-ish profile with no family and no bias → treat as absent.
        if not domain_profile.get("preferred_sources") and not domain_profile.get(
            "low_liquidity_market"
        ):
            return None

    liquidity = str(domain_profile.get("liquidity_level") or "").strip().lower()
    low_liquidity = bool(domain_profile.get("low_liquidity_market")) or liquidity == "low"

    preferred = domain_profile.get("preferred_sources") or []
    if not isinstance(preferred, (list, tuple, set)):
        preferred = []
    preferred_set = {str(s).strip().lower() for s in preferred if str(s).strip()}

    platform_tolerant = family in _PLATFORM_TOLERANT_FAMILIES or bool(
        preferred_set & _PLATFORM_SOURCE_HINTS
    )

    bias_raw = domain_profile.get("strictness_bias")
    try:
        bias = float(bias_raw) if bias_raw is not None else 0.0
        if bias != bias:  # NaN
            bias = 0.0
        bias = max(-0.5, min(0.5, bias))
    except (TypeError, ValueError):
        bias = 0.0

    # Strict domains (positive bias) do not get directory softening even if
    # preferred_sources happen to include reviews (e.g. finance + g2).
    if bias > 0.15:
        platform_tolerant = False

    profile_confidence = str(
        domain_profile.get("profile_confidence") or ""
    ).strip().lower()
    thin_campaign = bool(domain_profile.get("thin_campaign"))
    soft_domain = bool(domain_profile.get("soft_domain_adjustments"))
    low_profile = profile_confidence == "low" or thin_campaign or soft_domain

    soften_directories = platform_tolerant or low_liquidity or bias < -0.05
    # Low-liquidity markets get broader Medium retention for ambiguous pages.
    permissive_ambiguous = low_liquidity or bias <= -0.15

    # Thin / low-confidence profiles: do not open directory softening or
    # permissive ambiguous intake — risk of noise outweighs sparse benefits.
    # Exception: explicit platform-tolerant family with medium+ confidence.
    if low_profile:
        if profile_confidence == "medium" and platform_tolerant:
            permissive_ambiguous = False  # still no broad ambiguous pass
        else:
            soften_directories = False
            permissive_ambiguous = False

    if not soften_directories and not permissive_ambiguous:
        # Profile present but no knobs that change strictness — still return
        # a mode so callers can log family, without changing filter behaviour.
        return {
            "active": False,
            "domain_family": family or "general_services",
            "liquidity_level": liquidity or None,
            "low_liquidity": low_liquidity,
            "platform_tolerant": platform_tolerant,
            "strictness_bias": bias,
            "soften_directories": False,
            "permissive_ambiguous": False,
            "preferred_sources": sorted(preferred_set),
            "profile_confidence": profile_confidence or None,
            "thin_campaign": thin_campaign,
        }

    return {
        "active": True,
        "domain_family": family or "general_services",
        "liquidity_level": liquidity or ("low" if low_liquidity else None),
        "low_liquidity": low_liquidity,
        "platform_tolerant": platform_tolerant,
        "strictness_bias": bias,
        "soften_directories": soften_directories,
        "permissive_ambiguous": permissive_ambiguous,
        "preferred_sources": sorted(preferred_set),
        "profile_confidence": profile_confidence or None,
        "thin_campaign": thin_campaign,
    }


def _domain_prefilter_guidance(mode: dict[str, Any] | None) -> str:
    """Extra prompt section — adjusts strictness only; keeps core categories.

    When mode is inactive, returns empty (header/context is injected separately so
    domain_family is still visible even without softening).
    """
    if not mode or not mode.get("active"):
        return ""

    family = mode.get("domain_family") or "general_services"
    parts = [
        "",
        "# STEP 5 — DOMAIN-AWARE STRICTNESS ADJUSTMENT (do not invent new rejection categories)",
        f"Campaign domain_family: {family}",
        f"Liquidity: {mode.get('liquidity_level') or ('low' if mode.get('low_liquidity') else 'unknown')}",
        f"strictness_bias: {mode.get('strictness_bias')}",
        "",
        "HARD RULE: Core rejection categories are UNCHANGED and remain Low:",
        "- SEO listicles / Top-N posts / how-to guides / pure educational content",
        "- Wrong geography vs TARGET LOCATION",
        "- Direct competitors/vendors SELLING the same service as USER BIO",
        "",
    ]
    if mode.get("soften_directories"):
        parts.extend([
            "DIRECTORY / REVIEW / AGGREGATOR SOFTENING (this domain/strategy relies on platform mining):",
            "Do NOT auto-classify directories (Yelp, JustDial, IndiaMART, Clutch), review sites",
            "(G2, Capterra, Trustpilot, Google reviews), classified portals (Bayut, Property Finder,",
            "Dubizzle, OLX), or aggregator profile/listing pages as Low solely because they are",
            "directories — WHEN the entity is a buyer or channel target for USER BIO.",
            "- Matching ICP channel target (e.g. agent when we sell tools TO agents) → Medium/High",
            "- Snippet shows clear buyer/client pain or active demand → High",
            "- PEER SELLERS of the same service as USER BIO on that directory → Low (always)",
            "- Category SERP shells listing many same-service peers → Low",
            "",
        )
    if mode.get("permissive_ambiguous"):
        parts.extend([
            "LOW-LIQUIDITY / SPARSE-MARKET MODE:",
            "Prefer Medium over Low for ambiguous but industry- or location-relevant pages so the",
            "funnel still receives candidates. Only apply Low when a hard rejection category clearly fits.",
            "",
        ])
    return "\n".join(parts)


def _fallback_noise_signatures(mode: dict[str, Any] | None) -> set[str]:
    """Legacy noise set, optionally softened for platform-tolerant domains."""
    if not mode or not mode.get("active") or not mode.get("soften_directories"):
        return set(_LEGACY_FALLBACK_NOISE)

    # Keep hard noise + blog/listicle patterns; allow directory/review hosts through.
    softened = set(_LEGACY_FALLBACK_NOISE)
    for sig in _DIRECTORY_REVIEW_AGG_SIGNATURES:
        softened.discard(sig)
    # Always keep pure spam / listicle hosts even if also in directory list.
    softened.update({"expertise.com", "wikipedia.org", "amazon.com"})
    return softened


def _rescue_directory_urls_to_medium(
    snippets: list,
    output: dict[str, list],
    mode: dict[str, Any] | None,
) -> int:
    """Promote domain-valuable directory/review URLs that Gemini marked Low → Medium.

    Does not invent new categories: only rescues known platform surfaces when
    domain mode requests softening. Hard noise (listicles, etc.) stays out.
    """
    if not mode or not mode.get("active") or not mode.get("soften_directories"):
        return 0

    already = set(output.get("High") or []) | set(output.get("Medium") or [])
    rescued = 0
    medium = list(output.get("Medium") or [])

    for s in snippets:
        url = _snippet_url(s)
        if not url.startswith("http") or url in already:
            continue
        if _url_matches_any(url, _HARD_NOISE_SIGNATURES):
            continue
        if not _url_matches_any(url, _DIRECTORY_REVIEW_AGG_SIGNATURES):
            continue
        medium.append(url)
        already.add(url)
        rescued += 1

    output["Medium"] = medium
    return rescued


def pre_filter_gemini(
    snippets: list,
    bio: str,
    location_target: str,
    domain_profile: dict[str, Any] | None = None,
    sourcing_vector: str | None = None,
    primary_strategy: str | None = None,
) -> dict:
    """Gemini tiering gate: classify Serper snippet URLs as High/Medium/Low.

    Args:
        snippets:          List of dicts with ``url``, ``title``, ``snippet`` keys.
        bio:               User's business bio (context for scoring).
        location_target:   Geo target string.
        domain_profile:    Optional ``system_domain_profile`` (domain-v2).
        sourcing_vector:   Optional B2B/B2C/D2C/B2B2C — gates consumer STEP 4.
        primary_strategy:  Optional intelligence_strategy primary (e.g. PLATFORM_MINING).

    Returns:
        ``{"High": [url, ...], "Medium": [url, ...]}`` — Low results dropped.
        Returns ``{"High": [], "Medium": []}`` on any failure.

    Backward compatible: callers omitting vector/strategy keep legacy behaviour
    plus optional domain softening when profile is present.
    """
    if not snippets:
        return {"High": [], "Medium": []}

    vector = _normalize_sourcing_vector(sourcing_vector)
    strategy = _normalize_primary_strategy(primary_strategy)
    family = _domain_family_from_profile(domain_profile)

    mode = _resolve_prefilter_domain_mode(domain_profile)
    mode = _apply_strategy_to_prefilter_mode(mode, domain_profile, strategy)

    # Always surface domain/strategy/vector when known (even if softening inactive).
    context_header = _campaign_context_block(
        domain_profile=domain_profile if isinstance(domain_profile, dict) else None,
        sourcing_vector=vector,
        primary_strategy=strategy,
        enriched_context="",  # bio already carries enriched ICP for pre-filter
        max_enriched_chars=0,
    )
    strategy_guidance = _prefilter_strategy_guidance(strategy)
    step4 = _prefilter_step4_consumer(vector)
    few_shot = _prefilter_few_shot_guidance(family, vector)
    domain_guidance = _domain_prefilter_guidance(mode) if mode else ""

    # Directory default line: only auto-Low directories when strategy is NOT platform-like
    # and domain softening is off.
    directories_are_low = not (
        mode and mode.get("active") and mode.get("soften_directories")
    ) and strategy not in _PLATFORM_STRATEGIES
    low_directory_clause = (
        "directories (Yelp, G2, etc.), "
        if directories_are_low
        else "irrelevant pure-spam directories with no ICP entity, "
    )

    prompt = f"""CONFIDENCE TIERING GATE: Evaluate each URL snippet as an investigative OSINT engine against the user's business context.
USER BIO: '{bio}'
TARGET LOCATION: '{location_target}'
{context_header}
{strategy_guidance}
# STEP 1 — OSINT SCORING MATRIX
Evaluate the snippets purely on RAW INTENT and SYMPTOMS, ignoring corporate polish.

High Confidence: Raw, unpolished footprints. This includes niche forum complaints, municipal PDFs, unoptimized local business pages, or direct expressions of pain/need that match the USER BIO. "Ugly" is good if the intent is strong. Also includes community posts, Reddit threads, Slack/Discord exports, LinkedIn comments — where a PRACTITIONER is actively venting a problem they are experiencing RIGHT NOW. Listing/profile pages that match ICP+geo under PLATFORM_MINING also qualify as High when entity identity is clear.

Medium Confidence: Ambiguous intent, but highly relevant industry or location. Includes: news articles about company challenges, job posts implying a capability gap, product reviews expressing frustration, and (when strategy/domain allows) relevant directory or aggregator profile pages.

Low Confidence: SEO-optimised listicles, "Top 10" posts, how-to guides, {low_directory_clause}generic educational articles, or clear competitors/vendors SELLING the same service as in the USER BIO.

# STEP 2 — UNIVERSAL RULES
SOCIAL PLATFORM RULE: Evaluate the SPECIFIC POST intent, not the platform's general purpose.
GEO RULE: Wrong region → Low.
COMPETITOR / PEER SELLER RULE: If the snippet is a business that SELLS the same product/service
as USER BIO (peer on Justdial/Yelp/directory category pages), classify as Low — never High
just because the directory category keywords match the bio. Buyers of that service stay High/Medium.
DIRECTORY SHELL RULE: Category SERPs ('Popular X in City', nct- listing indexes) without a
single buyer entity → Low.
{few_shot}
# STEP 3 — BUYER FORUM EXCEPTION
Do NOT classify as Low merely because a URL domain is marketing- or community-related.
A practitioner COMPLAINING about a problem matching USER BIO is HIGH-confidence, not a blog.
Still Low: pure how-to guides, vendor comparisons, and Top-N listicles with no buyer present.
{step4}{domain_guidance}
Snippets: {json.dumps(snippets)}"""

    log.info(
        "pre_filter_context_applied",
        domain_family=family or (mode or {}).get("domain_family"),
        sourcing_vector=vector,
        primary_strategy=strategy,
        domain_adjustment_active=bool(mode and mode.get("active")),
        soften_directories=bool(mode and mode.get("soften_directories")),
        strategy_directory_softening=bool(mode and mode.get("strategy_directory_softening")),
        consumer_step4=bool(step4),
        url_count=len(snippets),
    )

    if mode and mode.get("active"):
        log.info(
            "pre_filter_domain_adjustment_applied",
            domain_family=mode.get("domain_family"),
            liquidity_level=mode.get("liquidity_level"),
            low_liquidity=bool(mode.get("low_liquidity")),
            platform_tolerant=bool(mode.get("platform_tolerant")),
            soften_directories=bool(mode.get("soften_directories")),
            permissive_ambiguous=bool(mode.get("permissive_ambiguous")),
            strictness_bias=mode.get("strictness_bias"),
            preferred_sources=mode.get("preferred_sources"),
            primary_strategy=strategy,
            strategy_directory_softening=bool(mode.get("strategy_directory_softening")),
            url_count=len(snippets),
            note="Domain/strategy softened pre-filter strictness for directories/reviews/aggregators "
                 "and/or low-liquidity ambiguous pages. Core rejection categories unchanged.",
        )

    try:
        tiered = call_gemini_2_5(prompt, expect_json=True, response_schema=_TIERING_SCHEMA)
        if not isinstance(tiered, list):
            raise ValueError("Expected list from tiering gate")
    except Exception as exc:
        # SF-010 FIX Hardening: Local heuristic fallback to prevent listicle/directory spills.
        log.warning(
            "pre_filter_gemini_failed_local_fallback",
            error=str(exc),
            url_count=len(snippets),
            domain_family=(mode or {}).get("domain_family") or family,
            primary_strategy=strategy,
            soften_directories=bool((mode or {}).get("soften_directories")),
            action="Running local heuristic fallback filter to drop obvious noise.",
        )
        fallback_high: list[str] = []
        fallback_medium: list[str] = []
        noise_signatures = _fallback_noise_signatures(mode)
        for s in snippets:
            link = _snippet_url(s)
            if not (link and link.startswith("http")):
                continue
            link_lower = link.lower()
            if any(sig in link_lower for sig in noise_signatures):
                # Domain-softened path: keep directory/review hosts as Medium
                # instead of hard-dropping them.
                if (
                    mode
                    and mode.get("soften_directories")
                    and _url_matches_any(link, _DIRECTORY_REVIEW_AGG_SIGNATURES)
                    and not _url_matches_any(link, _HARD_NOISE_SIGNATURES)
                ):
                    fallback_medium.append(link)
                continue
            fallback_high.append(link)

        if mode and mode.get("active") and fallback_medium:
            log.info(
                "pre_filter_domain_fallback_directory_kept",
                domain_family=mode.get("domain_family"),
                primary_strategy=strategy,
                kept_medium=len(fallback_medium),
                kept_high=len(fallback_high),
                note="Fallback kept domain-valuable directory/review URLs as Medium.",
            )
        return {"High": fallback_high, "Medium": fallback_medium}

    output: dict[str, list] = {"High": [], "Medium": []}
    for item in tiered:
        tier = item.get("confidence_tier", "Low")
        url = item.get("url", "").strip()
        if not url.startswith("http"):
            continue
        if tier in ("High", "Medium"):
            output[tier].append(url)

    # Deterministic safety net: if Gemini still auto-Low'd (or omitted)
    # platform surfaces that this domain/strategy values, promote them to Medium.
    rescued = 0
    if mode and mode.get("active") and mode.get("soften_directories"):
        rescued = _rescue_directory_urls_to_medium(snippets, output, mode)
        if rescued:
            log.info(
                "pre_filter_domain_directory_rescue",
                domain_family=mode.get("domain_family"),
                primary_strategy=strategy,
                rescued_to_medium=rescued,
                high=len(output["High"]),
                medium=len(output["Medium"]),
                note="Rescued domain-valuable directory/review/aggregator URLs from Low → Medium.",
            )

    log.info(
        "pre_filter_complete",
        high=len(output["High"]),
        medium=len(output["Medium"]),
        domain_family=(mode or {}).get("domain_family") or family or None,
        primary_strategy=strategy,
        sourcing_vector=vector,
        domain_adjustment_active=bool(mode and mode.get("active")),
        directory_rescued=rescued,
    )
    return output


# ---------------------------------------------------------------------------
# Inline signal scorer (V25.1.0 — Signal Harvest pipeline)
# ---------------------------------------------------------------------------
#
# pre_filter_gemini() scores Serper snippets (140 chars) → high rejection rate
# because there is insufficient content for intent classification.
#
# inline_score_signal() scores FULL content from Reddit/HN/RSS/PRISM — the
# actual words the buyer wrote, not a search-engine summary of them. This is
# the correct design: Gemini sees what the buyer said, not what Google said
# about what the buyer said.
#
# Used by signal_harvest.py exclusively. dispatch.py still uses pre_filter_gemini
# for backward compatibility with the Serper pathway.
# ---------------------------------------------------------------------------

_INLINE_SCORE_SCHEMA = {
    "type": "OBJECT",
    "properties": {
        "tier": {
            "type":        "STRING",
            "enum":        ["HIGH", "MEDIUM", "LOW"],
            "description": "Intent confidence tier.",
        },
        "pain_summary": {
            "type":        "STRING",
            "description": "1-2 sentence summary of the specific pain or need expressed.",
        },
        "contact_point": {
            "type":        "STRING",
            "description": "How to reach this person: username, profile URL, email, or empty if not determinable.",
        },
        "buyer_language_quote": {
            "type":        "STRING",
            "description": "Exact quote from the signal text that most strongly indicates buying intent.",
        },
        "geo_match": {
            "type":        "BOOLEAN",
            "description": "True if the signal's geographic context matches the campaign's geo target.",
        },
        "archetype_match": {
            "type":        "STRING",
            "description": "Which archetype this signal best matches: B2B, B2C, D2C, B2B2C, or NONE.",
        },
        "rejection_reason": {
            "type":        "STRING",
            "description": "If tier is LOW, explain why. Empty for HIGH/MEDIUM.",
        },
        "topic_coherence": {
            "type":        "NUMBER",
            "description": "0.0-1.0 — how strongly the content is PRIMARILY about the campaign topic vs. mentioning it incidentally.",
        },
    },
    "required": [
        "tier", "pain_summary", "contact_point",
        "buyer_language_quote", "geo_match", "archetype_match",
    ],
}


def inline_score_signal(
    signal_text: str,
    icp_context: str,
    source_url: str,
    source_type: str,
    geo_target: str,
    archetype: str,
    buyer_language_context: str = "",
) -> dict:
    """Score full signal content against campaign ICP.

    Unlike pre_filter_gemini which uses Serper snippets (140 chars),
    this function receives the FULL text of a signal — the complete
    Reddit post body, Hacker News story, RSS article, or job description.

    Args:
        signal_text:            Full text content of the signal (post/article/job).
        icp_context:            Campaign ICP context from context_builder.
        source_url:             Original signal URL (for context).
        source_type:            Source identifier ("reddit", "hackernews", etc.).
        geo_target:             Campaign geographic target (may be empty for global).
        archetype:              Campaign archetype (B2B, B2C, D2C, B2B2C).
        buyer_language_context: Optional Gemini-derived buyer language summary
                                from source_router for extra context.

    Returns:
        Dict with keys: tier, pain_summary, contact_point, buyer_language_quote,
        geo_match, archetype_match, rejection_reason.
        Returns LOW tier dict on any error.
    """
    if not signal_text or not signal_text.strip():
        return {
            "tier":                "LOW",
            "pain_summary":        "",
            "contact_point":       "",
            "buyer_language_quote": "",
            "geo_match":           False,
            "archetype_match":     "NONE",
            "rejection_reason":    "Empty signal text",
        }

    # Truncate to avoid context window overflow (Gemini 2.5 Flash = 1M tokens,
    # but we budget 6000 chars per signal to keep batch costs reasonable)
    truncated = signal_text[:6000]
    if len(signal_text) > 6000:
        truncated += "\n... [SIGNAL TRUNCATED — only first 6000 chars shown]"

    geo_instruction = (
        f"Target geography: {geo_target}. "
        f"If the signal is not relevant to {geo_target}, set geo_match=false and tier=LOW."
        if geo_target
        else "Geography: Global — geo_match=true unless signal is clearly irrelevant to a specific locale that contradicts the ICP."
    )

    buyer_language_hint = (
        f"\n\nBUYER LANGUAGE CONTEXT (from source router):\n{buyer_language_context}"
        if buyer_language_context
        else ""
    )

    prompt = f"""You are an OSINT intent analyst for a lead generation platform.

CAMPAIGN ICP (Ideal Customer Profile):
{icp_context}

CAMPAIGN ARCHETYPE: {archetype}
{geo_instruction}

SOURCE TYPE: {source_type.upper()} — {source_url[:120]}
{buyer_language_hint}

FULL SIGNAL CONTENT:
{truncated}

TASK:
Analyze the full signal content above. Determine whether this signal represents an
active buyer who matches the ICP and is currently experiencing a pain that the
campaign can address.

SCORING RULES:
HIGH tier: The signal contains explicit buyer intent — someone asking for a vendor
  recommendation, expressing frustration with a current solution, announcing a budget
  or project that matches the ICP, or describing a pain that is directly addressable.
  The person/company is identifiable and reachable.
  ALSO HIGH: A Google Maps review where the REVIEWER describes their own use case,
  pain, or project in a way that matches the ICP — the reviewer IS a proven buyer
  in this category (they already spent money on a competitor).

MEDIUM tier: The signal shows strong contextual relevance to the ICP but lacks explicit
  buying intent. Could be a decision influencer, a company going through a relevant
  change, or a topic discussion from the ICP audience.
  ALSO MEDIUM: A positive review on a competitor where the reviewer mentions their
  business type, use case, or project context that matches the ICP, even without
  explicit dissatisfaction.

LOW tier: The signal is a generic article, listicle, corporate announcement unrelated
  to buying, spam, or clearly irrelevant to the ICP.
  A review is LOW only if: (a) it contains no useful buyer context (e.g. just "Great
  service" with no detail), (b) the reviewer is clearly outside the ICP geo/category,
  or (c) the review is from a competitor/seller, not a buyer.

SCORING CALIBRATION (use these anchors when estimating the topic_coherence field):
- 0.9-1.0: Person explicitly states intent to buy/hire within 30 days
- 0.7-0.8: Actively researching, comparing options, asking for recommendations
- 0.5-0.6: Mentions topic but no clear buying signal
- 0.3-0.4: Tangentially related, no individual buyer
- 0.1-0.2: News, editorial, academic, or irrelevant
If content is news/megathread/editorial mentioning topic in passing, score topic_coherence 0.1-0.3.

TOPIC COHERENCE FIELD:
Set topic_coherence (0.0-1.0) to reflect how PRIMARILY the content is about the
campaign topic. 1.0 = entirely focused on the campaign topic with clear buyer intent.
0.0 = campaign topic is not present at all. Content that merely name-drops the topic
in a news roundup or editorial listicle should receive 0.1-0.3.

GEO RULE: If a geo_target is set and the signal is clearly from/about a different
  geography, set geo_match=false and tier=LOW.

SELLER EXCLUSION: If the signal author/company is a direct competitor (sells the same
  product/service described in the ICP), set tier=LOW.

Return a single JSON object matching the schema."""

    try:
        result = call_gemini_2_5(
            prompt,
            expect_json=True,
            response_schema=_INLINE_SCORE_SCHEMA,
        )
        if not isinstance(result, dict):
            raise ValueError(f"Unexpected response type: {type(result)}")

        # Normalise tier to uppercase
        result["tier"] = str(result.get("tier", "LOW")).upper()
        if result["tier"] not in ("HIGH", "MEDIUM", "LOW"):
            result["tier"] = "LOW"

        log.info(
            "inline_score_complete",
            tier=result["tier"],
            source_type=source_type,
            url=source_url[:80],
            geo_match=result.get("geo_match", False),
        )
        return result

    except Exception as exc:
        log.warning(
            "inline_score_failed",
            source_url=source_url[:80],
            source_type=source_type,
            error=str(exc),
        )
        lowered = (signal_text or "").lower()
        has_buy_intent = any(
            phrase in lowered
            for phrase in [
                "looking for",
                "need help",
                "recommend",
                "urgent",
                "broken",
                "problem",
                "need a",
                "budget",
                "hire",
                "buy",
                "switch",
            ]
        )
        topic_coherence = 0.8 if has_buy_intent else 0.35
        if archetype in {"B2C", "D2C"}:
            topic_coherence = max(topic_coherence, 0.6) if has_buy_intent else 0.4
        if geo_target and geo_target.lower() not in lowered:
            topic_coherence *= 0.7
        return {
            "tier":                "HIGH" if has_buy_intent and topic_coherence >= 0.6 else "MEDIUM",
            "pain_summary":        "Heuristic fallback inferred buyer urgency from signal text.",
            "contact_point":       "",
            "buyer_language_quote": "",
            "geo_match":           True,
            "archetype_match":     archetype,
            "rejection_reason":    f"Scoring error: {exc}",
            "topic_coherence":     round(min(1.0, max(0.0, topic_coherence)), 3),
        }


# ---------------------------------------------------------------------------
# Final score + DM generator
# ---------------------------------------------------------------------------


_SCORE_SCHEMA = {
    "type": "OBJECT",
    "properties": {
        "matched_campaigns": {
            "type": "ARRAY",
            "items": {
                "type": "OBJECT",
                "properties": {
                    "campaign_id": {"type": "STRING"},
                    "raw_score":  {"type": "INTEGER"},
                },
                "required": ["campaign_id", "raw_score"],
            },
        },
        "dm":                           {"type": "STRING"},
        "pain_point":                   {"type": "STRING"},
        "icebreaker_angle":             {"type": "STRING"},
        "intent_signal":                {"type": "STRING"},
        "hiring_intent_found":          {"type": "STRING", "enum": ["Yes", "No"]},
        "tech_stack_found":             {"type": "ARRAY", "items": {"type": "STRING"}},
        "contact_endpoints": {
            "type": "ARRAY",
            "items": {
                "type": "OBJECT",
                "properties": {
                    "platform": {
                        "type": "STRING",
                        "enum": ["instagram", "reddit", "whatsapp", "gmb",
                                 "email", "linkedin", "facebook", "other"],
                    },
                    "uri": {"type": "STRING"},
                },
                "required": ["platform", "uri"],
            },
        },
        "decision_maker_name":          {"type": "STRING"},
        "decision_maker_title":         {"type": "STRING"},
        "company_size_tier":            {"type": "STRING"},
        "primary_objection_hypothesis": {"type": "STRING"},
        "company_name":                 {"type": "STRING"},
        "score_reasoning":              {"type": "STRING"},
        "confidence_level":             {"type": "STRING", "enum": ["HIGH", "MEDIUM", "SPECULATIVE"]},
        "evidence_chain": {
            "type": "ARRAY",
            "items": {
                "type": "OBJECT",
                "properties": {
                    "signal_type": {
                        "type": "STRING",
                        "enum": ["PAIN_EXPRESSION", "HIRING_INTENT", "COMPETITOR_CHURN",
                                 "TECH_STACK_MATCH", "COMMUNITY_MENTION", "REVIEW_SIGNAL",
                                 "FUNDING_EVENT", "GENERAL_FIT"],
                    },
                    "evidence": {"type": "STRING"},
                    "confidence": {"type": "NUMBER"},
                },
                "required": ["signal_type", "evidence", "confidence"],
            },
        },
    },
    "required": [
        "matched_campaigns", "dm", "pain_point", "icebreaker_angle",
        "intent_signal", "hiring_intent_found", "tech_stack_found",
        "contact_endpoints", "decision_maker_name", "decision_maker_title",
        "company_size_tier", "primary_objection_hypothesis",
        "score_reasoning", "confidence_level", "evidence_chain",
    ],
}


def final_score_and_dm(
    text: str,
    active_campaigns: list,
    context_payload: str,
    tech_stack: list,
    historical_dms: Optional[list] = None,
    source_url: Optional[str] = None,
    domain_profile: Optional[dict[str, Any]] = None,
    sourcing_vector: Optional[str] = None,
    primary_strategy: Optional[str] = None,
    enriched_context: Optional[str] = None,
    campaign: Optional[dict[str, Any]] = None,
) -> dict:
    """Score a lead against all active campaigns and draft an outreach message.

    Args:
        text:              DOM text / snippet text of the lead page.
        active_campaigns:  List of campaign dicts with ``id``, ``bio``, ``keywords``.
        context_payload:   Contextual enrichment string (GMB, social, hiring).
        tech_stack:        List of detected tech stack strings.
        historical_dms:    Past successful DM strings (RLHF feedback loop).
        source_url:        Original source URL (used for social platform detection).
        domain_profile:    Optional ``system_domain_profile`` for domain-aware scoring.
        sourcing_vector:   Optional B2B/B2C/D2C/B2B2C (falls back to campaign).
        primary_strategy:  Optional intelligence strategy primary name.
        enriched_context:  Optional ``build_enriched_context`` output for the primary campaign.
        campaign:          Optional primary campaign dict (fills missing context fields).

    Returns:
        Dict with ``score``, ``dm``, ``pain_point``, ``matched_campaign_ids``, etc.
        Also includes ``scoring_context`` metadata for observability.

    Raises:
        ValueError: On LLM parse failure.

    Backward compatible: omitting domain/strategy/vector kwargs preserves prior
    call signatures; rules then default to B2B + COLLOQUIAL_DISCOVERY.
    """
    primary_campaign = campaign if isinstance(campaign, dict) else None
    if primary_campaign is None and active_campaigns:
        first = active_campaigns[0]
        if isinstance(first, dict):
            primary_campaign = first

    # Resolve context with graceful fallbacks (never require new kwargs).
    profile = domain_profile if isinstance(domain_profile, dict) else None
    if profile is None and primary_campaign:
        cached = primary_campaign.get("system_domain_profile")
        if isinstance(cached, dict) and cached.get("domain_family"):
            profile = cached

    vector = _normalize_sourcing_vector(
        sourcing_vector
        or (primary_campaign.get("sourcing_vector") if primary_campaign else None)
    )
    strategy = _extract_primary_strategy(primary_strategy, primary_campaign)
    family = _domain_family_from_profile(profile)

    enriched = str(enriched_context or "").strip()
    if not enriched and primary_campaign:
        # Prefer pre-built enriched context; otherwise assemble key fields only
        # (avoid importing context_builder here to keep this module lightweight).
        parts = []
        for label, key in (
            ("PRODUCT/SERVICE", "effective_bio"),
            ("BUYER PAIN", "pain_point"),
            ("BUYER HOOK", "target_angle_hook"),
            ("COMPETITIVE ADVANTAGE", "unfair_advantage"),
        ):
            val = primary_campaign.get(key)
            if _is_meaningful_field(val):
                parts.append(f"{label}: {str(val).strip()[:400]}")
        if not parts and _is_meaningful_field(primary_campaign.get("bio")):
            parts.append(f"PRODUCT/SERVICE: {str(primary_campaign.get('bio')).strip()[:400]}")
        enriched = "\n".join(parts)

    social_domains_check = [
        "reddit.com", "quora.com", "facebook.com",
        "linkedin.com", "instagram.com",
    ]
    is_social = source_url and any(d in source_url.lower() for d in social_domains_check)
    platform = "other"
    if source_url:
        for kw, name in [
            ("reddit.com", "reddit"),
            ("quora.com", "other"),
            ("facebook.com", "facebook"),
            ("linkedin.com", "linkedin"),
            ("instagram.com", "instagram"),
        ]:
            if kw in source_url:
                platform = name
                break

    social_uri_rule = ""
    if is_social:
        social_uri_rule = (
            f"\nSOCIAL PROFILE URI RULE (MANDATORY): "
            f"The source URL '{source_url}' is from a social platform. "
            f"Extract the original poster's user profile URL using platform enum ('{platform}'). "
            "Do NOT return empty contact_endpoints if a profile link is present."
        )

    campaign_cards = [
        _build_campaign_scoring_card(
            c if isinstance(c, dict) else {},
            enriched_context=enriched if i == 0 else "",
        )
        for i, c in enumerate(active_campaigns or [])
    ]
    campaigns_str = json.dumps(campaign_cards, indent=2)

    context_header = _campaign_context_block(
        domain_profile=profile,
        sourcing_vector=vector,
        primary_strategy=strategy,
        enriched_context=enriched,
        max_enriched_chars=1200,
    )
    intent_rules = _scoring_intent_rules(vector, strategy, family)
    domain_guidance = _domain_scoring_guidance(family, strategy)

    scoring_meta = {
        "domain_family": family or None,
        "profile_confidence": (profile or {}).get("profile_confidence") if profile else None,
        "liquidity_level": (profile or {}).get("liquidity_level") if profile else None,
        "sourcing_vector": vector,
        "primary_strategy": strategy,
        "has_enriched_context": bool(enriched),
        "intent_rules_mode": (
            "platform_mining" if strategy == "PLATFORM_MINING"
            else "competitor_touchpoint" if strategy == "COMPETITOR_TOUCHPOINT"
            else "consumer" if vector in _CONSUMER_VECTORS
            else "b2b_default"
        ),
    }
    log.info(
        "final_score_context_applied",
        source_url=(source_url or "")[:80],
        is_social=bool(is_social),
        platform=platform,
        campaign_count=len(campaign_cards),
        **scoring_meta,
    )

    prompt = f"""You are a Dynamic Intent Analyzer evaluating a lead against multiple campaigns.
SOURCE TYPE: {'SOCIAL/FORUM POST' if is_social else 'COMPANY WEBSITE/FORMAL PAGE'}
PLATFORM: {platform.upper()}
{context_header}
{domain_guidance}
# STEP 1 — CROSS-POLLINATION EVALUATION MATRIX
Evaluate the text DOM against EACH campaign below. Score 1-10. Return only campaigns where score >= 4.
Use each campaign's bio, keywords, pain_point, target_angle_hook, unfair_advantage, and enriched_icp_context when present.
{campaigns_str}

{intent_rules}

# STEP 2 — OUTREACH COPILOT DRAFT
Identify the campaign with the HIGHEST match score.
{'OSINT Community tone: Ultra-casual, empathetic, observational. Acknowledge their specific situation or complaint. Max 3 sentences, end with a soft, low-friction question.' if is_social else 'OSINT Discovery tone: Direct, highly contextual, and observant. Reference the specific operational footprint or symptom you found on their site. Speak operator-to-operator. Zero generic corporate fluff.'}

# STEP 3 — EXTRACTION RULES
- hiring_intent_found: ONLY 'Yes' or 'No'.
- contact_endpoints: Only explicitly present contacts. URI must have full protocol prefix (https://).
- PHONE DEDUPLICATION: Max 2 numbers.
{social_uri_rule}

# STEP 4 — EVIDENCE DOSSIER
For each piece of evidence you used to score this lead, create an evidence_chain entry:
- signal_type: classify as PAIN_EXPRESSION, HIRING_INTENT, COMPETITOR_CHURN, TECH_STACK_MATCH, COMMUNITY_MENTION, REVIEW_SIGNAL, FUNDING_EVENT, or GENERAL_FIT
- evidence: the exact quote or fact from the text (max 100 chars)
- confidence: 0.0-1.0 how confident you are this signal is real
Also provide:
- score_reasoning: 1-2 sentences explaining WHY this lead scored the way it did (mention domain/strategy fit if relevant)
- confidence_level: HIGH (multiple converging signals), MEDIUM (clear single signal), SPECULATIVE (weak/indirect signals)

CONTEXTUAL DORKING DATA:
{context_payload}

DETECTED TECH STACK:
{', '.join(tech_stack) if tech_stack else 'None extracted'}
"""
    if historical_dms:
        prompt += f"\nPast successful converted messages (match tone): {historical_dms}\n"
    # Token limit protection: truncate raw web text to prevent context window or credit overflow
    max_chars = int(os.environ.get("MAX_SCRAPED_TEXT_CHARS", 50000))
    truncated_text = text if len(text) <= max_chars else text[:max_chars] + "\n... [TRUNCATED DUE TO SIZE LIMIT] ..."
    prompt += f"\nText DOM:\n{truncated_text}"

    sys_inst = (
        "You are a Dynamic Intent Analyzer and OSINT Lead Profiler. "
        "\n\nCRITICAL RULE — CONTEXTUAL DISCOVERY: "
        "You are evaluating leads that were likely found via raw web footprints "
        "(PDFs, forums, unoptimized sites, listing portals). Score using the campaign's "
        "sourcing_vector, primary_strategy, and domain_family context — not a single "
        "generic B2B rubric."
        "\n\nOUTREACH RULE: "
        "Draft the DM to sound like a natural, serendipitous discovery. "
        "Acknowledge the specific context of where/how you found them without "
        "sounding intrusive. Never hallucinate contacts."
    )

    try:
        data = call_gemini_2_5(
            prompt,
            expect_json=True,
            response_schema=_SCORE_SCHEMA,
            system_instruction=sys_inst,
        )
        if not isinstance(data, dict):
            raise ValueError("Parsed JSON is not a dict")

        matched = data.get("matched_campaigns", [])
        if not matched:
            log.info(
                "final_score_no_matched_campaigns",
                source_url=(source_url or "")[:80],
                confidence_level=data.get("confidence_level"),
                score_reasoning=(data.get("score_reasoning") or "")[:160],
                **scoring_meta,
            )
            return {
                "score": 0, "matched_campaign_ids": [], "trend_mapped": False,
                "highest_campaign_id": "Unknown",
                "pain_point": data.get("pain_point", "Unknown"),
                "hiring_intent_found": data.get("hiring_intent_found", "No"),
                "tech_stack_found": data.get("tech_stack_found", []),
                "icebreaker_angle": data.get("icebreaker_angle", ""),
                "intent_signal": data.get("intent_signal", ""),
                "dm": data.get("dm", "Failed to generate DM"),
                "contact_endpoints": data.get("contact_endpoints", []),
                "decision_maker_name": data.get("decision_maker_name", "Unknown"),
                "decision_maker_title": data.get("decision_maker_title", "Unknown"),
                "company_size_tier": data.get("company_size_tier", "Unknown"),
                "primary_objection_hypothesis": data.get("primary_objection_hypothesis", "Unknown"),
                "company_name": data.get("company_name") or None,
                "score_reasoning": data.get("score_reasoning", ""),
                "confidence_level": data.get("confidence_level", "SPECULATIVE"),
                "evidence_chain": data.get("evidence_chain", []),
                "scoring_context": scoring_meta,
            }

        matched.sort(key=lambda x: x.get("raw_score", 0), reverse=True)
        base_score = float(matched[0].get("raw_score", 0))
        highest_campaign = matched[0].get("campaign_id", "Unknown")
        matched_ids = [str(c.get("campaign_id")) for c in matched]

        # Postmortem Fix #10: reduced multiplier table.
        # Old table {2: 1.3, else: 1.6} inflated base-6 leads → 9.6 (hot-lead alert).
        # New table caps at 1.3× for 4+ campaigns. A base-6 lead scores max 7.8 → 7,
        # staying below the WhatsApp trigger (>=8). Genuine 9+ leads still reach 10.
        multiplier = {1: 1.0, 2: 1.05, 3: 1.1}.get(len(matched), 1.15)
        final_score = int(min(base_score * multiplier, 10.0))

        log.info(
            "final_score_decision",
            source_url=(source_url or "")[:80],
            base_score=base_score,
            final_score=final_score,
            matched_count=len(matched_ids),
            highest_campaign_id=str(highest_campaign)[:64],
            confidence_level=data.get("confidence_level"),
            score_reasoning=(data.get("score_reasoning") or "")[:200],
            **scoring_meta,
        )

        return {
            "score": final_score,
            "matched_campaign_ids": matched_ids,
            "trend_mapped": len(matched) >= 3,
            "highest_campaign_id": highest_campaign,
            "pain_point": data.get("pain_point", "Unknown"),
            "hiring_intent_found": data.get("hiring_intent_found", "No"),
            "tech_stack_found": data.get("tech_stack_found", []),
            "icebreaker_angle": data.get("icebreaker_angle", ""),
            "intent_signal": data.get("intent_signal", ""),
            "dm": data.get("dm", "Failed to generate DM"),
            "contact_endpoints": data.get("contact_endpoints", []),
            "decision_maker_name": data.get("decision_maker_name", "Unknown"),
            "decision_maker_title": data.get("decision_maker_title", "Unknown"),
            "company_size_tier": data.get("company_size_tier", "Unknown"),
            "primary_objection_hypothesis": data.get("primary_objection_hypothesis", "Unknown"),
            "company_name": data.get("company_name") or None,
            "score_reasoning": data.get("score_reasoning", ""),
            "confidence_level": data.get("confidence_level", "SPECULATIVE"),
            "evidence_chain": data.get("evidence_chain", []),
            "scoring_context": scoring_meta,
        }

    except Exception as exc:
        raise ValueError(f"LLM Parsing Failure: {exc}") from exc
