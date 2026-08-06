"""Universal buyer-vs-peer-seller gate for OSINT lead quality.

Radical value rule (domain-agnostic):
  A high-quality lead is someone who would BUY from the campaign owner,
  or who matches the stated ICP as a buyer/channel target.
  A junk lead is a PEER SELLER — a commercial provider of the same (or
  highly substitutable) product/service the campaign owner sells.

This module is deterministic and strategy-aware without hard-coding vertical
domain families. Used after Gemini scoring, on entity extraction, and for
directory-shell URL rejection.

Design:
  - "offering" tokens = what WE sell (product/service layers)
  - "icp" tokens = who we sell TO (persona / buyer / pain / targeting)
  - Entity tokens from company_name, pain_summary, page text/category
  - Peer if offering-overlap is high AND icp-overlap is not higher
    (agents on Bayut can still pass when ICP is agents; Justdial peer
    consultants fail when offering is "education consultancy" and ICP is students)
"""
from __future__ import annotations

import os
import re
from typing import Any, Mapping, Optional
from urllib.parse import urlparse

# Fail-closed for known directory shells when PEER_SELLER_GATE_ENABLED
PEER_SELLER_GATE_ENABLED: bool = os.environ.get(
    "PEER_SELLER_GATE_ENABLED", "true"
).lower() in ("1", "true", "yes")

# Score ceiling for confirmed peer sellers (D)
PEER_SELLER_SCORE_CAP: int = max(0, min(5, int(os.environ.get("PEER_SELLER_SCORE_CAP", "2"))))

_STOP = frozenset({
    "a", "an", "the", "and", "or", "for", "to", "of", "in", "on", "at", "by",
    "with", "from", "as", "is", "are", "was", "were", "be", "been", "being",
    "this", "that", "these", "those", "we", "our", "you", "your", "they",
    "their", "it", "its", "i", "me", "my", "us", "them", "who", "what",
    "which", "when", "where", "how", "all", "any", "each", "few", "more",
    "most", "other", "some", "such", "no", "not", "only", "own", "same",
    "so", "than", "too", "very", "can", "will", "just", "should", "now",
    "about", "into", "over", "after", "also", "help", "need", "needs",
    "looking", "find", "best", "top", "near", "india", "uk", "us", "uae",
    "services", "service", "solutions", "solution", "company", "companies",
    "business", "businesses", "official", "limited", "ltd", "pvt", "private",
    "www", "http", "https", "com", "org", "net", "product", "products",
})

# Phrases that strongly indicate "what we sell" rather than "who buys"
_OFFERING_HINTS = (
    "consultancy", "consulting", "consultant", "agency", "broker", "brokers",
    "agent", "agents", "realtor", "clinic", "hospital", "coaching", "institute",
    "academy", "firm", "studio", "salon", "dealership", "vendor", "provider",
    "software", "saas", "platform", "tool", "tools", "crm", "erp",
)

# Phrases that strongly indicate buyer / demand-side ICP
_ICP_BUYER_HINTS = (
    "student", "students", "parent", "parents", "buyer", "buyers", "renter",
    "renters", "patient", "patients", "founder", "founders", "startup",
    "startups", "homeowner", "homeowners", "client", "clients", "customer",
    "customers", "aspirant", "aspirants", "applicant", "applicants",
    "looking for", "need help", "hire", "hiring", "want to", "planning to",
)

# Directory category / SERP shell URL patterns (not a single entity profile)
_DIRECTORY_SHELL_PATH = re.compile(
    r"("
    r"/nct-\d+"                          # Justdial category ids
    r"|/category/"
    r"|/categories/"
    r"|/search/"
    r"|/find/"
    r"|/browse/"
    r"|/listings?/"
    r"|/directory/"
    r"|/popular-"
    r"|education-consultants-for-"
    r"|consultants-for-"
    r"|agents-for-"
    r"|brokers-for-"
    r"|near-[a-z0-9-]+$"
    r")",
    re.IGNORECASE,
)

_SHELL_TITLE = re.compile(
    r"(popular\s+.+\s+in\s+|top\s+\d+\s+|list of best|best\s+.+\s+near|"
    r"\d+\+?\s*listings?)",
    re.IGNORECASE,
)


def _norm(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").lower()).strip()


def _tokens(text: str, *, min_len: int = 3) -> set[str]:
    raw = _norm(text)
    if not raw:
        return set()
    # Keep multi-word service phrases as joined tokens when present
    words = re.findall(r"[a-z0-9][a-z0-9\-]{2,}", raw)
    out = {w for w in words if w not in _STOP and len(w) >= min_len}
    # bigrams for service phrases
    for i in range(len(words) - 1):
        a, b = words[i], words[i + 1]
        if a in _STOP or b in _STOP:
            continue
        out.add(f"{a}_{b}")
    return out


def _campaign_field_blob(campaign: Mapping[str, Any], *keys: str) -> str:
    parts: list[str] = []
    for k in keys:
        v = campaign.get(k)
        if isinstance(v, (list, tuple)):
            parts.append(" ".join(str(x) for x in v if x))
        elif v:
            parts.append(str(v))
    se = campaign.get("system_enrichment")
    if isinstance(se, dict):
        for k in ("derived_persona_keywords", "derived_target_angle_hook", "derived_unfair_advantage"):
            if se.get(k):
                parts.append(str(se.get(k)))
    return " ".join(parts)


def extract_offering_and_icp_tokens(campaign: Mapping[str, Any] | None) -> tuple[set[str], set[str]]:
    """Split campaign text into offering (what we sell) vs ICP (who buys)."""
    camp = campaign if isinstance(campaign, Mapping) else {}

    offering_text = _campaign_field_blob(
        camp,
        "effective_bio", "bio", "campaign_focus", "name", "keywords",
        "persona_keywords", "unfair_advantage",
    )
    # Prefer labeled product layer if context_builder style text is present
    bio = str(camp.get("effective_bio") or camp.get("bio") or "")
    if "PRODUCT/SERVICE:" in bio.upper() or "PRODUCT/SERVICE:" in bio:
        for line in bio.split("\n"):
            if "PRODUCT" in line.upper() and "SERVICE" in line.upper():
                offering_text = line + " " + offering_text
                break

    icp_text = _campaign_field_blob(
        camp,
        "persona_name", "persona_bio", "pain_point", "target_angle_hook",
        "persona_targeting_signals", "persona_keywords",
    )
    # Context builder sections
    for line in bio.split("\n"):
        u = line.upper()
        if u.startswith("TARGET ICP") or u.startswith("ICP ") or u.startswith("BUYER PAIN") or u.startswith("INTENT"):
            icp_text = icp_text + " " + line
        if u.startswith("PRODUCT/SERVICE") or u.startswith("MARKET CONTEXT"):
            offering_text = offering_text + " " + line

    offering = _tokens(offering_text)
    icp = _tokens(icp_text)

    # Boost offering with explicit service-provider nouns present in offering text
    ot = _norm(offering_text)
    for hint in _OFFERING_HINTS:
        if hint in ot:
            offering.add(hint.replace(" ", "_"))

    # Boost ICP with buyer nouns
    it = _norm(icp_text)
    for hint in _ICP_BUYER_HINTS:
        if hint in it:
            icp.add(hint.replace(" ", "_"))

    # If ICP is empty, leave it empty — peer detection will rely on offering overlap only
    return offering, icp


def is_directory_shell_url(url: str, *, title: str = "", page_text: str = "") -> bool:
    """True when URL/page is a multi-listing directory SERP, not a single entity profile."""
    u = (url or "").lower()
    if not u:
        return False
    try:
        path = urlparse(u if "://" in u else f"https://{u}").path or ""
    except Exception:
        path = u
    if _DIRECTORY_SHELL_PATH.search(path):
        return True
    blob = f"{title} {page_text[:500]}"
    if _SHELL_TITLE.search(blob):
        # Category SERP language on a portal host
        return True
    return False


def _entity_tokens(
    *,
    company_name: str = "",
    pain_point: str = "",
    text: str = "",
    url: str = "",
) -> set[str]:
    parts = [company_name, pain_point]
    # URL path often encodes category: Education-Consultants-For-Europe
    try:
        path = urlparse(url if "://" in url else f"https://{url}").path
        parts.append(path.replace("-", " ").replace("/", " "))
    except Exception:
        pass
    # First 1500 chars of page for category chips / headings
    parts.append((text or "")[:1500])
    return _tokens(" ".join(parts))


def _jaccard(a: set[str], b: set[str]) -> float:
    if not a or not b:
        return 0.0
    inter = len(a & b)
    if inter == 0:
        return 0.0
    return inter / float(len(a | b))


def _overlap_ratio(entity: set[str], reference: set[str]) -> float:
    """Fraction of reference tokens found in entity (asymmetric recall)."""
    if not reference:
        return 0.0
    if not entity:
        return 0.0
    return len(entity & reference) / float(len(reference))


_PROVIDER_NAME_MARKERS = (
    "consultancy", "consultants", "consultant", "consulting", "agency",
    "agencies", "brokers", "brokerage", "clinic", "hospital", "institute",
    "academy", "coaching", "admissions", "immigration experts", "realtors",
    "services", "solutions", "associates", "group", "pvt", "limited", "ltd",
)


def _looks_like_provider_business(company_name: str) -> bool:
    n = _norm(company_name)
    if not n or n in {"unknown", "none", "n/a"}:
        return False
    return any(m in n for m in _PROVIDER_NAME_MARKERS)


def classify_entity_role(
    campaign: Mapping[str, Any] | None,
    *,
    company_name: str = "",
    pain_point: str = "",
    text: str = "",
    url: str = "",
    primary_strategy: str = "",
) -> dict[str, Any]:
    """Classify an entity as buyer / peer_seller / ambiguous.

    Returns dict with role, scores, and reasons — pure function, no I/O.
    """
    offering, icp = extract_offering_and_icp_tokens(campaign)

    # Company-name-only tokens (ignore Gemini-echoed campaign pain, which inflates ICP)
    company_only = _tokens(company_name)
    # URL path tokens (Justdial category paths encode the service category)
    url_tokens = _entity_tokens(company_name="", pain_point="", text="", url=url)
    page_tokens = _tokens((text or "")[:1200])
    # Pain only if it looks like first-person demand, not generic ICP copy
    pain_n = _norm(pain_point)
    pain_is_demand = any(
        h in pain_n
        for h in (
            "looking for", "need help", "i need", "we need", "anyone",
            "recommend", "suggest", "struggling", "confused", "planning to",
        )
    )
    entity = company_only | url_tokens | page_tokens
    if pain_is_demand:
        entity |= _tokens(pain_point)

    # Overlaps weighted toward company + URL (who they are), not campaign pain echo
    name_offer = _overlap_ratio(company_only | url_tokens, offering)
    name_icp = _overlap_ratio(company_only, icp)
    offer_hit = max(name_offer, _overlap_ratio(entity, offering) * 0.85)
    icp_hit = max(name_icp, _overlap_ratio(entity, icp) * 0.7 if pain_is_demand else name_icp)
    offer_j = _jaccard(company_only | url_tokens, offering)
    icp_j = _jaccard(company_only, icp)

    blob = _norm(f"{pain_point if pain_is_demand else ''} {text[:800]}")
    buyer_lang = any(h in blob for h in (
        "looking for", "need help", "need a", "want to", "recommend",
        "which agency", "anyone know", "struggling", "confused about",
        "planning to study", "planning to buy", "hire", "budget",
        "i am a student", "my son", "my daughter", "need consultant",
    ))
    seller_lang = any(h in blob for h in (
        "we offer", "our services", "contact us", "send enquiry",
        "book a free", "consult now", "admissions open", "we provide",
        "get the list of best",
    ))
    provider_name = _looks_like_provider_business(company_name)
    shell = is_directory_shell_url(url, title=(text or "")[:200], page_text=text)

    role = "ambiguous"
    reasons: list[str] = []

    # Provider business name + offering category match (Justdial peers)
    if provider_name and (name_offer >= 0.12 or offer_hit >= 0.18):
        # Unless company itself is the ICP channel type more than offering
        if name_icp > name_offer + 0.15:
            role = "buyer"
            reasons.append("provider_name_but_icp_channel")
        else:
            role = "peer_seller"
            reasons.append(
                f"provider_name+offering_match name_offer={name_offer:.2f}"
            )

    # Directory shell listing same-service category
    if shell and (name_offer >= 0.10 or offer_hit >= 0.15) and not buyer_lang:
        if role != "buyer":
            role = "peer_seller"
            reasons.append("directory_shell_same_category")

    # Strong peer: offering >> ICP
    if offer_hit >= 0.28 and offer_hit >= icp_hit + 0.08:
        role = "peer_seller"
        reasons.append(f"offering_overlap={offer_hit:.2f}>icp={icp_hit:.2f}")
    elif offer_hit >= 0.40 and icp_hit < 0.18:
        role = "peer_seller"
        reasons.append(f"strong_offering_weak_icp offer={offer_hit:.2f}")
    elif seller_lang and provider_name and not buyer_lang:
        role = "peer_seller"
        reasons.append("seller_language+provider_name")

    # Buyer when demand language is real (not Gemini-copied campaign pain)
    if buyer_lang and pain_is_demand:
        role = "buyer"
        reasons.append("buyer_language")
    if name_icp >= 0.30 and name_icp > name_offer:
        role = "buyer"
        reasons.append(f"name_icp={name_icp:.2f}")

    # PLATFORM_MINING channel: ICP names the entity type (agents/brokers) as target
    strategy = (primary_strategy or "").upper()
    if (
        strategy == "PLATFORM_MINING"
        and role == "peer_seller"
        and name_icp >= 0.22
        and name_icp >= name_offer
    ):
        role = "buyer"
        reasons.append("platform_mining_icp_channel_target")

    return {
        "role": role,
        "offering_overlap": round(offer_hit, 3),
        "icp_overlap": round(icp_hit, 3),
        "name_offering_overlap": round(name_offer, 3),
        "name_icp_overlap": round(name_icp, 3),
        "offering_jaccard": round(offer_j, 3),
        "icp_jaccard": round(icp_j, 3),
        "buyer_language": buyer_lang,
        "seller_language": seller_lang,
        "provider_business_name": provider_name,
        "directory_shell": shell,
        "reasons": reasons,
        "offering_token_count": len(offering),
        "icp_token_count": len(icp),
        "entity_token_count": len(entity),
    }


def apply_peer_seller_gate(
    evaluation: Optional[Mapping[str, Any]],
    *,
    campaign: Mapping[str, Any] | None,
    url: str = "",
    text: str = "",
    primary_strategy: str = "",
    enabled: Optional[bool] = None,
) -> dict[str, Any]:
    """Cap score / flag peer sellers and directory shells.

    Returns a new evaluation dict (does not mutate input) with optional keys:
      peer_seller_gate, peer_seller_blocked, directory_shell_blocked, score (capped)
    """
    if enabled is None:
        enabled = PEER_SELLER_GATE_ENABLED
    ev = dict(evaluation or {})
    if not enabled:
        ev["peer_seller_gate"] = {"enabled": False, "blocked": False}
        return ev

    company = str(ev.get("company_name") or "")
    pain = str(ev.get("pain_point") or ev.get("pain_summary") or "")
    score = 0
    try:
        score = int(float(ev.get("score") or 0))
    except (TypeError, ValueError):
        score = 0

    title_hint = ""
    if text:
        # first non-empty line as title proxy
        for line in text.split("\n")[:5]:
            if line.strip():
                title_hint = line.strip()[:200]
                break

    shell = is_directory_shell_url(url, title=title_hint, page_text=text)
    classification = classify_entity_role(
        campaign,
        company_name=company,
        pain_point=pain,
        text=text,
        url=url,
        primary_strategy=primary_strategy or (
            ((campaign or {}).get("intelligence_strategy") or {}).get("primary")
            if isinstance(campaign, Mapping) else ""
        ),
    )
    # Prefer classifier shell flag when present
    shell = shell or bool(classification.get("directory_shell"))

    blocked = False
    block_reason = ""
    new_score = score

    # A+D: Peer seller hard gate + score cap
    if classification["role"] == "peer_seller":
        blocked = True
        block_reason = "peer_seller_same_offering"
        new_score = min(new_score, PEER_SELLER_SCORE_CAP)
    # B: Directory shell as final lead surface — never high value unless clear buyer
    elif shell and classification["role"] != "buyer":
        blocked = True
        block_reason = "directory_shell_not_buyer"
        new_score = min(new_score, PEER_SELLER_SCORE_CAP)

    # Shell + weak company → always block promotion
    if shell and (not company or company.lower() in {"unknown", "none", "n/a", ""}):
        blocked = True
        block_reason = block_reason or "directory_shell_no_entity"
        new_score = min(new_score, PEER_SELLER_SCORE_CAP)

    # Directory + provider business name with any offering category hit
    if (
        not blocked
        and shell
        and classification.get("provider_business_name")
        and float(classification.get("name_offering_overlap") or 0) >= 0.10
    ):
        blocked = True
        block_reason = "directory_provider_same_category"
        new_score = min(new_score, PEER_SELLER_SCORE_CAP)

    ev["score"] = new_score
    if blocked and new_score < score:
        # Keep original for audit
        ev["score_before_peer_gate"] = score
    ev["peer_seller_gate"] = {
        "enabled": True,
        "blocked": blocked,
        "block_reason": block_reason or None,
        "directory_shell": shell,
        "classification": classification,
        "score_cap": PEER_SELLER_SCORE_CAP if blocked else None,
    }
    ev["peer_seller_blocked"] = blocked
    ev["directory_shell_blocked"] = bool(shell and blocked)
    if blocked:
        # Force confidence-hostile fields so adapter won't invent HIGH tier from score
        prev_reason = str(ev.get("score_reasoning") or "")
        ev["score_reasoning"] = (
            f"PEER_SELLER_GATE: {block_reason}. {prev_reason}"
        )[:500]
        if not str(ev.get("confidence_level") or "").upper() == "SPECULATIVE":
            ev["confidence_level"] = "SPECULATIVE"
    return ev


def filter_peer_seller_entities(
    entities: list,
    *,
    campaign: Mapping[str, Any] | None,
    source_url: str = "",
    page_text: str = "",
    primary_strategy: str = "",
    enabled: Optional[bool] = None,
) -> tuple[list, list]:
    """Split entities into (kept, rejected_peer_sellers).

    Rejected entities are peer sellers of the campaign owner's offering.
    """
    if enabled is None:
        enabled = PEER_SELLER_GATE_ENABLED
    if not enabled or not entities:
        return list(entities or []), []

    kept: list = []
    rejected: list = []
    strategy = primary_strategy or (
        ((campaign or {}).get("intelligence_strategy") or {}).get("primary")
        if isinstance(campaign, Mapping) else ""
    )
    for ent in entities:
        if not isinstance(ent, dict):
            continue
        name = str(ent.get("name") or "")
        note = str(ent.get("extraction_note") or ent.get("role") or "")
        clf = classify_entity_role(
            campaign,
            company_name=name,
            pain_point=note,
            text=page_text[:2000],
            url=source_url,
            primary_strategy=str(strategy or ""),
        )
        if clf["role"] == "peer_seller":
            rejected.append({**ent, "peer_seller_classification": clf})
        else:
            kept.append({**ent, "peer_seller_classification": clf})
    return kept, rejected


def peer_seller_prompt_block() -> str:
    """Universal prompt section for Gemini scoring / pre-filter / extraction."""
    return (
        "\n# PEER SELLER / COMPETITOR EXCLUSION (MANDATORY — OSINT VALUE RULE)\n"
        "The campaign USER BIO describes what the campaign OWNER sells and who they sell to.\n"
        "A GOOD lead is a BUYER (or channel target named in the ICP): someone who would pay "
        "for the owner's product/service, or who matches the stated target persona as a customer.\n"
        "A JUNK lead is a PEER SELLER: a business that commercially offers the SAME or highly "
        "substitutable product/service as the owner (e.g. another education consultancy when "
        "the owner IS an education consultancy; another HVAC firm when the owner sells HVAC).\n"
        "PEER SELLERS on directories (Justdial, Yelp, Yellow Pages, Clutch, etc.) must score 0–2 "
        "or confidence_tier Low — NEVER High/Medium just because the category matches the bio.\n"
        "Category keyword match ≠ buyer intent. 'Popular Education Consultants in X' listing "
        "other consultancies is competitor inventory, not demand.\n"
        "EXCEPTION: If the ICP explicitly targets those providers as CUSTOMERS of a different "
        "product (e.g. we sell CRM software TO real-estate agents), then agents are buyers of "
        "CRM — not peer sellers of CRM. Compare offering vs ICP carefully.\n"
        "Directory category/SERP pages without a single buyer entity must not be treated as leads.\n"
    )
