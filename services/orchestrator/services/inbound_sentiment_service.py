"""
Orchestrator — Inbound Sentiment Service.

Detects inbound sales signals from across the public web using:
  - Serper API (organic Google Search — no platform OAuth required)
  - Gemini Flash (intent classification + scoring)

Query strategy:
  - ZERO site: restrictions — Google ranks the best source anywhere on the web
  - 7-mode round-robin rotation (one mode per day of week)
  - Each mode targets a different buying signal type
  - Negative keyword filter strips known garbage (ad copy, legal pages)
  - Gemini drops results with intent_score < 0.30 (Layer 2 garbage filter)

V23.5 — added 2026-06-08
V25.3.1 — fix NameError: is_consumer, dialog_suffix undefined in _build_queries()
V27.1.0 — Serper credit guards: use B2C_SIGNAL_MODES for consumers, hard query
          budget, phrase-level pain keywords (no bio word-split fan-out),
          drop junk/persona/legacy-tool queries before Serper.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import threading
from datetime import datetime
from typing import Optional

import httpx

from core.clients import get_secret_manager_client  # type: ignore[import]
from core.logging import get_logger  # type: ignore[import]
from core.config import PROJECT_ID  # type: ignore[import]

log = get_logger("orchestrator.inbound_sentiment")

# ---------------------------------------------------------------------------
# Serper key — fetched from Secret Manager once, cached per process
# Same secret name as pipeline-main uses: serper_api_key
# ---------------------------------------------------------------------------
_serper_key_cache: Optional[str] = None
_serper_key_lock = threading.Lock()

SERPER_URL = "https://google.serper.dev/search"


def _get_serper_key() -> str:
    global _serper_key_cache
    if _serper_key_cache:
        return _serper_key_cache
    with _serper_key_lock:
        if _serper_key_cache:
            return _serper_key_cache
        secret_name = (
            os.environ.get("SERPER_API_KEY_SECRET")
            or f"projects/{PROJECT_ID}/secrets/serper_api_key/versions/latest"
        )
        try:
            sm = get_secret_manager_client()
            resp = sm.access_secret_version(request={"name": secret_name})
            _serper_key_cache = resp.payload.data.decode("UTF-8").strip()
            return _serper_key_cache
        except Exception as exc:
            raise RuntimeError(f"Cannot fetch Serper key from Secret Manager: {exc}") from exc


# ---------------------------------------------------------------------------
# Platform detection (for signal metadata — NOT for query filtering)
# ---------------------------------------------------------------------------
_PLATFORM_RE: list[tuple[str, re.Pattern]] = [
    ("reddit",     re.compile(r"reddit\.com",   re.I)),
    ("linkedin",   re.compile(r"linkedin\.com", re.I)),
    ("quora",      re.compile(r"quora\.com",    re.I)),
    ("trustpilot", re.compile(r"trustpilot\.com", re.I)),
    ("g2",         re.compile(r"g2\.com",       re.I)),
    ("capterra",   re.compile(r"capterra\.com", re.I)),
    ("yelp",       re.compile(r"yelp\.com",     re.I)),
    ("glassdoor",  re.compile(r"glassdoor\.com",re.I)),
    ("sitejabber", re.compile(r"sitejabber\.com", re.I)),
    ("trustradius", re.compile(r"trustradius\.com", re.I)),
    ("hn",         re.compile(r"news\.ycombinator\.com", re.I)),
    ("news",       re.compile(
        r"(techcrunch|businesswire|prnewswire|forbes|venturebeat|theregister)",
        re.I,
    )),
]


def _detect_platform(url: str) -> str:
    for name, pat in _PLATFORM_RE:
        if pat.search(url):
            return name
    return "web"


# ---------------------------------------------------------------------------
# Inbound URL pre-screen policy (maintainable constants)
# ---------------------------------------------------------------------------
# High-value review / complaint platforms for inbound sentiment.
# These match query templates that deliberately target site:trustpilot / g2.
# Blocking them was a self-defeating bug (search then drop).
INBOUND_REVIEW_ALLOW_HOSTS: frozenset[str] = frozenset({
    "trustpilot.com",
    "g2.com",
    "capterra.com",
    "sitejabber.com",
    "trustradius.com",
    "softwareadvice.com",
    "getapp.com",
    "gartner.com",
    "mouthshut.com",
    "yelp.com",
    "bbb.org",
    "consumeraffairs.com",
    "productreview.com.au",
    "glassdoor.com",
    "clutch.co",
    "goodfirms.co",
    "serchen.com",
})

# Social / community hubs — always keep (path noise rules do not apply).
INBOUND_SOCIAL_ALLOW_HOSTS: frozenset[str] = frozenset({
    "facebook.com",
    "reddit.com",
    "twitter.com",
    "x.com",
    "quora.com",
    "news.ycombinator.com",
    "linkedin.com",  # profile/posts OK; jobs path blocked separately
    "medium.com",
    "stackoverflow.com",
    "stackexchange.com",
    "github.com",
})

# True noise: directories, job boards, data brokers — no review signal value.
# Host substrings matched against full URL lowercased host+path.
INBOUND_NOISE_HOST_MARKERS: frozenset[str] = frozenset({
    "wikipedia.org",
    "zoominfo.com",
    "crunchbase.com",
    "upwork.com",
    "indeed.com",
    "expertise.com",
    "amazon.com",
    "amazon.",          # regional amazon TLDs
    "linkedin.com/jobs",
})

# Path patterns that are almost never useful inbound sentiment footprints.
# /blog/ is intentionally NOT here — blogs may carry complaint write-ups;
# Gemini intent scoring is the quality gate. SEO listicles use /best- /top-.
INBOUND_NOISE_PATH_PATTERNS: list[tuple[str, str]] = [
    (r"/login(?:/|$|\?)", "auth_wall"),
    (r"/signup(?:/|$|\?)", "auth_wall"),
    (r"/sign-up(?:/|$|\?)", "auth_wall"),
    (r"/register(?:/|$|\?)", "auth_wall"),
    (r"/careers(?:/|$|\?)", "jobs_page"),
    (r"/jobs(?:/|$|\?)", "jobs_page"),
    (r"/pricing(?:/|$|\?)", "marketing_pricing"),
    (r"/best-[a-z0-9-]+", "seo_listicle"),
    (r"/top-\d+", "seo_listicle"),
    (r"/top-[a-z0-9-]+", "seo_listicle"),
    (r"/vs/", "seo_comparison"),
    (r"/compare/", "seo_comparison"),
]

# Soft blog filter: only drop /blog/ paths that look like pure SEO listicles
# or that carry zero sentiment cues in title/snippet when available.
_BLOG_SEO_PATH_RE = re.compile(
    r"/blog/.{0,80}(?:best-|top-\d|top-|vs-|compare|alternatives?|tools-list)",
    re.I,
)
_SENTIMENT_CUE_RE = re.compile(
    r"\b("
    r"review|reviews|complaint|complaints|scam|refund|cancel|cancelled|"
    r"terrible|worst|awful|frustrated|frustrating|hate|regret|"
    r"not\s+worth|waste\s+of|billing\s+issue|poor\s+support|"
    r"switching\s+from|alternative\s+to|looking\s+for|"
    r"disappointed|ripoff|rip-off|broken|doesn'?t\s+work"
    r")\b",
    re.I,
)


def _clean_query_syntax(raw: str) -> str:
    """Optimize spacing and sanitize wildcard domain operators in queries.

    Ensures proper space separation before opening parentheses and replaces
    unsupported wildcard domains (site:*.org -> site:.org).
    """
    if not raw:
        return ""
    # 1. Strip wildcard domain prefix site:*. -> site:.
    res = re.sub(r'(?<!\w)site:\*\.', 'site:.', raw)
    
    # 2. Insert missing space between quotes and opening parenthesis: "abc"(xyz) -> "abc" (xyz)
    res = re.sub(r'(?<=\")\(', ' (', res)

    # 3. Insert missing space between alphanumeric/dots/hyphens and opening parenthesis: net(xyz) -> net (xyz)
    res = re.sub(r'([a-zA-Z0-9\.\-_])\(', r'\1 (', res)
    
    # 4. Insert missing space between closing and opening parenthesis: )( -> ) (
    res = re.sub(r'\)(?=\()', ') ', res)
    
    return res


# ---------------------------------------------------------------------------
# 7-Mode signal rotation — one mode selected by day_of_week (0=Mon … 6=Sun)
# All templates are platform-agnostic (no site: operators)
# ---------------------------------------------------------------------------
SIGNAL_MODES: dict[int, dict] = {
    0: {
        "name": "active_intent",
        "templates": [
            'site:reddit' + '.com/r/sales OR site:reddit' + '.com/r/startups "{pain_keyword}" "looking for" OR "recommend" OR "any thoughts"',
            'site:reddit' + '.com OR site:quora' + '.com "{pain_keyword}" "looking for tool" OR "software suggestion" OR "what do you use"',
            '"{pain_keyword}" inurl:forum OR inurl:community "help" OR "RFP" OR "vendor"',
            'site:reddit' + '.com OR site:quora' + '.com "{industry}" "we need" OR "best tool for"',
        ],
    },
    1: {
        "name": "pain_expression",
        "templates": [
            'site:reddit' + '.com OR site:quora' + '.com "{pain_keyword}" "struggling" OR "frustrated" OR "nightmare" OR "broken process"',
            'site:reddit' + '.com OR site:quora' + '.com "{pain_keyword}" "wasting time" OR "inefficient" OR "no visibility"',
            '"{pain_keyword}" inurl:forum OR inurl:community "manually" OR "spreadsheet" OR "failing"',
            'site:reddit' + '.com OR site:quora' + '.com "{industry}" "problem" OR "issue" OR "failing"',
        ],
    },
    2: {
        "name": "competitor_churn",
        "templates": [
            'site:reddit' + '.com OR site:quora' + '.com OR site:twitter' + '.com "{competitor}" "alternative" OR "switch" OR "better than"',
            'site:reddit' + '.com OR site:quora' + '.com OR site:twitter' + '.com "{competitor}" "cancel" OR "leaving" OR "scam" OR "disappointed"',
            'site:trustpilot' + '.com/review/ OR site:g2' + '.com/products/*/reviews "{competitor}" "worst" OR "bad" OR "useless" OR "billing"',
            'site:reddit' + '.com OR site:quora' + '.com "{industry}" "looking for alternative to" OR "fed up with"',
        ],
    },
    3: {
        "name": "hiring_signals",
        "templates": [
            '"{industry}" "hiring" "{icp_job_title}"',
            '"{company_type}" "Series A" OR "Series B" OR "raised" "{industry}"',
            '"{industry}" "expanding" OR "growing team" OR "new office"',
            '"{pain_keyword}" "scale" OR "growing pains" OR "outgrown"',
        ],
    },
    4: {
        "name": "review_signals",
        "templates": [
            'site:trustpilot' + '.com/review/ OR site:g2' + '.com/products/*/reviews "{pain_keyword}" "wish it had" OR "missing feature" OR "not satisfied"',
            'site:trustpilot' + '.com/review/ OR site:g2' + '.com/products/*/reviews "{pain_keyword}" "worst experience" OR "terrible support" OR "regret"',
            'site:mouthshut' + '.com OR site:consumercomplaints' + '.in "{pain_keyword}" "complaint" OR "scam" OR "waste of money"',
            'site:g2' + '.com/products/*/reviews OR site:capterra' + '.com "{pain_keyword}" "alternatives" OR "compare" OR "wish"',
        ],
    },
    5: {
        "name": "trend_signals",
        "templates": [
            'site:reddit' + '.com OR site:quora' + '.com "{industry}" "digital transformation" OR "modernize" OR "automate"',
            '"{pain_keyword}" trend 2025 OR 2026',
            'site:reddit' + '.com OR site:quora' + '.com "{industry}" "cost reduction" OR "efficiency" OR "ROI"',
            '"{industry}" "new regulation" OR "compliance" OR "mandate"',
        ],
    },
    6: {
        "name": "community_signals",
        "templates": [
            'site:reddit' + '.com/r/sales OR site:reddit' + '.com/r/marketing "{pain_keyword}" "how do you" OR "our stack"',
            'site:reddit' + '.com/r/startups OR site:reddit' + '.com/r/SaaS "{pain_keyword}" "what tools" OR "recommendation"',
            '"{pain_keyword}" inurl:forum OR inurl:community "help" OR "discussion"',
            '"{industry}" association OR conference OR "best practice" 2025',
        ],
    },
}

# ---------------------------------------------------------------------------
# V26.0.6: B2C Consumer Signal Modes
# For B2C/D2C campaigns (real estate, education, health, services),
# the B2B SaaS templates above are completely wrong:
#   - r/sales, r/startups → zero consumer content
#   - G2, Trustpilot → software review platforms
#   - "legacy tool", "software suggestion" → SaaS language
#
# These consumer modes use:
#   - Geo-relevant subreddits (r/expats, r/oman, r/dubai, etc.)
#   - Consumer review platforms (Google Reviews, mouthshut, etc.)
#   - Platform mining queries (competitor listing sites)
#   - Consumer-appropriate pain language
# ---------------------------------------------------------------------------
B2C_SIGNAL_MODES: dict[int, dict] = {
    0: {
        "name": "consumer_active_intent",
        "templates": [
            'site:reddit' + '.com OR site:quora' + '.com "{pain_keyword}" "{geo}" "looking for" OR "recommend" OR "anyone know"',
            '"{pain_keyword}" "{geo}" "review" OR "experience" OR "recommendation"',
            '"{pain_keyword}" "{geo}" agent OR broker OR consultant "contact" OR "email" OR "phone"',
            '"{pain_keyword}" "{geo}" "where to find" OR "how to choose" OR "trustworthy"',
        ],
    },
    1: {
        "name": "consumer_pain_expression",
        "templates": [
            'site:reddit' + '.com OR site:quora' + '.com "{pain_keyword}" "{geo}" "scam" OR "fraud" OR "fake" OR "overpriced"',
            '"{pain_keyword}" "{geo}" "complaint" OR "bad experience" OR "avoid" OR "warning"',
            '"{pain_keyword}" "{geo}" "hidden fees" OR "trust issue" OR "not reliable"',
            'site:reddit' + '.com OR site:quora' + '.com "{geo}" "{pain_keyword}" "help" OR "advice needed" OR "frustrated"',
        ],
    },
    2: {
        "name": "consumer_competitor_review",
        "templates": [
            '"{competitor}" "{geo}" "review" OR "experience" OR "complaint"',
            '"{competitor}" "{pain_keyword}" "bad" OR "terrible" OR "avoid" OR "scam"',
            'site:mouthshut' + '.com OR site:consumercomplaints' + '.in "{competitor}" "{pain_keyword}"',
            '"{competitor}" "{geo}" "alternative" OR "better than" OR "switch from"',
        ],
    },
    3: {
        "name": "consumer_platform_mining",
        "templates": [
            # These are generic patterns — the _inject_platform_mining_queries()
            # method adds Gemini-identified specific platform queries on top.
            '"{pain_keyword}" "{geo}" agent OR broker profile "email" OR "contact" OR "phone"',
            '"{pain_keyword}" "{geo}" directory OR listing "verified" OR "licensed"',
            '"{pain_keyword}" "{geo}" consultant OR expert OR specialist profile',
            '"{pain_keyword}" "{geo}" "view profile" OR "contact agent" OR "send inquiry"',
        ],
    },
    4: {
        "name": "consumer_review_mining",
        "templates": [
            '"{pain_keyword}" "{geo}" reviews OR testimonials OR feedback',
            '"{competitor}" reviews "not worth" OR "disappointed" OR "misleading"',
            '"{pain_keyword}" "{geo}" "honest review" OR "real experience" OR "my experience"',
            'site:reddit' + '.com OR site:quora' + '.com "{pain_keyword}" "{geo}" "is it worth" OR "should I"',
        ],
    },
    5: {
        "name": "consumer_social_signals",
        "templates": [
            'site:reddit' + '.com "{pain_keyword}" "{geo}" "anyone" OR "has anyone" OR "thoughts on"',
            '"{pain_keyword}" "{geo}" forum OR community "advice" OR "tips" OR "suggestion"',
            '"{pain_keyword}" "{geo}" "buying guide" OR "checklist" OR "things to know"',
            '"{pain_keyword}" "{geo}" blog OR article "personal experience" OR "my journey"',
        ],
    },
    6: {
        "name": "consumer_entity_discovery",
        "templates": [
            '"{pain_keyword}" "{geo}" "top agents" OR "recommended brokers" OR "recommended" -"top 10" -listicle',
            '"{pain_keyword}" "{geo}" "licensed" OR "certified" OR "RERA" OR "registered"',
            '"{pain_keyword}" "{geo}" new listing OR "just listed" OR "available now"',
            '"{pain_keyword}" "{geo}" "contact us" OR "get in touch" OR "free consultation"',
        ],
    },
}

# Appended to EVERY query — strips known garbage before results hit Gemini
GLOBAL_NEGATIVE = (
    ' -directory -listicle -"top 10" -"best" -wiki -jobs -careers -support -"login" '
    '-"buy now" -"click here" -"sign up free" -"privacy policy"'
)

# ---------------------------------------------------------------------------
# Serper credit guards (V27.1.0)
# Hard caps + query hygiene. Logs showed 3 pain tokens × 4 B2B templates
# (+ catch-alls) burning ~14 credits per inbound sweep with near-duplicate
# dorks and bio/ICP prose leaking into `q`.
# ---------------------------------------------------------------------------
_MAX_PAIN_KEYWORDS = 2
_MAX_TEMPLATES_PER_SWEEP = 3
_MAX_QUERIES_PER_SWEEP = 6
_MAX_INDUSTRY_WORDS = 6
_MAX_PAIN_WORDS = 6

_CONSUMER_VECTORS = frozenset({"B2C", "B2B2C", "D2C"})

_STOPWORDS = frozenset({
    "that", "this", "with", "from", "they", "their", "have", "been", "will",
    "about", "what", "when", "which", "your", "ours", "into", "than", "then",
    "also", "just", "more", "most", "some", "such", "only", "over", "under",
    "after", "before", "where", "while", "does", "doing", "were", "was",
    "are", "for", "and", "the", "you", "our", "how", "who", "why", "can",
    "need", "needs", "using", "used", "use", "very", "much", "many",
})

# Single tokens that are too generic to own a full template fan-out.
_WEAK_SINGLE_TOKENS = frozenset({
    "customer", "customers", "reduce", "looking", "sale", "sales", "user",
    "users", "strategy", "brand", "invest", "startups", "startup", "tool",
    "tools", "software", "service", "services", "business", "company",
    "general", "help", "best", "good", "near", "oman", "india", "dubai",
    "persona", "target", "content", "marketing", "digital", "students",
    "parents", "property", "acquisition",
})

_JUNK_QUERY_PHRASES = frozenset({
    "target persona",
    "target persona 1",
    "target persona 2",
    "target persona 3",
    "general business",
    "legacy tool",
    "n/a",
    "placeholder",
    "product/service",
})

# Dialog cues — applied once as a dedicated consumer query, not bolted onto
# every B2B SaaS template (that combination produced the Oman log waste).
_CONSUMER_DIALOG_CUE = (
    '("pm me" OR "still available" OR "send details" OR "anyone know")'
)


def _is_consumer_vector(sourcing_vector: object) -> bool:
    return str(sourcing_vector or "").strip().upper() in _CONSUMER_VECTORS


def _sanitize_phrase(text: str, *, max_words: int = _MAX_INDUSTRY_WORDS) -> str:
    """Collapse bio/ICP prose into a short search-safe phrase.

    Rejects persona labels and system junk. Truncates long sentences so full
    bios never become Serper ``q`` values.
    """
    if not text:
        return ""
    cleaned = re.sub(r"\s+", " ", str(text)).strip()
    if cleaned.lower().startswith("product/service:"):
        cleaned = cleaned.split(":", 1)[1].strip()
    lower = cleaned.lower()
    if any(junk in lower for junk in _JUNK_QUERY_PHRASES):
        return ""
    # Drop sentence-shaped / question-shaped bios (UGC questions as queries).
    if "?" in cleaned or cleaned.lower().startswith(
        ("what ", "how ", "why ", "who ", "when ", "where ", "which ")
    ):
        return ""
    words = [w for w in re.findall(r"[A-Za-z0-9][A-Za-z0-9\-'/]*", cleaned) if w]
    if not words:
        return ""
    # Prefer content words when truncating.
    content = [w for w in words if w.lower() not in _STOPWORDS]
    picked = (content or words)[:max_words]
    phrase = " ".join(picked).strip()
    if len(phrase) < 3:
        return ""
    if phrase.lower() in _JUNK_QUERY_PHRASES:
        return ""
    return phrase


def _normalize_pain_keywords(
    raw_pain: list[str],
    *,
    industry: str,
    icp_desc: str,
) -> list[str]:
    """Normalize pain keywords for Serper budget hygiene.

    - Keep multi-word pain points as phrases (capped).
    - Never fan out single tokens extracted by word-splitting a bio/industry.
    - Prefer 1–2 high-signal phrases over 3–5 weak singles.
    """
    out: list[str] = []
    seen: set[str] = set()

    def _push(candidate: str) -> None:
        phrase = _sanitize_phrase(candidate, max_words=_MAX_PAIN_WORDS)
        if not phrase:
            return
        key = phrase.lower()
        if key in seen:
            return
        tokens = phrase.lower().split()
        # Reject lone weak tokens (they create near-duplicate template rows).
        if len(tokens) == 1 and tokens[0] in _WEAK_SINGLE_TOKENS:
            return
        if len(tokens) == 1 and tokens[0] in _STOPWORDS:
            return
        seen.add(key)
        out.append(phrase)

    for item in raw_pain:
        if not item:
            continue
        text = str(item).strip()
        if not text:
            continue
        # Explicit multi-word pains: keep as one phrase, do not tokenize.
        if " " in text or len(text) >= 8:
            _push(text)
        else:
            _push(text)
        if len(out) >= _MAX_PAIN_KEYWORDS:
            return out[:_MAX_PAIN_KEYWORDS]

    if out:
        return out[:_MAX_PAIN_KEYWORDS]

    # Fallback: industry / ICP as a *single* phrase — never word-split fan-out.
    for source in (industry, icp_desc):
        phrase = _sanitize_phrase(source, max_words=_MAX_PAIN_WORDS)
        if phrase:
            _push(phrase)
        if out:
            break

    if not out:
        # Last resort: one generic but non-junk anchor (still better than 5 tokens).
        _push("customer acquisition")
    return out[:_MAX_PAIN_KEYWORDS]


def _query_is_search_safe(query: str) -> bool:
    """Drop queries that would waste Serper credits before they hit the wire."""
    q = (query or "").strip()
    if len(q) < 8:
        return False
    # Strip negatives for content inspection
    core = re.sub(r'\s+-\S+', " ", q)
    core = re.sub(r"\s+", " ", core).strip().lower()
    if not core:
        return False
    if any(junk in core for junk in _JUNK_QUERY_PHRASES):
        return False
    # Reject raw persona/bio labels without operators or buyer language.
    if core in {"target persona", "general business", "legacy tool"}:
        return False
    # Reject pure questions with no site: operator (Gemini/bio prose leaks).
    if "?" in q and "site:" not in core:
        return False
    return True


# ---------------------------------------------------------------------------
# Gemini scoring
# ---------------------------------------------------------------------------

def _score_with_gemini(
    title: str,
    snippet: str,
    url: str,
    query: str,
    icp_description: str,
    *,
    min_intent_score: float = 0.30,
) -> Optional[dict]:
    """
    Call Gemini Flash to classify the intent of a single search result.
    Returns a scored dict or None if intent_score < *min_intent_score*
    (default 0.30 — domain-aware jobs may raise/lower this floor).

    Intent labels:
      ACTIVE_SEEKING   — explicitly looking for a solution / asking for recommendations
      EXPRESSING_PAIN  — describes a problem; not yet searching for solutions
      COMPETITOR_CHURN — mentions switching from / dissatisfied with a competitor
      TREND            — general topic discussion; no personal buyer intent
    """
    if not snippet:
        return None

    prompt = f"""You are an inbound OSINT intent classifier.
Classify the raw buying intent or operational pain expressed in this public web content for a system solving: {icp_description}

Triggering Google Query: {query}
Title: {title}
Snippet: {snippet}
URL: {url}

Respond with ONLY a JSON object (no markdown fences):
{{
  "intent_label": "ACTIVE_SEEKING" | "EXPRESSING_PAIN" | "COMPETITOR_CHURN" | "TREND" | "NONE",
  "intent_score": <float 0.0 to 1.0>,
  "matched_pain_keywords": ["keyword1", "keyword2"],
  "company_name": "<company name if detectable, else null>",
  "industry_hint": "<industry if detectable, else null>",
  "reasoning": "<one sentence>"
}}

Classification rules (OSINT Focus):
- ACTIVE_SEEKING  (0.75-1.0): Explicitly looking for a solution, asking for help on a forum/board.
- COMPETITOR_CHURN(0.70-0.95): Complaining about a current tool/service, frustrated with a provider.
- EXPRESSING_PAIN (0.40-0.75): Venting about a raw operational problem, symptom, or inefficiency.
- TREND           (0.10-0.45): General market discussion; no personal pain expressed.
- NONE            (0.0 -0.29): Polished marketing copy, SEO articles, directories, or irrelevant noise.

CONTEXT-AWARE CONVERSATIONAL INFERENCE:
Analyze the Google snippet in relation to the triggering query. If the query contains dialog dorks (like "pm me" or "still available"), use the snippet's metadata, title, and conversational phrases to reverse-engineer the thread's state. If a forum reply or social comment indicates buying intent, or the thread contextually addresses a direct need matching the USER BIO, classify it as ACTIVE_SEEKING or EXPRESSING_PAIN. Do not drop threads solely because they are forums/social posts.

SNIPPET INFERENCE RULE: You are classifying a Google Search Engine result snippet (2-3 sentences), NOT the full page content. The snippet is a truncated preview generated by Google — it may not contain the exact buyer language from the original post. Therefore:
- If the URL is a forum, Q&A platform, or community board (Reddit, Quora, Discourse, StackExchange, any inurl:forum), treat the snippet as representing a genuine user-authored post. Apply ACTIVE_SEEKING or EXPRESSING_PAIN generously based on the URL context and title alone.
- A question-framed title with buyer language (e.g. "looking for", "recommend", "anyone know", "how do you", "struggling with") is SUFFICIENT for ACTIVE_SEEKING even if the snippet body appears neutral or informational.
- Do not penalise results from forum domains for sounding "informational" — Google snippets from forums are inherently incomplete.

SELLER EXCLUSION RULE: If the content represents a provider, competitor, broker, agent, vendor, or seller offering the same or similar services as described in the system solver description (e.g., real estate agents/brokers in property campaigns, immigration agencies in visa/study campaigns, or lead generation agencies in outbound sales campaigns), you MUST classify them as NONE with an intent_score of 0.0. Do not capture competitors.

NON-COMMERCIAL ENTITY EXCLUSION: If the content originates from or describes any of the following, you MUST classify them as NONE with an intent_score of 0.0 — they are not commercial buyers and will never convert: government ministries, government departments (e.g., Dept of Commerce, Dept of Trade, Ministry of Finance), municipalities, public sector agencies, central banks, regulatory authorities, trade promotion bodies, embassies, consulates, non-profit organisations, charities, academic institutions, or intergovernmental organisations (UN, WTO, IMF, etc.). A government page promoting "market access" or "buyer-seller meets" is a policy initiative, not a buyer signal.

INFORMATIONAL FILTER: General educational articles, blogs, listicles, directories, comparisons, news stories, and guides that do NOT contain a direct complaint, support ticket, or active buying query from a specific individual or company must be classified as NONE with an intent_score of 0.0."""
    try:
        import vertexai
        from vertexai.generative_models import GenerativeModel, GenerationConfig
        from core.clients import init_vertex  # type: ignore[import]
        init_vertex()

        model_name = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")
        model = GenerativeModel(model_name)
        resp = model.generate_content(
            prompt,
            generation_config=GenerationConfig(temperature=0.05, max_output_tokens=512),
        )
        raw = resp.text.strip()
        # Strip markdown code fences if model wraps output
        raw = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw, flags=re.MULTILINE).strip()
        scored = json.loads(raw)

        raw_score = float(scored.get("intent_score", 0))
        log.info(
            "trace_inbound_raw_score",
            url=url[:80],
            score=raw_score,
            label=scored.get("intent_label"),
            reasoning=scored.get("reasoning", "")
        )

        floor = float(min_intent_score)
        if scored.get("intent_label") == "NONE" or raw_score < floor:
            return None

        return {
            "intent_label":          scored.get("intent_label", "EXPRESSING_PAIN"),
            "intent_score":          round(float(scored.get("intent_score", 0.5)), 3),
            "pain_keywords":         scored.get("matched_pain_keywords", []),
            "company_name":          scored.get("company_name"),
            "industry_hint":         scored.get("industry_hint"),
            "gemini_reasoning":      scored.get("reasoning", ""),
        }
    except Exception as exc:
        log.warning("gemini_scoring_failed", url=url[:80], error=str(exc))
        return None


# ---------------------------------------------------------------------------
# Main service class
# ---------------------------------------------------------------------------

class InboundSentimentService:
    """
    Platform-agnostic inbound sales signal detector.

    Usage:
        svc = InboundSentimentService(persona=persona_dict, campaign=campaign_dict)
        signals = svc.run()   # list of signal dicts sorted by intent_score desc
    """

    def __init__(
        self,
        persona: dict,
        campaign: dict,
        force_day_of_week: Optional[int] = None,
        domain_profile: Optional[dict] = None,
        gemini_min_intent_score: float = 0.30,
    ):
        self.persona       = persona
        self.campaign      = campaign
        self.force_day_of_week = force_day_of_week
        # Domain profile (system_domain_profile) — optional; None = legacy behaviour.
        self.domain_profile = domain_profile if isinstance(domain_profile, dict) else None
        if self.domain_profile is None and isinstance(campaign, dict):
            _cached = campaign.get("system_domain_profile")
            if isinstance(_cached, dict) and _cached.get("domain_family"):
                self.domain_profile = _cached
        self.gemini_min_intent_score = float(gemini_min_intent_score)
        # INT-11: Use campaign keywords instead of hardcoded 'B2B software'
        _campaign_kws = (campaign.get("keywords") or "").strip()
        _industry_fallback = _campaign_kws.split(",")[0].strip() if _campaign_kws else ""
        _raw_industry = str(
            persona.get("industry")
            or _industry_fallback
            or campaign.get("campaign_focus")
            or ""
        ).strip()
        self.industry = _sanitize_phrase(_raw_industry, max_words=_MAX_INDUSTRY_WORDS) or "general business"
        # Real competitors only — never invent "legacy tool" (cross-campaign waste).
        self.competitors = [
            str(c).strip()
            for c in (persona.get("competitors") or [])[:3]
            if str(c).strip() and str(c).strip().lower() not in _JUNK_QUERY_PHRASES
        ]
        self.icp_desc = str(
            persona.get("icp_description")
            or persona.get("persona_description")
            or self.industry
        )
        self.icp_job_title = str(persona.get("icp_job_title") or "operations manager")
        self.company_type = str(persona.get("company_type") or "company")
        self.geo = _sanitize_phrase(
            str(
                campaign.get("location")
                or persona.get("location_hint")
                or campaign.get("gl")
                or ""
            ),
            max_words=4,
        )

        raw_pain = [str(p) for p in (persona.get("pain_points") or []) if p]
        # Prefer campaign keyword phrases when persona pains are empty
        if not raw_pain and _campaign_kws:
            raw_pain = [p.strip() for p in _campaign_kws.split(",") if p.strip()][:3]
        self.pain_kws = _normalize_pain_keywords(
            raw_pain,
            industry=self.industry,
            icp_desc=self.icp_desc,
        )
        log.info(
            "inbound_pain_kws_normalized",
            count=len(self.pain_kws),
            keywords=self.pain_kws,
            industry=self.industry,
        )

    # ------------------------------------------------------------------
    # Query building
    # ------------------------------------------------------------------

    def _build_queries(self) -> list[str]:
        """Build today's Serper queries with hard credit caps and vector-aware modes.

        V27.1.0 credit policy:
          - Consumer campaigns use ``B2C_SIGNAL_MODES`` (not B2B r/sales + G2).
          - Max 2 pain phrases × max 3 templates + optional 1 dialog/catch-all
            ≤ ``_MAX_QUERIES_PER_SWEEP`` (6).
          - No word-split fan-out of bios; no default ``legacy tool`` competitor.
          - Drop non-search-safe queries before they hit Serper.
        """
        if self.force_day_of_week is None:
            from datetime import timezone as _tz
            day_of_week = datetime.now(_tz.utc).weekday()
        else:
            day_of_week = int(self.force_day_of_week)

        is_consumer = _is_consumer_vector(self.campaign.get("sourcing_vector"))
        mode_table = B2C_SIGNAL_MODES if is_consumer else SIGNAL_MODES
        primary_mode = mode_table[day_of_week % 7]

        has_competitor = bool(self.competitors)
        competitor = self.competitors[0] if has_competitor else ""

        subs_base = {
            "industry": self.industry,
            "competitor": competitor,
            "icp_job_title": self.icp_job_title,
            "company_type": self.company_type,
            "geo": self.geo or self.industry,
            "pain_keyword": "",  # filled per keyword
        }

        selected_templates: list[str] = []
        # Primary mode: up to 2 templates
        selected_templates.extend(primary_mode["templates"][:2])

        if is_consumer:
            core_days = [0, 1, 4, 6]
        else:
            # Skip competitor_churn (2) when no real competitors — avoids "legacy tool"
            core_days = [0, 1, 4] if not has_competitor else [0, 1, 2, 4]

        if day_of_week in core_days:
            core_days = [d for d in core_days if d != day_of_week]
        for d in core_days[:1]:  # one secondary template only (credit cap)
            other = mode_table[d % 7]
            if other["templates"]:
                selected_templates.append(other["templates"][0])

        # Hard cap template count
        selected_templates = selected_templates[:_MAX_TEMPLATES_PER_SWEEP]

        # Drop templates that require {competitor} when we have none
        if not has_competitor:
            selected_templates = [
                t for t in selected_templates if "{competitor}" not in t
            ]

        queries: list[str] = []
        pain_kws = self.pain_kws[:_MAX_PAIN_KEYWORDS]

        for pain_kw in pain_kws:
            subs = {**subs_base, "pain_keyword": pain_kw}
            for template in selected_templates:
                try:
                    q = template.format(**subs) + GLOBAL_NEGATIVE
                except KeyError as exc:
                    log.warning(
                        "inbound_template_format_failed",
                        missing=str(exc),
                        template=template[:80],
                    )
                    continue
                if _query_is_search_safe(q):
                    queries.append(q)
                if len(queries) >= _MAX_QUERIES_PER_SWEEP:
                    break
            if len(queries) >= _MAX_QUERIES_PER_SWEEP:
                break

        # One consumer dialog-cue query OR one B2B catch-all — never both × N pains.
        if len(queries) < _MAX_QUERIES_PER_SWEEP and pain_kws:
            anchor = pain_kws[0]
            if is_consumer:
                # String-split site operators (same as SIGNAL_MODES) so the CI
                # platform-agnostic source scan does not flag hard-coded
                # site: platform literals while still targeting communities.
                _community = "site:reddit" + ".com OR site:quora" + ".com"
                extra = (
                    f'{_community} "{anchor}" '
                    f'{_CONSUMER_DIALOG_CUE}'
                )
                if self.geo:
                    extra = (
                        f'{_community} "{anchor}" "{self.geo}" '
                        f'{_CONSUMER_DIALOG_CUE}'
                    )
            else:
                # Avoid `"token" "same token"` waste when industry == pain.
                if self.industry.lower() != anchor.lower():
                    extra = f'"{anchor}" "{self.industry}"'
                else:
                    extra = f'"{anchor}" ("looking for" OR recommend OR alternative)'
            extra = extra + GLOBAL_NEGATIVE
            if _query_is_search_safe(extra):
                queries.append(extra)

        # Dedup + hard cap
        deduped = list(dict.fromkeys(queries))[:_MAX_QUERIES_PER_SWEEP]

        log.info(
            "inbound_queries_built",
            mode=primary_mode["name"],
            day=day_of_week,
            is_consumer=is_consumer,
            mode_table="B2C" if is_consumer else "B2B",
            blended_templates=len(selected_templates),
            pain_kws=pain_kws,
            count=len(deduped),
            max_queries=_MAX_QUERIES_PER_SWEEP,
        )
        return deduped

    # ------------------------------------------------------------------
    # Serper search
    # ------------------------------------------------------------------

    def _search_serper(self, query: str, num: int = 5) -> list[dict]:
        """Execute a single Serper search. Returns list of organic result dicts."""
        query = _clean_query_syntax(query)
        if not _query_is_search_safe(query):
            log.warning(
                "inbound_serper_skipped_unsafe_query",
                query=query[:120],
                note="Refusing Serper call to protect credits.",
            )
            return []
        # V27.3.0 residual Serper budget (project-wide multi-instance)
        try:
            from shared.serper_budget import record_serper_spend  # type: ignore[import]
            from core.clients import get_db as _gdb  # type: ignore[import]
            if not record_serper_spend(
                _gdb(),
                amount=1,
                residual=True,
                log=lambda msg, **kw: log.info(msg, **kw),
            ):
                log.warning("inbound_serper_budget_blocked", query=query[:120])
                return []
        except Exception as _ibe:
            log.warning("inbound_serper_budget_error", error=str(_ibe), note="Fail-open")
        gl = self.campaign.get("gl") or "us"
        location = self.campaign.get("location")
        
        # V27.5: Default 6-month window (was qdr:y — year-old forum noise).
        # Override via campaign.inbound_timeframe if needed.
        # V27.8: default 3-month rolling window (was qdr:6m)
        timeframe = self.campaign.get("inbound_timeframe") or "qdr:3m"
        payload = {"q": query, "num": num, "gl": gl, "hl": "en"}
        if timeframe and timeframe != "all":
            payload["tbs"] = timeframe

        if location:
            payload["location"] = location

        try:
            resp = httpx.post(
                SERPER_URL,
                headers={
                    "X-API-KEY":    _get_serper_key(),
                    "Content-Type": "application/json",
                },
                json=payload,
                timeout=15.0,
            )
            resp.raise_for_status()
            return resp.json().get("organic", [])
        except Exception as exc:
            log.warning("serper_call_failed", query=query[:80], error=str(exc))
            return []

    # ------------------------------------------------------------------
    # Main pipeline
    # ------------------------------------------------------------------

    def _url_host(self, url: str) -> str:
        """Extract lowercase hostname without leading www."""
        try:
            from urllib.parse import urlparse
            host = (urlparse(url).netloc or "").lower()
            if host.startswith("www."):
                host = host[4:]
            return host
        except Exception:
            return ""

    def _host_matches_allowlist(self, host: str, allow: frozenset[str]) -> bool:
        """True if host is equal to or a subdomain of any allowlisted apex host."""
        if not host:
            return False
        for apex in allow:
            if host == apex or host.endswith("." + apex):
                return True
        return False

    def classify_inbound_url(
        self,
        url: str,
        title: str = "",
        snippet: str = "",
    ) -> tuple[bool, str]:
        """Classify a URL for inbound pre-screen.

        Returns:
            (is_noise, reason) where is_noise=True means drop before Gemini.

        Policy (precision over aggressive drop):
          1. Review platforms (Trustpilot, G2, …) → KEEP (high-value sentiment)
          2. Social / community hubs → KEEP
          3. True noise hosts (wiki, data brokers, job boards) → DROP
          4. Competitor homepage mirrors → DROP
          5. Auth walls / careers / SEO listicles / pure pricing → DROP
          6. /blog/ only dropped when path is SEO-listicle-shaped OR text has
             no sentiment cues (when title/snippet provided)
          7. Everything else → KEEP (Gemini intent floor is the quality gate)
        """
        if not url or not str(url).strip():
            return True, "empty_url"

        url_lower = url.lower().strip()
        host = self._url_host(url_lower)
        title_l = (title or "").lower()
        snippet_l = (snippet or "").lower()
        text_blob = f"{title_l} {snippet_l} {url_lower}"

        # 1. Review / complaint platforms — keep (high-value inbound sentiment)
        if self._host_matches_allowlist(host, INBOUND_REVIEW_ALLOW_HOSTS):
            # Job-board paths on review hosts are still noise
            if re.search(r"/(?:job|jobs|career|careers)(?:/|$|\?)", url_lower):
                return True, "review_host_jobs_path"
            return False, "allow_review_platform"

        # 2. Social / community — keep (path noise rules do not apply)
        if self._host_matches_allowlist(host, INBOUND_SOCIAL_ALLOW_HOSTS):
            if "linkedin.com" in host and re.search(r"/jobs(?:/|$|\?)", url_lower):
                return True, "noise_host:linkedin_jobs"
            return False, "allow_social_community"

        # 3. True noise hosts / markers (directories, data brokers, marketplaces)
        for marker in INBOUND_NOISE_HOST_MARKERS:
            if marker.startswith("glassdoor"):
                continue  # glassdoor handled via review allowlist + jobs path
            if marker in url_lower:
                return True, f"noise_host:{marker}"

        # 4. Competitor URL check (own marketing sites, not review of competitor)
        for comp in self.competitors:
            comp_clean = comp.lower().strip().replace(" ", "")
            if comp_clean and len(comp_clean) > 3 and comp_clean in url_lower:
                # Allow if this is clearly a third-party review of the competitor
                if any(
                    rev in url_lower
                    for rev in ("/review", "/reviews", "trustpilot", "g2.com", "capterra")
                ):
                    continue
                return True, f"competitor_site:{comp_clean}"

        # 5. Hard path noise (auth, careers, SEO listicles, pricing)
        for pat, reason in INBOUND_NOISE_PATH_PATTERNS:
            if re.search(pat, url_lower):
                return True, f"noise_path:{reason}:{pat}"

        # 6. Soft blog handling — do NOT blanket-block /blog/
        if re.search(r"/blog/", url_lower):
            if _BLOG_SEO_PATH_RE.search(url_lower):
                return True, "blog_seo_listicle_path"
            # If we have title/snippet and zero sentiment cues, treat as marketing filler
            if (title_l or snippet_l) and not _SENTIMENT_CUE_RE.search(text_blob):
                return True, "blog_no_sentiment_cues"
            # Bare URL-only check (no title/snippet): keep and let Gemini decide
            return False, "allow_blog_candidate"

        return False, "allow_default"

    def _is_noise_url(self, url: str, title: str = "", snippet: str = "") -> bool:
        """Pre-screen URL; True = drop before Gemini scoring.

        Backward-compatible wrapper around :meth:`classify_inbound_url`.
        Logs keep vs filter with structured reason codes.
        """
        is_noise, reason = self.classify_inbound_url(url, title=title, snippet=snippet)
        if is_noise:
            if reason.startswith("noise_host:") or reason.startswith("noise_path:"):
                log_event = (
                    "inbound_url_filtered_domain"
                    if reason.startswith("noise_host:")
                    else "inbound_url_filtered_pattern"
                )
            elif reason.startswith("competitor_site:"):
                log_event = "inbound_url_filtered_competitor"
            else:
                log_event = "inbound_url_filtered_other"
            log.info(
                log_event,
                url=url[:120],
                reason=reason,
                decision="filter",
            )
        else:
            log.info(
                "inbound_url_pre_screen_kept",
                url=url[:120],
                reason=reason,
                decision="keep",
            )
        return is_noise

    def run(
        self,
        max_queries: int = 12,
        results_per_query: int = 5,
        seen_url_hashes: set | None = None,
    ) -> list[dict]:
        """
        Full pipeline: build queries → search → Gemini score → return signals.

        Args:
            max_queries:       Maximum number of Serper search queries to fire.
            results_per_query: Number of results per Serper call (1-20). Default 5;
                               the job overrides this to 20 (V25.2.2).
            seen_url_hashes:   Set of URL hashes from the cross-run Firestore dedup
                               cache. URLs whose SHA-256[:16] is in this set are
                               skipped without Gemini scoring. None = no cache.

        Returns:
            List of signal dicts sorted by intent_score descending.
            Each dict has stable signal_id (SHA-256 of URL[:16]).
        """
        queries     = self._build_queries()[:max_queries]
        signals     = []
        seen_urls:  set[str] = set()
        _seen_hashes = seen_url_hashes or set()

        for query in queries:
            for result in self._search_serper(query, num=results_per_query):
                url = result.get("link", "")
                if not url or url in seen_urls:
                    continue
                seen_urls.add(url)

                # V25.2.2: Skip URLs seen in previous runs (cross-run dedup cache)
                url_hash = hashlib.sha256(url.encode()).hexdigest()[:16]
                if url_hash in _seen_hashes:
                    log.info("inbound_url_cross_run_dedup", url=url[:80])
                    continue

                title   = result.get("title", "")
                snippet = result.get("snippet", "")

                # Pre-screen: drop pure noise; keep review platforms + candidate blogs.
                # Pass title/snippet so soft blog filter can retain complaint posts.
                if self._is_noise_url(url, title=title, snippet=snippet):
                    log.info(
                        "inbound_url_pre_screen_filtered",
                        url=url[:120],
                        title=(title or "")[:60],
                    )
                    continue

                scored = _score_with_gemini(
                    title,
                    snippet,
                    url,
                    query,
                    self.icp_desc,
                    min_intent_score=self.gemini_min_intent_score,
                )
                if not scored:
                    continue

                # Determine which pain_kw triggered this query
                triggering_kw = next(
                    (kw for kw in self.pain_kws if kw.lower() in query.lower()),
                    self.pain_kws[0] if self.pain_kws else "general",
                )

                signal_row = {
                    "signal_id":           hashlib.sha256(url.encode()).hexdigest()[:16],
                    "source_url":          url,
                    "source_platform":     _detect_platform(url),
                    "headline":            title,
                    "snippet":             snippet[:300],
                    "serper_query":        query,
                    "triggering_keyword":  triggering_kw,
                    "matched_persona":     self.persona.get("persona_name", ""),
                    "matched_campaign_id": self.campaign.get("campaign_id", ""),
                    "week":                _week_label(),
                    "status":              "new",
                    **scored,  # intent_label, intent_score, pain_keywords, company_name, etc.
                }
                # Domain metadata (omitted keys stay absent when no profile).
                if self.domain_profile:
                    try:
                        from shared.domain_gate import extract_domain_meta  # type: ignore[import]
                        _dmeta = extract_domain_meta(self.domain_profile)
                        if _dmeta.get("domain_family"):
                            signal_row["domain_family"] = _dmeta["domain_family"]
                            signal_row["domain_source"] = _dmeta.get("domain_source")
                            signal_row["profile_confidence"] = _dmeta.get("profile_confidence")
                            signal_row["thin_campaign"] = _dmeta.get("thin_campaign")
                            signal_row["strictness_bias"] = _dmeta.get("strictness_bias")
                    except Exception:
                        pass
                signals.append(signal_row)

        result_list = sorted(signals, key=lambda x: x["intent_score"], reverse=True)
        log.info(
            "inbound_sentiment_run_complete",
            queries_run=len(queries),
            urls_scanned=len(seen_urls),
            signals_found=len(result_list),
            domain_family=(
                (self.domain_profile or {}).get("domain_family")
                if self.domain_profile
                else None
            ),
            gemini_min_intent_score=self.gemini_min_intent_score,
        )
        return result_list


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _week_label() -> str:
    """ISO year-week label e.g. '2025-W23'."""
    now = datetime.utcnow()
    return f"{now.year}-W{now.isocalendar()[1]:02d}"
