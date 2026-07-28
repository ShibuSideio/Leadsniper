import os
import sys
import json
import unittest
from unittest.mock import patch, MagicMock

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)
_PIPELINE_ROOT = os.path.join(os.path.dirname(_HERE), "pipeline-main")
_PIPELINE_SERVICES = os.path.join(_PIPELINE_ROOT, "services")
if _PIPELINE_SERVICES not in sys.path:
    sys.path.insert(0, _PIPELINE_SERVICES)
if _PIPELINE_ROOT not in sys.path:
    sys.path.insert(0, _PIPELINE_ROOT)

# Mock services.telemetry in sys.modules to prevent path resolution failures when testing pipeline modules
sys.modules["services.telemetry"] = MagicMock()

from services.inbound_sentiment_service import InboundSentimentService
from services.inbound_maps_service import InboundMapsService
from gemini_service import pre_filter_gemini


class TestInboundRadarRemedies(unittest.TestCase):

    def test_url_pre_screen_filter(self):
        """Inbound pre-screen keeps review platforms; drops pure noise only."""
        svc = InboundSentimentService(
            persona={"competitors": ["competitorone", "competitor two"]},
            campaign={}
        )

        # Review platforms — HIGH VALUE for inbound sentiment (must KEEP)
        self.assertFalse(svc._is_noise_url(
            "https://www.trustpilot.com/review/acme-crm.com"
        ))
        self.assertFalse(svc._is_noise_url(
            "https://www.g2.com/products/competitor/reviews"
        ))
        self.assertFalse(svc._is_noise_url("https://yelp.com/biz/local-business"))
        self.assertFalse(svc._is_noise_url("https://capterra.com/reviews/123"))
        self.assertFalse(svc._is_noise_url(
            "https://www.sitejabber.com/reviews/example.com"
        ))
        self.assertFalse(svc._is_noise_url(
            "https://www.trustradius.com/products/acme/reviews"
        ))

        # True noise: data brokers, job boards, auth, SEO listicles
        self.assertTrue(svc._is_noise_url("https://www.zoominfo.com/c/acme"))
        self.assertTrue(svc._is_noise_url("https://www.wikipedia.org/wiki/CRM"))
        self.assertTrue(svc._is_noise_url("https://example.com/best-lead-generation-tools"))
        self.assertTrue(svc._is_noise_url("https://example.com/top-10-alternatives"))
        self.assertTrue(svc._is_noise_url("https://example.com/vs/competitor"))
        self.assertTrue(svc._is_noise_url("https://example.com/compare/competitor"))
        self.assertTrue(svc._is_noise_url("https://example.com/pricing"))
        self.assertTrue(svc._is_noise_url("https://example.com/login"))
        self.assertTrue(svc._is_noise_url("https://example.com/careers/sdr-opening"))

        # Competitor own sites — still filtered
        self.assertTrue(svc._is_noise_url("https://competitorone.com"))
        self.assertTrue(svc._is_noise_url("https://competitortwo.com/features"))

        # Valid footprint URLs (forums, social, github)
        self.assertFalse(svc._is_noise_url(
            "https://nicheforum.net/threads/123-issues-with-billing"
        ))
        self.assertFalse(svc._is_noise_url("https://github.com/org/repo/issues/456"))
        self.assertFalse(svc._is_noise_url("https://reddit.com/r/sales/comments/xyz"))
        self.assertFalse(svc._is_noise_url(
            "https://www.facebook.com/groups/muscat/posts/123456"
        ))

        # Blogs: complaint content kept; pure SEO blog listicle dropped
        self.assertFalse(svc._is_noise_url(
            "https://example.com/blog/our-billing-nightmare",
            title="We regret switching — terrible billing support",
            snippet="Worst experience with refunds and cancellations",
        ))
        self.assertTrue(svc._is_noise_url(
            "https://example.com/blog/best-crm-tools-2026",
            title="Best CRM tools of 2026",
            snippet="Our roundup of top software",
        ))
        # Bare blog URL without title still allowed (Gemini is quality gate)
        self.assertFalse(svc._is_noise_url("https://example.com/blog/customer-story"))

    def test_classify_inbound_url_reasons(self):
        svc = InboundSentimentService(persona={}, campaign={})
        is_noise, reason = svc.classify_inbound_url(
            "https://www.trustpilot.com/review/foo.com"
        )
        self.assertFalse(is_noise)
        self.assertEqual(reason, "allow_review_platform")

        is_noise, reason = svc.classify_inbound_url("https://example.com/login")
        self.assertTrue(is_noise)
        self.assertIn("auth_wall", reason)
    @patch("gemini_service.call_gemini_2_5")
    def test_pre_filter_gemini_fallback(self, mock_call):
        """Verify that pre_filter_gemini falls back to python-based heuristic on exception."""
        mock_call.side_effect = Exception("Vertex AI Timeout")
        
        snippets = [
            {"link": "https://nicheforum.net/help/123", "title": "Help", "snippet": "Need alternative"},
            {"link": "https://g2.com/products/reviews", "title": "G2 Reviews", "snippet": "Spam reviews"},
            {"link": "https://example.com/blog/listicle", "title": "Blog", "snippet": "SEO post"},
            {"link": "https://github.com/org/repo/issues/1", "title": "Issue", "snippet": "Bug report"}
        ]
        
        res = pre_filter_gemini(snippets, "B2B SaaS", "US")
        
        # High tier should only contain the niche forum and the github issue
        self.assertIn("https://nicheforum.net/help/123", res["High"])
        self.assertIn("https://github.com/org/repo/issues/1", res["High"])
        # Obvious directories and blogs should be dropped
        self.assertNotIn("https://g2.com/products/reviews", res["High"])
        self.assertNotIn("https://example.com/blog/listicle", res["High"])

    @patch("services.inbound_maps_service.httpx.post")
    @patch("services.inbound_maps_service._get_serper_key")
    def test_fetch_place_reviews(self, mock_key, mock_post):
        """Verify reviews retrieval client parses Serper Reviews response correctly."""
        mock_key.return_value = "mock-key"
        
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "reviews": [
                {"name": "Alice", "rating": 1, "text": "Terrible customer support"},
                {"name": "Bob", "rating": 5, "text": "I loved it"}
            ]
        }
        mock_post.return_value = mock_resp
        
        svc = InboundMapsService(persona={}, campaign={})
        reviews = svc._fetch_place_reviews("mock-cid")
        
        self.assertEqual(len(reviews), 2)
        self.assertEqual(reviews[0]["name"], "Alice")
        self.assertEqual(reviews[0]["rating"], 1)

    @patch("services.inbound_sentiment_service.httpx.post")
    @patch("services.inbound_sentiment_service._get_serper_key")
    def test_inbound_sentiment_service_timeframe(self, mock_key, mock_post):
        """Verify Serper payload contains appropriate tbs date filters."""
        mock_key.return_value = "mock-key"
        
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"organic": []}
        mock_post.return_value = mock_resp
        
        # 1. Default timeframe should be qdr:3m (V27.8 — was qdr:6m / qdr:y)
        svc = InboundSentimentService(persona={}, campaign={})
        svc._search_serper("test query")
        
        args, kwargs = mock_post.call_args
        payload = kwargs.get("json", {})
        self.assertEqual(payload.get("tbs"), "qdr:3m")
        
        # 2. Overridden timeframe should propagate
        svc_custom = InboundSentimentService(persona={}, campaign={"inbound_timeframe": "qdr:m"})
        svc_custom._search_serper("test query")
        
        args, kwargs = mock_post.call_args
        payload = kwargs.get("json", {})
        self.assertEqual(payload.get("tbs"), "qdr:m")

        # 3. Timeframe "all" should omit tbs
        svc_all = InboundSentimentService(persona={}, campaign={"inbound_timeframe": "all"})
        svc_all._search_serper("test query")
        
        args, kwargs = mock_post.call_args
        payload = kwargs.get("json", {})
        self.assertNotIn("tbs", payload)

    @patch("serper_service.httpx.post")
    @patch("serper_service._get_serper_api_key")
    def test_pipeline_serper_service_timeframe(self, mock_key, mock_post):
        """Verify pipeline search_serper applies tbs date filters conditionally."""
        mock_key.return_value = "mock-key"
        
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"organic": []}
        mock_post.return_value = mock_resp
        
        from serper_service import search_serper
        
        # V27.8: B2C and B2B colloquial both use qdr:3m
        search_serper("test query", sourcing_vector="B2C")
        args, kwargs = mock_post.call_args
        payload = json.loads(kwargs.get("data", "{}"))
        self.assertEqual(payload.get("tbs"), "qdr:3m")

        search_serper("test query", sourcing_vector="B2B")
        args, kwargs = mock_post.call_args
        payload = json.loads(kwargs.get("data", "{}"))
        self.assertEqual(payload.get("tbs"), "qdr:3m")

        # Social site: also 3m (not evergreen)
        search_serper("site:reddit.com AI startup Asia", sourcing_vector="B2B")
        args, kwargs = mock_post.call_args
        payload = json.loads(kwargs.get("data", "{}"))
        self.assertEqual(payload.get("tbs"), "qdr:3m")

    def test_inbound_sentiment_service_dialog_cue_dorking(self):
        """B2C gets consumer dialog cues (once); B2B does not. V27.1.0."""
        # 1. B2C campaign — dialog cues on dedicated consumer query, not B2B SaaS
        svc_b2c = InboundSentimentService(
            persona={"pain_points": ["rent villa"]},
            campaign={"sourcing_vector": "B2C", "location": "Muscat", "gl": "om"},
            force_day_of_week=0,
        )
        queries_b2c = svc_b2c._build_queries()
        self.assertTrue(len(queries_b2c) > 0)
        self.assertLessEqual(len(queries_b2c), 6, "hard Serper budget cap")
        joined_b2c = " | ".join(queries_b2c)
        self.assertIn("pm me", joined_b2c)
        self.assertIn("still available", joined_b2c)
        # Must NOT use B2B SaaS inventory for consumer campaigns
        self.assertNotIn("r/sales", joined_b2c)
        self.assertNotIn("looking for tool", joined_b2c)
        self.assertNotIn("software suggestion", joined_b2c)
        self.assertNotIn("legacy tool", joined_b2c)

        # 2. B2B campaign — no consumer dialog cues
        svc_b2b = InboundSentimentService(
            persona={"pain_points": ["customer acquisition cost"]},
            campaign={"sourcing_vector": "B2B"},
            force_day_of_week=0,
        )
        queries_b2b = svc_b2b._build_queries()
        self.assertTrue(len(queries_b2b) > 0)
        self.assertLessEqual(len(queries_b2b), 6)
        for q in queries_b2b:
            self.assertNotIn("pm me", q)
            self.assertNotIn("still available", q)
            self.assertNotIn("legacy tool", q)

    def test_inbound_no_bio_word_split_fanout(self):
        """Industry/ICP prose must not explode into single-token pain keywords."""
        svc = InboundSentimentService(
            persona={
                "pain_points": [],
                "industry": "reduce customer acquisition cost for B2B SaaS",
                "icp_description": "We help reduce customer acquisition cost",
            },
            campaign={"sourcing_vector": "B2B", "keywords": ""},
            force_day_of_week=1,
        )
        # Phrase-level pains only — not customer/reduce/acquisition as three keys
        self.assertLessEqual(len(svc.pain_kws), 2)
        for kw in svc.pain_kws:
            self.assertNotEqual(kw.lower(), "customer")
            self.assertNotEqual(kw.lower(), "reduce")
            self.assertNotEqual(kw.lower(), "acquisition")
        queries = svc._build_queries()
        self.assertLessEqual(len(queries), 6)
        joined = " | ".join(queries)
        self.assertNotIn("legacy tool", joined)

    def test_inbound_b2c_uses_consumer_mode_table(self):
        """Oman-style B2C must not emit G2/SaaS templates."""
        svc = InboundSentimentService(
            persona={
                "pain_points": ["property for sale Oman", "villa rent Muscat"],
            },
            campaign={
                "sourcing_vector": "B2C",
                "gl": "om",
                "location": "Muscat Oman",
            },
            force_day_of_week=0,
        )
        queries = svc._build_queries()
        self.assertTrue(queries)
        self.assertLessEqual(len(queries), 6)
        joined = " | ".join(queries).lower()
        self.assertNotIn("g2.com", joined)
        self.assertNotIn("r/sales", joined)
        self.assertNotIn("r/startups", joined)
        self.assertNotIn("software suggestion", joined)

    def test_inbound_maps_skips_bio_as_near_me(self):
        """Maps must not search full bio / persona labels as 'near me'."""
        svc = InboundMapsService(
            persona={"competitors": [], "industry": ""},
            campaign={
                "bio": "What are the best examples of user generated content campaigns?",
                "effective_bio": "Target Persona",
                "campaign_focus": "",
                "keywords": "",
            },
        )
        queries = svc._build_queries()
        # Question bio + Target Persona must not produce Maps queries
        self.assertEqual(queries, [])

    def test_inbound_maps_uses_short_industry(self):
        svc = InboundMapsService(
            persona={"competitors": [], "industry": "Oman Realty agents and brokers"},
            campaign={"gl": "om"},
        )
        queries = svc._build_queries()
        self.assertEqual(len(queries), 1)
        self.assertTrue(queries[0].endswith("near me"))
        # Truncated — not a full paragraph
        self.assertLessEqual(len(queries[0].split()), 6)

    @patch("vertexai.generative_models.GenerativeModel")
    @patch("core.clients.init_vertex")
    def test_inbound_sentiment_service_context_aware_llm(self, mock_init, mock_model_cls):
        """Verify that _score_with_gemini includes query context in prompt instructions."""
        mock_model = MagicMock()
        mock_model_cls.return_value = mock_model
        mock_resp = MagicMock()
        mock_resp.text = '{"intent_label": "ACTIVE_SEEKING", "intent_score": 0.9, "matched_pain_keywords": [], "company_name": null, "industry_hint": null, "reasoning": "Fits search query"}'
        mock_model.generate_content.return_value = mock_resp
        
        from services.inbound_sentiment_service import _score_with_gemini
        
        res = _score_with_gemini("Test Title", "Test Snippet", "https://example.com", "Oman villa pm me", "ICP description")
        
        self.assertIsNotNone(res)
        self.assertEqual(res["intent_label"], "ACTIVE_SEEKING")
        args, kwargs = mock_model.generate_content.call_args
        prompt = args[0]
        self.assertIn("Triggering Google Query: Oman villa pm me", prompt)
        self.assertIn("CONTEXT-AWARE CONVERSATIONAL INFERENCE", prompt)


if __name__ == "__main__":
    unittest.main()
