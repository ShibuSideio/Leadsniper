"""Universal peer-seller gate — domain-agnostic OSINT value filter."""
import importlib.util
import os
import sys
from pathlib import Path

PIPELINE = Path(__file__).resolve().parents[2] / "pipeline-main"
if str(PIPELINE) not in sys.path:
    sys.path.insert(0, str(PIPELINE))


def _load():
    path = PIPELINE / "services" / "peer_seller_gate.py"
    spec = importlib.util.spec_from_file_location("services.peer_seller_gate", path)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    # Ensure package parent exists for imports if needed
    sys.modules["services.peer_seller_gate"] = mod
    spec.loader.exec_module(mod)
    return mod


def test_justdial_education_peers_blocked():
    """Screenshot scenario: education consultancy tenant + Justdial peer consultancies."""
    gate = _load()
    campaign = {
        "name": "Atlanta Overseas Education",
        "bio": "We help students study abroad in UK and Europe. Education consultancy.",
        "effective_bio": "PRODUCT/SERVICE: Overseas education consultancy for UK and Europe",
        "keywords": "study abroad, UK admission, Europe visa, education consultants",
        "pain_point": "students need guidance for studying in Europe",
        "persona_name": "Aspiring students and parents",
        "persona_bio": "Students seeking overseas education counselling",
        "intelligence_strategy": {"primary": "PLATFORM_MINING"},
    }
    url = (
        "https://www.justdial.com/Vizianagaram/"
        "Education-Consultants-For-Europe-in-Vizianagaram-Collectorate/nct-10180086"
    )
    evaluation = {
        "score": 9,
        "company_name": "PVK Educational Consultancy",
        "pain_point": "Students need guidance for studying in Europe",
        "confidence_level": "HIGH",
    }
    text = (
        "Popular Education Consultants For Europe in Vizianagaram. "
        "PVK Educational Consultancy. Overseas Education Consultants. "
        "Get the list of best Education Consultants For Europe. Send Enquiry."
    )
    out = gate.apply_peer_seller_gate(
        evaluation,
        campaign=campaign,
        url=url,
        text=text,
        primary_strategy="PLATFORM_MINING",
    )
    assert out["peer_seller_blocked"] is True, out.get("peer_seller_gate")
    assert int(out["score"]) <= gate.PEER_SELLER_SCORE_CAP
    assert int(out.get("score_before_peer_gate") or 9) == 9
    role = out["peer_seller_gate"]["classification"]["role"]
    assert role in ("peer_seller",) or out["directory_shell_blocked"]


def test_immigration_experts_peer_blocked():
    gate = _load()
    campaign = {
        "bio": "Education and immigration consultancy for UK study visas",
        "effective_bio": "Study abroad and immigration consulting services",
        "keywords": "UK education consultants, immigration, IELTS, student visa",
        "pain_point": "students need UK education guidance",
        "persona_bio": "Students looking for UK admission help",
    }
    out = gate.apply_peer_seller_gate(
        {
            "score": 8,
            "company_name": "Immigration Experts",
            "pain_point": "Students need guidance for studying in UK",
        },
        campaign=campaign,
        url="https://www.justdial.com/Delhi/Education-Consultants-For-UK-in-Shivalik-Malviya-Nagar/nct-10180270",
        text="Popular Education Consultants For UK. Immigration Experts. Verified. Send Enquiry.",
        primary_strategy="PLATFORM_MINING",
    )
    assert out["peer_seller_blocked"] is True
    assert int(out["score"]) <= 2


def test_bayut_agent_allowed_when_icp_is_agents():
    """PLATFORM_MINING realty: agents are channel/ICP targets, not peer of 'connecting buyers'."""
    gate = _load()
    campaign = {
        "name": "Oman Realty",
        "bio": "We connect property buyers with verified real estate agents in Muscat",
        "effective_bio": "PRODUCT/SERVICE: Buyer-agent matching for Oman property purchases",
        "keywords": "property agent, broker, villa, apartment, Muscat",
        "pain_point": "buyers struggle to find trusted agents",
        "persona_name": "Property agents and brokers",
        "persona_bio": "Licensed real estate agents and brokers with local listings",
        "intelligence_strategy": {"primary": "PLATFORM_MINING"},
    }
    out = gate.apply_peer_seller_gate(
        {
            "score": 8,
            "company_name": "Ahmed Al Balushi",
            "pain_point": "Property agent specializing in villas for sale in Muscat",
        },
        campaign=campaign,
        url="https://www.bayut.com/brokers/ahmed-123.html",
        text="Ahmed Al Balushi - Property Agent in Muscat. Contact for villa viewings.",
        primary_strategy="PLATFORM_MINING",
    )
    # Should NOT be blocked as peer when ICP is agents
    assert out.get("peer_seller_blocked") is False, out.get("peer_seller_gate")
    assert int(out["score"]) == 8


def test_directory_shell_without_entity_blocked():
    gate = _load()
    campaign = {
        "bio": "HVAC repair services for homeowners",
        "effective_bio": "Air conditioning repair and maintenance",
        "persona_bio": "Homeowners with broken AC units",
    }
    out = gate.apply_peer_seller_gate(
        {"score": 7, "company_name": "Unknown", "pain_point": "Unknown"},
        campaign=campaign,
        url="https://www.justdial.com/Delhi/AC-Repair-Services/nct-12345",
        text="Popular AC Repair Services in Delhi. Get the list of best. 50+ listings.",
        primary_strategy="COLLOQUIAL_DISCOVERY",
    )
    assert out["peer_seller_blocked"] is True
    assert out.get("directory_shell_blocked") or out["peer_seller_gate"].get("directory_shell")


def test_true_buyer_pain_not_blocked():
    gate = _load()
    campaign = {
        "bio": "Overseas education consultancy UK and Europe",
        "effective_bio": "Study abroad consultancy",
        "keywords": "UK admission, student visa",
        "pain_point": "students confused about UK universities",
        "persona_bio": "Students and parents seeking study abroad help",
    }
    out = gate.apply_peer_seller_gate(
        {
            "score": 8,
            "company_name": "Unknown",
            "pain_point": "Looking for a good education consultant for UK admission, need help with visa",
        },
        campaign=campaign,
        url="https://www.reddit.com/r/IndiaCareers/comments/abc/need_uk_consultant/",
        text="Looking for a good education consultant for UK. Anyone recommend? Need help with visa and IELTS.",
        primary_strategy="COLLOQUIAL_DISCOVERY",
    )
    assert out.get("peer_seller_blocked") is False, out.get("peer_seller_gate")
    assert int(out["score"]) == 8


def test_filter_entities_removes_peers():
    gate = _load()
    campaign = {
        "bio": "Education consultancy for Europe admissions",
        "effective_bio": "Overseas education consultants",
        "persona_bio": "Students seeking Europe study guidance",
    }
    entities = [
        {"name": "PVK Educational Consultancy", "relevance_score": 90},
        {"name": "Ravi Kumar", "relevance_score": 80, "extraction_note": "student looking for Europe options"},
    ]
    kept, rejected = gate.filter_peer_seller_entities(
        entities,
        campaign=campaign,
        source_url="https://www.justdial.com/x/Education-Consultants-For-Europe/nct-1",
        page_text="Popular Education Consultants. PVK Educational Consultancy. Overseas Education.",
        primary_strategy="PLATFORM_MINING",
    )
    names_kept = {e["name"] for e in kept}
    names_rej = {e["name"] for e in rejected}
    assert "PVK Educational Consultancy" in names_rej or "PVK Educational Consultancy" not in names_kept
