# LeadGenie — Full Inconsistency & Scalability Audit Register

| Field | Value |
|-------|--------|
| **Audit date** | 2026-07-20 |
| **Last remediation** | **V27.4.0 deferred cutover** — queue dual-path SSOT (hybrid default; subcollection mode), agent/DT residual Serper budget |
| **Scope** | Codebase + architecture docs + Firestore schema/indexes/rules as declared in repo |
| **Method** | Static analysis of services, `public/app.js`, `architecture.md`, `firestore.indexes.json`, `firestore.rules` |
| **Live Firestore / prod env** | **Not measured** — any claim about production document sizes, wallet balances, or env flags is **unverified** |
| **Prior related audit** | `architecture.md` §23 CEO gap audit (2026-07-18); this register **supersedes** it for *inconsistency* inventory and extends to Firestore dual-SSOT / scale |
| **Serper credit fix** | V27.1.0 inbound + **V27.2.0** G2 admit + free-tier site: keep — **code only**; prod effect **unverified** until green deploy |

### V27.2.0 remediation status (code shipped, prod unverified)

| ID | Item | Code status |
|----|------|-------------|
| P0-01 | Wallet dual formula | **Fixed in code** — `shared/wallet.py` + all gates |
| P0-02 | Dual lead collections | **Mitigated** — cluster writes top-level `leads` + mirror |
| P0-03 | Status sprawl | **Partial** — `normalize_user_status` (rejected→ignored, approved→converted) |
| P0-04 | G2/Capterra hard drop | **Fixed in code** — public channels never enterprise-blocked |
| P0-05 | Free-tier site: strip | **Fixed in code** — keep-list always preserved |
| P0-06 | estimated_value drop | **Fixed in code** — alias to deal_value |
| P0-07 | Residual Serper | **Mitigated** — V27.3 `shared/serper_budget.py` residual daily cap (default 800); search_serper residual=True; deep_context/inbound gated |
| P1-01 | Dedup 500 | **Fixed in code** — paginated to 2500 + source_url |
| P1-05 | Harvest queue race | **Fixed in code** — backpressure + trim |
| P1-06 | Campaign hot doc | **V27.4 dual-path SSOT** — `load/append/pop` merge array+`queue_items`; mode `hybrid` (default) or `subcollection` (array cleared on write). Feature-safe cutover. |
| Residual agent/DT Serper | **V27.4** residual budget on agent_engine + digital-twin |
| P1-07 | Unbounded blocklist | **Fixed in code** — cap 500 |
| P1-10 | Entity process-local | **Fixed in code** — Firestore daily counter |
| P1-11 | Strategy mutability | **Fixed in code** — primary locked after create |
| P1-12 | enrichment_pending | **Fixed in code** — cron resume/expire job |
| P2-02 | Dual preferred-sources | **Fixed in code** — domain_platform_config SSOT wins |

**Operating rule (CEO):** Radical truth. Code change ≠ production fixed. Paste live evidence before claiming remediation.

---

## 0. Executive summary

The system has a **strong modular pipeline** but accumulated **multiple sources of truth** for the same concepts (wallet, leads, credits, channel admit, domain profile, strategy, status). That produces:

1. **Billing / quota drift** (wallet dual-write formulas).
2. **Credit burn with zero yield** (query allow vs result block; residual Serper paths).
3. **Scale fragility** (process-local limits, 500-doc dedup, fat campaign docs, unbounded arrays).
4. **Silent product bugs** (UI field names not accepted by API).
5. **Operator confusion** (architecture vs code; dual clocks; dual lead collections).

### Severity counts (this register)

| Severity | Count (approx.) | Meaning |
|----------|-----------------|--------|
| **P0** | 8 | Correctness, billing, silent data loss, or systematic credit waste |
| **P1** | 18 | High operational / multi-instance / lifecycle risk |
| **P2** | 22 | Dual-SSOT, doc drift, scale hygiene |
| **P3** | 12 | Polish, stale comments, config footguns |

---

## 1. Failure mode taxonomy

All findings map to one or more of:

| Code | Failure mode | Example |
|------|----------------|---------|
| **FM-A** | Dual formula / dual field for one concept | Wallet `total_consumed` vs shards |
| **FM-B** | Dual storage path | Top-level `leads` vs `campaigns/{id}/leads` |
| **FM-C** | Query allow / result block conflict | G2 site: dorks then hard domain drop |
| **FM-D** | Residual ungated external spend | Inbound / mesh / deep_context Serper |
| **FM-E** | Process-local state on multi-instance | `_ENTITY_DOMAIN_COUNTS` |
| **FM-F** | Hot document as work queue | Campaign `unprocessed_queue` + profiles |
| **FM-G** | Architecture / API / UI contract drift | `estimated_value` vs `deal_value` |
| **FM-H** | Incomplete lifecycle | `enrichment_pending` parking lot |
| **FM-I** | Unbounded growth | `dynamic_blocklist`, `accepted_patterns` |

---

## 2. Firestore collection inventory

### 2.1 Top-level collections (code-referenced)

| Collection | Role | In architecture §4? |
|------------|------|---------------------|
| `users` | Tenant + wallet | Yes |
| `campaigns` | Campaign config + runtime state | Yes |
| `leads` | Primary lead store | Yes |
| `global_lead_locks` | Cross-tenant lock | Yes |
| `scraped_cache` | URL scrape cache | Yes |
| `system_telemetry` | Circuit breaker, feature flags, kill switch | Partial |
| `system_config` | Router, sweep lock, swarm weights | **No** |
| `tenant_profiles` | Personas, agents | Partial (personas only) |
| `usage_metrics` | Serper/Gemini counters (+ shards) | **No** |
| `inbound_signals` | Inbound Radar | **No** (§ elsewhere) |
| `visitor_signals` | Beacon | Mentioned elsewhere |
| `ontology_map` | Autonomous / RLHF | **No** |
| `autonomous_dedup` | Autonomous | **No** |
| `market_trend_cache` | Digital twin / campaigns | **No** |
| `admin_audit_log` | L0 | **No** |
| `dead_letter_leads` | Zombie recovery | **No** |
| `outbound_emails` | Email / WhatsApp (disabled products still in tree) | **No** |
| `macro_trends` | Email summary | **No** |
| `social_tokens` | Social redirect | **No** |
| `tenants` | **email-summary only** | **No** — likely orphan vs `users` |

### 2.2 Subcollections

| Path | Notes |
|------|--------|
| `users/{tid}/wallet_shards/{0-9}` | Legacy consumption path |
| `users/{tid}/predictive_cache/*` | Autonomous cache |
| `usage_metrics/{tid}/shards/{n}` | Gemini call shards |
| `tenant_profiles/{tid}/personas/*` | Persona vault |
| `tenant_profiles/{tid}/agents/*` | Agents |
| `campaigns/{id}/source_stats/*` | Adaptive learning |
| `campaigns/{id}/accepted_patterns/*` | Unbounded `.add` on convert |
| `campaigns/{id}/leads/*` | **Cluster analyst only — dual lead path** |
| `leads/{id}/signals/*` | Mesh attachments |

### 2.3 Rules coverage

`firestore.rules` client-allows mainly `leads`, `campaigns`, `users`. Backend Admin SDK bypasses rules. Not a security hole by itself; inventory incompleteness is a **docs/ops** gap (**P2**).

---

## 3. P0 — Correctness / billing / silent loss / systematic waste

### P0-01 · Wallet dual accounting (FM-A)

**Canonical (architecture §4.1):**  
`balance = allocated − max(total_consumed, consumed_credits + SUM(wallet_shards)) − reserved`

**Code reality — formulas differ by path:**

| Path | Formula | File |
|------|---------|------|
| `check_quota` | max(total, legacy+shards); **no reserved** | `orchestrator/core/helpers.py` |
| `_reserve_credits_txn` | allocated − **total_consumed only** − reserved; **ignores shards** | same |
| Atomic settle success | `total_consumed += N`, release reserved | same |
| Dispatch fallback settle | **shards only**; may not release reserved | `dispatch.py` |
| Harvest direct lead | **shards only** | `signal_harvest.py` |
| Sweep / harvest-sweep gates | max(total, legacy) **no shards** | `internal.py` |
| Requeue gate | max(total, legacy) **no shards, no reserved** | `leads.py` |
| Inbound convert | max(total, legacy) **no shards** | `leads.py` |
| `/api/me` | max(total, legacy+shards) **no reserved**; returned as `consumed_credits` | `me.py` |
| Frontend | top-level wallet fields only | `public/app.js` |

**Impact:** Same tenant can pass one gate and fail another; UI balance can lie; over-delivery or over-block possible.

---

### P0-02 · Dual lead collections (FM-B)

| Path | Writer | In GET `/api/leads`? |
|------|--------|----------------------|
| `leads/{id}` | dispatch, autonomous, inbound convert | Yes |
| `campaigns/{id}/leads/{id}` | `signal_cluster_analyst` only | **No** |

Credit settle targets top-level `leads` → cluster path **misses settle / feed / CRM**.

---

### P0-03 · Lead status sprawl without SSOT (FM-A / FM-G)

**Architecture** lists one enum; **`LeadPayload` allowlist** is a different subset; **dispatch / UI / WhatsApp** write still more:

- Pipeline: `processing`, `new`, `scored_out`, `enrichment_pending`, `failed`, `failed_scrape`, `rlhf_filtered`, …
- User: `contacted`, `converted`, `ignored`, `rejected`, `queued`, …
- WhatsApp: **`approved`** (not in main architecture lead enum)
- Inbound signals: `reviewed`, `dismissed`, `converted_to_lead` (signal statuses, not root leads)
- CRM parallel: `crm_status` ∈ {new, contacted, replied, negotiating, won, lost}

L0 already queries both `rejected` and `ignored` because “dispatcher may write either.”

---

### P0-04 · G2 / Capterra: pay then drop (FM-C)

- **Queries** target G2/Capterra (inbound B2B modes, PLATFORM_MINING, mesh ReviewProvider).
- **Legacy** `filter_serper_noise`: hard-drop `g2.com`, `capterra.com` via `_ENTERPRISE_DOMAINS`.
- **V27** admits them only when orchestrator active + intent profile passed.

**Default off V27 ⇒ systematic Serper waste.** Flag state in prod: **unverified**.

---

### P0-05 · Free-tier `sanitize_query` strips positive social `site:` (FM-C)

Unless `SERPER_PAID_TIER=true` or V27 preserve path, reddit/quora/youtube tokens stripped → empty yield after paid search. Prod env: **unverified**.

---

### P0-06 · CRM deal value silent drop (FM-G)

- UI: `PUT { estimated_value }` (`app.js`).
- API allowlist: **`deal_value` only** (`leads.py`).
- Toast can show success; **value not persisted**.

---

### P0-07 · Residual Serper outside produce budget (FM-D)

Always-on or dispatch-path Serper with **no** `allow_serper` / `BudgetGuard`:

| Path | Approx cost shape |
|------|-------------------|
| Produce QueryBrain `search_serper` | N queries / cycle |
| Produce harvest SerperDiscovery / Reviews / Reddit (when allow_serper) | multi |
| Dispatch `deep_context` | 2 / lead (V27.7: places+hiring B2B; places+reviews B2C; 48h cache) |
| Intelligence mesh | ≤2 / lead (V27.7: funding+reviews; hiring/news off by default; 48h domain cache) |
| PRISM WalledGarden | ≤3 |
| Inbound organic | ≤6 after V27.1 (build cap) |
| Inbound maps + reviews | ≤3 × ≤5 places × 2 |
| Agent engine | ≤3 |
| Digital twin | 2 |

`BudgetGuard` default `SERPER_DAILY_LIMIT=0` → **disabled**. Many clients bypass central `search_serper` (no audit/sanitize/tbs).

---

### P0-08 · Architecture wallet formulas self-contradict (FM-G)

§4.1 documents `max(total_consumed, …)`; later sections still document legacy-only balance without `total_consumed`. Operators implementing “from docs” will get wrong ops runbooks.

---

## 4. P1 — High operational / scale / lifecycle risk

| ID | Area | Finding | FM |
|----|------|---------|-----|
| P1-01 | Dedup | Produce scan **limit 500**, no reliable recency order; large tenants re-queue or miss | E |
| P1-02 | Dedup fields | Select/filter on **`url`**; promote emphasizes **`source_url`** → miss | A |
| P1-03 | Campaign identity | `campaign_id` vs `matched_campaigns` vs `matched_campaign_ids` — delete/count/filter differ | A |
| P1-04 | Dual clocks | `next_produce_due` + `next_drip_due` — architecture documents drip only | A/G |
| P1-05 | Queue race | Harvest `ArrayUnion` without produce’s depth-150 / trim discipline → can exceed 200 | F |
| P1-06 | Campaign hot doc | Queue + `system_domain_profile` + `intent_profile` + `system_enrichment` + strategy + novelty (80) on one doc; risk rises with concurrent writers / large queue payloads | F |
| P1-07 | Unbounded | `users.dynamic_blocklist` ArrayUnion — no cap | I |
| P1-08 | Unbounded | `tenant_profiles.knowledge_base_text` ArrayUnion text chunks — no cap | I |
| P1-09 | Indexes | Missing composite for CRM filter + status + createdAt; **no index on `normalized_score`** for score sort | — |
| P1-10 | Entity rate | `_ENTITY_DOMAIN_COUNTS` process-local → N instances × 5 pages/domain | E |
| P1-11 | Strategy mutability | Arch says strategy immutable; PUT reclassifies full `intelligence_strategy` on bio/kw change | A/G |
| P1-12 | enrichment_pending | Parking lot; velocity counts it; **no verified resume job**; still in dedup set | H |
| P1-13 | BudgetGuard undercount | Records 1 when multi-query source added | D |
| P1-14 | Serper audit gap | BQ audit mainly via `search_serper`; Discovery/maps/inbound/agent/DT bypass | D |
| P1-15 | Inbound maps | Outside V27.1 organic cap of 6; multi-credit Maps+Reviews | D |
| P1-16 | usage_metrics | `serper_searches` incomplete (deep_context path only in places) | A |
| P1-17 | Medium quota | Tenant gate uses **status**; campaign soft quota uses **confidence_tier** + limit 200 fail-open | A |
| P1-18 | Score UI | `score` 0–10 vs `normalized_score` 0–100; rejected-table colors assume 0–100 | G |

---

## 5. P2 — Dual-SSOT, doc drift, scale hygiene

| ID | Finding |
|----|---------|
| P2-01 | Domain profile: runtime **domain-v4**; architecture sample **domain-v2** |
| P2-02 | Dual preferred-sources: `domain_intelligence` local maps + `domain_platform_config` |
| P2-03 | `education_profiles` deprecated shim; tests still dual |
| P2-04 | Strategy field name: docs `mining_targets` vs code `platform_targets` |
| P2-05 | Dual classifiers for strategy (Gemini vs heuristic) |
| P2-06 | Bio stack intentional layers but length-based pick can surprise |
| P2-07 | Novelty / exhaustion / `last_cycle_funnel` / `intent_profile` under-documented in §4.3 |
| P2-08 | Architecture: queue as Serper **objects**; code: primarily **URL strings** (reduces 1MB risk vs objects, but doc wrong) |
| P2-09 | Cluster lead schema drift (`created_at` vs `createdAt`, bool flags) |
| P2-10 | `origin_engine` includes `cluster_analyst` not in arch enum |
| P2-11 | Contact endpoints: `platform`/`uri` vs `type`/`value` |
| P2-12 | Parallel pipeline `status` vs CRM `crm_status` |
| P2-13 | Content-farm hard block vs EVENT_TRIGGER / news channel ambition |
| P2-14 | Mesh skip list vs enrichment blacklist inconsistency (trustpilot vs g2) |
| P2-15 | GLOBAL_NEGATIVE `-"best"` vs B2C `"best brokers"` templates |
| P2-16 | Unbounded `accepted_patterns` subcollection |
| P2-17 | Unbounded `leads.interactions` ArrayUnion |
| P2-18 | Visitor rate limit in-memory (per instance) |
| P2-19 | Collections omitted from architecture §4 inventory |
| P2-20 | `tenants` collection only in email-summary |
| P2-21 | Adaptive still has legacy domain heuristics when bias missing |
| P2-22 | Batch return codes (`score_drop`) ≠ Firestore status (`scored_out`) |

---

## 6. P3 — Footguns / polish

| ID | Finding |
|----|---------|
| P3-01 | Inbound job `INBOUND_MAX_QUERIES=14` vs build cap 6 |
| P3-02 | Stale comments (domain-v2, education resolve) |
| P3-03 | Frontend dead UI for non-`new` statuses while feed API only returns `new` |
| P3-04 | WhatsApp/email paths still in tree while product disabled |
| P3-05 | free-tier sanitize footgun if env wrong |
| P3-06 | Dual import paths `shared.intent_orchestrator` vs `intelligence.orchestrator` |
| P3-07 | Validate script message says domain-v2 while comparing constant |
| P3-08 | Shadow tracker deprecation still imported in smoke tests |
| P3-09 | BQ serper_audit schema vs broker non-200 (known I-2) |
| P3-10 | Serper vendor credits ≠ tenant wallet credits (naming confusion) |
| P3-11 | Mis-set `sourcing_vector` → wrong inbound mode table |
| P3-12 | Preferences_weights unbounded key growth |

---

## 7. Campaign document & 1 MB risk (deep dive)

### 7.1 What lives on `campaigns/{id}`

| Category | Fields | Growth |
|----------|--------|--------|
| User config | name, bio, keywords, location, gl, status, persona denorm | Slow |
| Intelligence | `intelligence_strategy`, `system_domain_profile`, `system_enrichment`, `intent_profile` | Medium fixed blobs |
| Runtime | `unprocessed_queue` (≤200 produce), novelty (≤80), exhaustion counters, `last_cycle_funnel`, dual due timestamps | Hot |
| Counters | leads_generated, drip interval, force refresh flags | Small |

### 7.2 1 MiB assessment (radical truth)

- Firestore limit: **1 MiB / document**.
- Current produce path primarily stores **URL strings** in queue (not full Serper objects) — **architecture doc is wrong** about “objects”; this **reduces** but does not eliminate risk.
- Risk drivers that remain: concurrent harvest+produce ArrayUnion races; large profile/enrichment JSON; future reintroduction of fat queue items; multi-writer contention (every produce rewrites large fields).
- **Live max campaign doc size: unverified.** Recommended evidence: export one heavy campaign JSON and measure bytes.

### 7.3 Enterprise shape (target)

```text
campaigns/{id}                    # config + small counters only (target ≪ 200 KB)
campaigns/{id}/queue/{itemId}     # work items
campaigns/{id}/memory/...         # novelty / exhaustion
leads / scraped_cache / BQ        # outcomes & audit
```

---

## 8. Serper spend matrix (condensed)

| Family | Gate | BudgetGuard | Central audit |
|--------|------|-------------|---------------|
| Produce QueryBrain | Always on produce | No | Yes (`search_serper`) |
| Produce harvest Serper plugins | `allow_serper=True` | Partial if limit>0 | Partial (Discovery bypasses) |
| `/harvest` | Hard False | N/A | N/A |
| Dispatch deep_context / mesh / PRISM | None | No | Partial |
| Inbound organic | V27.1 ≤6 build | No | No (raw httpx) |
| Inbound maps | None | No | No |
| Agent / digital twin | None | No | No |

---

## 9. Lead identity & status matrix

### 9.1 Identity fields

| Field | Writers | Readers that matter |
|-------|---------|---------------------|
| `url` | Stub create, some paths | Produce dedup **select** |
| `source_url` | Promotion | UI fallback, arch schema |
| `campaign_id` | Final write | Delete orphan, L0 counts |
| `matched_campaigns` | Stub + final | UI filter, Medium quota |
| `matched_campaign_ids` | Final write | Rare |

### 9.2 Status channels

| Channel | Field | Values (non-exhaustive) |
|---------|-------|-------------------------|
| Pipeline | `status` | processing, new, scored_out, enrichment_pending, failed*, … |
| User ops | `status` | contacted, converted, ignored, rejected, queued |
| CRM board | `crm_status` | new → won/lost |
| Inbound | signal `status` | new, reviewed, dismissed, converted_to_lead |
| WhatsApp | lead `status` | approved |

---

## 10. Domain / strategy dual-SSOT

| Concept | Intended SSOT | Competing truth |
|---------|---------------|-----------------|
| Platform packs | `shared/domain_platform_config.py` | `domain_intelligence` preferred_sources maps |
| Profile version | `DOMAIN_PROFILE_VERSION=domain-v4` | Arch sample domain-v2; comments v2/v3 |
| Strategy primary | Immutable post-create (arch) | Campaigns PUT reclassify |
| Strategy platforms | `platform_targets` | Docs `mining_targets` |
| Education vertical | domain_platform_config | education_profiles shim + tests |

---

## 11. Frontend ↔ API contract defects

| UI | API | Result |
|----|-----|--------|
| `estimated_value` | allowlist `deal_value` | **Silent drop (P0)** |
| `score` display /10 | also `normalized_score` 0–100 | Wrong color thresholds |
| `lead.url \|\| source_url` | dual fields | OK if both set |
| `crm_status` | allowlist includes crm fields | Parallel system |
| Reject `rejected` | backend may store `ignored` | Dual reject |

---

## 12. Indexes & queries

**Declared** in `firestore.indexes.json` (deploy state **unverified**).

| Gap | Query | Severity |
|-----|-------|----------|
| CRM + status + createdAt | data_reads with crm filter | P1 |
| normalized_score order | sort_by=score | P1 |
| campaign_id + tenant_id only | delete orphan patterns | P2 verify |

---

## 13. BigQuery / telemetry

| Issue | Notes |
|-------|--------|
| I-2 serper_audit schema mismatch | Open in architecture §18 |
| Incomplete Serper row coverage | Bypass clients |
| credit_cost NULL → was phantom bill (historical fix claimed V24.5) | Verify still default 0 |
| Wallet ≠ Serper credits | Naming confusion for ops |

---

## 14. Multi-instance scale risks

| Mechanism | Safety |
|-----------|--------|
| Firestore wallet txn | Multi-instance OK if single formula used |
| Wallet dual-write | **Not safe** for correctness |
| Entity domain counts | **Process-local** |
| Visitor rate limit | **Process-local** |
| Neg shield cache | TTL stale OK fail-open |
| Global lead locks | Designed multi-instance |
| Produce queue ArrayUnion | Contention + race with harvest |

---

## 15. Cross-link to CEO gap audit §23 (2026-07-18)

| §23 theme | Status in this register |
|-----------|-------------------------|
| G2/Capterra hard block | **P0-04** still dual-path on V27 flag |
| Free-tier sanitize | **P0-05** |
| Yield funnel | Partial (`last_cycle_funnel`); not full product SLO |
| Residual Serper | **P0-07** |
| Entity rate limit distributed | **P1-10** still open |
| Dedup 500 | **P1-01** still open |
| Nourish / enrichment_pending | **P1-12** still open |
| Channel matrix | **P0-04**, P2-13 |

V27.1.0 closed a **slice** of inbound Serper fan-out only — not the register.

---

## 16. Prioritized remediation roadmap (enterprise)

Do **not** patch randomly. Order by blast radius.

### Wave 0 — Stop silent money/data loss (1–2 weeks)

1. **Single wallet SSOT function** used by every gate, reserve, settle, harvest, inbound, UI (`check_quota` = reserve = settle = me = L0).  
2. **API accept `estimated_value` as alias of `deal_value`** (or fix UI); add regression test.  
3. **V27 channel admit default ON in prod** *or* stop generating G2/Capterra queries when flag off; log `noise_filter_channel_*`.  
4. **Confirm `SERPER_PAID_TIER=true`** on Cloud Run; startup log.

### Wave 1 — Stop dual storage & status chaos (2–3 weeks)

5. Deprecate `campaigns/{id}/leads` → write only top-level `leads` (migrate or dual-write with read fan-in).  
6. Lead status enum SSOT module + reject dual (`rejected`/`ignored`); WhatsApp map to `converted`/`approved` policy.  
7. Identity SSOT: always set `url` + `source_url` + `campaign_id` + `matched_campaigns` on every write; dedup reads both URL fields.  
8. Document or kill dual clocks (`next_produce_due` / `next_drip_due`).

### Wave 2 — Serper spend SSOT (2–3 weeks)

9. All Serper HTTP through **one client** (sanitize, tbs, audit, budget).  
10. Residual path catalog with owner + daily cap (inbound maps inside organic budget).  
11. `BudgetGuard` meaningful default or remove false sense of safety.  
12. Fix GLOBAL_NEGATIVE vs B2C template self-conflict.

### Wave 3 — Hot document & scale (ongoing)

13. Offload `unprocessed_queue` to subcollection / collection group.  
14. Cap `dynamic_blocklist`, `knowledge_base_text`, `accepted_patterns` TTL.  
15. Distributed entity rate limit.  
16. Cursor-based dedup beyond 500.  
17. Resume or TTL `enrichment_pending`.  
18. Indexes for CRM+status and score sort.  
19. Doc size telemetry: log campaign approx size / queue depth every produce.

### Wave 4 — Domain / strategy SSOT

20. One preferred-sources table (`domain_platform_config` only).  
21. Resolve strategy immutability product decision (enforce or update architecture).  
22. Align architecture §4 with domain-v4 + full collection inventory.

---

## 17. Live evidence checklist (before claiming “fixed”)

| Claim | Evidence required |
|-------|-------------------|
| Wallet correct | Same tenant: me API + check_quota + L0 + shard sum + reserved — one table of numbers |
| No 1 MB risk | Top 20 campaigns by estimated size; none near 900 KB |
| G2 yield | Serper audit rows with g2 + produce noise_filter admit counts after deploy |
| Dedup | Produce logs `produce_dedup_scan_cap_hit` rate → 0 under load |
| Deal value | PUT then GET persists |
| Serper spend down | Daily BQ audit cost by engine before/after |
| enrichment_pending | Count of docs stuck >7d → decreasing |

---

## 18. What this audit did **not** do

- No live Firestore export / sampling  
- No Cloud Run env dump (`SERPER_PAID_TIER`, `V27_*`, `SERPER_DAILY_LIMIT`)  
- No production Serper remaining credits  
- No load test of multi-instance entity limits  
- No claim that V27.1.0 fixed production Serper waste until deploy evidence  

---

## 19. Suggested ownership matrix

| Domain | Primary code owners (paths) |
|--------|-----------------------------|
| Wallet / credits | `orchestrator/core/helpers.py`, `me.py`, `leads.py`, `dispatch.py`, `signal_harvest.py` |
| Campaign hot doc | `produce.py`, `signal_harvest.py`, `campaigns.py` |
| Serper client | `serper_service.py`, inbound/*, agent, digital-twin |
| Lead identity/status | `dispatch.py`, `leads.py`, `data_reads.py`, `app.js` |
| Domain/strategy | `domain_intelligence.py`, `domain_platform_config.py`, `campaigns.py` |
| Indexes | `firestore.indexes.json` + data_reads queries |

---

## 20. Document control

| Version | Date | Notes |
|---------|------|-------|
| 1.0 | 2026-07-20 | Full static register from triple-path code audit + architecture cross-check |

**Canonical location:** repo root `AUDIT_INCONSISTENCY_REGISTER.md`  
**Related:** `architecture.md` §18, §23; `AGENTS.md` evidence gate.
