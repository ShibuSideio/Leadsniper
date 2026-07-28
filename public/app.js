// Firebase configuration (Placeholder)
const firebaseConfig = {
    apiKey: "AIzaSyCxqimZJ7kspuJJ8qXF34zguLkNXi6MWd4",
    authDomain: "lead-sniper-prod.firebaseapp.com",
    projectId: "lead-sniper-prod",
    storageBucket: "lead-sniper-prod.firebasestorage.app",
    messagingSenderId: "222247989819",
    appId: "1:222247989819:web:17066a1bbf0b1f3df2221e",
    measurementId: "G-SQ6DDQ7HW0"
};

// Initialize Firebase Auth (Firestore explicitly stripped)
firebase.initializeApp(firebaseConfig);
const auth = firebase.auth();

const API_BASE = "";

// Digital Twin Engine — Reverse Proxy via Firebase
const DT_ENGINE_URL = "";

// ── P0-XSS: HTML escape helper for server-controlled data in innerHTML ──────
function _escapeHTML(str) {
    if (!str) return '';
    return String(str)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}

// ── P0-XSS: Safe href helper — blocks javascript:/data:/vbscript: scheme injection ──
function _safeHref(url) {
    if (!url || typeof url !== 'string') return '#';
    var trimmed = url.trim().toLowerCase();
    if (trimmed.startsWith('javascript:') || trimmed.startsWith('data:') || trimmed.startsWith('vbscript:')) return '#';
    return _escapeHTML(url);
}



// DOM Elements
const authContainer = document.getElementById('auth-container');
const appContainer  = document.getElementById('app-container');
const loginBtn      = document.getElementById('login-btn');
const leadsList     = document.getElementById('leads-list');

// Selected Filter State
let currentCampaignFilter = 'all';
let rawLeadsCache = [];
let unsubscribeLeads = null;   // declared here to avoid temporal dead zone
const _leadsMap = new Map();

// ── V23.8: VIP Inbox — Feed Mode State ──────────────────────────────────────
// Controls whether the main lead feed shows outbound campaigns or inbound radar
// signals. Toggled by #feed-mode-toggle buttons in the UI.
let CURRENT_FEED_MODE = 'outbound';

// ── V23.9: Indexed Split Caches ─────────────────────────────────────────────
// Pre-routed during onSnapshot so renderLeads() reads O(1) instead of
// filtering the full rawLeadsCache on every render.
let inboundCache  = [];
let outboundCache = [];

// Shared predicate — single source of truth for inbound classification.
// Catches: lead.is_inbound, lead.type === 'inbound', or lead.source in
// ['webhook','form','typeform','inbound']. Defaults false if undefined.
function _isInbound(lead) {
    if (!lead) return false;
    if (lead.is_inbound === true) return true;
    const type   = (lead.type || '').toLowerCase();
    const source = (lead.source || '').toLowerCase();
    return type === 'inbound' || ['webhook', 'form', 'typeform', 'inbound'].includes(source);
}

// Debounced render — coalesces rapid onSnapshot bursts into one paint.
let _renderRAF = null;
function _scheduleRender() {
    if (_renderRAF) return;           // already scheduled
    _renderRAF = requestAnimationFrame(() => {
        _renderRAF = null;
        renderLeads();
    });
}

// Helper: remove a lead from ALL caches by docId (used by CRM push, rejection)
function _evictLeadFromCaches(docId) {
    const match = l => (l.id || l.doc_id) !== docId;
    rawLeadsCache  = rawLeadsCache.filter(match);
    inboundCache   = inboundCache.filter(match);
    outboundCache  = outboundCache.filter(match);
    _leadsMap.delete(docId);
}

// =============================================================================
// CASCADING GEO DROPDOWN — V23.6
// Loads /assets/geo_data.json once, drives continent→country→region cascade.
// Compiles selection into flat string for backward-compat with Python prompts.
// =============================================================================
let _geoDataCache = null;

async function loadGeoData() {
    if (_geoDataCache) return _geoDataCache;
    try {
        const resp = await fetch('/assets/geo_data.json');
        if (!resp.ok) throw new Error(`geo_data.json fetch failed: ${resp.status}`);
        _geoDataCache = await resp.json();
    } catch (e) {
        console.error('[GEO CASCADE] Failed to load geo data:', e);
        _geoDataCache = { continents: [] };
    }
    return _geoDataCache;
}

function _resetSelect(el, placeholder) {
    el.innerHTML = `<option value="">${placeholder}</option>`;
    el.disabled = true;
    el.value = '';
    const container = document.getElementById(el.id + '-container');
    if (container && typeof container.reset === 'function') {
        container.reset();
    }
}

/** V27.8.1: Mark region multiselect as "All" without requiring state pick. */
function _setRegionSelectAll(regionEl) {
    if (!regionEl) return;
    regionEl.disabled = false;
    regionEl.innerHTML = '<option value="All">All</option>';
    regionEl.value = 'All';
    const container = document.getElementById(regionEl.id + '-container');
    if (container) {
        const trigger = container.querySelector('.custom-multiselect-trigger');
        const label = container.querySelector('.trigger-label');
        if (trigger) trigger.classList.remove('disabled');
        if (label) label.textContent = '📍 All';
        if (typeof container.populateRegions === 'function') {
            // Single synthetic choice so UI shows All selected
            container.populateRegions(['All regions'], ['All']);
            // Force select value back to All (populateRegions may set "All regions")
            regionEl.innerHTML = '<option value="All">All</option>';
            regionEl.value = 'All';
            if (label) label.textContent = '📍 All';
        }
    }
}

function _updateGeoPreview() {
    const continent = document.getElementById('geo-continent-select')?.value || '';
    const country   = document.getElementById('geo-country-select')?.value   || '';
    const region    = document.getElementById('geo-region-select')?.value    || '';
    const preview   = document.getElementById('geo-compiled-preview');
    const compiled  = _compileGeoString(continent, country, region);
    if (preview) preview.textContent = compiled ? `📍 ${compiled}` : '';
    // Sync hidden fields for backward compat
    const glHidden  = document.getElementById('edit-camp-gl');
    const locHidden = document.getElementById('edit-camp-location');
    if (glHidden)  glHidden.value  = _resolveGlCode(country, continent) || '';
    if (locHidden) locHidden.value = compiled;
}

/**
 * V27.8.1: Compile hierarchy to location string.
 * Global → "Global". "All" at country/region is omitted so continent-only
 * or country-wide targets stay clean (e.g. "Asia", "India, Asia").
 */
function _compileGeoString(continent, country, region) {
    if (!continent) return '';
    if (continent === 'Global') return 'Global';
    const parts = [];
    if (region && region !== 'All' && region !== 'All regions') parts.push(region);
    if (country && country !== 'All') parts.push(country);
    parts.push(continent);
    return parts.join(', ');
}

// ── V24.1.15: Custom Multiselect Checkbox Dropdown Helper ─────────────────
function setupCustomMultiselect(selectId, containerId, triggerId, dropdownId) {
    const selectEl = document.getElementById(selectId);
    const containerEl = document.getElementById(containerId);
    const triggerEl = document.getElementById(triggerId);
    const dropdownEl = document.getElementById(dropdownId);
    if (!selectEl || !containerEl || !triggerEl || !dropdownEl) return;

    // Toggle dropdown visibility
    triggerEl.addEventListener('click', function(e) {
        if (triggerEl.classList.contains('disabled')) return;
        
        // Close other custom multiselects first
        document.querySelectorAll('.custom-multiselect-container').forEach(c => {
            if (c !== containerEl) c.classList.remove('open');
        });
        
        containerEl.classList.toggle('open');
        e.stopPropagation();
    });

    // Close when clicking outside
    document.addEventListener('click', function(e) {
        if (!containerEl.contains(e.target)) {
            containerEl.classList.remove('open');
        }
    });

    containerEl.populateRegions = function(regions, selectedValues = []) {
        dropdownEl.innerHTML = '';
        triggerEl.classList.remove('disabled');
        
        if (!regions || regions.length === 0) {
            triggerEl.classList.add('disabled');
            triggerEl.querySelector('.trigger-label').textContent = '📍 Region';
            return;
        }

        // Add "All" option
        const allOptionDiv = document.createElement('div');
        allOptionDiv.className = 'custom-multiselect-option select-all';
        
        const allCheckbox = document.createElement('input');
        allCheckbox.type = 'checkbox';
        allCheckbox.id = `${selectId}-opt-all`;
        
        const allLabel = document.createElement('label');
        allLabel.htmlFor = allCheckbox.id;
        allLabel.textContent = '🌍 All';
        
        allOptionDiv.appendChild(allCheckbox);
        allOptionDiv.appendChild(allLabel);
        dropdownEl.appendChild(allOptionDiv);

        // Divider
        const divider = document.createElement('div');
        divider.className = 'custom-multiselect-divider';
        dropdownEl.appendChild(divider);

        // Add each region
        const optionCheckboxes = [];
        regions.forEach((r, idx) => {
            const optDiv = document.createElement('div');
            optDiv.className = 'custom-multiselect-option';
            
            const cb = document.createElement('input');
            cb.type = 'checkbox';
            cb.value = r;
            cb.id = `${selectId}-opt-${idx}`;
            
            const label = document.createElement('label');
            label.htmlFor = cb.id;
            label.textContent = r;
            
            optDiv.appendChild(cb);
            optDiv.appendChild(label);
            dropdownEl.appendChild(optDiv);
            optionCheckboxes.push(cb);
            
            // Check if initially selected
            if (selectedValues.includes(r) || selectedValues.includes('All')) {
                cb.checked = true;
            }
        });

        // If 'All' is in selectedValues, or all checkboxes are checked:
        if (selectedValues.includes('All') || (optionCheckboxes.length > 0 && optionCheckboxes.every(c => c.checked))) {
            allCheckbox.checked = true;
            optionCheckboxes.forEach(c => c.checked = true);
        }

        // Handle check changes
        function updateSelection() {
            const checkedValues = optionCheckboxes.filter(c => c.checked).map(c => c.value);
            let displayVal = '📍 Region';
            let selectVal = '';

            if (allCheckbox.checked && checkedValues.length === optionCheckboxes.length) {
                displayVal = '📍 All';
                selectVal = 'All';
            } else if (checkedValues.length === optionCheckboxes.length) {
                allCheckbox.checked = true;
                displayVal = '📍 All';
                selectVal = 'All';
            } else if (checkedValues.length > 0) {
                allCheckbox.checked = false;
                displayVal = '📍 ' + checkedValues.join(', ');
                selectVal = checkedValues.join(', ');
            } else {
                allCheckbox.checked = false;
                displayVal = '📍 Region';
                selectVal = '';
            }

            triggerEl.querySelector('.trigger-label').textContent = displayVal;
            
            // Set value on the hidden select
            selectEl.innerHTML = selectVal ? `<option value="${_escapeHTML(selectVal)}">${_escapeHTML(selectVal)}</option>` : '<option value="">📍 Region</option>';
            selectEl.value = selectVal;

            if (typeof selectEl.onchange === 'function') {
                selectEl.onchange();
            } else {
                selectEl.dispatchEvent(new Event('change'));
            }
        }

        // Checkbox click handlers
        allCheckbox.addEventListener('change', function() {
            const isChecked = allCheckbox.checked;
            optionCheckboxes.forEach(c => c.checked = isChecked);
            updateSelection();
        });

        optionCheckboxes.forEach(cb => {
            cb.addEventListener('change', function() {
                if (!cb.checked) {
                    allCheckbox.checked = false;
                } else if (optionCheckboxes.every(c => c.checked)) {
                    allCheckbox.checked = true;
                }
                updateSelection();
            });
        });

        // Also run once initially to set the trigger label
        updateSelection();
    };

    containerEl.reset = function() {
        dropdownEl.innerHTML = '';
        triggerEl.classList.add('disabled');
        triggerEl.querySelector('.trigger-label').textContent = '📍 Region';
        containerEl.classList.remove('open');
    };
}


function _resolveGlCode(countryName, continentName) {
    if (!countryName || countryName === 'All' || continentName === 'Global') return '';
    if (!_geoDataCache) return '';
    for (const c of _geoDataCache.continents) {
        const match = c.countries.find(co => co.name === countryName);
        if (match) return match.gl || '';
    }
    return '';
}

/**
 * V27.8.1: Shared cascade wiring — Global + All country + All region.
 * Selecting a continent no longer forces a specific country/state.
 */
function _wireGeoCascade(data, continentEl, countryEl, regionEl, updatePreview) {
    // Populate continents: Global first, then named continents
    continentEl.innerHTML = '<option value="">🌍 Continent / Global</option>';
    continentEl.innerHTML += '<option value="Global">🌐 Global (worldwide)</option>';
    data.continents.forEach(c => {
        continentEl.innerHTML += `<option value="${_escapeHTML(c.name)}">${_escapeHTML(c.name)}</option>`;
    });

    _resetSelect(countryEl, '🏳️ Country / All');
    _resetSelect(regionEl,  '📍 Region / All');

    continentEl.onchange = function() {
        const cName = this.value;
        _resetSelect(countryEl, '🏳️ Country / All');
        _resetSelect(regionEl,  '📍 Region / All');
        if (!cName) {
            updatePreview();
            return;
        }
        // Global: no country/state required
        if (cName === 'Global') {
            countryEl.disabled = false;
            countryEl.innerHTML = '<option value="All">🏳️ All countries</option>';
            countryEl.value = 'All';
            _setRegionSelectAll(regionEl);
            updatePreview();
            return;
        }
        const continent = data.continents.find(c => c.name === cName);
        if (!continent) {
            updatePreview();
            return;
        }
        countryEl.disabled = false;
        countryEl.innerHTML = '<option value="All">🏳️ All countries</option>';
        continent.countries.forEach(co => {
            countryEl.innerHTML += `<option value="${_escapeHTML(co.name)}">${_escapeHTML(co.name)}</option>`;
        });
        // Default to All countries so continent-only campaigns work immediately
        countryEl.value = 'All';
        countryEl.onchange();
    };

    countryEl.onchange = function() {
        const coName = this.value;
        _resetSelect(regionEl, '📍 Region / All');
        if (!coName || coName === 'All') {
            // Entire continent (or Global) — region defaults to All
            if (continentEl.value) {
                _setRegionSelectAll(regionEl);
            }
            updatePreview();
            return;
        }
        const continentName = continentEl.value;
        if (continentName === 'Global') {
            _setRegionSelectAll(regionEl);
            updatePreview();
            return;
        }
        const continent = data.continents.find(c => c.name === continentName);
        const country   = continent?.countries.find(co => co.name === coName);
        if (!country || !country.regions || country.regions.length === 0) {
            _setRegionSelectAll(regionEl);
            updatePreview();
            return;
        }
        regionEl.disabled = false;
        const container = document.getElementById(regionEl.id + '-container');
        const preferred = regionEl._targetSelectedRegions || [];
        // Default selection: All regions unless hydrating a specific region
        const initial = preferred.length ? preferred : ['All'];
        if (container && typeof container.populateRegions === 'function') {
            container.populateRegions(country.regions || [], initial);
        } else {
            regionEl.innerHTML = '<option value="All">📍 All</option>';
            country.regions.forEach(r => {
                regionEl.innerHTML += `<option value="${_escapeHTML(r)}">${_escapeHTML(r)}</option>`;
            });
            regionEl.value = initial.includes('All') ? 'All' : (initial[0] || 'All');
            updatePreview();
        }
    };

    regionEl.onchange = function() {
        updatePreview();
    };
}

function _hydrateGeoCascade(data, continentEl, countryEl, regionEl, existingGeoHierarchy, existingGl, existingLocation, updatePreview) {
    const previewFn = typeof updatePreview === 'function' ? updatePreview : _updateGeoPreview;
    if (existingGeoHierarchy && existingGeoHierarchy.continent) {
        continentEl.value = existingGeoHierarchy.continent;
        continentEl.onchange();
        if (existingGeoHierarchy.country) {
            countryEl.value = existingGeoHierarchy.country;
            countryEl.onchange();
            if (existingGeoHierarchy.region && existingGeoHierarchy.region !== 'All'
                && !document.getElementById(regionEl.id + '-container')) {
                regionEl.value = existingGeoHierarchy.region;
                regionEl.onchange();
            }
        }
    } else if (existingLocation && String(existingLocation).toLowerCase() === 'global') {
        continentEl.value = 'Global';
        continentEl.onchange();
    } else if (existingGl) {
        for (const cont of data.continents) {
            const match = cont.countries.find(co => co.gl === existingGl);
            if (match) {
                continentEl.value = cont.name;
                continentEl.onchange();
                countryEl.value = match.name;
                countryEl.onchange();
                if (existingLocation) {
                    const regionMatch = (match.regions || []).find(
                        r => existingLocation.toLowerCase().includes(r.toLowerCase())
                    );
                    if (regionMatch) {
                        regionEl._targetSelectedRegions = [regionMatch];
                        const container = document.getElementById(regionEl.id + '-container');
                        if (container && typeof container.populateRegions === 'function') {
                            container.populateRegions(match.regions || [], [regionMatch]);
                        } else {
                            regionEl.value = regionMatch;
                            regionEl.onchange();
                        }
                    }
                }
                break;
            }
        }
    }
    previewFn();
}

/** Edit-campaign modal: #geo-continent-select / country / region */
async function initGeoCascade(existingGeoHierarchy, existingGl, existingLocation) {
    const data = await loadGeoData();
    const continentEl = document.getElementById('geo-continent-select');
    const countryEl   = document.getElementById('geo-country-select');
    const regionEl    = document.getElementById('geo-region-select');
    if (!continentEl || !countryEl || !regionEl) return;

    if (existingGeoHierarchy && existingGeoHierarchy.region) {
        regionEl._targetSelectedRegions = existingGeoHierarchy.region.split(',').map(s => s.trim());
    } else if (existingLocation) {
        regionEl._targetSelectedRegions = [existingLocation];
    } else {
        regionEl._targetSelectedRegions = [];
    }

    _wireGeoCascade(data, continentEl, countryEl, regionEl, _updateGeoPreview);
    _hydrateGeoCascade(
        data, continentEl, countryEl, regionEl,
        existingGeoHierarchy, existingGl, existingLocation, _updateGeoPreview
    );
}

// =============================================================================
// GENERIC GEO CASCADE — works with any ID prefix (e.g. 'cc-geo', 'geo')
// Used by: Launch Campaign modal (cc-geo-*), Edit Campaign modal (geo-*-select)
// =============================================================================
async function initGeoCascadeFor(prefix, existingGeoHierarchy, existingGl, existingLocation) {
    const data = await loadGeoData();
    const continentEl = document.getElementById(prefix + '-continent');
    const countryEl   = document.getElementById(prefix + '-country');
    const regionEl    = document.getElementById(prefix + '-region');
    const previewEl   = document.getElementById(prefix + '-preview');
    const locHidden   = document.getElementById(prefix === 'cc-geo' ? 'cc-location' : 'edit-camp-location');
    const glHidden    = document.getElementById(prefix === 'cc-geo' ? 'cc-gl' : 'edit-camp-gl');

    if (!continentEl || !countryEl || !regionEl) return;

    if (existingGeoHierarchy && existingGeoHierarchy.region) {
        regionEl._targetSelectedRegions = existingGeoHierarchy.region.split(',').map(s => s.trim());
    } else if (existingLocation) {
        regionEl._targetSelectedRegions = [existingLocation];
    } else {
        regionEl._targetSelectedRegions = [];
    }

    function updatePreview() {
        const continent = continentEl.value || '';
        const country   = countryEl.value   || '';
        const region    = regionEl.value    || '';
        const compiled  = _compileGeoString(continent, country, region);
        if (previewEl) previewEl.textContent = compiled ? `📍 ${compiled}` : '';
        if (glHidden)  glHidden.value  = _resolveGlCode(country, continent) || '';
        if (locHidden) locHidden.value = compiled;
    }

    _wireGeoCascade(data, continentEl, countryEl, regionEl, updatePreview);
    _hydrateGeoCascade(
        data, continentEl, countryEl, regionEl,
        existingGeoHierarchy, existingGl, existingLocation, updatePreview
    );
}

// Toast — defined as function declaration so it is hoisted and available
// everywhere, including in loadLeads which runs before window.showToast assignment.
function showToast(message, type = 'info') {
    const container = document.getElementById('toast-container');
    if (!container) { console.warn('[Toast]', type, message); return; }
    const toast = document.createElement('div');
    toast.className = `toast ${type}`;
    toast.textContent = message;
    container.appendChild(toast);
    setTimeout(() => toast.classList.add('show'), 10);
    setTimeout(() => { toast.classList.remove('show'); setTimeout(() => toast.remove(), 300); }, 3500);
}
window.showToast = showToast; // expose to inline HTML handlers

function handleAuthRejection() {
    showToast('Session Expired or Unauthorized Access.', 'error');
    auth.signOut();
}

// ─── MODAL UTILITY — single source of truth for all modal show/hide ──────────
// All modals now use style.display. Never classList.add/remove('hidden') for modals.
// V23.10: Focus trapping + focus restore for accessibility (FIX 6).

// Generic focus trap — keeps Tab cycling within the modal.
function _trapFocus(modalEl) {
    const FOCUSABLE = 'a[href], button:not([disabled]), textarea:not([disabled]), input:not([disabled]):not([type="hidden"]), select:not([disabled]), [tabindex]:not([tabindex="-1"])';
    function _handleKeydown(e) {
        if (e.key !== 'Tab') return;
        const focusable = modalEl.querySelectorAll(FOCUSABLE);
        if (focusable.length === 0) return;
        const first = focusable[0];
        const last  = focusable[focusable.length - 1];
        if (e.shiftKey) {
            if (document.activeElement === first) { e.preventDefault(); last.focus(); }
        } else {
            if (document.activeElement === last) { e.preventDefault(); first.focus(); }
        }
    }
    modalEl.addEventListener('keydown', _handleKeydown);
    modalEl._trapCleanup = () => modalEl.removeEventListener('keydown', _handleKeydown);
}

window._modalPreviousFocus = null;

window.showModal = function(id) {
    const el = document.getElementById(id);
    if (!el) return;
    // Save the currently focused element so we can restore it on close
    window._modalPreviousFocus = document.activeElement;
    el.classList.add('open'); // .sio-modal-overlay.open => display:flex via CSS
    // Focus the first focusable element inside the modal
    requestAnimationFrame(() => {
        const FOCUSABLE = 'a[href], button:not([disabled]), textarea:not([disabled]), input:not([disabled]):not([type="hidden"]), select:not([disabled]), [tabindex]:not([tabindex="-1"])';
        const firstFocusable = el.querySelector(FOCUSABLE);
        if (firstFocusable) firstFocusable.focus();
        _trapFocus(el);
    });
};
window.closeModal = function(id) {
    const el = document.getElementById(id);
    if (el) {
        el.classList.remove('open');
        // Remove focus trap listener
        if (el._trapCleanup) { el._trapCleanup(); el._trapCleanup = null; }
    }
    // Restore focus to the element that was focused before the modal opened
    if (window._modalPreviousFocus && typeof window._modalPreviousFocus.focus === 'function') {
        window._modalPreviousFocus.focus();
        window._modalPreviousFocus = null;
    }
};
// ─────────────────────────────────────────────────────────────────────────────

// Authentication state observer
auth.onAuthStateChanged(async user => {
    const authEl = document.getElementById('auth-container');
    const appEl  = document.getElementById('app-container');
    if (user) {
        if (authEl) authEl.style.display = 'none';
        if (appEl)  appEl.style.display  = 'flex';
        loadDashboard();
    } else {
        // ── Cleanup: unsubscribe Firestore listener to prevent memory leak ──
        if (unsubscribeLeads) { unsubscribeLeads(); unsubscribeLeads = null; }
        // Clear waitroom poll if running
        if (window._waitroomPollInterval) {
            clearInterval(window._waitroomPollInterval);
            window._waitroomPollInterval = null;
        }
        if (authEl) authEl.style.display = '';
        if (appEl)  appEl.style.display  = 'none';
    }
});

// Login Handler
if (loginBtn) {
    loginBtn.addEventListener('click', () => {
        const provider = new firebase.auth.GoogleAuthProvider();
        auth.signInWithPopup(provider).catch(err => console.error('Sign-in error:', err));
    });
}
// Note: Sign-out is handled via onclick="firebase.auth().signOut()" in the user dropdown.

// Unified Dashboard Loader
async function loadDashboard() {
    const user = firebase.auth().currentUser;
    if (!user) return;

    await Promise.all([
        loadMe(),
        loadCampaigns(),
        loadLeads(),
    ]);

    await initializeDashboardState();

    // V23.9: Ensure default view state — CRM hidden, dashboard visible
    const crmView = document.getElementById('view-crm-test');
    if (crmView) crmView.style.display = 'none';
    const adminView = document.getElementById('view-l0-admin');
    if (adminView) adminView.style.display = 'none';
    const dashView = document.getElementById('view-dashboard');
    if (dashView) dashView.style.display = '';

    if (window.crmAutoOpen) {
        window.crmAutoOpen = false;
        switchTab('crm-test');
    }
}

async function fetchTenantProfile() {
    try {
        const user = window.firebase.auth().currentUser;
        const token = await user.getIdToken();
        const response = await fetch(`${API_BASE}/api/tenant_profiles`, { 
            method: 'GET',
            headers: { 'Authorization': `Bearer ${token}` } 
        });
        // BUG FIX: Corrected from double-nested if(ok){if(!ok)} which made error
        // path dead code. Now correctly structured as if(!ok)/else.
        if (!response.ok) {
            console.error('Backend Error (fetchTenantProfile):', await response.text());
            return null;
        }
        const data = await response.json();
        if (data && data.data && data.data.length > 0) return data.data[0];
    } catch (e) { console.error("fetchTenantProfile error", e); }
    return null;
}

async function initializeDashboardState() {
    try {
        const tenantProfile = await fetchTenantProfile();
        // Fallback or count length of existing items
        const rawRows = document.querySelectorAll('#campaign-list-table tr').length;
        const fallbackCount = document.getElementById('campaign-list-table').innerHTML.includes('No active campaigns') ? 0 : rawRows;
        const activeCount = window.activeCampaignCount !== undefined ? window.activeCampaignCount : fallbackCount;
        
        if (tenantProfile) {
            renderExpansionState(activeCount);
        } else {
            renderZeroState();
        }
    } catch (error) {
        console.error("Failed to load tenant state:", error);
    }
}

function renderZeroState() {
    const h = document.getElementById('btn-new-twin-hero'); if(h) h.style.display = 'block';
    const ah = document.getElementById('btn-add-campaign-hero'); if(ah) ah.style.display = 'none';
    const m = document.getElementById('btn-new-twin-matrix'); if(m) m.style.display = 'block';
    const am = document.getElementById('btn-add-campaign-matrix'); if(am) am.style.display = 'none';
}

function renderExpansionState(activeCount) {
    const h = document.getElementById('btn-new-twin-hero'); if(h) h.style.display = 'none';
    const m = document.getElementById('btn-new-twin-matrix'); if(m) m.style.display = 'none';
    
    const ah = document.getElementById('btn-add-campaign-hero');
    const am = document.getElementById('btn-add-campaign-matrix');
    if(ah) ah.style.display = 'block';
    if(am) am.style.display = 'block';
    
    if (activeCount >= 5) {
        const txt = 'Campaign Limit Reached (5/5)';
        if(ah) { ah.innerText = txt; ah.disabled = true; ah.style.opacity = '0.5'; ah.style.cursor = 'not-allowed'; }
        if(am) { am.innerText = txt; am.disabled = true; am.style.opacity = '0.5'; am.style.cursor = 'not-allowed'; }
    } else {
        const txt = `+ Add Campaign (${activeCount}/5)`;
        if(ah) { ah.innerHTML = txt; ah.disabled = false; ah.style.opacity = '1'; ah.style.cursor = 'pointer'; }
        if(am) { am.innerHTML = txt; am.disabled = false; am.style.opacity = '1'; am.style.cursor = 'pointer'; }
    }
}

// BUG FIX: must be window.activeWallet (not let) so the property
// is accessible as window.activeWallet from any inline handler.
window.activeWallet = { allocated_credits: 0, consumed_credits: 0 };
async function loadMe() {
    try {
        const user = firebase.auth().currentUser;
        const token = await user.getIdToken();
        const response = await fetch(`${API_BASE}/api/me?rt=${new Date().getTime()}`, { 
            method: 'GET',
            headers: { 
                'Authorization': `Bearer ${token}`
            } 
        });
        // BUG FIX: Corrected from double-nested if(ok){if(!ok)} — error path
        // was dead code because !ok is always false inside if(ok).
        if (!response.ok) {
            console.error('Backend Error (loadMe):', await response.text());
            return;
        }
        {
            const payload = await response.json();
            const data = payload.data || {};
            
            const waitroom = document.getElementById('waitroom-overlay');
            // TECH DEBT: .dashboard-grid selector kept for compat. Also targets .app-main.
            // Refactor to remove .dashboard-grid dependency in next sprint.
            const mainGrid = document.querySelector('.app-main, .dashboard-grid');
            const navMenu = document.querySelector('.glass-nav');

            if (data.approval_status === 'pending') {
                if (mainGrid) mainGrid.style.display = 'none';
                if (navMenu) navMenu.style.display = 'none';
                if (waitroom) {
                    waitroom.style.position = 'relative';
                    waitroom.style.height = '100vh';
                    waitroom.style.display = 'flex';
                }
                // J-2 FIX: Poll /api/me every 30s while in the waitroom.
                // Without this, a user whose account is approved stays stuck
                // in the waitroom until they manually refresh the page.
                if (!window._waitroomPollInterval) {
                    window._waitroomPollInterval = setInterval(async () => {
                        try {
                            const pollUser = firebase.auth().currentUser;
                            if (!pollUser) return;
                            const pollToken = await pollUser.getIdToken(true);
                            const pollResp  = await fetch(`${API_BASE}/api/me`, {
                                headers: { 'Authorization': `Bearer ${pollToken}` }
                            });
                            if (!pollResp.ok) return;
                            const pollPayload = await pollResp.json();
                            if ((pollPayload.data || {}).approval_status !== 'pending') {
                                clearInterval(window._waitroomPollInterval);
                                window._waitroomPollInterval = null;
                                showToast('Your account is now active! Welcome to Sideio 🎉', 'success');
                                loadDashboard(); // re-run full dashboard init
                            }
                        } catch (pollErr) {
                            console.warn('[Waitroom] Poll error:', pollErr);
                        }
                    }, 30000); // 30s
                }
                return;
            } else {
                // Clear any running poll (user already approved on this session)
                if (window._waitroomPollInterval) {
                    clearInterval(window._waitroomPollInterval);
                    window._waitroomPollInterval = null;
                }
                if (mainGrid) mainGrid.style.display = '';
                if (navMenu) navMenu.style.display = '';
                if (waitroom) waitroom.style.display = 'none';
            }

            // V24.1.1: CRM is user-facing — visible to ALL authenticated users.
            const crmTab = document.getElementById('tab-crm');
            if (crmTab) {
                crmTab.style.display = 'inline-block';
            }

            // Admin tab: super_admin only
            if (data.role === 'super_admin') {
                const l0Tab = document.getElementById('tab-l0-admin');
                if (l0Tab) {
                    l0Tab.classList.remove('hidden');
                    l0Tab.style.display = 'inline-block';
                }
            }

            // V27.2.0: prefer API available_credits (SSOT includes reserved + shards)
            const w = payload.wallet || data.wallet || {};
            const allocated = Number(w.allocated_credits  || data.allocated_credits  || 0) || 0;
            const consumed  = Number(w.consumed_credits   || data.consumed_credits   || 0) || 0;
            const reserved  = Number(w.reserved_credits   || 0) || 0;
            const credits   = (w.available_credits != null)
                ? Number(w.available_credits) || 0
                : (allocated - consumed - reserved);
            window.activeWallet = {
                allocated_credits: allocated,
                consumed_credits: consumed,
                reserved_credits: reserved,
                available_credits: credits,
            };
            if (window.SIO_DEBUG) console.log('[WALLET] payload.wallet:', payload.wallet, '| data.wallet:', data.wallet,
                        '| allocated:', allocated, '| consumed:', consumed, '| balance:', credits);
            const el = document.getElementById('wallet-balance');
            if (el) el.innerText = credits.toLocaleString();

            // Credit usage bar in dropdown
            const barEl = document.getElementById('ud-credit-bar');
            const consumedEl = document.getElementById('ud-credits-consumed');
            const totalEl = document.getElementById('ud-credits-total');
            if (barEl && allocated > 0) {
                const pct = Math.max(0, Math.min(100, Math.round((credits / allocated) * 100)));
                barEl.style.width = pct + '%';
                barEl.style.background = pct < 20 ? '#ef4444' : pct < 50 ? '#f59e0b' : 'var(--primary)';
            }
            if (consumedEl) consumedEl.textContent = consumed.toLocaleString();
            if (totalEl) totalEl.textContent = allocated.toLocaleString();
            
            const alertBanner = document.getElementById('wallet-alert-banner');
            const newCampBtn = document.querySelector('button[onclick="openNewCampaignModal()"]');
            if (credits <= 0) {
                if (alertBanner) { alertBanner.textContent = 'Wallet Empty: You have 0 credits. Contact admin to top up.'; alertBanner.style.display = 'block'; }
                if (newCampBtn) { newCampBtn.textContent = 'Upgrade Plan (0 Credits)'; newCampBtn.disabled = true; newCampBtn.style.background = '#94a3b8'; }
            } else if (credits < 50) {
                if (alertBanner) alertBanner.style.display = 'block';
            } else {
                if (alertBanner) alertBanner.style.display = 'none';
                if (newCampBtn) { newCampBtn.textContent = '+ Find New Clients'; newCampBtn.disabled = false; newCampBtn.style.background = 'var(--primary)'; }
            }

            // V18: Populate user dropdown header
            const udName = document.getElementById('ud-display-name');
            const udEmail = document.getElementById('ud-email');
            const avatarEl = document.getElementById('user-avatar-initials');
            const avatarLg = document.getElementById('ud-avatar-large');
            const displayName = auth.currentUser?.displayName || data.email || 'User';
            const initials = displayName.split(' ').map(p => p[0]).join('').substring(0, 2).toUpperCase();
            if (udName) udName.textContent = displayName;
            if (udEmail) udEmail.textContent = auth.currentUser?.email || '';
            if (avatarEl) avatarEl.textContent = initials;
            if (avatarLg) avatarLg.textContent = initials;

            if (!data.agreed_to_terms) {
                const tosModal = document.getElementById('tos-modal');
                if (tosModal) tosModal.style.display = 'flex';
            }

            window.currentUserData = data;

            // ── V23.5: Inbound Radar widget bootstrap ────────────────────────────
            // payload.inbound_radar is injected by /api/me V23.5
            if (payload.inbound_radar) {
                _renderInboundRadarBanner(payload.inbound_radar);
            }

            // V17: Greeting with first name
            const dName = auth.currentUser?.displayName || '';
            const firstName = dName.split(' ')[0] || '';
            fcUpdateGreeting(firstName);
        }
    } catch (error) {
        console.error('Execution failed:', error);
    }

    // ── Silent Persona Vault Migration ─────────────────────────────────────
    // Fire-and-forget: idempotent on backend. Does NOT block UI.
    _runPersonaMigration();
}

async function _runPersonaMigration() {
    try {
        const user = firebase.auth().currentUser;
        if (!user) return;
        const token = await user.getIdToken();
        const resp  = await fetch(`${API_BASE}/api/migrate-personas`, {
            method:  'POST',
            headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
            body:    JSON.stringify({})
        });
        const json = await resp.json().catch(() => ({}));
        if (json.migrated) {
            console.log(`[MIGRATION] Legacy persona created: ${json.name} (${json.persona_id})`);
            window._personasCache = []; // invalidate so next vault load is fresh
        }
    } catch(e) {
        // Non-fatal — migration will retry on next login
        console.warn('[MIGRATION] Silent migration failed (non-fatal):', e);
    }
}

// ─── CAMPAIGN DATA STORE ────────────────────────────────────────────────────
// Campaign objects are stored here at load time.
// Buttons in the DOM only carry data-campaign-id — no data in onclick attrs.
// openEditModal(id) does an O(1) Map lookup — eliminates all char-escaping bugs.
window._campaignsStore = new Map();

// Dynamic Campaign Hydration via REST API
window.campaignsCurrentPage = 0;
const CAMPAIGNS_PAGE_SIZE = 10;
window._cachedCampaigns = [];

async function loadCampaigns() {
    const feed      = document.getElementById('active-campaign-feed');
    const tableBody = document.getElementById('campaign-list-table');
    const filterSel = document.getElementById('campaign-filter');

    try {
        const user = firebase.auth().currentUser;
        if (!user) return handleAuthRejection();

        const token = await user.getIdToken();
        const res   = await fetch(`${API_BASE}/api/campaigns`, {
            method: 'GET',
            headers: { 'Authorization': `Bearer ${token}` }
        });

        if (res.status === 401 || res.status === 403) return handleAuthRejection();
        if (!res.ok) {
            console.error('loadCampaigns HTTP error:', res.status, await res.text());
            if (tableBody) tableBody.innerHTML = '<tr><td colspan="4" class="empty-state">Failed to load campaigns.</td></tr>';
            return;
        }

        const payload   = await res.json();
        const campaigns = Array.isArray(payload.data) ? payload.data : [];

        // populate store — all downstream reads come from here
        window._campaignsStore.clear();
        campaigns.forEach(c => window._campaignsStore.set(c.id, c));

        if (campaigns.length === 0) {
            window.activeCampaignCount = 0;
            renderExpansionState(0);
            if (feed)      feed.innerHTML = '';
            if (tableBody) tableBody.innerHTML = '<tr><td colspan="4" style="padding:16px;text-align:center;">No campaigns found. Click \u201cFind New Clients\u201d to get started.</td></tr>';
            return;
        }

        campaigns.sort((a, b) => (b.createdAt || '').localeCompare(a.createdAt || ''));
        window._cachedCampaigns = campaigns;

        let activeCount = 0;
        let filterOpts  = '<option value="all">All Searches</option>';

        campaigns.forEach(camp => {
            if (camp.status === 'active') activeCount++;
            filterOpts += `<option value="${_escapeHTML(camp.id)}">${_escapeHTML(camp.name)}</option>`;
        });

        window.activeCampaignCount = activeCount;
        renderCampaignsTable(campaigns, activeCount);
        renderExpansionState(activeCount);

        if (filterSel) {
            const prev = filterSel.value;
            filterSel.innerHTML = filterOpts;
            filterSel.value = prev || 'all';
        }

        if (feed) {
            feed.innerHTML = `
                <div style="background:rgba(79,70,229,0.05);border:1px solid rgba(79,70,229,0.2);padding:12px;border-radius:8px;margin-bottom:24px;">
                    <span class="badge" style="background:var(--primary);">System Status: Online</span>
                    <span style="color:var(--text-muted);font-size:0.9rem;margin-left:8px;">Scanning ${activeCount} Active Target Matrix${activeCount !== 1 ? 'es' : ''}</span>
                </div>`;
        }
    } catch (error) {
        console.error("Campaign Hook Error:", error);
        if (tableBody) tableBody.innerHTML = '<tr><td colspan="4" style="padding:16px; text-align:center; color: #ef4444;">API Connection Error</td></tr>';
    }
}

function renderCampaignsTable(campaigns, activeCount) {
    const tableBody = document.getElementById('campaign-list-table');
    const pagEl = document.getElementById('campaigns-pagination');
    if (!tableBody) return;

    const pageCount = Math.ceil(campaigns.length / CAMPAIGNS_PAGE_SIZE) || 1;
    if (window.campaignsCurrentPage >= pageCount) window.campaignsCurrentPage = pageCount - 1;
    if (window.campaignsCurrentPage < 0) window.campaignsCurrentPage = 0;

    const startIndex = window.campaignsCurrentPage * CAMPAIGNS_PAGE_SIZE;
    const pageCampaigns = campaigns.slice(startIndex, startIndex + CAMPAIGNS_PAGE_SIZE);

    let tableRows = '';
    pageCampaigns.forEach(camp => {
        const id       = camp.id;
        const isActive = camp.status === 'active';
        const statusColor = isActive ? '#25D366' : '#ef4444';
        const statusBadge = `<span style="font-size:0.75rem;padding:2px 8px;border-radius:4px;border:1px solid ${statusColor};color:${statusColor};">${(camp.status || 'unknown').toUpperCase()}</span>`;
        const geoWarn     = (camp.gl && camp.location) ? '' : '<span style="color:#ea580c;font-size:0.75rem;display:block;margin-top:4px;">&#9888; Location Missing: Edit to set targeting</span>';

        const kw = (camp.keywords || 'N/A');
        const kwDisplay = kw.length > 80 ? kw.substring(0, 80) + '\u2026' : kw;

        // V26.0.2: Intelligence strategy badge — plain English labels
        const _stratMap = {
            'PLATFORM_MINING':       { icon: '💠', label: 'Scanning directories', color: '#8b5cf6' },
            'COLLOQUIAL_DISCOVERY':  { icon: '💬', label: 'Searching forums', color: '#06b6d4' },
            'COMPETITOR_TOUCHPOINT': { icon: '⭐', label: 'Mining reviews', color: '#f59e0b' },
            'PROFESSIONAL_NETWORK':  { icon: '🔗', label: 'Finding professionals', color: '#3b82f6' },
            'EVENT_TRIGGER_MINING':  { icon: '📰', label: 'Tracking events', color: '#ef4444' },
        };
        const _strat = (camp.intelligence_strategy || {}).primary || '';
        const _stratInfo = _stratMap[_strat] || { icon: '🔄', label: 'Awaiting AI scan', color: '#6b7280' };
        const stratBadge = `<span style="font-size:0.7rem;padding:2px 6px;border-radius:4px;border:1px solid ${_stratInfo.color};color:${_stratInfo.color};white-space:nowrap;margin-left:6px;" title="AI will pick the best discovery method on next update">${_stratInfo.icon} ${_stratInfo.label}</span>`;

        tableRows += `
            <tr style="border-bottom:1px solid var(--glass-border);">
                <td style="padding:12px;">
                    <strong>${_escapeHTML(camp.name || 'Untitled')}</strong>
                    ${geoWarn}
                </td>
                <td style="padding:12px;">
                    <span style="color:var(--text-muted);font-size:0.85rem;">${_escapeHTML(kwDisplay)}</span>
                </td>
                <td style="padding:12px;">${statusBadge} ${stratBadge}</td>
                <td style="padding:12px;text-align:right;white-space:nowrap;">
                    <button class="secondary-btn" style="padding:4px 10px;font-size:0.75rem;margin-right:4px;"
                        data-campaign-id="${id}"
                        onclick="openEditModal(this.dataset.campaignId)">Edit</button>
                    <button class="secondary-btn" style="padding:4px 10px;font-size:0.75rem;margin-right:4px;"
                        data-campaign-id="${id}"
                        title="Export all lead fields as CSV"
                        onclick="exportLeadsDownload('csv', this.dataset.campaignId)">CSV</button>
                    <button class="secondary-btn" style="padding:4px 10px;font-size:0.75rem;margin-right:4px;"
                        data-campaign-id="${id}"
                        title="Export all lead fields as JSON"
                        onclick="exportLeadsDownload('json', this.dataset.campaignId)">JSON</button>
                    <button class="secondary-btn" style="padding:4px 10px;font-size:0.75rem;border-color:${statusColor};color:${statusColor};"
                        onclick="toggleCampaignStatus('${id}','${camp.status}')">${isActive ? 'Pause' : 'Resume'}</button>
                </td>
            </tr>`;
    });

    tableBody.innerHTML = tableRows;

    if (!pagEl) return;
    if (pageCount <= 1) {
        pagEl.innerHTML = '';
        return;
    }

    let html = '';
    html += `<button class="sio-page-btn" ${window.campaignsCurrentPage === 0 ? 'disabled' : ''} onclick="changeCampaignsPage(${window.campaignsCurrentPage - 1})">&larr; Prev</button>`;
    for (let i = 0; i < pageCount; i++) {
        html += `<button class="sio-page-btn ${i === window.campaignsCurrentPage ? 'active' : ''}" onclick="changeCampaignsPage(${i})">${i + 1}</button>`;
    }
    html += `<button class="sio-page-btn" ${window.campaignsCurrentPage === pageCount - 1 ? 'disabled' : ''} onclick="changeCampaignsPage(${window.campaignsCurrentPage + 1})">Next &rarr;</button>`;
    pagEl.innerHTML = html;
}

window.changeCampaignsPage = function(pageIndex) {
    window.campaignsCurrentPage = pageIndex;
    if (window._cachedCampaigns) {
        renderCampaignsTable(window._cachedCampaigns, window.activeCampaignCount);
    }
};

/**
 * V27.6: Export leads with all Firestore fields via GET /api/leads/export.
 * Nested objects/arrays are JSON-stringified in CSV; JSON keeps full structure.
 * @param {'json'|'csv'} format
 * @param {string} [campaignId] optional campaign filter; falls back to #campaign-filter
 */
window.exportLeadsDownload = async function(format, campaignId) {
    const fmt = (format === 'csv') ? 'csv' : 'json';
    try {
        const user = firebase.auth().currentUser;
        if (!user) {
            showToast('Sign in required to export.', 'error');
            return;
        }
        const token = await user.getIdToken();
        const filterEl = document.getElementById('campaign-filter');
        const statusEl = document.getElementById('export-status-filter');
        let camp = (campaignId || '').trim();
        if (!camp && filterEl && filterEl.value && filterEl.value !== 'all') {
            camp = filterEl.value;
        }
        const status = (statusEl && statusEl.value) ? statusEl.value : 'new';
        const params = new URLSearchParams({
            format: fmt,
            status: status,
            limit: '5000',
        });
        if (camp) params.set('campaign_id', camp);

        showToast('Preparing export…', 'success');
        const res = await fetch(`${API_BASE}/api/leads/export?${params.toString()}&rt=${Date.now()}`, {
            method: 'GET',
            headers: { 'Authorization': `Bearer ${token}` },
        });
        if (!res.ok) {
            let msg = `Export failed (${res.status})`;
            try {
                const errBody = await res.json();
                if (errBody.error) msg = errBody.error;
            } catch (_) { /* ignore parse errors */ }
            showToast(msg, 'error');
            return;
        }

        let countHdr = res.headers.get('X-Export-Count');
        let filename = `sideio-leads-${camp || 'all'}.${fmt}`;
        const cd = res.headers.get('Content-Disposition') || '';
        const m = /filename="([^"]+)"/.exec(cd);
        if (m) filename = m[1];

        let blob;
        if (fmt === 'json') {
            const text = await res.text();
            if (countHdr == null) {
                try {
                    const parsed = JSON.parse(text);
                    if (parsed && typeof parsed.count === 'number') {
                        countHdr = String(parsed.count);
                    }
                } catch (_) { /* keep header-less count */ }
            }
            blob = new Blob([text], { type: 'application/json;charset=utf-8' });
        } else {
            blob = await res.blob();
        }

        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = filename;
        a.setAttribute('aria-label', `Download leads export as ${fmt.toUpperCase()}`);
        document.body.appendChild(a);
        a.click();
        a.remove();
        URL.revokeObjectURL(url);
        const n = countHdr != null ? countHdr : '?';
        showToast(`Exported ${n} lead(s) as ${fmt.toUpperCase()}.`, 'success');
    } catch (err) {
        console.error('exportLeadsDownload', err);
        showToast('Export failed — check network and try again.', 'error');
    }
};



// =============================================================================
// FIRESTORE LIVE FEED
// =============================================================================
// (unsubscribeLeads declared at top-level to avoid temporal dead zone)

async function loadLeads() {
    leadsList.innerHTML = '<div class="lead-card pulse">Connecting to Secure Orchestrator...</div>';
    try {
        const user = firebase.auth().currentUser;
        if (!user) return handleAuthRejection();
        if (unsubscribeLeads) { unsubscribeLeads(); }
        unsubscribeLeads = firebase.firestore()
            .collection('leads')
            .where('tenant_id', '==', user.uid)
            .where('is_in_crm', '==', false)
            // NOTE: No where('type','==','outbound') filter — this is a unified
            // firehose. Inbound/outbound split is handled client-side by _isInbound().
            // The only filters are tenant_id + is_in_crm (CRM leads load separately).
            .orderBy('createdAt', 'desc')
            .limit(200)
            .onSnapshot((snapshot) => {
                // ── V23.9: Indexed ingestion + split routing ────────────────
                rawLeadsCache = [];
                inboundCache  = [];
                outboundCache = [];
                _leadsMap.clear();
                let _inboundArrived = false;
                snapshot.forEach(doc => {
                    let data = doc.data();
                    data.id = doc.id;
                    if ((data.status || 'new') !== 'new') return;
                    rawLeadsCache.push(data);
                    _leadsMap.set(doc.id, data);  // O(1) lookup for observer
                    // V23.9: Raw data shape logger — traces ALL leads for Radar debugging
                    if (window.SIO_DEBUG) console.log('[Radar Raw DB Shape]', { id: data.id, source: data.source, type: data.type, is_inbound: data.is_inbound, sourcing_vector: data.sourcing_vector });
                    // Route into split caches
                    if (_isInbound(data)) {
                        inboundCache.push(data);
                        _inboundArrived = true;
                        if (window.SIO_DEBUG) console.log('[Radar] Routed to Inbound:', data.id, '| source:', data.source, '| type:', data.type);
                    } else {
                        outboundCache.push(data);
                    }
                });
                // V23.8: Show radar pulse if inbound leads exist but user is on outbound tab
                if (_inboundArrived && CURRENT_FEED_MODE !== 'inbound') {
                    const dot = document.querySelector('.radar-pulse-dot');
                    if (dot) dot.classList.remove('d-none');
                }
                if (rawLeadsCache.length === 0) { _scheduleRender(); return; }
                // Client-side tiebreaker: highest score wins when createdAt is equal.
                // Primary sort is already chronological desc from Firestore.
                const _sortChron = (a, b) => {
                    const tA = a.createdAt?.toMillis ? a.createdAt.toMillis() : (a.createdAt ? new Date(a.createdAt).getTime() : 0);
                    const tB = b.createdAt?.toMillis ? b.createdAt.toMillis() : (b.createdAt ? new Date(b.createdAt).getTime() : 0);
                    if (tB !== tA) return tB - tA;          // newest first
                    return (b.score || 0) - (a.score || 0); // score tiebreaker
                };
                rawLeadsCache.sort(_sortChron);
                inboundCache.sort(_sortChron);
                outboundCache.sort(_sortChron);
                fcUpdateKPIs(rawLeadsCache);
                _scheduleRender();  // V23.9: debounced via rAF
            }, async (error) => {
                console.error('[Firestore] onSnapshot error:', error);
                if (error.code === 'failed-precondition') {
                    showToast('Feed index missing — see console for index link.', 'error');
                    leadsList.innerHTML = '<div class="lead-card" style="color:#f59e0b;border-color:#f59e0b;padding:16px;">⚠ Firestore composite index required.<br><small>Open the browser console for the GCP link.</small></div>';
                    return;
                }
                // J-16 FIX: On permission-denied (Firebase Auth token expired after 1h),
                // force-refresh the token and re-subscribe instead of silently dying.
                // Without this, the lead feed freezes after 1 hour with no visible error.
                if (error.code === 'permission-denied') {
                    console.warn('[Firestore] Token expired — forcing refresh and reconnecting listener.');
                    try {
                        const u = firebase.auth().currentUser;
                        if (u) {
                            await u.getIdToken(true);
                            setTimeout(() => loadLeads(), 2000); // re-subscribe after token refresh
                        }
                    } catch(refreshErr) {
                        console.error('[Firestore] Token refresh failed:', refreshErr);
                        showToast('Session expired — please refresh.', 'error');
                    }
                    return;
                }
                showToast('Live feed error — refresh to reconnect.', 'error');
            });
    } catch (error) {
        console.error('Firestore Initialization Error:', error);
        leadsList.innerHTML = '<div class="lead-card" style="color:#ef4444;border-color:#ef4444;">Could not connect to database. Check your network.</div>';
        showToast('Connection Refused', 'error');
    }
}

// =============================================================================
// VIRTUAL SCROLL OBSERVER
// =============================================================================

let virtualObserver = new IntersectionObserver((entries) => {
    entries.forEach(entry => {
        if (entry.isIntersecting && !entry.target.hasAttribute('data-rendered')) {
            const leadId = entry.target.getAttribute('data-lead-id');
            // V23.9: O(1) lookup via _leadsMap (populated during onSnapshot)
            const lead = _leadsMap.get(leadId);
            if (lead) {
                const newCard = window.createLeadCardV2(leadId, lead);
                entry.target.replaceWith(newCard);
                virtualObserver.observe(newCard);
                newCard.setAttribute('data-rendered', 'true');
            }
        }
    });
}, { rootMargin: '600px' });

window.leadsCurrentPage = 0;
const LEADS_PAGE_SIZE = 10;

function renderLeads() {
    const baseData = (CURRENT_FEED_MODE === 'inbound') ? inboundCache : outboundCache;

    const filteredLeads = baseData.filter(lead => {
        if ((lead.status || 'new') !== 'new') return false;
        if (currentCampaignFilter !== 'all') {
            const matched = Array.isArray(lead.matched_campaigns)
                ? lead.matched_campaigns.includes(currentCampaignFilter)
                : lead.campaign_id === currentCampaignFilter;
            if (!matched) return false;
        }
        return true;
    });

    const pagEl = document.getElementById('leads-pagination');

    if (filteredLeads.length === 0) {
        leadsList.innerHTML = '<div class="lead-card" style="text-align:center;padding:40px;border:none;background:transparent;box-shadow:none;"><div style="font-size:3rem;margin-bottom:12px;opacity:0.8;">🎯</div><h3 style="color:var(--text-main);margin-bottom:8px;">Hunting for leads...</h3><p style="color:var(--text-muted);font-size:0.95rem;line-height:1.5;">We are actively scanning the web. Check back in a few minutes.</p></div>';
        if (pagEl) pagEl.innerHTML = '';
        return;
    }

    const pageCount = Math.ceil(filteredLeads.length / LEADS_PAGE_SIZE) || 1;
    if (window.leadsCurrentPage >= pageCount) window.leadsCurrentPage = pageCount - 1;
    if (window.leadsCurrentPage < 0) window.leadsCurrentPage = 0;

    const startIndex = window.leadsCurrentPage * LEADS_PAGE_SIZE;
    const pageLeads = filteredLeads.slice(startIndex, startIndex + LEADS_PAGE_SIZE);

    leadsList.innerHTML = '';
    virtualObserver.disconnect();

    pageLeads.forEach(lead => {
        const docId = lead.id || lead.doc_id;
        const cardEl = window.createLeadCardV2(docId, lead);
        leadsList.appendChild(cardEl);
    });

    renderLeadsPagination(filteredLeads.length);
}

function renderLeadsPagination(totalCount) {
    const pagEl = document.getElementById('leads-pagination');
    if (!pagEl) return;

    const pageCount = Math.ceil(totalCount / LEADS_PAGE_SIZE) || 1;
    if (pageCount <= 1) {
        pagEl.innerHTML = '';
        return;
    }

    let html = '';
    html += `<button class="sio-page-btn" ${window.leadsCurrentPage === 0 ? 'disabled' : ''} onclick="changeLeadsPage(${window.leadsCurrentPage - 1})">&larr; Prev</button>`;

    for (let i = 0; i < pageCount; i++) {
        if (pageCount > 8) {
            if (i === 0 || i === pageCount - 1 || Math.abs(i - window.leadsCurrentPage) <= 1) {
                html += `<button class="sio-page-btn ${i === window.leadsCurrentPage ? 'active' : ''}" onclick="changeLeadsPage(${i})">${i + 1}</button>`;
            } else if (i === 1 && window.leadsCurrentPage > 2) {
                html += `<span class="sio-page-info">&hellip;</span>`;
            } else if (i === pageCount - 2 && window.leadsCurrentPage < pageCount - 3) {
                html += `<span class="sio-page-info">&hellip;</span>`;
            }
        } else {
            html += `<button class="sio-page-btn ${i === window.leadsCurrentPage ? 'active' : ''}" onclick="changeLeadsPage(${i})">${i + 1}</button>`;
        }
    }

    html += `<button class="sio-page-btn" ${window.leadsCurrentPage === pageCount - 1 ? 'disabled' : ''} onclick="changeLeadsPage(${window.leadsCurrentPage + 1})">Next &rarr;</button>`;
    pagEl.innerHTML = html;
}

window.changeLeadsPage = function(pageIndex) {
    window.leadsCurrentPage = pageIndex;
    renderLeads();
    const leadsListEl = document.getElementById('leads-list');
    if (leadsListEl) {
        leadsListEl.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }
};

// =============================================================================
// TOAST UI ENGINE
// =============================================================================

// window.showToast is already defined as a hoisted function at top of file.

// Timeline Audit Modal
window.viewLeadTimeline = function(eventsJson) {
    try {
        const events = JSON.parse(decodeURIComponent(eventsJson)) || [];
        const feed = document.getElementById('audit-timeline-feed');
        if (!feed) return;
        if (events.length === 0) {
            feed.innerHTML = '<p style="color:var(--text-muted);text-align:center;">No CRM interactions recorded yet.</p>';
        } else {
            feed.innerHTML = events.map(e => `
                <div style="padding:12px;border-left:3px solid var(--primary);margin-bottom:12px;background:white;border-radius:0 4px 4px 0;box-shadow:0 1px 2px rgba(0,0,0,0.05);">
                    <small style="color:var(--text-muted);display:block;margin-bottom:4px;">${_escapeHTML(e.date)}</small>
                    <strong style="color:var(--text-main);font-size:0.95rem;">${_escapeHTML(e.action)}</strong>
                </div>`).join('');
        }
        document.getElementById('audit-log-modal').style.display = 'flex';
    } catch(e) { console.error('Timeline error', e); }
};


// =============================================================================
// CORE API MUTATION HELPER — used by pushToCRM, updateLeadStatus, etc.
// =============================================================================

// FE-12: Double-click / concurrent-call protection guard
let _apiMutationInFlight = false;
let _apiMutationTimer = null;

async function performApiMutation(url, method, payload) {
    if (_apiMutationInFlight) {
        console.warn('[performApiMutation] Blocked concurrent call to', url);
        return false;
    }
    const user = auth.currentUser;
    if (!user) return false;
    _apiMutationInFlight = true;
    // V26.0.3: Safety timeout — auto-reset after 10s in case fetch hangs
    // or an unhandled rejection leaves the flag stuck forever.
    _apiMutationTimer = setTimeout(() => {
        if (_apiMutationInFlight) {
            console.warn('[performApiMutation] Safety timeout — resetting stale lock');
            _apiMutationInFlight = false;
        }
    }, 10000);
    try {
        // iOS Safari fix: force=true guarantees a fresh token even when the
        // Safari background throttling has invalidated the in-memory cached token.
        const token = await user.getIdToken(true);
        const response = await fetch(`${API_BASE}${url}`, {
            method: method,
            headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });
        if (response.status === 401 || response.status === 403) { handleAuthRejection(); return false; }
        if (!response.ok) throw new Error('API Execution Failed');
        return true;
    } finally {
        clearTimeout(_apiMutationTimer);
        _apiMutationInFlight = false;
    }
}

// =============================================================================
// CRM PUSH — moves lead from main feed into the CRM pipeline
// =============================================================================

window.pushToCRM = async function(docId, leadStr) {
    try {
        const success = await performApiMutation(`/api/leads/${docId}`, 'PUT', {
            is_in_crm:       true,
            crm_status:      'new',
            estimated_value: 0,
            notes:           [],
            expire_at:       null
        });
        if (success) {
            const cardEl = document.getElementById(docId);
            if (cardEl) { virtualObserver.unobserve(cardEl); cardEl.remove(); }
            _evictLeadFromCaches(docId);  // V23.9: sync all split caches
            showToast('✅ Lead saved to CRM pipeline — view it in the CRM sidebar.', 'success');
            const userUrl = window.currentUserData?.crm_webhook_url;
            if (userUrl) {
                if (!userUrl.startsWith('https://')) {
                    showToast('Webhook URL must use HTTPS', 'error');
                } else {
                    try {
                        const lead = JSON.parse(decodeURIComponent(leadStr));
                        fetch(userUrl, { method:'POST', headers:{'Content-Type':'application/json'}, mode:'no-cors', body: JSON.stringify({event:'lead_pushed', lead}) });
                    } catch(_) {}
                }
            }
        }
    } catch(e) {
        console.error('CRM Push failure', e);
        showToast('CRM push failed — try again.', 'error');
    }
};

// =============================================================================
// =============================================================================
// CATEGORICAL REJECTION MODAL (RLHF)
// Replaces binary 'ignored'. User selects one of 5 reasons, which drives the
// dynamic ontology weight penalty in the Orchestrator's RLHF engine.
// =============================================================================

window.openRejectionModal = function(docId) {
    document.getElementById('rejection-lead-id').value = docId;
    // Optimistic removal from visible feed so UX feels instant
    _evictLeadFromCaches(docId);  // V23.9: sync all split caches
    renderLeads();
    showModal('rejection-modal');
};

window.submitRejection = async function(reason) {
    const VALID = ['not_b2b', 'wrong_industry', 'too_small', 'competitor', 'author', 'bad_data'];
    if (!VALID.includes(reason)) return;
    const docId = document.getElementById('rejection-lead-id').value;
    if (!docId) return;
    closeModal('rejection-modal');
    const labels = { not_b2b: 'Not B2B', wrong_industry: 'Wrong Industry',
                     too_small: 'Too Small', competitor: 'Competitor',
                     author: 'Author / Non-Prospect', bad_data: 'Bad Data' };
    try {
        const user = firebase.auth().currentUser;
        if (!user) return;
        const token = await user.getIdToken(true);
        const resp  = await fetch(`${API_BASE}/api/leads/${docId}`, {
            method:  'PUT',
            headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
            body:    JSON.stringify({ status: 'rejected', rejection_reason: reason })
        });
        if (resp.ok) {
            showToast(`Lead rejected: ${labels[reason] || reason}. AI is learning.`, 'success');
            loadDashboard();
        } else {
            showToast('Lead removed. API sync failed — will retry.', 'info');
        }
    } catch(err) {
        console.error('[rejection]', err);
        showToast('Lead removed. Background sync failed.', 'info');
    }
};

// =============================================================================
// LEAD STATUS UPDATE — contacted / converted (rejection goes through modal above)
// =============================================================================
window.updateLeadStatus = async function(docId, newStatus) {
    // Route all skip/reject actions through the categorical RLHF modal
    if (newStatus === 'ignored' || newStatus === 'rejected') {
        openRejectionModal(docId);
        return;
    }
    try {
        const success = await performApiMutation(`/api/leads/${docId}`, 'PUT', { status: newStatus });
        if (success) {
            showToast(`Lead status updated: ${newStatus}`, 'success');
            loadDashboard();
        }
    } catch(err) {
        showToast('Error saving update to database', 'error');
    }
};

// =============================================================================
// CAMPAIGN ACTIONS — toggle pause/resume, update details
// =============================================================================

window.toggleCampaignStatus = async function(id, currentStatus) {
    const newStatus = currentStatus === 'active' ? 'paused' : 'active';
    try {
        const success = await performApiMutation(`/api/campaigns/${id}`, 'PUT', { status: newStatus });
        if (success) { showToast(`Campaign ${newStatus} successfully`, 'success'); loadDashboard(); }
    } catch(err) {
        showToast('Status update failed', 'error');
    }
};

// Campaign filter dropdown — called by onchange="filterLeadsByCampaign(this.value)"
window.filterLeadsByCampaign = function(campaignId) {
    currentCampaignFilter = campaignId || 'all';
    window.leadsCurrentPage = 0;
    renderLeads();
};



window.updateCampaignAction = async function(id) {
    const nameInput = document.getElementById(`edit-camp-name-${id}`);
    const bioInput  = document.getElementById(`edit-camp-bio-${id}`);
    const keysInput = document.getElementById(`edit-camp-keys-${id}`);
    const urlsInput = document.getElementById(`edit-camp-urls-${id}`);
    if (!nameInput || !keysInput) return;
    try {
        const success = await performApiMutation(`/api/campaigns/${id}`, 'PUT', {
            name: nameInput.value, bio: bioInput?.value || '', keywords: keysInput.value
        });
        if (success) { showToast('Campaign successfully updated!', 'success'); loadDashboard(); }
    } catch(err) {
        showToast('Error modifying campaign', 'error');
    }
};


// =============================================================================
// CAMPAIGN EDIT MODAL — openEditModal / closeEditModal / saveEditedCampaign
// These functions were removed during the campaign<>persona decoupling but
// are still called from the dynamically-generated campaign table HTML.
// All three were missing — causing ReferenceError on every Edit button click.
// =============================================================================

window.handleCountryChange = function(glSelectId, locationInputId) {
    const gl = document.getElementById(glSelectId)?.value || '';
    const locInput = document.getElementById(locationInputId);
    if (!locInput) return;
    if (!gl) {
        locInput.placeholder = 'City, State/Region';
        locInput.value = '';
    } else {
        locInput.placeholder = 'e.g. New York, London, Mumbai';
    }
};

window.openEditModal = function(id) {
    const camp = window._campaignsStore.get(id);
    if (!camp) {
        showToast('Campaign data not found. Please refresh.', 'error');
        console.error('[openEditModal] id not in store:', id);
        return;
    }

    document.getElementById('edit-camp-id').value   = id;
    document.getElementById('edit-camp-name').value = camp.name || '';

    // ── Child Campaign Bio Resolution ──────────────────────────────────────────
    // CHILD_CAMPAIGN_OVERRIDE is set when a campaign originates from the
    // Digital Twin website analysis flow. The real bio comes from
    // campaign_focus + pain_point + unfair_advantage fields on the campaign doc.
    // ───────────────────────────────────────────────────
    const isChildCampaign = camp.bio === 'CHILD_CAMPAIGN_OVERRIDE';
    let bioDisplay, keywordsDisplay;

    if (isChildCampaign) {
        const focus = camp.campaign_focus    || camp.name || '';
        const pain  = camp.pain_point        || '';
        const adv   = camp.unfair_advantage  || '';
        bioDisplay = [
            focus ? `Product/Service: ${focus}` : '',
            pain  ? `Market Hook: ${pain}` : '',
            adv   ? `Competitive Advantage: ${adv}` : ''
        ].filter(Boolean).join('\n\n');
        const tenantBio = window.currentUserData?.company_description || window.currentUserData?.bio || '';
        keywordsDisplay = tenantBio || camp.keywords || '';
    } else {
        bioDisplay      = camp.bio      || '';
        keywordsDisplay = camp.keywords || '';
    }

    document.getElementById('edit-camp-bio').value  = bioDisplay;
    document.getElementById('edit-camp-keys').value = keywordsDisplay;

    // ── Active Agent / Persona Lock Logic ─────────────────────────────────────
    // If the campaign has a persona_id attached, the AI is already using the
    // detailed Agent instructions. Lock the legacy bio/keywords fields to prevent
    // a confusing override UX where basic text fields appear editable but have
    // no effect on the actual AI pipeline.
    // ──────────────────────────────────────────────────────────────────────────
    const personaId   = camp.persona_id   || '';
    const personaName = camp.persona_name || (personaId ? 'Attached Agent' : '');

    const agentBlock  = document.getElementById('edit-active-agent-block');
    const agentLabel  = document.getElementById('edit-agent-name-label');
    const bioEl       = document.getElementById('edit-camp-bio');
    const keysEl      = document.getElementById('edit-camp-keys');
    const bioHint     = document.getElementById('edit-bio-lock-hint');
    const keysHint    = document.getElementById('edit-keys-lock-hint');

    if (personaId) {
        // ── PERSONA ATTACHED: show badge, lock fields ──────────────────────
        agentLabel.textContent     = personaName;
        agentBlock.style.display   = 'block';

        // Gray out both textareas
        const lockedStyle = {
            opacity:         '0.45',
            pointerEvents:   'none',
            background:      '#f3f4f6',
            borderColor:     '#e5e7eb',
            color:           '#6b7280',
            cursor:          'not-allowed',
        };
        Object.assign(bioEl.style,  lockedStyle);
        Object.assign(keysEl.style, lockedStyle);
        bioEl.setAttribute('disabled', 'disabled');
        keysEl.setAttribute('disabled', 'disabled');

        // Lock hint messages
        const lockMsg = `🔒 Locked: The AI is using the detailed instructions from your attached Agent (${personaName}).`;
        bioHint.textContent  = lockMsg;
        keysHint.textContent = lockMsg;
        bioHint.style.display  = 'block';
        keysHint.style.display = 'block';

    } else {
        // ── NO PERSONA: hide badge, unlock fields (legacy campaign) ────────
        agentBlock.style.display = 'none';
        agentLabel.textContent   = '—';

        const unlockedStyle = {
            opacity:       '',
            pointerEvents: '',
            background:    '',
            borderColor:   '',
            color:         '',
            cursor:        '',
        };
        Object.assign(bioEl.style,  unlockedStyle);
        Object.assign(keysEl.style, unlockedStyle);
        bioEl.removeAttribute('disabled');
        keysEl.removeAttribute('disabled');

        bioHint.style.display  = 'none';
        keysHint.style.display = 'none';
        bioHint.textContent    = '';
        keysHint.textContent   = '';
    }

    // ── Cascading Geo Dropdown hydration (V23.6) ─────────────────────────────
    // Initialise the cascade and hydrate from geo_hierarchy (new) or gl/location (legacy)
    initGeoCascade(camp.geo_hierarchy || null, camp.gl || '', camp.location || '');

    // ── V26.0.2: Intelligence Strategy hydration (read-only, plain English) ──
    const _stratHint   = document.getElementById('edit-strategy-current');
    const _autoStrat   = (camp.intelligence_strategy || {}).primary || '';
    if (_stratHint && _autoStrat) {
        const _stratDescriptions = {
            'PLATFORM_MINING':       '💠 Scanning business directories and listing platforms to find leads already listed by competitors.',
            'COLLOQUIAL_DISCOVERY':  '💬 Searching forums and social media for people talking about problems your business solves.',
            'COMPETITOR_TOUCHPOINT': '⭐ Mining competitor reviews and ratings to find their unhappy customers.',
            'PROFESSIONAL_NETWORK':  '🔗 Finding decision-makers discussing relevant topics on professional platforms.',
            'EVENT_TRIGGER_MINING':  '📰 Tracking public events (funding, hiring, expansions) that signal buying intent.',
        };
        _stratHint.textContent = _stratDescriptions[_autoStrat] || `AI Mode: ${_autoStrat}`;
    } else if (_stratHint) {
        _stratHint.textContent = '🔄 Not classified yet — save any edit and our AI will pick the best discovery method automatically.';
    }

    showModal('edit-campaign-modal');
};


window.closeEditModal = function() {
    closeModal('edit-campaign-modal');
};

window.saveEditedCampaign = async function() {
    const id       = document.getElementById('edit-camp-id')?.value        || '';
    const name     = document.getElementById('edit-camp-name')?.value      || '';
    const bio      = document.getElementById('edit-camp-bio')?.value       || '';
    const keywords = document.getElementById('edit-camp-keys')?.value      || '';
    // gl + location are synced from cascade via hidden inputs (_updateGeoPreview)
    const gl       = document.getElementById('edit-camp-gl')?.value        || '';
    const location = document.getElementById('edit-camp-location')?.value  || '';
    // Build structured geo_hierarchy (V27.8.1: empty country/region → All)
    const _ghContinent = document.getElementById('geo-continent-select')?.value || '';
    const geo_hierarchy = {
        continent: _ghContinent,
        country:   document.getElementById('geo-country-select')?.value   || (_ghContinent ? 'All' : ''),
        region:    document.getElementById('geo-region-select')?.value    || (_ghContinent ? 'All' : ''),
    };
    if (!id)   { showToast('Campaign ID missing. Please refresh.', 'error'); return; }
    if (!name) { showToast('Campaign name is required.', 'error'); return; }
    if (!geo_hierarchy.continent) {
        showToast('Target Geography is required. Select Global or a continent.', 'error');
        return;
    }

    // V26.0.2: Strategy is 100% backend — no client-side override
    const payload = {
        name, bio, keywords, gl,
        location: location || _compileGeoString(geo_hierarchy.continent, geo_hierarchy.country, geo_hierarchy.region),
        geo_hierarchy,
    };

    try {
        const success = await performApiMutation(`/api/campaigns/${id}`, 'PUT', payload);
        if (success) {
            showToast('Campaign updated successfully!', 'success');
            window.closeEditModal();
            loadDashboard();
        } else {
            showToast('Update failed. Please try again.', 'error');
        }
    } catch(err) {
        console.error('saveEditedCampaign error:', err);
        showToast('API error updating campaign.', 'error');
    }
};

// =============================================================================
// SAVE CAMPAIGN ACTION — called from fc modal Step 2 "Launch" button
// =============================================================================

window.saveCampaignAction = async function(payload) {
    let cpName = '', cpBio = '', cpKeys = '', cpGl = '', cpLoc = '';

    if (payload) {
        cpName = payload.name || '';
        cpBio = payload.bio || '';
        cpKeys = payload.keywords || '';
        cpGl = payload.gl || '';
        cpLoc = payload.location || '';
    } else {
        const nameInput      = document.getElementById('camp-name');
        const bioInput       = document.getElementById('camp-bio');
        const keysInput      = document.getElementById('camp-keys');
        const glInput        = document.getElementById('camp-gl');
        const locationInput  = document.getElementById('camp-location');

        cpName = nameInput?.value || '';
        cpBio = bioInput?.value || '';
        cpKeys = keysInput?.value || '';
        cpGl = glInput?.value || '';
        cpLoc = locationInput?.value || '';
    }

    // ── PLG Validation: Child Campaign Path ──────────────────────────────────
    // DT/AI-generated campaigns pass bio='CHILD_CAMPAIGN_OVERRIDE' + campaign_focus
    // instead of manual keywords. Synthesize keywords from AI fields so the backend
    // producer guard (requires non-empty keywords) never trips on AI payloads.
    // Manual campaigns still require both name and keywords.
    // ──────────────────────────────────────────────────────────────────────────
    if (!cpName) {
        showToast('Campaign Name is required.', 'error');
        return;
    }

    const isAIGenerated = (cpBio === 'CHILD_CAMPAIGN_OVERRIDE') || (payload && payload.campaign_focus);
    if (!cpKeys) {
        if (isAIGenerated) {
            // Synthesize keywords from the AI-extracted DT fields
            const focus = payload?.campaign_focus || cpName;
            const pain  = payload?.pain_point     || '';
            const adv   = payload?.unfair_advantage || '';
            cpKeys = [focus, pain, adv].filter(Boolean).join(', ');
        } else {
            showToast('Target Keywords are required.', 'error');
            return;
        }
    }

    // ── Loading state: disable the active CTA button to prevent double-submit
    // on slow mobile connections (3G tap → spinner → no duplicate POSTs).
    const _launchBtns = document.querySelectorAll(
        '#fc-step2-submit, .dt-launch-btn, [onclick*="saveCampaignAction"], [onclick*="deployPredictiveCard"], [onclick*="saveChildCampaign"]'
    );
    _launchBtns.forEach(b => { b.disabled = true; b._origText = b.textContent; b.textContent = '⏳ Launching...'; });
    const _restoreLaunch = () => _launchBtns.forEach(b => { b.disabled = false; b.textContent = b._origText || 'Launch'; });

    showToast('Setting up your search...', 'info');
    try {
        const user = firebase.auth().currentUser;
        if (!user) { _restoreLaunch(); showToast('Session expired. Please sign in again.', 'error'); return; }
        // force=true: iOS Safari aggressively caches tokens — always fetch a
        // fresh token immediately before any state-mutating API call.
        const token = await user.getIdToken(true);

        const createResp = await fetch(`${API_BASE}/api/campaigns`, {
            method: 'POST',
            headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
            body: JSON.stringify({
                name:             cpName,
                bio:              cpBio,
                keywords:         cpKeys,
                gl:               cpGl,
                location:         cpLoc,
                persona_id:       (payload && payload.persona_id) || window._selectedPersonaId || '',
                persona_bio:      (payload && payload.persona_bio) || '',
                persona_keywords: (payload && payload.persona_keywords) || '',
                campaign_focus:     (payload && payload.campaign_focus) || '',
                pain_point:         (payload && payload.pain_point) || '',
                unfair_advantage:   (payload && payload.unfair_advantage) || '',
                human_edited:       (payload && payload.human_edited) || false,
                target_angle_hook:  (payload && payload.target_angle_hook) || '',
                target_angle_adv:   (payload && payload.target_angle_adv) || '',
                geo_hierarchy:      (payload && payload.geo_hierarchy) || {},
                status:           'active'
            })
        });
        if (!createResp.ok) throw new Error('Campaign creation failed');
        const createData = await createResp.json();
        const campaignId = createData.id;

        // FE-05: Surface backend warnings (e.g., geo validation, quota) as toasts
        if (Array.isArray(createData.warnings) && createData.warnings.length > 0) {
            createData.warnings.forEach(function(w) {
                showToast(_escapeHTML(String(w)), 'warning');
            });
        }

        // ── V23 IGNITION: fire Day-1 producer immediately ────────────────────
        // J-13 FIX: create_campaign already enqueues a zero-wait producer task.
        // Only call /ignite when zero_wait_enqueued is false (enqueue failed).
        // Previously, both ran unconditionally, doubling Serper spend on Day 1.
        let igniteMsg = 'Campaign active — scanning for leads...';
        console.log('[V23] Create response:', createData);
        if (campaignId && !createData.zero_wait_enqueued) {
            // Zero-wait failed at create time — use /ignite as explicit fallback
            try {
                const igniteResp = await fetch(`${API_BASE}/api/campaigns/${campaignId}/ignite`, {
                    method:  'POST',
                    headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
                    body:    JSON.stringify({})
                });
                const igniteData = await igniteResp.json();
                console.log('[V23 IGNITE] Response:', igniteData);
                if (igniteResp.ok && igniteData.ignite) {
                    igniteMsg = `🚀 Engine ignited! First leads expected in ~3 minutes.`;
                } else {
                    console.warn('[IGNITE] Ignition call returned error:', igniteData);
                    igniteMsg = 'Campaign created — first scan queued by cron (≤5 min).';
                }
            } catch (igniteErr) {
                console.warn('[IGNITE] Ignition fetch failed:', igniteErr);
            }
        } else if (campaignId && createData.zero_wait_enqueued) {
            // Zero-wait succeeded — no duplicate /ignite needed.
            // J-18 FIX: Show a persistent countdown banner so users know the system
            // is working during the 3-minute window before the first lead arrives.
            igniteMsg = '🚀 Search engine ignited! First leads expected in ~3 minutes.';
            const banner = document.createElement('div');
            banner.id = 'ignition-banner';
            banner.style.cssText = [
                'position:fixed', 'bottom:80px', 'left:50%', 'transform:translateX(-50%)',
                'background:linear-gradient(135deg,#4f46e5,#7c3aed)', 'color:#fff',
                'padding:12px 24px', 'border-radius:30px', 'font-size:0.88rem',
                'font-weight:600', 'z-index:9999', 'box-shadow:0 4px 20px rgba(79,70,229,0.4)',
                'display:flex', 'align-items:center', 'gap:10px'
            ].join(';');
            let secs = 180;
            banner.innerHTML = `<span>🚀</span><span id="ignition-banner-text">Scanning for leads — first results in ~3 min</span>`;
            document.body.appendChild(banner);
            const _bannerTick = setInterval(() => {
                secs--;
                const textEl = document.getElementById('ignition-banner-text');
                if (secs <= 0 || !textEl) {
                    clearInterval(_bannerTick);
                    banner.remove();
                } else {
                    const m = Math.floor(secs / 60);
                    const s = secs % 60;
                    if (textEl) textEl.textContent = `Scanning for leads — first results in ${m}:${String(s).padStart(2,'0')}`;
                }
            }, 1000);
            // Also dismiss the banner immediately when the first lead arrives via onSnapshot
            // Guard: don't nest patches if saveCampaignAction is called again before leads arrive
            if (!window.renderLeads._ignitionPatched) {
                const _origRender = window.renderLeads;
                window.renderLeads = function() {
                    if (rawLeadsCache.length > 0) {
                        clearInterval(_bannerTick);
                        banner.remove();
                        window.renderLeads = _origRender;
                        delete window.renderLeads._ignitionPatched;
                    }
                    _origRender.apply(this, arguments);
                };
                window.renderLeads._ignitionPatched = true;
            }
        }

        const targetUrlsInput = document.getElementById('camp-target-urls');
        if (targetUrlsInput) targetUrlsInput.value = '';

        // Clear persona selection state — prevents accidental carry-over
        // to the next campaign creation (e.g., a second deployPredictiveCard).
        window._selectedPersonaId = '';
        window._ccActivePersonaKeywords = '';

        _restoreLaunch();
        loadDashboard();
        showToast(igniteMsg, 'success');
    } catch(err) {
        console.error('[saveCampaignAction]', err);
        _restoreLaunch();
        showToast('Failed to save campaign. Please check your connection and try again.', 'error');
    }
};

// SPA Router — V18: syncs both top cmd-bar (desktop) and bottom dock (mobile)
window.switchTab = function(tabName) {
    // Hide all views via style.display (new HTML uses style="display:none", not class="hidden")
    document.querySelectorAll('.main-feed').forEach(el => el.style.display = 'none');
    document.querySelectorAll('.cmd-btn').forEach(el => el.classList.remove('active'));
    document.querySelectorAll('.dock-btn').forEach(el => el.classList.remove('active'));

    const show = id => {
        const el = document.getElementById(id);
        if (el) el.style.display = '';   // restore to CSS default (block/flex from stylesheet)
    };
    const activateNav = (topId, dockId) => {
        const t = document.getElementById(topId);
        const d = document.getElementById(dockId);
        if (t) t.classList.add('active');
        if (d) d.classList.add('active');
    };

    if (tabName === 'dashboard') {
        show('view-dashboard');
        activateNav('tab-dashboard', 'dock-tab-dashboard');
        window.leadsCurrentPage = 0;
        renderLeads();
    } else if (tabName === 'target') {
        show('view-target');
        activateNav('tab-campaigns', 'dock-tab-campaigns');
        window.campaignsCurrentPage = 0;
        loadCampaigns();
    } else if (tabName === 'reports') {
        show('view-reports');
        activateNav('tab-reports', 'dock-tab-reports');
        const savedRange = document.getElementById('roi-range-select')?.value || 30;
        loadROIDashboard(savedRange);
    } else if (tabName === 'l0-admin') {
        show('view-l0-admin');
        const l0Tab = document.getElementById('tab-l0-admin');
        if (l0Tab) l0Tab.classList.add('active');
        fetchL0Telemetry();
        (async function() {
            try {
                var user = firebase.auth().currentUser;
                if (!user) return;
                var token = await user.getIdToken();
                var resp = await fetch(API_BASE + '/api/l0/pending-count', {
                    headers: { 'Authorization': 'Bearer ' + token }
                });
                if (resp.ok) {
                    var json = await resp.json();
                    var count = (json.data || {}).pending_count || 0;
                    var tabBtn = document.getElementById('tab-l0-admin');
                    if (tabBtn && count > 0) {
                        var existing = tabBtn.querySelector('.admin-badge');
                        if (existing) existing.remove();
                        var badge = document.createElement('span');
                        badge.className = 'admin-badge';
                        badge.style.cssText = 'display:inline-flex;align-items:center;justify-content:center;min-width:18px;height:18px;border-radius:9px;background:#ef4444;color:#fff;font-size:0.65rem;font-weight:700;margin-left:6px;padding:0 5px;';
                        badge.textContent = count;
                        tabBtn.appendChild(badge);
                    }
                }
            } catch(e) {}
        })();
    } else if (tabName === 'macro') {
        show('view-macro');
        if (typeof fetchMacroTrends === 'function') fetchMacroTrends();
    } else if (tabName === 'crm-test') {
        show('view-crm-test');
        document.querySelectorAll('.cmd-btn').forEach(b => b.classList.remove('active'));
        const crmBtn = document.getElementById('tab-crm');
        if (crmBtn) crmBtn.classList.add('active');
        loadCrmBoard();
    } else if (tabName === 'persona-vault') {
        show('view-persona-vault');
        activateNav('tab-personas', 'dock-tab-personas');
        window.personasCurrentPage = 0;
        loadPersonaVault();
        loadAgents();
    }
};

// V24.1.1: Hash-based route for #crm-test (all authenticated users)
window.addEventListener('hashchange', () => {
    if (window.location.hash === '#crm-test' && firebase.auth().currentUser) {
        switchTab('crm-test');
    }
});
if (window.location.hash === '#crm-test') {
    // Deferred until auth + loadMe resolve (loadDashboard sets window.currentUserData)
    window.crmAutoOpen = true;
}

window.lastL0FetchTime = 0;
window.l0TelemetryCache = { macro: {}, tenants: [], sortKey: 'leads', sortDesc: true };

window.fetchL0Telemetry = async function() {
    const now = Date.now();
    if (now - window.lastL0FetchTime < 30000 && window.l0TelemetryCache.tenants.length > 0) {
        console.log("L0 Telemetry debounced natively.");
        return; // debounce heartbeat
    }
    window.lastL0FetchTime = now;
    
    const tableBody = document.getElementById('l0-telemetry-table');
    try {
        const user = firebase.auth().currentUser;
        if (!user) return;
        tableBody.innerHTML = '<tr><td colspan="3" style="padding:16px; text-align:center;">Fetching L0 Telemetry Arrays...</td></tr>';
        
        const token = await user.getIdToken();
        const response = await fetch(`${API_BASE}/api/l0/telemetry`, {
            method: 'GET',
            headers: { 'Authorization': `Bearer ${token}` }
        });
        
        if (response.status === 200) {
            // Reveal admin tab — uses style="display:none" (not hidden class)
            const adminTabEl = document.getElementById('tab-l0-admin');
            if (adminTabEl) adminTabEl.style.display = '';
            // V24.1.1: CRM tab visible to all users (revealed at auth time)
            const payload = await response.json();
            window.l0TelemetryCache.macro = payload.data.macro || {};
            window.l0TelemetryCache.tenants = payload.data.tenants || [];
            
            // Macro updates
            const m = window.l0TelemetryCache.macro;
            const tLeads = m.total_leads || 0;
            const actionable = (m.new || 0) + (m.contacted || 0);
            const conv = tLeads > 0 ? ((actionable / tLeads) * 100).toFixed(1) : 0;
            
            document.getElementById('l0-stat-total-leads').innerText = tLeads.toLocaleString();
            document.getElementById('l0-stat-conversion').innerText = `${conv}%`;
            document.getElementById('l0-stat-tenants').innerText = window.l0TelemetryCache.tenants.length;
            
            renderL0Table();
            
            // Trigger companion table refresh silently
            if (typeof fetchMacroTrends === 'function') fetchMacroTrends();
        } else {
            tableBody.innerHTML = '<tr><td colspan="3" style="padding:16px; text-align:center; color: #ef4444;">Access Denied. L0 Privilege Missing.</td></tr>';
        }
    } catch(err) {
        console.error(err);
    }
};

window.sortL0Table = function(key) {
    if (window.l0TelemetryCache.sortKey === key) {
        window.l0TelemetryCache.sortDesc = !window.l0TelemetryCache.sortDesc;
    } else {
        window.l0TelemetryCache.sortKey = key;
        window.l0TelemetryCache.sortDesc = true;
    }
    renderL0Table();
};

window.renderL0Table = function() {
    const tableBody = document.getElementById('l0-telemetry-table');
    if (!tableBody) return;
    
    let tenants = [...window.l0TelemetryCache.tenants];
    const key = window.l0TelemetryCache.sortKey;
    const desc = window.l0TelemetryCache.sortDesc ? -1 : 1;
    
    tenants.sort((a,b) => {
        let valA, valB;
        if (key === 'email') { valA = a.email || ''; valB = b.email || ''; }
        else if (key === 'wallet') { valA = a.wallet_balance || 0; valB = b.wallet_balance || 0; }
        else { valA = a.total_leads_generated || 0; valB = b.total_leads_generated || 0; }
        
        if (valA < valB) return -1 * desc;
        if (valA > valB) return 1 * desc;
        return 0;
    });
    
    let html = '';
    tenants.forEach(t => {
        const isSuspended = t.is_active === false; 
        const isPending = t.approval_status === 'pending';
        const statusColor = isSuspended ? '#ef4444' : (isPending ? '#f59e0b' : '#25D366');
        const statusBadge = `<strong style="color:${statusColor}">${isSuspended ? 'SUSPENDED' : (isPending ? 'PENDING' : 'ACTIVE')}</strong>`;
        
        let actionHTML = '';
        if (isPending) {
            actionHTML = `
                <input type="number" id="approve-days-${t.tenant_id}" value="180" style="width: 45px; padding: 4px; font-size: 0.75rem; border: 1px solid #ccc; border-radius: 4px;" title="Days">
                <input type="number" id="approve-amt-${t.tenant_id}" value="20000" style="width: 60px; padding: 4px; font-size: 0.75rem; border: 1px solid #ccc; border-radius: 4px;" title="Credits">
                <button class="primary-btn" style="padding: 4px 8px; font-size:0.75rem;" onclick="approveCredentials('${t.tenant_id}')">APPROVE</button>
            `;
        } else {
            actionHTML = `
                <input type="number" id="mint-${t.tenant_id}" placeholder="Amt" style="width: 50px; padding: 4px; font-size: 0.75rem; border: 1px solid #ccc; border-radius: 4px;">
                <button class="secondary-btn" style="padding: 4px 8px; font-size:0.75rem; color:#4F46E5; border-color:#4F46E5" onclick="mintCredentials('${t.tenant_id}')">MINT</button>
                <button class="secondary-btn" style="padding: 4px 8px; font-size:0.75rem; color:${statusColor}; border-color:${statusColor}" onclick="toggleTenantSuspend('${t.tenant_id}', ${!isSuspended})">
                    ${isSuspended ? 'UNSUSPEND' : 'SUSPEND'}
                </button>
            `;
        }
        
        html += `
        <tr style="border-bottom: 1px solid var(--glass-border);">
            <td style="padding: 12px; font-weight: 500;">
                ${t.email || 'No email saved'}<br>
                <small style="font-family:monospace; color:var(--text-muted);">${(t.tenant_id||'Unknown').substring(0,8)}...</small>
            </td>
            <td style="padding: 12px;">${statusBadge}</td>
            <td style="padding: 12px; font-family:monospace;">${(t.wallet_balance || 0).toLocaleString()} CR</td>
            <td style="padding: 12px; text-align:right;">${(t.total_leads_generated || 0).toLocaleString()}</td>
            <td style="padding: 12px; text-align:right;">${actionHTML}</td>
        </tr>`;
    });
    tableBody.innerHTML = html || '<tr><td colspan="5" style="padding:16px; text-align:center;">No tenants found.</td></tr>';
}

let rawMacroTrends = null;
let macroChartObj = null;

window.fetchMacroTrends = async function() {
    const tableBody = document.getElementById('campaign-intelligence-table');
    if(!tableBody) return;
    try {
        const user = firebase.auth().currentUser;
        if (!user) return;
        tableBody.innerHTML = '<tr><td colspan="5" style="padding:16px; text-align:center;">Fetching Campaign Intelligence Constraints...</td></tr>';
        
        const token = await user.getIdToken();
        const response = await fetch(`${API_BASE}/api/l0/trends`, {
            method: 'GET',
            headers: { 'Authorization': `Bearer ${token}` }
        });
        
        if (response.ok) {
            const payload = await response.json();
            rawMacroTrends = payload.data || {};
            renderMacroTrends();
        } else {
            tableBody.innerHTML = '<tr><td colspan="5" style="padding:16px; text-align:center; color:#ef4444;">Access Denied. L0 Privilege Missing.</td></tr>';
        }
    } catch (e) {
        tableBody.innerHTML = '<tr><td colspan="5" style="padding:16px; text-align:center; color:#ef4444;">Gateway Error.</td></tr>';
    }
};

window.renderMacroTrends = function() {
    const tableBody = document.getElementById('campaign-intelligence-table');
    if (!rawMacroTrends || !tableBody) return;
    
    const campaigns = rawMacroTrends.campaign_trends || [];
    
    if (campaigns.length === 0) {
        tableBody.innerHTML = '<tr><td colspan="5" style="padding:16px; text-align:center;">No tracking data available.</td></tr>';
        return;
    }
    
    tableBody.innerHTML = campaigns.map(c => `
        <tr style="border-bottom: 1px solid var(--glass-border);">
            <td style="padding: 12px; font-weight: 500;">
                ${_escapeHTML(c.email)}<br>
                <small style="font-family:monospace; color:var(--text-muted); font-size:0.75rem;">${_escapeHTML((c.tenant_id||'').substring(0,8))}</small>
            </td>
            <td style="padding: 12px; font-weight: 500;">
                ${_escapeHTML(c.name)}
            </td>
            <td style="padding: 12px;">
                <div style="font-size:0.85rem; max-height: 4.8em; overflow:hidden; text-overflow: ellipsis; display: -webkit-box; -webkit-line-clamp: 4; -webkit-box-orient: vertical;" title="${_escapeHTML(c.bio||'')}">${_escapeHTML(c.bio)}</div>
            </td>
            <td style="padding: 12px; font-size:0.8rem; font-family:monospace; color:var(--primary);">
                ${_escapeHTML(c.keywords)}
            </td>
            <td style="padding: 12px; text-align:right; font-weight:bold; color:var(--success);">
                ${(c.leads_generated||0).toLocaleString()}
            </td>
        </tr>
    `).join('');
};

window.toggleTenantSuspend = async function(uid, isCurrentlyActive) {
    if(!confirm(`Are you absolutely sure you want to ${isCurrentlyActive ? 'SUSPEND' : 'REACTIVATE'} this tenant globally?`)) return;
    try {
        const user = firebase.auth().currentUser;
        const token = await user.getIdToken();
        await fetch(`${API_BASE}/api/l0/users/suspend`, {
            method: 'POST',
            headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
            body: JSON.stringify({ uid: uid, is_active: !isCurrentlyActive })
        });
        showToast('Telemetry Command Accepted', 'info');
        fetchL0Telemetry();
    } catch(err) {
        showToast('Super Admin action explicitly failed.', 'error');
    }
};

window.approveCredentials = async function(tenantId) {
    const amtEl = document.getElementById(`approve-amt-${tenantId}`);
    const daysEl = document.getElementById(`approve-days-${tenantId}`);
    if (!amtEl || !amtEl.value || !daysEl || !daysEl.value) return;
    try {
        const user = firebase.auth().currentUser;
        const token = await user.getIdToken();
        const resp = await fetch(`${API_BASE}/api/l0/users/${tenantId}/approve`, {
            method: 'POST',
            headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
            body: JSON.stringify({ amount: parseInt(amtEl.value), days: parseInt(daysEl.value) })
        });
        if (resp.ok) {
            showToast(`Approved ${tenantId}.`, 'success');
            fetchL0Telemetry();
        } else {
            showToast('Failed to approve.', 'error');
        }
    } catch(err) {
        showToast('Approve action failed.', 'error');
    }
};

window.mintCredentials = async function(tenantId) {
    const amtEl = document.getElementById(`mint-${tenantId}`);
    if (!amtEl || !amtEl.value) return;
    try {
        const user = firebase.auth().currentUser;
        const token = await user.getIdToken();
        const resp = await fetch(`${API_BASE}/api/l0/users/${tenantId}/mint`, {
            method: 'POST',
            headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
            body: JSON.stringify({ amount: amtEl.value })
        });
        if (resp.ok) {
            showToast(`Minted ${amtEl.value} credits.`, 'success');
            amtEl.value = '';
            fetchL0Telemetry();
        } else {
            showToast('Failed to mint.', 'error');
        }
    } catch(err) {
        showToast('Failed to mint credits.', 'error');
    }
};



// =============================================================================
// L0 SUPER ADMIN — TAB SYSTEM
// =============================================================================
window._l0ActiveTab = 'tenants';

window.l0SwitchTab = function(tab) {
    // Guard: only super_admin
    if (window.currentUserData?.role !== 'super_admin') {
        showToast('L0 access denied.', 'error');
        return;
    }
    // Hide all panels, deactivate all tab buttons
    ['tenants','operations','ledger','health','serper','radar','credits','audit'].forEach(t => {
        const panel = document.getElementById(`l0-panel-${t}`);
        const btn   = document.getElementById(`l0-tab-${t}`);
        if (panel) panel.classList.remove('active');
        if (btn)   btn.classList.remove('active');
    });
    const activePanel = document.getElementById(`l0-panel-${tab}`);
    const activeBtn   = document.getElementById(`l0-tab-${tab}`);
    if (activePanel) activePanel.classList.add('active');
    if (activeBtn) {
        activeBtn.classList.add('active');
        activeBtn.scrollIntoView({ behavior: 'smooth', block: 'nearest', inline: 'center' });
    }
    window._l0ActiveTab = tab;

    // Auto-load data on first tab switch
    if (tab === 'tenants')    fetchL0Telemetry();
    if (tab === 'operations') fetchGlobalOperations();
    if (tab === 'ledger')     fetchShadowLedger();
    if (tab === 'health')     fetchSystemHealth();
    if (tab === 'serper')     fetchSerperAuditLogs();
    if (tab === 'radar')      fetchRadarStatus();
    if (tab === 'credits')    fetchCreditTrends();
    if (tab === 'audit')      fetchAuditLog();
};

window.l0RefreshCurrentTab = function() {
    l0SwitchTab(window._l0ActiveTab || 'tenants');
};

// =============================================================================
// L0 GLOBAL OPERATIONS — BQ Geo Heatmap + Domain Affinity Matrix
// =============================================================================
window.fetchGlobalOperations = async function() {
    const geoLoad  = document.getElementById('l0-geo-loading');
    const geoBars  = document.getElementById('l0-geo-bars');
    const matLoad  = document.getElementById('l0-matrix-loading');
    const domList  = document.getElementById('l0-domain-list');
    const errBox   = document.getElementById('l0-ops-error');
    const badge    = document.getElementById('l0-matrix-cache-badge');

    if (geoLoad)  { geoLoad.style.display = 'block'; geoBars.style.display  = 'none'; }
    if (matLoad)  { matLoad.style.display = 'block'; domList.style.display  = 'none'; }
    if (errBox)   errBox.style.display = 'none';

    try {
        const user = firebase.auth().currentUser;
        if (!user) throw new Error('Not authenticated');
        const token = await user.getIdToken(true);
        const resp  = await fetch(`${API_BASE}/api/internal/l0/operations-telemetry`, {
            headers: { 'Authorization': `Bearer ${token}` }
        });
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
        const json = await resp.json();
        const d    = json.data || {};

        // Show cache badge
        if (badge) badge.style.display = json.cache_hit ? 'inline' : 'none';

        // ── Geo Heatmap: horizontal bar chart built in plain HTML ──
        const heatmap = d.geo_heatmap || [];
        if (geoBars) {
            if (heatmap.length === 0) {
                geoBars.innerHTML = '<div style="text-align:center; color:var(--text-muted); padding:20px 0;">No active campaign data yet.</div>';
            } else {
                const maxVal = Math.max(...heatmap.map(r => r.active_campaigns), 1);
                geoBars.innerHTML = heatmap.map(r => {
                    const pct  = Math.round((r.active_campaigns / maxVal) * 100);
                    const flag = r.region ? r.region.slice(0,2).toUpperCase() : '';
                    return `<div style="margin-bottom:10px;">
                        <div style="display:flex; justify-content:space-between; font-size:0.82rem; margin-bottom:4px;">
                            <span style="font-weight:500;">${_escapeHTML(r.region)}</span>
                            <span style="color:var(--text-muted);">${r.active_campaigns} campaigns</span>
                        </div>
                        <div style="height:10px; background:#f1f5f9; border-radius:6px; overflow:hidden;">
                            <div style="height:100%; width:${pct}%; background:linear-gradient(90deg,#4f46e5,#7c3aed); border-radius:6px; transition:width 0.6s;"></div>
                        </div>
                    </div>`;
                }).join('');
            }
            geoLoad.style.display  = 'none';
            geoBars.style.display  = 'block';
        }

        // ── Domain Affinity Matrix ──
        const domains = d.domain_matrix || [];
        if (domList) {
            if (domains.length === 0) {
                domList.innerHTML = '<div style="text-align:center; color:var(--text-muted); padding:20px 0;">No domain data yet — RLHF needs more cycles.</div>';
            } else {
                const maxW = Math.max(...domains.map(dm => dm.baseline_weight), 1);
                domList.innerHTML = domains.map((dm, i) => {
                    const barPct  = Math.round((dm.baseline_weight / maxW) * 100);
                    const trend   = dm.baseline_weight > 1.0 ? '&#x2191;' : dm.baseline_weight < 1.0 ? '&#x2193;' : '&mdash;';
                    const tColor  = dm.baseline_weight > 1.0 ? '#10b981' : dm.baseline_weight < 1.0 ? '#ef4444' : '#64748b';
                    return `<div style="display:flex; align-items:center; gap:10px; padding:8px 0; border-bottom:1px solid #f1f5f9;">
                        <div style="font-size:0.72rem; font-weight:700; color:var(--text-muted); width:18px; text-align:right;">${i+1}</div>
                        <div style="flex:1; min-width:0;">
                            <div style="font-size:0.83rem; font-weight:600; white-space:nowrap; overflow:hidden; text-overflow:ellipsis;">${_escapeHTML(dm.domain)}</div>
                            <div style="height:4px; background:#f1f5f9; border-radius:4px; margin-top:4px; overflow:hidden;">
                                <div style="height:100%; width:${barPct}%; background:linear-gradient(90deg,#6366f1,#a21caf); border-radius:4px;"></div>
                            </div>
                        </div>
                        <div style="text-align:right; flex-shrink:0;">
                            <div style="font-size:0.85rem; font-weight:700; color:${tColor};">${dm.baseline_weight.toFixed(3)} ${trend}</div>
                            <div style="font-size:0.68rem; color:var(--text-muted);">${dm.total_yield} yields</div>
                        </div>
                    </div>`;
                }).join('');
            }
            matLoad.style.display = 'none';
            domList.style.display = 'block';
        }

        // Surface partial errors — but NEVER show bigquery errors.
        // BQ is an optional enrichment; the geo heatmap runs on Firestore primary.
        // A BQ 403 is an infra config issue (IAM), not a data issue.
        if (d.partial_errors && errBox) {
            const displayErrors = Object.entries(d.partial_errors)
                .filter(([k]) => k !== 'bigquery')  // suppress BQ 403 noise
                .map(([k, v]) => `${k}: ${v}`)
                .join(' | ');
            if (displayErrors) {
                errBox.style.display = 'block';
                errBox.textContent = 'Partial data: ' + displayErrors;
            } else {
                errBox.style.display = 'none';
            }
        }

    } catch(err) {
        console.error('[L0 Ops]', err);
        if (geoLoad)  geoLoad.textContent  = 'Failed to load. ' + err.message;
        if (matLoad)  matLoad.textContent  = 'Failed to load. ' + err.message;
    }
};

// =============================================================================
// L0 SHADOW LEDGER — Rejected Leads Table
// =============================================================================
const REJECTION_LABELS = {
    wrong_topic: '&#128683; Wrong Topic',
    wrong_geography: '&#127758; Wrong Geography',
    news_article: '&#128240; News Article',
    too_old: '&#9203; Too Old',
    cant_contact: '&#128222; Can&#39;t Contact',
    competitor: '&#9876;&#65039; Competitor',
    other: '&#128300; Other'
};

window.fetchShadowLedger = async function() {
    const tbody = document.getElementById('l0-shadow-ledger-table');
    if (!tbody) return;
    tbody.innerHTML = '<tr><td colspan="5" style="padding:20px; text-align:center; color:var(--text-muted);">&#8987; Fetching rejected leads&hellip;</td></tr>';

    try {
        const user = firebase.auth().currentUser;
        if (!user) throw new Error('Not authenticated');
        const token = await user.getIdToken(true);
        // Dedicated L0 cross-tenant endpoint — standard /api/leads is tenant-scoped
        // and would only return the admin's own leads (usually 0 rejected).
        const resp  = await fetch(`${API_BASE}/api/l0/shadow-ledger?limit=200&rt=${Date.now()}`, {
            headers: { 'Authorization': `Bearer ${token}` }
        });
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
        const json  = await resp.json();
        const leads = json.leads || json.data || [];

        // Update KPI tile
        const kpi = document.getElementById('l0-stat-rejected');
        if (kpi) kpi.textContent = leads.length;

        if (leads.length === 0) {
            tbody.innerHTML = '<tr><td colspan="5" style="padding:20px; text-align:center; color:var(--text-muted);">No rejected leads found.</td></tr>';
            return;
        }

        tbody.innerHTML = leads.map(lead => {
            const domain    = lead.base_path || lead.source_url || lead.domain || lead.company_domain || '&mdash;';
            const score     = lead.score != null ? `<span style="font-weight:700; color:${lead.score>=70?'#10b981':lead.score>=40?'#f59e0b':'#ef4444'}">${lead.score}</span>` : '&mdash;';
            const userRej   = REJECTION_LABELS[lead.rejection_reason] || lead.rejection_reason || '<span style="color:var(--text-muted);">Legacy ignore</span>';
            const aiRej     = lead.ai_rejection_reason ? `<span title="${_escapeHTML(lead.ai_rejection_reason)}">${_escapeHTML(lead.ai_rejection_reason.slice(0,60))}${lead.ai_rejection_reason.length>60?'&hellip;':''}</span>` : '<span style="color:var(--text-muted);">N/A</span>';
            return `<tr style="border-bottom:1px solid #f8fafc; transition:background 0.15s;" onmouseover="this.style.background='#fafafa'" onmouseout="this.style.background=''">
                <td style="padding:10px 12px; font-weight:500; max-width:160px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;">${domain}</td>
                <td style="padding:10px 12px; text-align:center;">${score}</td>
                <td style="padding:10px 12px;">${userRej}</td>
                <td style="padding:10px 12px; font-size:0.8rem; color:var(--text-muted); max-width:200px;">${aiRej}</td>
                <td style="padding:10px 12px; text-align:right;">
                    <button onclick="recycleRejectedLead('${lead.id}', this)" style="padding:6px 14px; border:1px solid #d1d5db; border-radius:8px; background:#fff; font-size:0.78rem; font-weight:600; color:#4f46e5; cursor:pointer; transition:all 0.15s;" onmouseover="this.style.background='#ede9fe'" onmouseout="this.style.background='#fff'">
                        &#9851; Recycle
                    </button>
                </td>
            </tr>`;
        }).join('');

    } catch(err) {
        console.error('[Shadow Ledger]', err);
        tbody.innerHTML = `<tr><td colspan="5" style="padding:16px; text-align:center; color:#ef4444;">Error: ${_escapeHTML(err.message)}</td></tr>`;
    }
};

window.recycleRejectedLead = async function(leadId, btn) {
    if (!leadId || !btn) return;
    btn.disabled = true;
    btn.textContent = 'Recycling...';
    try {
        const user = firebase.auth().currentUser;
        if (!user) return;
        const token = await user.getIdToken(true);
        const resp  = await fetch(`${API_BASE}/api/leads/${leadId}`, {
            method:  'PUT',
            headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
            // Reset to 'unprocessed' and clear rejection metadata
            body:    JSON.stringify({ status: 'unprocessed', rejection_reason: null })
        });
        if (resp.ok) {
            btn.closest('tr').style.opacity = '0.3';
            setTimeout(() => btn.closest('tr')?.remove(), 600);
            showToast('Lead recycled and re-queued for processing.', 'success');
        } else {
            throw new Error(`HTTP ${resp.status}`);
        }
    } catch(err) {
        btn.disabled = false;
        btn.textContent = '&#9851; Recycle';
        showToast('Recycle failed: ' + err.message, 'error');
    }
};

// ── Manual Re-queue for failed leads (V23.7 → V23.9 fix) ─────────────────
let _requeueDebouncing = false;
window.requeueFailedLead = async function(leadId, btn) {
    if (_requeueDebouncing) return;
    _requeueDebouncing = true;
    setTimeout(() => { _requeueDebouncing = false; }, 500);
    if (btn) { btn.disabled = true; btn.innerHTML = '⏳ Re-queuing…'; }

    // V23.9: Optimistically swap the card DOM to skeleton state
    const cardEl = document.getElementById(leadId);
    if (cardEl) {
        cardEl.classList.remove('lead-card--failed');
        cardEl.classList.add('lead-card--processing');
        const errorBadge = cardEl.querySelector('.lead-error-badge');
        if (errorBadge) errorBadge.remove();
        const skel = document.createElement('div');
        skel.style.cssText = 'padding:20px;display:flex;flex-direction:column;gap:12px;';
        skel.innerHTML =
            '<div class="skeleton-block" style="width:140px;height:16px;"></div>' +
            '<div class="skeleton-block" style="width:100%;height:10px;"></div>' +
            '<div class="skeleton-block" style="width:60%;height:10px;"></div>' +
            '<div style="display:flex;align-items:center;gap:6px;margin-top:4px;color:#b45309;font-size:0.8rem;font-weight:500;">' +
                '<span>⏳</span> <span>Re-queued for processing…</span>' +
            '</div>';
        cardEl.appendChild(skel);
    }

    // V23.9: Evict from local caches to prevent stale re-render
    _evictLeadFromCaches(leadId);
    _scheduleRender();

    try {
        const user = firebase.auth().currentUser;
        if (!user) { showToast('Session expired. Please sign in again.', 'error'); return; }
        const token = await user.getIdToken(true);
        const resp  = await fetch(`${API_BASE}/api/leads/${leadId}`, {
            method:  'PUT',
            headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
            body:    JSON.stringify({
                status: 'queued',
                lock_entity: null,
                error: null,
                error_details: null,
                processing_attempts: 0,
                requeue_source: 'manual_ui'
            })
        });
        if (resp.status === 402) {
            showToast('Insufficient credits to re-queue this lead.', 'error');
            if (btn) { btn.disabled = false; btn.innerHTML = '🔄 Re-queue'; }
            return;
        }
        if (resp.status === 422) {
            // Terminal failure — requeue will never succeed for this lead
            var errBody = {};
            try { errBody = await resp.json(); } catch(_) {}
            var terminalMsg = errBody.error || 'This lead cannot be reprocessed.';
            showToast(terminalMsg, 'error');
            if (btn) { btn.disabled = false; btn.innerHTML = '⛔ Cannot retry'; btn.style.opacity = '0.5'; }
            // Restore card from skeleton to failed state
            if (cardEl) {
                cardEl.classList.remove('lead-card--processing');
                cardEl.classList.add('lead-card--failed');
            }
            return;
        }
        if (resp.ok) {
            showToast('Lead re-queued for processing.', 'success');
        } else {
            showToast('Re-queue failed. Please try again.', 'error');
            if (btn) { btn.disabled = false; btn.innerHTML = '🔄 Re-queue'; }
        }
    } catch (err) {
        console.error('requeueFailedLead error:', err);
        showToast('Network error re-queuing lead.', 'error');
        if (btn) { btn.disabled = false; btn.innerHTML = '🔄 Re-queue'; }
    }
};

// =============================================================================
// L0 QUERY AUDIT — Serper Telemetry Tab (V23.4)
// =============================================================================

/** _ensureSerperPanel — no-op: panel now exists statically in index.html.
 *  Kept as a stub so existing fetchSerperAuditLogs() call doesn't error. */
function _ensureSerperPanel() {
    // Panel (#l0-panel-serper) is now a static HTML sibling of the other l0-panels.
    // Tab button (#l0-tab-serper) is also static. Dynamic injection no longer needed.
    return;
}

function _initSerperDateDefaults() {
    const from = document.getElementById('serper-filter-from');
    const to   = document.getElementById('serper-filter-to');
    if (!from || !to || from.value) return;  // already set
    const now  = new Date();
    const ago7 = new Date(now);  ago7.setDate(ago7.getDate() - 7);
    const fmt  = d => d.toISOString().slice(0, 10);
    from.value = fmt(ago7);
    to.value   = fmt(now);
}

window.fetchSerperAuditLogs = async function() {
    _ensureSerperPanel();
    _initSerperDateDefaults();

    const tbody    = document.getElementById('serper-audit-table');
    const statToday   = document.getElementById('serper-stat-today');
    const statAvg     = document.getElementById('serper-stat-avg');
    const statCredits = document.getElementById('serper-stat-credits');
    const statTotal   = document.getElementById('serper-stat-total');
    const topDiv      = document.getElementById('serper-top-campaigns');
    const topList     = document.getElementById('serper-top-campaigns-list');

    if (tbody) tbody.innerHTML = '<tr><td colspan="6" style="padding:24px; text-align:center; color:#9ca3af;">⏳ Loading query audit logs…</td></tr>';
    if (statToday)   statToday.textContent   = '…';
    if (statAvg)     statAvg.textContent     = '…';
    if (statCredits) statCredits.textContent = '…';
    if (statTotal)   statTotal.textContent   = '…';

    try {
        const user = firebase.auth().currentUser;
        if (!user) throw new Error('Not authenticated');
        const token = await user.getIdToken(true);

        const dateFrom = document.getElementById('serper-filter-from')?.value || '';
        const dateTo   = document.getElementById('serper-filter-to')?.value   || '';
        const campId   = document.getElementById('serper-filter-campaign')?.value.trim() || '';

        let url = `${API_BASE}/api/admin/telemetry/serper-logs?limit=500`;
        if (dateFrom) url += `&date_from=${encodeURIComponent(dateFrom)}`;
        if (dateTo)   url += `&date_to=${encodeURIComponent(dateTo)}`;
        if (campId)   url += `&campaign_id=${encodeURIComponent(campId)}`;
        url += `&rt=${Date.now()}`;

        const resp = await fetch(url, { headers: { 'Authorization': `Bearer ${token}` } });
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
        const json = await resp.json();

        const logs    = json.logs    || [];
        const summary = json.summary || {};

        // Update metric cards
        if (statToday)   statToday.textContent   = (summary.total_today   ?? 0).toLocaleString();
        if (statAvg)     statAvg.textContent      = (summary.avg_results   ?? 0).toFixed(1);
        if (statCredits) statCredits.textContent  = (summary.total_credits ?? 0).toLocaleString();
        if (statTotal)   statTotal.textContent    = (summary.total_queries ?? 0).toLocaleString();

        // Top campaigns
        const topCamps = summary.top_campaigns || [];
        if (topDiv && topList && topCamps.length > 0) {
            topDiv.style.display = 'block';
            topList.innerHTML = topCamps.map(c =>
                `<span style="background:#ede9fe; color:#4f46e5; border-radius:20px; padding:5px 14px; font-size:0.78rem; font-weight:700;">${c.campaign_id.slice(0,12)}… <span style="opacity:0.7;">${c.calls} calls</span></span>`
            ).join('');
        } else if (topDiv) {
            topDiv.style.display = 'none';
        }

        if (!tbody) return;

        if (logs.length === 0) {
            tbody.innerHTML = '<tr><td colspan="6" style="padding:24px; text-align:center; color:#9ca3af;">No queries found for the selected date range.</td></tr>';
            return;
        }

        tbody.innerHTML = logs.map(row => {
            const ts      = row.timestamp ? row.timestamp.replace('T', ' ').slice(0, 19) : '—';
            const campShort = (row.campaign_id || '—').slice(0, 14);
            const query   = (row.raw_query || '').length > 80
                ? `<span title="${_escapeHTML(row.raw_query||'')}">${_escapeHTML((row.raw_query||'').slice(0,80))}&hellip;</span>`
                : _escapeHTML(row.raw_query || '') || '—';
            // Parse serper_parameters (JSON string from BQ)
            let params = '—';
            try {
                const p = typeof row.serper_parameters === 'string'
                    ? JSON.parse(row.serper_parameters)
                    : (row.serper_parameters || {});
                params = Object.entries(p)
                    .filter(([k]) => k !== 'q')  // 'q' is the raw_query itself
                    .map(([k, v]) => `<span style="background:#f3f4f6; border-radius:4px; padding:2px 6px; font-size:0.72rem;">${_escapeHTML(k)}=${_escapeHTML(String(v))}</span>`)
                    .join(' ') || '<span style="color:#9ca3af;">none</span>';
            } catch(_) {}

            const yield_ = row.result_count != null
                ? `<span style="font-weight:700; color:${row.result_count >= 10 ? '#10b981' : row.result_count >= 5 ? '#f59e0b' : '#ef4444'}">${row.result_count}</span>`
                : '—';
            const status  = row.serper_status_code === 200
                ? `<span style="background:#d1fae5; color:#065f46; border-radius:20px; padding:2px 10px; font-size:0.72rem; font-weight:700;">200 OK</span>`
                : `<span style="background:#fee2e2; color:#991b1b; border-radius:20px; padding:2px 10px; font-size:0.72rem; font-weight:700;">${row.serper_status_code || '?'}</span>`;

            return `<tr style="border-bottom:1px solid #f3f4f6; transition:background 0.12s;" onmouseover="this.style.background='#fafafa'" onmouseout="this.style.background=''">
                <td style="padding:10px 14px; white-space:nowrap; color:#6b7280; font-size:0.78rem;">${ts}</td>
                <td style="padding:10px 14px; font-family:monospace; font-size:0.78rem; color:#4f46e5;" title="${row.campaign_id||''}">${campShort}</td>
                <td style="padding:10px 14px; max-width:300px;">${query}</td>
                <td style="padding:10px 14px; max-width:200px;">${params}</td>
                <td style="padding:10px 10px; text-align:center;">${yield_}</td>
                <td style="padding:10px 10px; text-align:center;">${status}</td>
            </tr>`;
        }).join('');

    } catch(err) {
        console.error('[Serper Audit]', err);
        if (tbody) tbody.innerHTML = `<tr><td colspan="6" style="padding:16px; text-align:center; color:#ef4444;">Error: ${_escapeHTML(err.message)}</td></tr>`;
        if (statToday) statToday.textContent = 'ERR';
    }
};

// =============================================================================
// L0 SYSTEM HEALTH — Circuit Breaker + Error Rates
// =============================================================================
window.fetchSystemHealth = async function() {
    const breakerEl = document.getElementById('l0-health-breaker');
    const serperEl  = document.getElementById('l0-health-serper');
    const oomEl     = document.getElementById('l0-health-oom');
    const bqEl      = document.getElementById('l0-health-bq');
    const velocEl   = document.getElementById('l0-health-velocity');
    const campEl    = document.getElementById('l0-health-camps');
    try {
        const user = firebase.auth().currentUser;
        if (!user) return;
        const token = await user.getIdToken(true);
        const resp  = await fetch(`${API_BASE}/api/l0/system-health`, {
            headers: { 'Authorization': `Bearer ${token}` }
        });
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
        const json = await resp.json();
        const d    = json.data || {};
        const cb   = d.circuit_breaker || {};

        // Circuit Breaker
        const state      = cb.state || 'UNKNOWN';
        const stateColor = state === 'CLOSED' ? '#10b981' : state === 'OPEN' ? '#ef4444' : '#f59e0b';
        if (breakerEl) {
            const reason = cb.last_open_reason ? `<div style="font-size:0.68rem;color:#6b7280;margin-top:4px;max-width:200px;white-space:normal;">${cb.last_open_reason.slice(0,80)}${cb.last_open_reason.length>80?'…':''}</div>` : '';
            breakerEl.innerHTML = `<strong style="color:${stateColor};">${state}</strong>${reason}`;
        }

        // Serper 429 Rate
        if (serperEl) {
            const rate = cb.serper_429_rate != null ? (cb.serper_429_rate * 100).toFixed(1) + '%' : 'N/A';
            const calls = cb.serper_calls != null ? ` <span style="font-size:0.68rem;color:#6b7280;">(${cb.serper_429s||0}/${cb.serper_calls||0} calls)</span>` : '';
            const color = (cb.serper_429_rate||0) > 0.10 ? '#ef4444' : (cb.serper_429_rate||0) > 0.05 ? '#f59e0b' : '#10b981';
            serperEl.innerHTML = `<strong style="color:${color};">${rate}</strong>${calls}`;
        }

        // Scraper OOM Rate
        if (oomEl) {
            const rate = cb.scraper_oom_rate != null ? (cb.scraper_oom_rate * 100).toFixed(1) + '%' : 'N/A';
            const color = (cb.scraper_oom_rate||0) > 0.03 ? '#ef4444' : '#10b981';
            oomEl.innerHTML = `<strong style="color:${color};">${rate}</strong>`;
        }

        // Pipeline Velocity (leads last 24h)
        if (velocEl) {
            velocEl.innerHTML = d.leads_last_24h != null
                ? `<strong style="color:var(--primary);">${d.leads_last_24h}</strong> <span style="font-size:0.72rem;color:#6b7280;">new leads (24h)</span>`
                : 'N/A';
        }

        // Active Campaigns
        if (campEl) {
            const ont = d.ontology_domains != null ? ` &nbsp;·&nbsp; <span style="font-size:0.72rem;color:#6b7280;">${d.ontology_domains} RLHF domains</span>` : '';
            campEl.innerHTML = d.active_campaigns != null
                ? `<strong style="color:#7c3aed;">${d.active_campaigns}</strong> active${ont}`
                : 'N/A';
        }

        // BQ Last Sync tile — repurpose for rejection count
        if (bqEl) {
            bqEl.innerHTML = d.total_rejected != null
                ? `<strong style="color:#ef4444;">${d.total_rejected}</strong> <span style="font-size:0.72rem;color:#6b7280;">total rejections</span>`
                : 'N/A';
        }

        // Load kill switch state from health data
        loadKillSwitchState(d);

        showToast('System health loaded.', 'success');
    } catch(err) {
        console.error('[System Health]', err);
        if (breakerEl) breakerEl.textContent = 'Error: ' + err.message;
        showToast('System health load failed: ' + err.message, 'error');
    }
};

// =============================================================================
// L0 ADMIN — RADAR STATUS
// =============================================================================
window.fetchRadarStatus = async function() {
    var tableBody = document.getElementById('l0-radar-table');
    if (tableBody) tableBody.innerHTML = '<tr><td colspan="6" style="padding:20px; text-align:center; color:var(--text-muted);">&#8987; Loading radar data&hellip;</td></tr>';
    try {
        var user = firebase.auth().currentUser;
        if (!user) return;
        var token = await user.getIdToken();
        var resp = await fetch(API_BASE + '/api/l0/radar-status', {
            headers: { 'Authorization': 'Bearer ' + token }
        });
        if (!resp.ok) throw new Error('HTTP ' + resp.status);
        var json = await resp.json();
        var rows = json.data || [];
        if (rows.length === 0) {
            if (tableBody) tableBody.innerHTML = '<tr><td colspan="6" style="padding:20px; text-align:center; color:var(--text-muted);">No radar data available.</td></tr>';
            return;
        }
        var html = '';
        rows.forEach(function(r) {
            var tenantShort = _escapeHTML((r.tenant_id || '').substring(0, 8));
            var statusColor = r.status === 'active' ? '#10b981' : r.status === 'paused' ? '#f59e0b' : '#6b7280';
            html += '<tr style="border-bottom:1px solid var(--glass-border);">' +
                '<td style="padding:10px 12px; font-family:monospace; font-size:0.78rem;">' + tenantShort + '</td>' +
                '<td style="padding:10px 12px;">' + _escapeHTML(r.email || '\u2014') + '</td>' +
                '<td style="padding:10px 12px;"><span style="color:' + statusColor + '; font-weight:600;">' + _escapeHTML(r.status || 'unknown') + '</span></td>' +
                '<td style="padding:10px 12px; font-size:0.8rem; color:var(--text-muted);">' + _escapeHTML(r.last_run || '\u2014') + '</td>' +
                '<td style="padding:10px 12px; text-align:center; font-weight:600;">' + (r.signals_per_week != null ? r.signals_per_week : '\u2014') + '</td>' +
                '<td style="padding:10px 12px; font-size:0.8rem;">' + _escapeHTML(r.top_keywords || '\u2014') + '</td>' +
                '</tr>';
        });
        if (tableBody) tableBody.innerHTML = html;
        showToast('Radar status loaded.', 'success');
    } catch(err) {
        console.error('[Radar]', err);
        if (tableBody) tableBody.innerHTML = '<tr><td colspan="6" style="padding:20px; text-align:center; color:#ef4444;">Error: ' + _escapeHTML(err.message) + '</td></tr>';
        showToast('Radar load failed: ' + err.message, 'error');
    }
};

// =============================================================================
// L0 ADMIN — CREDIT TRENDS
// =============================================================================
window.fetchCreditTrends = async function() {
    var chartEl = document.getElementById('l0-credit-chart');
    var tableBody = document.getElementById('l0-credit-table');
    var totalEl = document.getElementById('l0-credits-total-30d');
    var avgEl = document.getElementById('l0-credits-daily-avg');
    var projEl = document.getElementById('l0-credits-projected');
    if (tableBody) tableBody.innerHTML = '<tr><td colspan="3" style="padding:20px; text-align:center; color:var(--text-muted);">&#8987; Loading credit data&hellip;</td></tr>';
    try {
        var user = firebase.auth().currentUser;
        if (!user) return;
        var token = await user.getIdToken();
        var resp = await fetch(API_BASE + '/api/l0/credit-trends', {
            headers: { 'Authorization': 'Bearer ' + token }
        });
        if (!resp.ok) throw new Error('HTTP ' + resp.status);
        var json = await resp.json();
        var days = json.data || [];
        var summary = json.summary || {};

        // Summary stats
        if (totalEl) totalEl.textContent = (summary.total_credits_30d || 0).toLocaleString();
        if (avgEl) avgEl.textContent = (summary.daily_avg || 0).toFixed(1);
        if (projEl) projEl.textContent = (summary.projected_monthly || 0).toLocaleString();

        // CSS Bar Chart
        if (chartEl && days.length > 0) {
            var maxCredits = Math.max.apply(null, days.map(function(d) { return d.credits || 0; })) || 1;
            var chartHtml = '';
            days.forEach(function(d) {
                var pct = Math.round(((d.credits || 0) / maxCredits) * 100);
                var barH = Math.max(4, pct);
                chartHtml += '<div style="flex:1; min-width:8px; display:flex; flex-direction:column; align-items:center; justify-content:flex-end; height:100%;" title="' + _escapeHTML(d.date || '') + ': ' + (d.credits || 0) + ' credits">' +
                    '<div style="width:100%; max-width:24px; height:' + barH + '%; background:linear-gradient(180deg,#7c3aed,#4f46e5); border-radius:4px 4px 0 0; transition:height 0.3s;"></div>' +
                    '</div>';
            });
            chartEl.innerHTML = chartHtml;
        } else if (chartEl) {
            chartEl.innerHTML = '<div style="color:var(--text-muted); font-size:0.85rem; width:100%; text-align:center; align-self:center;">No chart data available.</div>';
        }

        // Detail Table
        if (days.length === 0) {
            if (tableBody) tableBody.innerHTML = '<tr><td colspan="3" style="padding:20px; text-align:center; color:var(--text-muted);">No credit data available.</td></tr>';
            return;
        }
        var html = '';
        days.forEach(function(d) {
            html += '<tr style="border-bottom:1px solid var(--glass-border);">' +
                '<td style="padding:10px 12px;">' + _escapeHTML(d.date || '\u2014') + '</td>' +
                '<td style="padding:10px 12px; text-align:right; font-weight:600;">' + (d.credits || 0).toLocaleString() + '</td>' +
                '<td style="padding:10px 12px; text-align:right;">' + (d.query_count || 0).toLocaleString() + '</td>' +
                '</tr>';
        });
        if (tableBody) tableBody.innerHTML = html;
        showToast('Credit trends loaded.', 'success');
    } catch(err) {
        console.error('[Credit Trends]', err);
        if (tableBody) tableBody.innerHTML = '<tr><td colspan="3" style="padding:20px; text-align:center; color:#ef4444;">Error: ' + _escapeHTML(err.message) + '</td></tr>';
        showToast('Credit trends load failed: ' + err.message, 'error');
    }
};

// =============================================================================
// L0 ADMIN — AUDIT TRAIL
// =============================================================================
window.fetchAuditLog = async function() {
    var tableBody = document.getElementById('l0-audit-table');
    if (tableBody) tableBody.innerHTML = '<tr><td colspan="5" style="padding:20px; text-align:center; color:var(--text-muted);">&#8987; Loading audit trail&hellip;</td></tr>';
    try {
        var user = firebase.auth().currentUser;
        if (!user) return;
        var token = await user.getIdToken();
        var resp = await fetch(API_BASE + '/api/l0/audit-log', {
            headers: { 'Authorization': 'Bearer ' + token }
        });
        if (!resp.ok) throw new Error('HTTP ' + resp.status);
        var json = await resp.json();
        var entries = json.data || [];
        if (entries.length === 0) {
            if (tableBody) tableBody.innerHTML = '<tr><td colspan="5" style="padding:20px; text-align:center; color:var(--text-muted);">No audit entries found.</td></tr>';
            return;
        }
        var html = '';
        entries.forEach(function(e) {
            var ts = e.timestamp ? new Date(e.timestamp).toLocaleString() : '\u2014';
            html += '<tr style="border-bottom:1px solid var(--glass-border);">' +
                '<td style="padding:10px 12px; font-size:0.8rem; color:var(--text-muted); white-space:nowrap;">' + _escapeHTML(ts) + '</td>' +
                '<td style="padding:10px 12px;">' + _escapeHTML(e.admin || '\u2014') + '</td>' +
                '<td style="padding:10px 12px;"><span style="background:rgba(79,70,229,0.1); color:#4f46e5; padding:2px 8px; border-radius:4px; font-size:0.78rem; font-weight:600;">' + _escapeHTML(e.action || '\u2014') + '</span></td>' +
                '<td style="padding:10px 12px; font-size:0.85rem;">' + _escapeHTML(e.target || '\u2014') + '</td>' +
                '<td style="padding:10px 12px; font-size:0.8rem; color:var(--text-muted); max-width:200px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;">' + _escapeHTML(e.details || '\u2014') + '</td>' +
                '</tr>';
        });
        if (tableBody) tableBody.innerHTML = html;
        showToast('Audit trail loaded.', 'success');
    } catch(err) {
        console.error('[Audit Trail]', err);
        if (tableBody) tableBody.innerHTML = '<tr><td colspan="5" style="padding:20px; text-align:center; color:#ef4444;">Error: ' + _escapeHTML(err.message) + '</td></tr>';
        showToast('Audit trail load failed: ' + err.message, 'error');
    }
};

// =============================================================================
// L0 ADMIN — KILL SWITCH
// =============================================================================
window.toggleKillSwitch = async function(action) {
    if (!action) action = 'pause';
    var confirmMsg = action === 'pause'
        ? 'Are you sure you want to PAUSE all pipelines? This will stop all lead processing immediately.'
        : 'Are you sure you want to RESUME all pipelines?';
    if (!confirm(confirmMsg)) return;
    try {
        var user = firebase.auth().currentUser;
        if (!user) return;
        var token = await user.getIdToken();
        var resp = await fetch(API_BASE + '/api/l0/kill-switch', {
            method: 'POST',
            headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' },
            body: JSON.stringify({ action: action })
        });
        if (!resp.ok) throw new Error('HTTP ' + resp.status);
        var json = await resp.json();
        var newState = (json.data || {}).state || action;
        _renderKillSwitchButton(newState === 'paused' || action === 'pause');
        showToast(action === 'pause' ? 'All pipelines PAUSED.' : 'Pipelines RESUMED.', action === 'pause' ? 'error' : 'success');
    } catch(err) {
        console.error('[Kill Switch]', err);
        showToast('Kill switch failed: ' + err.message, 'error');
    }
};

window.loadKillSwitchState = function(healthData) {
    if (!healthData) return;
    var isPaused = healthData.pipelines_paused === true;
    _renderKillSwitchButton(isPaused);
};

function _renderKillSwitchButton(isPaused) {
    var btn = document.getElementById('l0-kill-switch-btn');
    if (!btn) return;
    if (isPaused) {
        btn.textContent = '\u2705 Resume All Pipelines';
        btn.dataset.action = 'resume';
        btn.style.background = '#059669';
        btn.style.boxShadow = '0 4px 14px rgba(5,150,105,0.3)';
    } else {
        btn.textContent = '\ud83d\uded1 Emergency Pause All Pipelines';
        btn.dataset.action = 'pause';
        btn.style.background = '#dc2626';
        btn.style.boxShadow = '0 4px 14px rgba(220,38,38,0.3)';
    }
}

window.loadMoreLeads = function() {
    showToast('Historical offset cursors must be mapped in Orchestrator Endpoint v2.', 'info');
};

// ============================================================================
// V15: NATIVE CRM SANDBOX ENGINE â€” /crm-test
// ============================================================================

const CRM_STATUSES = ['new', 'contacted', 'replied', 'negotiating', 'won', 'lost'];

// State: keyed by lead id
let crmLeadsCache = [];
let crmActiveLead = null;   // lead object currently open in side panel
let crmDraggedId  = null;   // id of card being dragged

// â”€â”€ loadCrmBoard â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
window.loadCrmBoard = async function() {
    const user = firebase.auth().currentUser;
    if (!user) return;

    // Show loading state in all columns
    CRM_STATUSES.forEach(s => {
        const body = document.getElementById(`body-${s}`);
        if (body) body.innerHTML = '<div style="padding:8px; color:var(--text-muted); font-size:0.8rem;">Loading...</div>';
    });

    try {
        const token = await user.getIdToken();
        const res   = await fetch(`${API_BASE}/api/leads?crm=true&rt=${Date.now()}`, {
            headers: { 'Authorization': `Bearer ${token}` }
        });
        if (res.status === 401 || res.status === 403) return handleAuthRejection();
        const payload = await res.json();
        // Filter strictly: is_in_crm === true only
        crmLeadsCache = (payload.data || []).filter(l => l.is_in_crm === true);
        renderKanban();
    } catch(e) {
        console.error('[CRM] Load failed', e);
        showToast('Failed to load CRM data', 'error');
    }
};

// â”€â”€ renderKanban â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
function renderKanban() {
    const now = Date.now();
    // Group by crm_status (fallback to 'new')
    const grouped = {};
    CRM_STATUSES.forEach(s => grouped[s] = []);
    crmLeadsCache.forEach(lead => {
        const st = CRM_STATUSES.includes(lead.crm_status) ? lead.crm_status : 'new';
        grouped[st].push(lead);
    });

    // Render each column
    CRM_STATUSES.forEach(status => {
        const body    = document.getElementById(`body-${status}`);
        const counter = document.getElementById(`cnt-${status}`);
        const leads   = grouped[status];
        if (!body) return;
        if (counter) counter.textContent = leads.length;

        body.innerHTML = '';
        leads.forEach(lead => {
            const card     = buildKanbanCard(lead, now);
            body.appendChild(card);
        });
    });

    // Attach drop targets — V23.9: enhanced with placeholder + hovered class
    document.querySelectorAll('.kanban-col').forEach(col => {
        const body = col.querySelector('.kanban-col-body');
        col.addEventListener('dragover', e => {
            e.preventDefault();
            e.dataTransfer.dropEffect = 'move';
            col.classList.add('kanban-col--hovered');
            col.classList.add('drag-over'); // backward compat
            if (!body) return;
            // Insert placeholder at correct position
            let existing = body.querySelector('.kanban-placeholder');
            if (!existing) {
                existing = document.createElement('div');
                existing.className = 'kanban-placeholder';
            }
            // Find the card we're hovering over to insert before it
            const afterEl = _getDragAfterElement(body, e.clientY);
            if (afterEl) {
                body.insertBefore(existing, afterEl);
            } else {
                body.appendChild(existing);
            }
        });
        col.addEventListener('dragleave', e => {
            // Only remove if actually leaving the column (not entering a child)
            if (col.contains(e.relatedTarget)) return;
            col.classList.remove('kanban-col--hovered', 'drag-over');
            const ph = body && body.querySelector('.kanban-placeholder');
            if (ph) ph.remove();
        });
        col.addEventListener('drop', e => {
            col.classList.remove('kanban-col--hovered', 'drag-over');
            const ph = body && body.querySelector('.kanban-placeholder');
            if (ph) ph.remove();
            handleKanbanDrop(e, col);
        });
    });

    // Helper: find the card element directly after the mouse Y position
    function _getDragAfterElement(container, y) {
        const cards = [...container.querySelectorAll('.crm-card:not(.kanban-card--dragging)')];
        let closest = null;
        let closestOffset = Number.NEGATIVE_INFINITY;
        cards.forEach(child => {
            const box = child.getBoundingClientRect();
            const offset = y - box.top - box.height / 2;
            if (offset < 0 && offset > closestOffset) {
                closestOffset = offset;
                closest = child;
            }
        });
        return closest;
    }

    // Health widget
    const fmt = v => `\u20B9${Number(v || 0).toLocaleString('en-IN')}`;
    const negotiating = grouped['negotiating'].reduce((a, l) => a + (l.estimated_value || 0), 0);
    const won         = grouped['won'].reduce((a, l) => a + (l.estimated_value || 0), 0);
    const el1 = document.getElementById('crm-negotiating-sum'); if (el1) el1.textContent = fmt(negotiating);
    const el2 = document.getElementById('crm-won-sum');         if (el2) el2.textContent = fmt(won);
    const el3 = document.getElementById('crm-pipeline-total');  if (el3) el3.textContent = fmt(negotiating + won);
    const el4 = document.getElementById('crm-total-count');     if (el4) el4.textContent = crmLeadsCache.length;
}

// â”€â”€ buildKanbanCard â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
function buildKanbanCard(lead, now) {
    const card   = document.createElement('div');
    const id     = lead.id || lead.doc_id || '';
    card.className   = 'crm-card';
    card.draggable   = true;
    card.dataset.id  = id;

    // Follow-up date badge
    let fueBadge = '';
    if (lead.follow_up_date) {
        const fueTs = lead.follow_up_date._seconds
            ? lead.follow_up_date._seconds * 1000
            : new Date(lead.follow_up_date).getTime();
        if (fueTs < now) fueBadge = '<span class="due-badge">Due</span>';
    }

    const domain   = (() => { try { return new URL(lead.url || 'https://unknown').hostname.replace('www.', ''); } catch(_) { return lead.url || 'Unknown'; } })();
    const signal   = lead.intent_signal || lead.pain_point || '';
    const value    = lead.estimated_value ? `\uD83D\uDCB0 \u20B9${Number(lead.estimated_value).toLocaleString('en-IN')}` : '';

    card.innerHTML = `
        <div class="card-domain">${domain}${fueBadge}</div>
        <div class="card-score">Score: ${lead.score || 'N/A'}/10 · ${(lead.confidence_tier || 'High')}</div>
        ${signal ? `<div class="card-signal">${signal}</div>` : ''}
        ${value ? `<div class="card-value">${value}</div>` : ''}
    `;

    card.addEventListener('dragstart', e => {
        crmDraggedId = id;
        card.classList.add('dragging', 'kanban-card--dragging');
        e.dataTransfer.effectAllowed = 'move';
    });
    card.addEventListener('dragend', () => {
        card.classList.remove('dragging', 'kanban-card--dragging');
        // Clean up any stale placeholders across all columns
        document.querySelectorAll('.kanban-placeholder').forEach(ph => ph.remove());
        document.querySelectorAll('.kanban-col--hovered').forEach(c => c.classList.remove('kanban-col--hovered', 'drag-over'));
    });
    card.addEventListener('click',   () => openCrmPanel(lead));
    return card;
}

// â”€â”€ handleKanbanDrop â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
async function handleKanbanDrop(e, col) {
    e.preventDefault();
    col.classList.remove('drag-over');
    const newStatus = col.dataset.status;
    if (!crmDraggedId || !newStatus) return;

    // Optimistic UI
    const lead = crmLeadsCache.find(l => (l.id || l.doc_id) === crmDraggedId);
    if (!lead) return;
    const oldStatus  = lead.crm_status || 'new';
    if (oldStatus === newStatus) return;
    lead.crm_status  = newStatus;
    renderKanban();

    // Persist
    try {
        await performApiMutation(`/api/leads/${crmDraggedId}`, 'PUT', { crm_status: newStatus });
        showToast(`Moved to "${newStatus}"`, 'success');
    } catch(err) {
        // Rollback
        lead.crm_status = oldStatus;
        renderKanban();
        showToast('Status update failed', 'error');
    }
    crmDraggedId = null;
}

// â”€â”€ openCrmPanel / closeCrmPanel â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
// ── PLATFORM_META ─────────────────────────────────────────────────────────────
// Maps contact_endpoints[].platform -> { icon, label } for the CRM slide-out.
// Defined here because openCrmPanel() references it directly.
// NOTE: _PRISM_PLATFORM_META (below) is domain-keyed (for copilot labels);
//       this map is platform-name-keyed (for CRM panel Smart Action buttons).
const PLATFORM_META = {
    linkedin:  { icon: '&#x1F4BC;', label: 'Open LinkedIn'    },
    email:     { icon: '&#x2709;',  label: 'Send Email'        },
    twitter:   { icon: '&#x1F426;', label: 'Open X / Twitter' },
    x:         { icon: '&#x1F426;', label: 'Open X'           },
    instagram: { icon: '&#x1F4F7;', label: 'Open Instagram'   },
    facebook:  { icon: '&#x1F4AC;', label: 'Open Facebook'    },
    reddit:    { icon: '&#x1F47E;', label: 'Open Reddit'      },
    quora:     { icon: '&#x1F4AC;', label: 'Open Quora'       },
    phone:     { icon: '&#x1F4DE;', label: 'Call'             },

    website:   { icon: '&#x1F310;', label: 'Visit Website'    },
    other:     { icon: '&#x1F4CB;', label: 'Contact'          },
};

window.openCrmPanel = function(lead) {
    crmActiveLead = lead;
    const panel = document.getElementById('crm-side-panel');
    const body  = document.getElementById('crm-panel-body');
    const title = document.getElementById('crm-panel-title');
    if (!panel || !body) return;

    const id      = lead.id || lead.doc_id || '';
    const domain  = (() => { try { return new URL(lead.url || 'https://x').hostname.replace('www.',''); } catch(_) { return lead.url || id; } })();
    if (title) title.textContent = domain;

    const fueVal  = lead.follow_up_date
        ? (() => { try { const ts = lead.follow_up_date._seconds ? lead.follow_up_date._seconds*1000 : new Date(lead.follow_up_date).getTime(); return new Date(ts).toISOString().slice(0,10); } catch(_) { return ''; } })()
        : '';

    const notes   = Array.isArray(lead.notes) ? lead.notes : [];
    const notesFeed = notes.length === 0
        ? '<div style="color:var(--text-muted); font-size:0.8rem; font-style:italic;">No notes yet.</div>'
        : notes.slice().reverse().map(n => `
            <div class="crm-note-item">
                <div class="note-ts">${new Date(n.timestamp?._seconds ? n.timestamp._seconds*1000 : n.timestamp).toLocaleString()}</div>
                <div class="note-text">${_escapeHTML(n.text || '')}</div>
            </div>`).join('');

    // Pull meeting/asset from user data
    const meetingUrl = window.currentUserData?.meeting_url || '';
    const assetUrl   = window.currentUserData?.asset_url || lead.attached_asset_url || '';

    // Primary endpoint for Smart Action
    const endpoints = lead.contact_endpoints || [];
    const primary   = endpoints[0] || null;
    const primaryLabel = primary
        ? `${PLATFORM_META[primary.platform]?.icon || 'ðŸ”—'} ${PLATFORM_META[primary.platform]?.label || 'Contact'}`
        : 'ðŸ“‹ Copy DM';

    body.innerHTML = `
        <!-- Intent Signal -->
        <div class="crm-panel-section">
            <div class="crm-panel-label">Intent Signal</div>
            <div class="crm-panel-intent">${_escapeHTML(lead.intent_signal || lead.pain_point || 'No signal captured.')}</div>
        </div>

        <!-- AI-Drafted DM -->
        <div class="crm-panel-section">
            <div class="crm-panel-label">AI-Drafted Message</div>
            <div class="crm-panel-dm" id="crm-dm-preview">${_escapeHTML(lead.dm || 'No draft available.')}</div>
        </div>

        <!-- Smart Action Toggles -->
        <div class="crm-panel-section" style="background:#f8fafc; padding:12px; border-radius:10px; border:1px solid var(--glass-border);">
            <div class="crm-panel-label" style="margin-bottom:8px;">Smart Action Settings</div>
            ${meetingUrl ? `<div class="crm-toggle-row">
                <span class="crm-toggle-label">ðŸ“… Include Meeting Link</span>
                <label class="crm-toggle"><input type="checkbox" id="toggle-meeting" onchange="refreshCrmDmPreview('${id}')"><span class="crm-toggle-slider"></span></label>
            </div>` : ''}
            ${assetUrl ? `<div class="crm-toggle-row">
                <span class="crm-toggle-label">ðŸ”— Include Asset Link</span>
                <label class="crm-toggle"><input type="checkbox" id="toggle-asset" onchange="refreshCrmDmPreview('${id}')"><span class="crm-toggle-slider"></span></label>
            </div>` : ''}
            <button class="crm-smart-action-btn" onclick="crmSmartAction('${id}', '${primary ? encodeURIComponent(primary.uri) : ''}', '${primary ? primary.platform : ''}')">
                ${primaryLabel}
            </button>
        </div>

        <!-- Estimated Value -->
        <div class="crm-panel-section">
            <div class="crm-panel-label">Estimated Deal Value (\u20B9)</div>
            <input type="number" id="crm-est-value" class="crm-input" value="${lead.estimated_value || 0}" placeholder="e.g. 50000" min="0">
            <button class="crm-save-btn" onclick="saveCrmValue('${id}')">Save Value</button>
        </div>

        <!-- Follow-Up Date -->
        <div class="crm-panel-section">
            <div class="crm-panel-label">Follow-Up Date</div>
            <input type="date" id="crm-followup" class="crm-input" value="${fueVal}">
            <button class="crm-save-btn" onclick="saveCrmFollowup('${id}')">Set Reminder</button>
        </div>

        <!-- Notes -->
        <div class="crm-panel-section">
            <div class="crm-panel-label">Notes</div>
            <div class="crm-notes-feed" id="crm-notes-feed">${notesFeed}</div>
            <textarea id="crm-note-input" class="crm-input" rows="3" placeholder="Add a note..." style="margin-top:8px; resize:vertical;"></textarea>
            <button class="crm-save-btn" onclick="saveCrmNote('${id}')">Add Note</button>
        </div>
    `;

    panel.classList.add('open');
};

window.closeCrmPanel = function() {
    const panel = document.getElementById('crm-side-panel');
    if (panel) panel.classList.remove('open');
    crmActiveLead = null;
};

// â”€â”€ refreshCrmDmPreview â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
window.refreshCrmDmPreview = function(id) {
    if (!crmActiveLead) return;
    const meetEl  = document.getElementById('toggle-meeting');
    const assetEl = document.getElementById('toggle-asset');
    const preview = document.getElementById('crm-dm-preview');
    if (!preview) return;
    let dm = crmActiveLead.dm || '';
    if (meetEl && meetEl.checked && window.currentUserData?.meeting_url) {
        dm += `\n\nðŸ“… Book a quick call: ${window.currentUserData.meeting_url}`;
    }
    if (assetEl && assetEl.checked) {
        const assetUrl = window.currentUserData?.asset_url || crmActiveLead.attached_asset_url || '';
        if (assetUrl) dm += `\n\nðŸ”— Here's our resource: ${assetUrl}`;
    }
    preview.textContent = dm;
};

// â”€â”€ crmSmartAction â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
window.crmSmartAction = function(id, uriEnc, platform) {
    if (!crmActiveLead) return;
    const meetEl  = document.getElementById('toggle-meeting');
    const assetEl = document.getElementById('toggle-asset');
    let dm = crmActiveLead.dm || '';

    if (meetEl && meetEl.checked && window.currentUserData?.meeting_url) {
        dm += `\n\nðŸ“… Book a quick call: ${window.currentUserData.meeting_url}`;
    }
    if (assetEl && assetEl.checked) {
        const assetUrl = window.currentUserData?.asset_url || crmActiveLead.attached_asset_url || '';
        if (assetUrl) dm += `\n\nðŸ”— Here's our resource: ${assetUrl}`;
    }

    navigator.clipboard.writeText(dm).catch(() => {});

    if (uriEnc) {
        const uri = decodeURIComponent(uriEnc);
        const isEmail = platform === 'email' || (uri.includes('@') && !uri.startsWith('http'));
        const isPhone = platform === 'other' && /^[\d\s+()\\-]{6,}$/.test(uri);
        let href;
        if (isEmail)      href = `mailto:${uri}`;
        else if (isPhone) href = `tel:${uri}`;
        else if (/^(https?:\/\/|mailto:|tel:)/i.test(uri)) href = uri;
        else              href = `https://${uri}`;
        window.open(href, '_blank', 'noopener,noreferrer');
    }

    showToast('DM copied with appended links!', 'success');
    updateLeadStatus(id, 'contacted');
};

// â”€â”€ saveCrmValue â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
window.saveCrmValue = async function(id) {
    const val = parseInt(document.getElementById('crm-est-value')?.value || '0', 10);
    try {
        const ok = await performApiMutation(`/api/leads/${id}`, 'PUT', { estimated_value: val });
        if (ok) {
            const lead = crmLeadsCache.find(l => (l.id || l.doc_id) === id);
            if (lead) lead.estimated_value = val;
            renderKanban();
            showToast('Deal value saved!', 'success');
        }
    } catch(e) { showToast('Failed to save value', 'error'); }
};

// â”€â”€ saveCrmFollowup â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
window.saveCrmFollowup = async function(id) {
    const dateStr = document.getElementById('crm-followup')?.value;
    if (!dateStr) return showToast('Pick a date first', 'error');
    const ts = new Date(dateStr).toISOString();
    try {
        const ok = await performApiMutation(`/api/leads/${id}`, 'PUT', { follow_up_date: ts });
        if (ok) {
            const lead = crmLeadsCache.find(l => (l.id || l.doc_id) === id);
            if (lead) lead.follow_up_date = ts;
            renderKanban();
            showToast('Reminder set!', 'success');
        }
    } catch(e) { showToast('Failed to save date', 'error'); }
};

// â”€â”€ saveCrmNote â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
window.saveCrmNote = async function(id) {
    const input = document.getElementById('crm-note-input');
    const text  = input?.value?.trim();
    if (!text) return showToast('Note cannot be empty', 'error');

    const note  = { timestamp: new Date().toISOString(), text };
    const lead  = crmLeadsCache.find(l => (l.id || l.doc_id) === id);
    const notes = Array.isArray(lead?.notes) ? [...lead.notes, note] : [note];

    try {
        const ok = await performApiMutation(`/api/leads/${id}`, 'PUT', { notes });
        if (ok) {
            if (lead) lead.notes = notes;
            if (input) input.value = '';
            // Re-render notes feed
            const feed = document.getElementById('crm-notes-feed');
            if (feed) {
                feed.innerHTML = notes.slice().reverse().map(n => `
                    <div class="crm-note-item">
                        <div class="note-ts">${new Date(n.timestamp?._seconds ? n.timestamp._seconds*1000 : n.timestamp).toLocaleString()}</div>
                        <div class="note-text">${n.text}</div>
                    </div>`).join('');
            }
            showToast('Note saved!', 'success');
        }
    } catch(e) { showToast('Failed to save note', 'error'); }
};
// NOTE: The loadDashboard override that previously lived here was removed in
// V15.1 HOTFIX. It caused infinite recursion due to JS function-declaration
// hoisting: the 'original' reference captured the hoisted new declaration,
// making _origLoadDashboard === the new loadDashboard (self-reference).
// The crmAutoOpen check is now inlined directly in loadDashboard() above.

// =============================================================================
// V17: CONVERSATIONAL "FIND NEW CLIENTS" ENGINE
// =============================================================================

// --- State ---
window._fcState = {
    gl: '',       // country code
    location: '', // city/region text
    whoConfirmed: '',
    whatConfirmed: '',
};

// --- Utility: parse intent sentence for key signals ---
function fcParseIntent(sentence) {
    const s = sentence.toLowerCase();
    const result = { who: sentence.trim(), where: '', gl: '' };

    // Location extraction â€” order matters (longer matches first)
    const locationMap = [
        { re: /\b(united\s*states|usa|u\.s\.a|u\.s)\b/i,  gl: 'us', label: 'United States' },
        { re: /\b(united\s*kingdom|uk|u\.k|britain|england|london)\b/i, gl: 'uk', label: 'United Kingdom' },
        { re: /\b(canada|toronto|vancouver|montreal)\b/i,  gl: 'ca', label: 'Canada' },
        { re: /\b(australia|sydney|melbourne)\b/i,         gl: 'au', label: 'Australia' },
        { re: /\b(india|mumbai|delhi|bangalore|bengaluru|hyderabad|pune|chennai|kolkata|kerala|kochi|jaipur)\b/i, gl: 'in', label: 'India' },
    ];

    for (const loc of locationMap) {
        if (loc.re.test(sentence)) {
            result.where = loc.label;
            result.gl    = loc.gl;
            break;
        }
    }

    // Attempt to strip the location phrase from the "who" summary
    if (result.where) {
        result.who = sentence
            .replace(/\s+in\s+(the\s+)?(united states|usa|united kingdom|uk|canada|australia|india|[a-z\s,]+)/gi, '')
            .replace(/\s+from\s+(the\s+)?(united states|usa|united kingdom|uk|canada|australia|india)/gi, '')
            .trim() || sentence.trim();
    }

    return result;
}

// --- Auto-generate a readable campaign name from the parsed intent ---
function fcBuildCampaignName(who, where) {
    const now = new Date();
    const month = now.toLocaleString('en', { month: 'short' });
    const year  = now.getFullYear();
    // Take first 35 chars of who
    const base = who.length > 35 ? who.substring(0, 35).trim() + 'â€¦' : who;
    return where ? `${base} · ${where} · ${month} ${year}` : `${base} · ${month} ${year}`;
}

// --- Relative time helper ---
function fcTimeAgo(ts) {
    if (!ts) return '';
    const then = typeof ts.toDate === 'function' ? ts.toDate() : new Date(ts);
    const diffMs = Date.now() - then.getTime();
    const m = Math.floor(diffMs / 60000);
    const h = Math.floor(m / 60);
    const d = Math.floor(h / 24);
    if (m < 2)  return 'just now';
    if (m < 60) return `${m}m ago`;
    if (h < 24) return `${h}h ago`;
    if (d < 7)  return `${d}d ago`;
    return then.toLocaleDateString('en', { day: 'numeric', month: 'short' });
}

// =============================================================================
// V18 DIGITAL TWIN OVERRIDES
// =============================================================================

window.openNewCampaignModal = async function() {
    // Defensive dual-path: activeWallet is the closure var (always set);
    // also check flattened root fields in case of DB schema migration.
    const aw = window.activeWallet || {};
    const ud = window.currentUserData || {};
    const allocated = Number(aw.allocated_credits || ud.allocated_credits || 0) || 0;
    const consumed  = Number(aw.consumed_credits  || ud.consumed_credits  || 0) || 0;
    const reserved  = Number(aw.reserved_credits || 0) || 0;
    const remaining = (aw.available_credits != null)
        ? Number(aw.available_credits) || 0
        : (allocated - consumed - reserved);
    if (window.SIO_DEBUG) console.log('[DEBUG WALLET] window.activeWallet:', aw,
                '| currentUserData:', ud,
                '| allocated:', allocated, '| consumed:', consumed, '| remaining:', remaining);
    if (remaining <= 0) {
        showToast('Credits exhausted. Contact admin to reload.', 'error');
        return;
    }
    
    // V23.9: Reset geo cascade and hidden fields before opening modal
    const geoContinent = document.getElementById('geo-continent-select');
    const geoCountry   = document.getElementById('geo-country-select');
    const geoRegion    = document.getElementById('geo-region-select');
    if (geoContinent) geoContinent.value = '';
    if (geoCountry)   { geoCountry.value = ''; geoCountry.disabled = true; }
    if (geoRegion)    { geoRegion.value = ''; geoRegion.disabled = true; }
    const glHidden  = document.getElementById('edit-camp-gl');
    const locHidden = document.getElementById('edit-camp-location');
    if (glHidden)  glHidden.value  = '';
    if (locHidden) locHidden.value = '';
    const geoPreview = document.getElementById('geo-compiled-preview');
    if (geoPreview) geoPreview.textContent = '';
    // Reset the edit form if it exists
    const editForm = document.getElementById('edit-campaign-form');
    if (editForm) editForm.reset();

    // Pass control to the Digital Twin Onboarding Flow (View A)
    window.openDTModal();
};

// =============================================================================
// V17: DASHBOARD GREETING + KPI TILES
// =============================================================================

function fcUpdateGreeting(firstName) {
    const el = document.getElementById('greeting-message');
    if (!el) return;
    const hr = new Date().getHours();
    const g  = hr < 12 ? 'Good morning' : hr < 17 ? 'Good afternoon' : 'Good evening';
    el.textContent = firstName ? `${g}, ${firstName}.` : `${g}.`;
}

function fcUpdateKPIs(leadsArray) {
    let cNew = 0, cContacted = 0, cConverted = 0;
    let cDiscovered = leadsArray.length;
    let cIgnored = 0;
    let cActionable = 0;
    let cValue = 0;

    leadsArray.forEach(l => {
        if (l.status === 'ignored') {
            cIgnored++;
        } else {
            if (l.status === 'converted') {
                cConverted++;
            } else if (l.status === 'contacted' || l.status === 'replied') {
                cContacted++;
            } else {
                cNew++;
            }
            cActionable++;
            cValue += Number(l.estimated_value || 0);
        }
    });

    const setEl = (id, v) => { const e = document.getElementById(id); if (e) e.textContent = v; };
    
    setEl('kpi-new-count', cNew);
    setEl('kpi-contacted-count', cContacted);
    setEl('kpi-won-count', cConverted);
    
    let valueStr = new Intl.NumberFormat('en-IN', { style: 'currency', currency: 'INR', maximumFractionDigits: 0 }).format(cValue);
    setEl('kpi-value-count', valueStr);

    const setHtml = (id, html) => { const e = document.getElementById(id); if (e) e.innerHTML = html; };
    setHtml('kpi-new-subtext', `Discovered: ${cDiscovered} &middot; Ignored: ${cIgnored}`);
    setHtml('kpi-contacted-subtext', `Out of ${cActionable} actionable leads`);
    
    let rate = cActionable > 0 ? ((cConverted / cActionable) * 100).toFixed(1) : '0.0';
    setHtml('kpi-converted-subtext', `Conversion rate: ${rate}%`);
}

// =============================================================================
// V18: LEAD CARD RENDERER - Predictive Copilot Architecture
// prism_mode routing:
//   GeneralDomain / legacy -> B2B dossier (Company Size, Tech Stack, Objection)
//   WalledGarden / Social  -> Social dossier (Platform, Snippet, Handle)
//   B2B2C                  -> Split view (Consumer Demand + Distributor)
// =============================================================================

function getScoreEmoji(score) {
    if (score >= 9) return '&#x1F525;';
    if (score >= 7) return '&#x26A1;';
    if (score >= 5) return '&#x1F511;';
    return '&#x1F4CB;';
}

var _PRISM_PLATFORM_META = {
    'reddit.com':    'Copy Reply & Open Reddit',
    'linkedin.com':  'Copy Pitch & Open LinkedIn',
    'facebook.com':  'Copy Message & Open Facebook',
    'instagram.com': 'Copy DM & Open Instagram',
    'twitter.com':   'Copy Reply & Open X',
    'x.com':         'Copy Reply & Open X',
    'quora.com':     'Copy Answer & Open Quora',
    'youtube.com':   'Copy Comment & Open YouTube',
    'team-bhp.com':  'Copy Reply & Open Team-BHP',
};

function _copilotBtnLabel(lead) {
    var url = lead.url || lead.source_url || '';
    var rawUrl = url.toLowerCase();
    var isPdf = rawUrl.split('?')[0].endsWith('.pdf');
    if (isPdf) return '&#x1F4CB; Copy Pitch & Open PDF &#x2197;';

    var hostname = '';
    try { hostname = new URL(url).hostname.replace('www.', '').toLowerCase(); } catch(e) {}
    var domains = Object.keys(_PRISM_PLATFORM_META);
    for (var i = 0; i < domains.length; i++) {
        if (hostname.endsWith(domains[i])) return '&#x1F4CB; ' + _PRISM_PLATFORM_META[domains[i]] + ' &#x2197;';
    }
    var mode = (lead.prism_mode || '').toLowerCase();
    if (mode.indexOf('walledgarden') !== -1) return '&#x1F4CB; Copy Reply & Open Platform &#x2197;';
    if (mode === 'b2b2c')                    return '&#x1F4CB; Copy Pitch & Open Distributor &#x2197;';
    if (lead.source === 'inbound_radar')     return '&#x1F4CB; Copy Pitch & View Signal &#x2197;';
    return '&#x1F4CB; Copy Pitch & Open Website &#x2197;';
}

function _prismDossierHTML(lead) {
    var mode = (lead.prism_mode || 'GeneralDomain').toLowerCase();

    if (mode.indexOf('walledgarden') !== -1 || mode === 'social') {
        var platform = 'Social Platform';
        try {
            var h = new URL(lead.url || lead.source_url || '').hostname.replace('www.','');
            platform = h.split('.')[0].charAt(0).toUpperCase() + h.split('.')[0].slice(1);
        } catch(e) {}
        var snippet = _escapeHTML(lead.intent_signal || lead.pain_point || '');
        var handleEntry = null;
        var eps = lead.contact_endpoints || [];
        for (var ei = 0; ei < eps.length; ei++) {
            var uri = eps[ei].uri || '';
            if (!uri.includes('@') && (uri.includes('linkedin') || uri.includes('reddit') || uri.includes('facebook') || uri.includes('instagram') || uri.includes('/u/') || uri.includes('/user/'))) {
                handleEntry = eps[ei]; break;
            }
        }
        var handle = handleEntry ? handleEntry.uri : '';
        return '<div class="lc-section lc-dossier lc-dossier--social">' +
            '<div class="lc-section-label">Social Intelligence</div>' +
            '<div class="lc-dossier-row"><span class="lc-dossier-key">Intent Detected On</span><span class="lc-dossier-val">' + _escapeHTML(platform) + '</span></div>' +
            (snippet ? '<div class="lc-dossier-row"><span class="lc-dossier-key">Snippet Context</span><span class="lc-dossier-val lc-dossier-snippet">' + snippet + '</span></div>' : '') +
            (handle ? '<div class="lc-dossier-row"><span class="lc-dossier-key">Profile</span><span class="lc-dossier-val"><a href="' + _safeHref(handle) + '" target="_blank" rel="noopener" style="color:var(--primary);text-decoration:none;">View &#x2197;</a></span></div>' : '') +
            '</div>';
    }

    if (mode === 'b2b2c') {
        var demand = _escapeHTML(lead.intent_signal || lead.pain_point || 'Consumer demand signal captured.');
        var obj    = _escapeHTML(lead.primary_objection_hypothesis || lead.objection || '');
        var tech   = _escapeHTML((lead.tech_stack_found || []).slice(0,4).join(', ') || '-');
        return '<div class="lc-section lc-dossier lc-dossier--b2b2c">' +
            '<div class="lc-section-label">Consumer Demand Context</div>' +
            '<div class="lc-dossier-row"><span class="lc-dossier-key">Demand Signal</span><span class="lc-dossier-val">' + demand + '</span></div>' +
            '</div><div class="lc-section lc-dossier lc-dossier--b2b2c-dist">' +
            '<div class="lc-section-label">Distributor Contact Dossier</div>' +
            '<div class="lc-dossier-row"><span class="lc-dossier-key">Tech Stack</span><span class="lc-dossier-val">' + tech + '</span></div>' +
            (obj ? '<div class="lc-dossier-row"><span class="lc-dossier-key">Primary Objection</span><span class="lc-dossier-val">' + obj + '</span></div>' : '') +
            '</div>';
    }

    var csz   = _escapeHTML(lead.company_size_tier || '-');
    var tech2 = _escapeHTML((lead.tech_stack_found || []).slice(0,4).join(', ') || '-');
    var obj2  = _escapeHTML(lead.primary_objection_hypothesis || lead.objection || '');
    var dmN   = lead.decision_maker_name  || '';
    var dmT   = lead.decision_maker_title || '';
    var dmStr = _escapeHTML([dmN, dmT].filter(Boolean).join(' / ') || '-');
    return '<div class="lc-section lc-dossier lc-dossier--b2b">' +
        '<div class="lc-section-label">Company Dossier</div>' +
        '<div class="lc-dossier-row"><span class="lc-dossier-key">Company Size</span><span class="lc-dossier-val">' + csz + '</span></div>' +
        '<div class="lc-dossier-row"><span class="lc-dossier-key">Tech Stack</span><span class="lc-dossier-val">' + tech2 + '</span></div>' +
        '<div class="lc-dossier-row"><span class="lc-dossier-key">Decision Maker</span><span class="lc-dossier-val">' + dmStr + '</span></div>' +
        (obj2 ? '<div class="lc-dossier-row"><span class="lc-dossier-key">Primary Objection</span><span class="lc-dossier-val">' + obj2 + '</span></div>' : '') +
        '</div>';
}

window.createLeadCardV2 = function(docId, lead) {
    // Store lead in O(1) lookup cache — eliminates all encoding for copilot action
    _leadsMap.set(docId, lead);

    var rawUrl = (lead.url || lead.source_url || '').toLowerCase();
    var isPdf = rawUrl.split('?')[0].endsWith('.pdf');

    var card = document.createElement('div');
    card.className = 'lead-card-v2';
    card.id = docId;
    // ── Status-aware card styling (V23.7) ──
    if (lead.status === 'failed' && !isPdf) card.classList.add('lead-card--failed');
    if (lead.status === 'processing' || lead.status === 'queued') card.classList.add('lead-card--processing');

    // ── Source-aware card styling (V24.1.11) ──
    
    var hostname = '';
    try { hostname = rawUrl ? new URL(rawUrl).hostname.replace('www.','') : ''; } catch(e) {}
    var SOCIAL_DOMAINS = ['linkedin.com', 'twitter.com', 'x.com', 'reddit.com', 'facebook.com', 'instagram.com'];
    var isSocial = (lead.prism_mode || '').indexOf('WalledGarden') !== -1 || 
                   (lead.prism_mode || '').indexOf('walledgarden') !== -1 ||
                   SOCIAL_DOMAINS.some(function(d) { return hostname.indexOf(d) !== -1; });
                   
    var isRadar = lead.source === 'inbound_radar';
    var isAI = lead.origin_engine === 'autonomous' || 
               (lead.sourcing_vector || '').indexOf('Autonomous') !== -1 ||
               lead.origin_engine === 'research_agent';

    if (isPdf) {
        card.classList.add('lead-card--pdf');
    } else if (isSocial) {
        card.classList.add('lead-card--social');
    } else if (isRadar) {
        card.classList.add('lead-card--radar');
    } else if (isAI) {
        card.classList.add('lead-card--ai');
    }

    // ── V23.9: Skeleton card for queued/processing leads ─────────────────
    // Returns a clean shimmer placeholder instead of the full card body.
    if (lead.status === 'queued' || lead.status === 'processing') {
        var stLabel = lead.status === 'queued' ? 'Queued for processing…' : 'Processing in progress…';
        card.innerHTML =
            '<div style="padding:20px; display:flex; flex-direction:column; gap:12px;">' +
                '<div style="display:flex; align-items:center; gap:10px;">' +
                    '<div class="skeleton-block" style="width:140px; height:16px;"></div>' +
                    '<div class="skeleton-block" style="width:50px; height:14px; margin-left:auto;"></div>' +
                '</div>' +
                '<div class="skeleton-block" style="width:100%; height:10px;"></div>' +
                '<div class="skeleton-block" style="width:85%; height:10px;"></div>' +
                '<div class="skeleton-block" style="width:60%; height:10px;"></div>' +
                '<div style="display:flex; align-items:center; gap:6px; margin-top:4px; color:#b45309; font-size:0.8rem; font-weight:500;">' +
                    '<span>⏳</span> <span>' + stLabel + '</span>' +
                '</div>' +
            '</div>';
        return card;
    }

    // ── V24.1.15: Clean failed leads layout ───────────────────────────────
    if (lead.status === 'failed') {
        var displayName = _escapeHTML(lead.company_name || '');
        var hostname = '';
        try { var raw = lead.url || lead.source_url || ''; hostname = raw ? new URL(raw).hostname.replace('www.','') : ''; } catch(e) {}
        if (!displayName) displayName = _escapeHTML(hostname) || 'Unknown Company';

        var titlePrefix = '';
        var titleSuffix = ' &#8599;';
        if (isPdf) {
            titlePrefix = '📄 ';
            titleSuffix = '';
        } else if (isSocial) {
            titlePrefix = '💬 ';
            titleSuffix = '';
        } else if (isRadar) {
            titlePrefix = '📡 ';
            titleSuffix = '';
        } else if (isAI) {
            titlePrefix = '🔮 ';
            titleSuffix = '';
        }

        var timeAgo = fcTimeAgo(lead.createdAt || lead.promotedAt);
        var srcLbl  = '';
        if (isPdf) {
            srcLbl = '📄 PDF Document';
        } else if (lead.origin_engine === 'research_agent') {
            srcLbl = '🤖 Research Agent';
        } else if (isSocial) {
            srcLbl = '💬 Social Signal';
        } else if (isRadar) {
            srcLbl = '📡 Radar Signal';
        } else {
            srcLbl = (lead.sourcing_vector || lead.source || '').indexOf('Autonomous') !== -1
                ? 'AI Match' : (lead.source || 'Web Signal');
        }

        var scoreBadgeHTML = '';
        var bannerHTML = '';
        var btnHTML = '';

        if (isPdf) {
            scoreBadgeHTML = '<span class="lc-badge" style="background:#e0e7ff;color:#3730a3;border:1px solid #c7d2fe;font-weight:700;font-size:0.75rem;padding:4px 8px;border-radius:6px;">PDF Prospect</span>';
            bannerHTML = 
                '<div class="lead-error-badge" style="margin: 12px 0; background: #f0f9ff; border: 1px solid #bae6fd; border-radius: 8px; padding: 10px 14px; display: flex; align-items: center; gap: 8px; color: #0369a1; font-size: 0.82rem; font-weight: 500;">' +
                    '<span>ℹ️</span>' +
                    '<span>We found this prospect for you from a PDF document. Open the PDF to review.</span>' +
                '</div>';
            btnHTML = 
                '<button class="lc-contact-btn lc-copilot-btn" data-action="open-pdf" data-lead-id="' + docId + '">' +
                    '📂 Open PDF Document' +
                '</button>' +
                '<button class="lc-reject-btn-failed" data-action="reject" data-lead-id="' + docId + '" style="padding: 8px 16px; border: 1px solid #fca5a5; background: #fff; border-radius: 8px; font-size: 0.8rem; font-weight: 600; color: #dc2626; cursor: pointer; transition: all 0.2s; display: inline-flex; align-items: center; justify-content: center;">' +
                    '🚫 Skip Lead' +
                '</button>';
        } else {
            var rawErr = (lead.error || '').toLowerCase();
            var userMsg = 'Pipeline error. Requeue to try again.';

            if (rawErr.indexOf('402') !== -1)
                userMsg = 'Out of Credits — Top up to retry.';
            else if (rawErr.indexOf('timeout') !== -1 || rawErr.indexOf('playwright') !== -1)
                userMsg = 'Website blocked AI scraper. Requeue to try fallback.';
            else if (rawErr.indexOf('zombie') !== -1)
                userMsg = 'Processing timed out. Requeue to retry.';
            else if (rawErr.indexOf('rate') !== -1 || rawErr.indexOf('429') !== -1)
                userMsg = 'Rate limited by source. Requeue in a few minutes.';
            else if (rawErr.indexOf('requeued') !== -1 && lead.error)
                userMsg = lead.error;

            scoreBadgeHTML = '<span class="lc-badge" style="background:#fee2e2;color:#991b1b;border:1px solid #fca5a5;font-weight:700;font-size:0.75rem;padding:4px 8px;border-radius:6px;">Failed</span>';
            bannerHTML = 
                '<div class="lead-error-badge" style="margin: 12px 0; background: rgba(239, 68, 68, 0.06); border: 1px solid rgba(239, 68, 68, 0.18); border-radius: 8px; padding: 10px 14px; display: flex; align-items: center; gap: 8px; color: #b91c1c; font-size: 0.82rem; font-weight: 500;">' +
                    '<span class="error-icon">⚠️</span>' +
                    '<span>' + _escapeHTML(userMsg) + '</span>' +
                '</div>';
            btnHTML = 
                '<button class="lc-contact-btn lc-copilot-btn" data-action="requeue" data-lead-id="' + docId + '">' +
                    '🔄 Re-queue Lead' +
                '</button>' +
                '<button class="lc-reject-btn-failed" data-action="reject" data-lead-id="' + docId + '" style="padding: 8px 16px; border: 1px solid #fca5a5; background: #fff; border-radius: 8px; font-size: 0.8rem; font-weight: 600; color: #dc2626; cursor: pointer; transition: all 0.2s; display: inline-flex; align-items: center; justify-content: center;">' +
                    '🚫 Skip Lead' +
                '</button>';
        }

        card.innerHTML =
            '<div class="lc-header" style="margin-bottom: 12px;">' +
                '<div class="lc-left">' +
                    '<div class="lc-company-name"><a href="'+_safeHref(lead.url||lead.source_url||'#')+'" target="_blank" rel="noopener noreferrer">'+titlePrefix+displayName+titleSuffix+'</a></div>' +
                    '<div class="lc-meta"><span>'+srcLbl+'</span>'+(timeAgo?' &middot; '+timeAgo:'')+'</div>' +
                '</div>' +
                '<div class="lc-score-wrap" style="align-items: center;">' +
                    scoreBadgeHTML +
                '</div>' +
            '</div>' +
            bannerHTML +
            '<div class="lc-actions-primary" style="margin-top: 14px; gap: 8px; display: flex; align-items: center;">' +
                btnHTML +
            '</div>';

        return card;
    }

    var displayName = _escapeHTML(lead.company_name || '');
    var hostname = '';
    try { var raw = lead.url || lead.source_url || ''; hostname = raw ? new URL(raw).hostname.replace('www.','') : ''; } catch(e) {}
    if (!displayName) displayName = _escapeHTML(hostname) || 'Unknown Company';

    var titlePrefix = '';
    var titleSuffix = ' &#8599;';
    if (isPdf) {
        titlePrefix = '📄 ';
        titleSuffix = '';
    } else if (isSocial) {
        titlePrefix = '💬 ';
        titleSuffix = '';
    } else if (isRadar) {
        titlePrefix = '📡 ';
        titleSuffix = '';
    } else if (isAI) {
        titlePrefix = '🔮 ';
        titleSuffix = '';
    }

    var score   = lead.score || 0;
    var heatPct = Math.round((score / 10) * 100);
    var emoji   = getScoreEmoji(score);
    var signal  = _escapeHTML(lead.intent_signal || lead.pain_point || '');
    var dm      = _escapeHTML(lead.dm || '');
    var timeAgo = fcTimeAgo(lead.createdAt || lead.promotedAt);
    var srcLbl  = '';
    if (isPdf) {
        srcLbl = '📄 PDF Document';
    } else if (lead.origin_engine === 'research_agent') {
        srcLbl = '\u{1F916} Research Agent';
    } else if (isSocial) {
        srcLbl = '💬 Social Signal';
    } else if (isRadar) {
        srcLbl = '📡 Radar Signal';
    } else {
        srcLbl = (lead.sourcing_vector || lead.source || '').indexOf('Autonomous') !== -1
            ? 'AI Match' : (lead.source || 'Web Signal');
    }

    var pm = lead.prism_mode || '';
    var prismBadge = '';
    if (pm.indexOf('WalledGarden') !== -1 || pm.indexOf('walledgarden') !== -1) {
        prismBadge = '<span class="lc-badge" style="background:#f0f9ff;color:#0369a1;border-color:#bae6fd;">Social</span>';
    } else if (pm === 'B2B2C' || pm === 'b2b2c') {
        prismBadge = '<span class="lc-badge" style="background:#fff7ed;color:#c2410c;border-color:#fed7aa;">B2B2C</span>';
    } else if (pm.indexOf('General') !== -1 || pm.indexOf('legacy') !== -1) {
        prismBadge = '<span class="lc-badge" style="background:#f0fdf4;color:#166534;border-color:#bbf7d0;">B2B</span>';
    }

    var badges = [];
    if (lead.origin_engine === 'autonomous') badges.push({t:'Predictive',bg:'#faf5ff',c:'#7c3aed',b:'#ddd6fe'});
    badges.push({t:'Exclusive',bg:'#f3e8ff',c:'#6b21a8',b:'#e9d5ff'});
    if (lead.hiring_intent_found === 'Yes') badges.push({t:'Hiring',bg:'#ecfdf5',c:'#059669',b:'#a7f3d0'});
    if (lead.competitor_match) badges.push({t:lead.competitor_match,bg:'#fee2e2',c:'#b91c1c',b:'#fecaca'});
    
    if (lead.trend_mapped) badges.push({t:'Trend Mapped',bg:'#fff1f2',c:'#be123c',b:'#fecdd3'});
    else if (lead.matched_campaign_ids && lead.matched_campaign_ids.length > 1) {
        badges.push({t:'Cross-Pollinated',bg:'#eff6ff',c:'#1d4ed8',b:'#bfdbfe'});
    }
    var lqs = lead.lqs_score;
    if (lqs !== undefined && lqs !== null) {
        var lqsColor = lqs >= 0.7 ? '#22c55e' : lqs >= 0.4 ? '#f59e0b' : '#ef4444';
        var lqsBg    = lqs >= 0.7 ? '#f0fdf4' : lqs >= 0.4 ? '#fffbeb' : '#fef2f2';
        var lqsBorder = lqs >= 0.7 ? '#bbf7d0' : lqs >= 0.4 ? '#fde68a' : '#fecaca';
        badges.push({t:'LQS ' + (lqs * 100).toFixed(0) + '%', bg:lqsBg, c:lqsColor, b:lqsBorder});
    }
    var bHTML = badges.map(function(x) {
        return '<span class="lc-badge" style="background:'+x.bg+';color:'+x.c+';border-color:'+x.b+'">'+x.t+'</span>';
    }).join('');

    // ── V24.0: Evidence Dossier (collapsible confidence breakdown) ────────
    var evidenceChain = lead.evidence_chain || [];
    var scoreReasoning = _escapeHTML(lead.score_reasoning || '');
    var confidenceLevel = lead.confidence_level || 'SPECULATIVE';
    var confIcon = confidenceLevel === 'HIGH' ? '🟢' : confidenceLevel === 'MEDIUM' ? '🟡' : '🔴';
    var dossierHtml = '';
    if (evidenceChain.length > 0 || scoreReasoning) {
        var signalIcons = {
            'PAIN_EXPRESSION': '🔴', 'HIRING_INTENT': '💼', 'COMPETITOR_CHURN': '🔄',
            'TECH_STACK_MATCH': '⚙️', 'COMMUNITY_MENTION': '💬', 'FIRST_PARTY_VISIT': '🏠',
            'REVIEW_SIGNAL': '⭐', 'FUNDING_EVENT': '💰', 'GENERAL_FIT': '🎯'
        };
        var evidenceItems = evidenceChain.map(function(e) {
            var icon = signalIcons[e.signal_type] || '📌';
            var conf = Math.round((e.confidence || 0) * 100);
            return '<div class="evidence-entry">' +
                '<span class="evidence-icon">' + icon + '</span>' +
                '<span class="evidence-type">' + _escapeHTML((e.signal_type || '').replace(/_/g, ' ')) + '</span>' +
                '<span class="evidence-text">' + _escapeHTML(e.evidence || '') + '</span>' +
                '<span class="evidence-conf">' + conf + '%</span>' +
            '</div>';
        }).join('');
        dossierHtml = '<div class="evidence-dossier">' +
            '<div class="evidence-header" onclick="this.parentElement.classList.toggle(\'open\')">' +
                '<span class="evidence-toggle">▶</span>' +
                ' ' + confIcon + ' <strong>' + _escapeHTML(confidenceLevel) + '</strong> — ' + (scoreReasoning || 'Score breakdown') +
            '</div>' +
            '<div class="evidence-body">' + evidenceItems + '</div>' +
        '</div>';
    }
    // ── END Evidence Dossier ─────────────────────────────────────────────

    var expandId   = 'lc-expand-'  + docId;
    var moreId     = 'lc-more-'    + docId;
    var overflowId = 'lc-of-'      + docId;
    var copilotLbl = _copilotBtnLabel(lead);
    var isCont     = lead.status === 'contacted' || lead.status === 'replied';

    var cInfo = '';
    if (lead.email || lead.phone) {
        cInfo = '<div class="lc-section" style="font-size:0.85rem;">' +
            '<div class="lc-section-label">Contact Info</div>' +
            (lead.email ? '<a href="mailto:'+_escapeHTML(lead.email)+'" style="color:#2563eb;text-decoration:none;">'+_escapeHTML(lead.email)+'</a>&nbsp;' : '') +
            (lead.phone ? '<a href="tel:'+_escapeHTML(lead.phone)+'" style="color:#2563eb;text-decoration:none;">'+_escapeHTML(lead.phone)+'</a>' : '') +
            '</div>';
    }

    var crmCls = 'lc-crm-btn' + (lead.is_in_crm ? ' in-crm' : '');
    // XSS-FIX: crmOC removed. CRM button uses data-action delegation.


    card.innerHTML =
        '<div class="lc-header">' +
            '<div class="lc-left">' +
                '<div class="lc-company-name"><a href="'+_safeHref(lead.url||lead.source_url||'#')+'" target="_blank" rel="noopener noreferrer">'+titlePrefix+displayName+titleSuffix+'</a></div>' +
                '<div class="lc-meta"><span>'+srcLbl+'</span>'+(timeAgo?' &middot; '+timeAgo:'')+' </div>' +
            '</div>' +
            '<div class="lc-score-wrap">' +
                '<div class="lc-score-emoji">'+emoji+'</div>' +
                '<div class="lc-heat-bar"><div class="lc-heat-fill" style="width:'+heatPct+'%"></div></div>' +
                '<div class="lc-score-label">'+score+'/10</div>' +
            '</div>' +
        '</div>' +
        (signal ? '<div class="lc-signal">'+signal+'</div>' : '') +
        '<div class="lc-badges">'+prismBadge+bHTML+'</div>' +
        dossierHtml +
        // XSS-FIX: expand btn — data-action delegation
        '<button class="lc-expand-btn" data-action="expand" data-lead-id="'+docId+'">' +
            '<span id="lc-expand-icon-'+docId+'">&#x2193;</span> See opening message' +
        '</button>' +
        '<div class="lc-expanded" id="'+expandId+'">' +
            (dm ? '<div class="lc-section"><div class="lc-section-label">Your Opening Message</div><div class="lc-icebreaker">'+dm+'</div></div>' : '') +
            (lead.pain_point && lead.pain_point !== signal ? '<div class="lc-section"><div class="lc-section-label">Why This Lead</div><div class="lc-why">'+_escapeHTML(lead.pain_point)+'</div></div>' : '') +
            '<button class="expand-btn" data-action="toggle-dossier" data-lead-id="'+docId+'">' +
                '<span class="dossier-chevron">&#x25BC;</span> View Full Dossier &amp; Tags' +
            '</button>' +
            '<div class="dossier-container" id="dossier-'+docId+'">' +
                _prismDossierHTML(lead) +
                cInfo +
            '</div>' +
        '</div>' +
        '<div class="lc-actions-primary">' +
            '<button class="lc-contact-btn lc-copilot-btn'+(isCont?' lc-copilot-btn--contacted':'')+'"' +
                ' id="copilot-btn-'+docId+'"' +
                ' data-action="copilot" data-lead-id="'+docId+'"' +
                (isCont?' disabled':'')+'>' +
                (isCont ? '&#x2713; Contacted' : copilotLbl) +
            '</button>' +
            // XSS-FIX (P3): CRM btn — data-action only, no JS string injection.
            '<button class="'+crmCls+'" id="crm-btn-'+docId+'"' +
                ' data-action="crm" data-lead-id="'+docId+'"' +
                (lead.is_in_crm?' disabled':'') +
                ' title="Send to pipeline CRM">' +
                (lead.is_in_crm?'In CRM':'→ CRM') +
            '</button>' +
            '<div style="position:relative;">' +
                // XSS-FIX (P4): more — data-action delegation
                '<button class="lc-more-btn" id="'+moreId+'" data-action="more" data-lead-id="'+docId+'" title="More options">...</button>' +
                '<div class="lc-overflow-menu" id="'+overflowId+'">' +
                    // XSS-FIX: overflow items — data-action delegation
                    '<button class="lc-overflow-item" data-action="converted" data-lead-id="'+docId+'">Mark Converted</button>' +
                    '<button class="lc-overflow-item" data-action="timeline"  data-lead-id="'+docId+'">View Timeline</button>' +
                    '<button class="lc-overflow-item danger" data-action="reject" data-lead-id="'+docId+'">&#128683; Skip This Lead</button>' +
                '</div>' +
            '</div>' +
        '</div>';

    // ── Fault Recovery: Error Translation (V23.9) ────────────────────────
    if (lead.status === 'failed') {
        var rawErr = (lead.error || '').toLowerCase();
        var userMsg = isPdf ? 'Pipeline error.' : 'Pipeline error. Requeue to try again.';

        if (rawErr.indexOf('402') !== -1)
            userMsg = 'Out of Credits \u2014 Top up to retry.';
        else if (rawErr.indexOf('timeout') !== -1 || rawErr.indexOf('playwright') !== -1)
            userMsg = isPdf ? 'Website blocked download or scraper.' : 'Website blocked AI scraper. Requeue to try fallback.';
        else if (rawErr.indexOf('zombie') !== -1)
            userMsg = isPdf ? 'Processing timed out.' : 'Processing timed out. Requeue to retry.';
        else if (rawErr.indexOf('rate') !== -1 || rawErr.indexOf('429') !== -1)
            userMsg = isPdf ? 'Rate limited by source.' : 'Rate limited by source. Requeue in a few minutes.';
        else if (rawErr.indexOf('requeued') !== -1 && rawErr.indexOf('times') !== -1)
            userMsg = rawErr;  // Pass through max-requeue message from backend

        var errorBadge = document.createElement('div');
        errorBadge.className = 'lead-error-badge';
        errorBadge.innerHTML =
            '<span class="error-icon">\u26a0\ufe0f</span>' +
            '<span>' + _escapeHTML(userMsg) + '</span>' +
            (isPdf ? '' : '<button class="lead-requeue-btn" data-action="requeue" data-lead-id="' + docId + '">' +
            '\ud83d\udd04 Re-queue</button>');
        card.appendChild(errorBadge);
    }

    return card;
};


// =============================================================================
// XSS-FIX: DELEGATED CLICK LISTENER — replaces ALL inline onclick in lead cards
//
// ROOT CAUSE: createLeadCardV2 built onclick attributes via string concatenation:
//   onclick="pushToCRM('id','<JSON>')"  when lead.dm has a quote → SyntaxError
//   onclick="viewLeadTimeline('<URI>')" when interactions has emoji → breaks
//
// FIX: Buttons carry data-action + data-lead-id only.
// Listener reads the full lead from _leadsMap[docId] — zero encoding needed.
// =============================================================================
(function lcDelegatedListener() {
    var container = document.getElementById('leads-list');
    if (!container) {
        document.addEventListener('DOMContentLoaded', lcDelegatedListener);
        return;
    }
    container.addEventListener('click', function(e) {
        var btn    = e.target.closest('[data-action]');
        if (!btn) return;
        var action = btn.dataset.action;
        var docId  = btn.dataset.leadId;

        if (action === 'copilot') {
            if (!docId) return;
            window.copilotAction(docId);

        } else if (action === 'open-pdf') {
            if (!docId) return;
            var lead = _leadsMap.get(docId);
            var url = lead ? (lead.url || lead.source_url || '') : '';
            if (url && url !== '#') {
                window.open(url, '_blank', 'noopener,noreferrer');
            } else {
                showToast('PDF URL not available.', 'error');
            }

        } else if (action === 'crm') {
            if (!docId || btn.disabled) return;
            var lead = _leadsMap.get(docId);
            if (!lead) { showToast('Lead data unavailable. Please refresh.', 'error'); return; }
            // Open the CRM slide-out dossier panel immediately (visual confirmation)
            window.openCrmPanel(lead);
            // Also save to CRM pipeline in background (marks is_in_crm=true)
            window.pushToCRM(docId, encodeURIComponent(JSON.stringify(lead)));

        } else if (action === 'expand') {
            if (!docId) return;
            window.lcToggleExpand(docId);

        } else if (action === 'more') {
            if (!docId) return;
            window.lcToggleMore(docId);

        } else if (action === 'converted') {
            if (!docId) return;
            updateLeadStatus(docId, 'converted');
            lcCloseMore(docId);

        } else if (action === 'timeline') {
            if (!docId) return;
            var l2      = _leadsMap.get(docId);
            var evtJson = encodeURIComponent(JSON.stringify((l2 && l2.interactions) || []));
            window.viewLeadTimeline(evtJson);
            lcCloseMore(docId);

        } else if (action === 'reject') {
            if (!docId) return;
            window.openRejectionModal(docId);
            lcCloseMore(docId);

        } else if (action === 'toggle-dossier') {
            // V23.9: Progressive disclosure — toggle dossier container
            if (!docId) return;
            var dossier = document.getElementById('dossier-' + docId);
            if (!dossier) return;
            var isExpanded = dossier.classList.toggle('expanded');
            var chevron = btn.querySelector('.dossier-chevron');
            if (chevron) chevron.innerHTML = isExpanded ? '&#x25B2;' : '&#x25BC;';
            btn.childNodes[btn.childNodes.length - 1].textContent =
                isExpanded ? ' Hide Dossier' : ' View Full Dossier & Tags';
        } else if (action === 'requeue') {
            if (!docId || btn.disabled) return;
            window.requeueFailedLead(docId, btn);
        }
    });
})();


// Toggle expand/collapse
window.lcToggleExpand = function(docId) {
    const panel = document.getElementById(`lc-expand-${docId}`);
    const icon  = document.getElementById(`lc-expand-icon-${docId}`);
    if (!panel) return;
    const isOpen = panel.classList.contains('open');
    panel.classList.toggle('open', !isOpen);
    if (icon) icon.textContent = isOpen ? '\u2193' : '\u2191';
};

// Overflow menu toggle
window.lcToggleMore = function(docId) {
    const menu = document.getElementById(`lc-of-${docId}`);
    if (!menu) return;
    const isOpen = menu.classList.contains('open');
    // Close all others first
    document.querySelectorAll('.lc-overflow-menu.open').forEach(m => m.classList.remove('open'));
    if (!isOpen) {
        menu.classList.add('open');
        // Auto-close on outside click
        setTimeout(() => {
            const handler = (e) => {
                if (!menu.contains(e.target)) { menu.classList.remove('open'); document.removeEventListener('click', handler); }
            };
            document.addEventListener('click', handler);
        }, 0);
    }
};
window.lcCloseMore = function(docId) {
    document.getElementById('lc-of-' + docId)?.classList.remove('open');
};

// =============================================================================
// V18: COPILOT ACTION — Zero-Liability Execution Flow
// Simultaneously: (1) copies the AI-drafted DM to clipboard, (2) opens the
// lead URL in a new tab. Immediately flips the card to 'Contacted' (optimistic).
// Fires background PUT /api/leads/{id} to persist status + trigger RLHF loop.
//
// dm and url are passed as base64 to avoid all quote-escaping issues in the
// inline onclick attribute generated by createLeadCardV2.
// =============================================================================

// =============================================================================
// V18: COPILOT ACTION — Event Delegation Architecture (Unicode-Safe)
//
// ARCHITECTURAL UPGRADE from btoa/atob inline encoding:
// - Lead data is stored in _leadsMap by docId when the card is created.
// - The copilot button carries only data-lead-id="docId" (no encoding at all).
// - A single delegated listener on #leads-list calls copilotAction(docId).
// - This eliminates the btoa() DOMException that fires on emoji/UTF-8 DMs.
//
// copilotAction() is still window-exposed for backward-compat (e.g. devtools).
// =============================================================================

window.copilotAction = async function(docId) {
    // Retrieve the live lead object from the in-memory cache
    var lead = _leadsMap.get(docId);
    if (!lead) {
        console.warn('[Copilot] Lead not found in _leadsMap for id:', docId);
        showToast('Lead data unavailable. Please refresh.', 'error');
        return;
    }

    var dm  = lead.dm  || '';
    var url = lead.url || lead.source_url || '';

    // Step 1: Optimistic UI — immediately flip button to Contacted state
    var btn = document.getElementById('copilot-btn-' + docId);
    if (btn) {
        btn.innerHTML = '&#x2713; Contacted';
        btn.disabled  = true;
        btn.classList.add('lc-copilot-btn--contacted');
    }

    // Step 2: Clipboard write (full Unicode support; execCommand fallback for non-HTTPS)
    if (dm) {
        try {
            await navigator.clipboard.writeText(dm);
        } catch(e) {
            var ta = document.createElement('textarea');
            ta.value = dm;
            ta.style.position = 'fixed';
            ta.style.opacity  = '0';
            document.body.appendChild(ta);
            ta.select();
            try { document.execCommand('copy'); } catch(_) {}
            document.body.removeChild(ta);
        }
    }

    // Step 3: Open lead source in new tab
    if (url && url !== '#') {
        window.open(url, '_blank', 'noopener,noreferrer');
    }

    // Step 4: Contextual toast
    showToast(dm ? 'Message copied — paste it in the new tab to reply.' : 'Opening lead source…', 'success');

    // Step 5: Background status persist + RLHF trigger (fire-and-forget; non-blocking)
    updateLeadStatus(docId, 'contacted');
};

// =============================================================================
// V18: USER DROPDOWN TOGGLE
// =============================================================================

window.toggleUserDropdown = function() {
    const dropdown = document.getElementById('user-dropdown');
    const pill = document.getElementById('user-pill-btn');
    if (!dropdown) return;
    const isOpen = dropdown.style.display === 'flex' || dropdown.classList.contains('open');
    if (isOpen) {
        dropdown.style.display = 'none';
        dropdown.classList.remove('open');
        if (pill) pill.classList.remove('open');
    } else {
        dropdown.style.display = 'block'; // FIX T4: absolute dropdown must be block
        dropdown.classList.add('open');
        if (pill) pill.classList.add('open');
        // Auto-close on outside click
        setTimeout(() => {
            const handler = (e) => {
                const wrap = pill?.closest('.user-pill-wrap');
                if (!wrap || !wrap.contains(e.target)) {
                    dropdown.style.display = 'none';
                    dropdown.classList.remove('open');
                    if (pill) pill.classList.remove('open');
                    document.removeEventListener('click', handler);
                }
            };
            document.addEventListener('click', handler);
        }, 0);
    }
};

// =============================================================================
// V18: SETTINGS / INTEGRATIONS MODAL
// =============================================================================

window.openSettingsModal = function() {
    document.getElementById('user-dropdown')?.classList.remove('open');
    showModal('settings-modal');
};

window.closeSettingsModal = function() {
    closeModal('settings-modal');
};

// =============================================================================
// TERMS OF SERVICE: agreeToTerms()
//
// BUG A ROOT CAUSE: This function was called by the TOS modal button
// (onclick="agreeToTerms()") but was NEVER DEFINED anywhere in the codebase.
// Every click threw: ReferenceError: agreeToTerms is not defined
// This trapped ALL new users permanently in the TOS modal with no escape.
// =============================================================================
window.agreeToTerms = async function() {
    try {
        const user = firebase.auth().currentUser;
        if (!user) return;
        const token = await user.getIdToken();
        // Persist agreement timestamp to user doc via PUT /api/me
        await fetch(`${API_BASE}/api/me`, {
            method: 'PUT',
            headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
            body: JSON.stringify({ agreed_to_terms: true })
        });
    } catch (e) {
        console.warn('[TOS] Failed to persist agreement, continuing anyway:', e);
    } finally {
        // Always dismiss the modal regardless of API success —
        // network failure should not re-trap the user.
        document.getElementById('tos-modal').style.display = 'none';
        showToast('Terms accepted. Welcome to Sideio!', 'success');
    }
};

document.addEventListener('DOMContentLoaded', () => {
    // Setup custom multiselect region dropdowns (V24.1.15)
    setupCustomMultiselect('cc-geo-region', 'cc-geo-region-container', 'cc-geo-region-trigger', 'cc-geo-region-dropdown');
    setupCustomMultiselect('geo-region-select', 'geo-region-select-container', 'geo-region-select-trigger', 'geo-region-select-dropdown');

    const settingsOverlay = document.getElementById('settings-modal');
    if (settingsOverlay) {
        settingsOverlay.addEventListener('click', (e) => {
            if (e.target === settingsOverlay) closeSettingsModal();
        });
    }
    const dtOverlay = document.getElementById('dt-onboarding-modal');
    if (dtOverlay) {
        dtOverlay.addEventListener('click', (e) => {
            if (e.target === dtOverlay) closeDTModal();
        });
    }

    // ── V23.8: VIP Inbox — Feed Mode Toggle ──────────────────────────────────
    // Delegated listener on #feed-mode-toggle switches between outbound/inbound.
    const feedToggle = document.getElementById('feed-mode-toggle');
    if (feedToggle) {
        feedToggle.addEventListener('click', (e) => {
            const btn = e.target.closest('.toggle-btn');
            if (!btn || btn.classList.contains('active')) return;
            // Swap active state
            feedToggle.querySelectorAll('.toggle-btn').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            // Update global mode
            CURRENT_FEED_MODE = btn.dataset.feedMode || 'outbound';
            window.leadsCurrentPage = 0;
            // Clear radar pulse dot when switching to inbound
            if (CURRENT_FEED_MODE === 'inbound') {
                const dot = document.querySelector('.radar-pulse-dot');
                if (dot) dot.classList.add('d-none');
            }
            // Re-render feed with new mode (use _scheduleRender for consistency)
            _scheduleRender();
            console.log('[Radar] Feed mode switched to:', CURRENT_FEED_MODE, '| inboundCache:', inboundCache.length, '| outboundCache:', outboundCache.length);
        });
    }

    // ── V18 copilot/requeue listener REMOVED ──────────────────────────────────
    // Duplicate of V23.9 XSS-safe delegated listener at lcDelegatedListener()
    // (line ~3073). Keeping this caused copilotAction() and requeueFailedLead()
    // to fire TWICE per click.
});

// =============================================================================
// V18: DIGITAL TWIN ONBOARDING ENGINE
//
// SECURITY NOTE (per architectural review):
// Mock persona data is ONLY injected in localhost/dev environments.
// In production, a failed analyze-website call shows a graceful toast and
// falls back to the existing manual fc-step-2 flow.
// =============================================================================

const _dtIsLocal = window.location.hostname === 'localhost' || window.location.hostname === '127.0.0.1';

// Internal state store for extracted personas
window._dtState = {
    companyName: '',
    companyDesc: '',
    companyValue: '',
    targets: [
        { name: '', desc: '' },
        { name: '', desc: '' },
        { name: '', desc: '' }
    ],
    extractedBio: '',
    extractedWho: '',
    extractedGl: ''
};

// ─── MODAL: Digital Twin Onboarding ─────────────────────────────────────────
function dtSwitchView(viewId) {
    ['dt-view-a','dt-view-b','dt-view-c','dt-view-d'].forEach(id => {
        const el = document.getElementById(id);
        if (el) el.classList.toggle('active', el.id === viewId);
    });
}

window.openDTModal = function() {
    // Close the user dropdown if open
    document.getElementById('user-dropdown')?.classList.remove('open');
    // Reset to View A
    dtSwitchView('dt-view-a');
    const urlInput = document.getElementById('dt-url-input');
    if (urlInput) urlInput.value = '';
    // Show modal via CSS class toggle (no !important conflicts)
    showModal('dt-onboarding-modal');
    setTimeout(() => urlInput?.focus(), 150);
};

window.closeDTModal = function() {
    closeModal('dt-onboarding-modal');
};

// Re-analyze Website — triggered from user dropdown
// Pre-fills the saved website URL from the tenant profile, then opens the DT modal
window.dtReanalyzeWebsite = function() {
    toggleUserDropdown(); // close dropdown first
    const savedUrl = window.currentUserData?.website_url
                  || window.currentUserData?.website
                  || window.currentUserData?.url
                  || '';
    dtSwitchView('dt-view-a');
    const urlInput = document.getElementById('dt-url-input');
    if (urlInput && savedUrl) urlInput.value = savedUrl;
    showModal('dt-onboarding-modal');
    setTimeout(() => urlInput?.focus(), 150);
};

window.dtFallbackToNaturalLanguage = function() { dtSwitchView('dt-view-d'); };
window.dtBackToViewA = function() { dtSwitchView('dt-view-a'); };

// View A → View B: validate URL and start analysis
window.dtStartAnalysis = async function() {
    const urlInput = document.getElementById('dt-url-input');
    const rawUrl = (urlInput?.value || '').trim();

    if (!rawUrl || rawUrl.length < 5) {
        if (urlInput) { urlInput.style.borderColor = '#ef4444'; setTimeout(() => { urlInput.style.borderColor = ''; }, 2000); }
        showToast('Please enter a valid website URL.', 'error');
        return;
    }

    // Ensure protocol
    const url = /^https?:\/\//i.test(rawUrl) ? rawUrl : `https://${rawUrl}`;

    // Transition to View B
    dtSwitchView('dt-view-b');

    // Animate progress text
    const statusEl = document.getElementById('dt-status-text');
    const progressEl = document.getElementById('dt-progress-fill');
    const steps = [
        { text: 'Reading site...', pct: '15%', delay: 0 },
        { text: 'Extracting brand signals...', pct: '35%', delay: 1200 },
        { text: 'Building Company Persona...', pct: '55%', delay: 2400 },
        { text: 'Identifying Decision Makers...', pct: '75%', delay: 3600 },
        { text: 'Finalising target profiles...', pct: '90%', delay: 4800 }
    ];

    steps.forEach(({ text, pct, delay }) => {
        setTimeout(() => {
            if (statusEl) { statusEl.style.opacity = '0'; setTimeout(() => { statusEl.textContent = text; statusEl.style.opacity = '1'; }, 150); }
            if (progressEl) progressEl.style.width = pct;
        }, delay);
    });

    // API call with a 6s minimum animation window
    const animDone = new Promise(resolve => setTimeout(resolve, 5800));

    let personaData = null;
    let apiError    = null;
    let wafBlocked  = false;

    try {
        const user = firebase.auth().currentUser;
        if (!user) throw new Error('Not authenticated');
        const token = await user.getIdToken();

        const apiCall = fetch('/api/analyze-website', {
            method: 'POST',
            headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' },
            body: JSON.stringify({ url })
        });

        const [, resp] = await Promise.all([animDone, apiCall]);
        if (resp.ok) {
            const payload = await resp.json();
            personaData = payload.data || null;
        } else if (resp.status === 422) {
            // Inspect the JSON body to distinguish WAF_BLOCKED from generic 422
            try {
                const errPayload = await resp.json();
                if (errPayload.code === 'WAF_BLOCKED') {
                    wafBlocked = true;
                } else {
                    apiError = 'not_ready';
                }
            } catch (_) {
                apiError = 'not_ready';
            }
        } else if (resp.status === 404 || resp.status === 501) {
            apiError = 'not_ready';
        } else {
            apiError = 'api_error';
        }
    } catch (e) {
        console.warn('[DT] analyze-website call failed:', e);
        apiError = 'network';
        await animDone;
    }

    // ── Handle result ──────────────────────────────────────────────────────────
    if (personaData) {
        dtPopulatePersonas(personaData, url);

    } else if (wafBlocked) {
        // WAF_BLOCKED — polite notification + seamless auto-transition to manual entry
        // Stop the progress bar immediately and show the explanation inside view-b
        const progressEl = document.getElementById('dt-progress-fill');
        const statusEl   = document.getElementById('dt-status-text');
        if (progressEl) progressEl.style.background = 'linear-gradient(90deg, #f59e0b, #ef4444)';
        if (statusEl)   statusEl.textContent = 'Security firewall detected ⚠️';

        // Inject polite inline notice
        const viewB = document.getElementById('dt-view-b');
        if (viewB && !viewB.querySelector('.dt-waf-notice')) {
            const notice = document.createElement('div');
            notice.className = 'dt-waf-notice';
            notice.style.cssText = [
                'margin:18px auto 0', 'max-width:380px', 'padding:14px 18px',
                'background:rgba(245,158,11,0.12)', 'border:1px solid rgba(245,158,11,0.4)',
                'border-radius:10px', 'color:#fbbf24', 'font-size:13.5px',
                'line-height:1.55', 'text-align:left'
            ].join(';');
            notice.innerHTML =
                '<strong style="display:block;margin-bottom:4px">🛡️ Website security blocked our AI reader</strong>' +
                'The target site\'s firewall (e.g. Cloudflare) blocked our automated scan. ' +
                'No problem — switching to <strong>Manual Entry</strong> so you can paste your offering directly.';
            viewB.appendChild(notice);
        }

        // Auto-transition to Manual Entry (dt-view-d) after 2.5 s
        setTimeout(() => { dtSwitchView('dt-view-d'); }, 2500);

    } else if (apiError === 'not_ready' && _dtIsLocal) {
        // DEV-ONLY mock — never runs in production
        console.warn('[DT] Using mock persona data — localhost only');
        dtPopulatePersonas(dtMockPersona(url), url);

    } else {
        // All other production failures — reset to view-A with toast
        dtSwitchView('dt-view-a');
        showToast('Digital Twin engine is currently provisioning. Please use manual entry.', 'error');
    }
};

// DEV-ONLY mock persona generator (localhost guard enforced above)
function dtMockPersona(url) {
    const domain = (() => { try { return new URL(url).hostname.replace('www.', ''); } catch(e) { return url; } })();
    return {
        company:  { name: domain, description: `${domain} is a growing business looking to expand its client base.`, value: 'B2B Services' },
        targets: [
            { name: 'Small Business Owners', description: 'Businesses with 1-10 employees actively seeking growth tools.' },
            { name: 'Marketing Managers', description: 'Mid-market companies with active social media budgets.' },
            { name: 'Agency Decision Makers', description: 'Agencies managing multiple client accounts and seeking automation.' }
        ]
    };
}

// Populate View C with API or mock data
function dtPopulatePersonas(data, url) {
    const company = data.company || {};
    const targets = data.targets || [];

    // Store in state
    window._dtState.companyName = company.name || '';
    window._dtState.companyDesc = company.description || '';
    window._dtState.companyValue = company.value || '';
    window._dtState.targets = [
        targets[0] || { name: '', desc: '' },
        targets[1] || { name: '', desc: '' },
        targets[2] || { name: '', desc: '' }
    ];

    // Store recommendations
    window._dtState.recommendedCampaigns = data.recommended_campaigns || [];
    
    // Derive campaign payload
    window._dtState.extractedBio    = company.description || '';
    window._dtState.extractedWho    = (targets[0]?.name || '') + (targets.length > 1 ? `, ${targets[1]?.name || ''}` : '');
    window._dtState.extractedGl     = data.detected_gl || '';

    // Populate DOM
    const setText = (id, val) => { const el = document.getElementById(id); if (el) el.textContent = val || '—'; };
    setText('dt-company-name', company.name);
    setText('dt-company-desc', company.description);
    setText('dt-company-value', company.value);
    for (let i = 0; i < 3; i++) {
        const t = targets[i] || {};
        setText(`dt-target-${i+1}-name`, t.name);
        setText(`dt-target-${i+1}-desc`, t.description || t.desc);
    }

    // Store for handoff
    document.getElementById('dt-extracted-bio').value  = window._dtState.extractedBio;
    document.getElementById('dt-extracted-who').value  = window._dtState.extractedWho;
    document.getElementById('dt-extracted-gl').value   = window._dtState.extractedGl;

    // Final progress
    const statusEl = document.getElementById('dt-status-text');
    const progressEl = document.getElementById('dt-progress-fill');
    if (statusEl) statusEl.textContent = 'Analysis complete \u2713';
    if (progressEl) progressEl.style.width = '100%';

    // Transition to View C
    setTimeout(() => {
        dtSwitchView('dt-view-c');
    }, 500);
}

// View C "Launch Campaign" — hands off to direct API call, completely bypassing legacy DOM
window.dtPrefillAndLaunch = function() {
    const bio = document.getElementById('dt-extracted-bio')?.value || window._dtState.extractedBio;
    const who = document.getElementById('dt-extracted-who')?.value || window._dtState.extractedWho;
    const gl  = document.getElementById('dt-extracted-gl')?.value  || window._dtState.extractedGl;

    if (!bio || !who) {
        showToast('Please fill in company and target descriptions before launching.', 'error');
        return;
    }

    const now = new Date();
    const month = now.toLocaleString('en', { month: 'short' });
    const campName = `${who.substring(0, 35)} \u00B7 ${month} ${now.getFullYear()}`;
    const keys = who.substring(0, 120);

    closeDTModal();
    window.saveTenantProfileAction({
        name: campName,
        bio: bio,
        keywords: keys,
        gl: gl,
        recommended_campaigns: window._dtState.recommendedCampaigns || []
    });
};

window.saveTenantProfileAction = async function(payload) {
    // \u2500\u2500 STRICT LOADING STATE (abolish optimistic UI) \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
    // The UI must NOT show "Digital Twin created" until we receive a verified
    // 200/201 from the backend. Disable all onboarding CTAs and show a spinner.
    const _twBtns = document.querySelectorAll(
        '.dt-launch-btn, #dt-launch-btn, [onclick*="dtPrefillAndLaunch"], [onclick*="dtLaunchFallback"]'
    );
    _twBtns.forEach(b => { b.disabled = true; b._orig = b.innerHTML; b.innerHTML = '\u23f3 Creating Twin...'; });
    const _restoreTwin = () => _twBtns.forEach(b => { b.disabled = false; b.innerHTML = b._orig || 'Launch'; });

    showToast('Setting up Master Twin Profile...', 'info');
    try {
        const user = firebase.auth().currentUser;
        if (!user) {
            _restoreTwin();
            showToast('Session expired \u2014 please sign in again.', 'error');
            return;
        }

        // force=true: mandatory on iOS Safari \u2014 background tab throttling
        // silently expires Firebase tokens without triggering onIdTokenChanged.
        // Without force=true, the Orchestrator returns 401 and the Firestore
        // write is silently dropped with no error shown to the user.
        const token = await user.getIdToken(true);

        const createResp = await fetch(`${API_BASE}/api/tenant_profiles`, {
            method:  'POST',
            headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
            body:    JSON.stringify(payload)
        });

        // STRICT CHECK: only proceed on verified 200/201. Any other status
        // (including 204, 400, 403, 500) is treated as a failure. Never
        // show success on assumption.
        if (!createResp.ok) {
            let errDetail = '';
            try { const errBody = await createResp.json(); errDetail = errBody.message || errBody.error || ''; } catch(_) {}
            throw new Error(`HTTP ${createResp.status}${errDetail ? ': ' + errDetail : ''}`);
        }

        // \u2500\u2500 Verified success \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
        _restoreTwin();
        loadDashboard();
        showToast('\u2705 Master Twin active! You can now add child campaigns.', 'success');

    } catch(err) {
        console.error('[saveTenantProfileAction]', err);
        _restoreTwin();
        // Surface a visible, actionable error \u2014 never silently fail.
        showToast(`Failed to create Digital Twin: ${err.message || 'Network error'}. Please try again.`, 'error');
    }
};

// Natural Language Fallback Launch
window.dtLaunchFallback = async function() {
    const fallbackInp = document.getElementById('dt-intent-fallback');
    const txt = fallbackInp?.value.trim();
    if (!txt || txt.length < 5) {
        if (fallbackInp) { fallbackInp.style.borderColor = '#ef4444'; setTimeout(() => fallbackInp.style.borderColor='', 2000); }
        showToast('Please type a few words about your ideal clients.', 'warn');
        return;
    }

    const now = new Date();
    const month = now.toLocaleString('en', { month: 'short' });
    const campName = `${txt.substring(0, 35)} \u00B7 ${month} ${now.getFullYear()}`;

    closeDTModal();

    // ── Step 0: Disable CTAs + loading state ────────────────────────────────
    const _twBtns = document.querySelectorAll(
        '.dt-launch-btn, #dt-launch-btn, [onclick*="dtPrefillAndLaunch"], [onclick*="dtLaunchFallback"]'
    );
    _twBtns.forEach(b => { b.disabled = true; b._orig = b.innerHTML; b.innerHTML = '\u23f3 Creating Twin...'; });
    const _restoreTwin = () => _twBtns.forEach(b => { b.disabled = false; b.innerHTML = b._orig || 'Launch'; });

    try {
        const user = firebase.auth().currentUser;
        if (!user) { _restoreTwin(); showToast('Session expired.', 'error'); return; }
        const token = await user.getIdToken(true);

        // ── Step 1: Save tenant profile (same as website-analysis path) ─────
        // Without this, first-time users have no tenant_profiles doc and
        // persona migration on next login finds "no_profile" and aborts.
        showToast('Setting up Master Twin Profile...', 'info');
        const profileResp = await fetch(`${API_BASE}/api/tenant_profiles`, {
            method:  'POST',
            headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
            body:    JSON.stringify({
                company_name:       txt.substring(0, 100),
                onboarding_complete: true
            })
        });
        if (!profileResp.ok) {
            console.warn('[dtLaunchFallback] Tenant profile save failed (non-fatal):', profileResp.status);
        }

        // ── Step 2: Auto-create persona from free-text (mirrors deployPredictiveCard) ─
        // Without this, the Persona Vault is empty and the user cannot create
        // child campaigns or link any persona to this campaign.
        const personaBio = [
            `[Who we help]: ${txt}`,
            `[The problem we solve]: ${txt}`,
            `[Our unfair advantage / Unique Value]: Our unique positioning in this market`
        ].join('\n');
        const personaName = `${txt.substring(0, 40)} Strategy`;

        let savedPersonaId = '';
        let savedPersonaBio = '';
        let savedPersonaKeywords = '';

        showToast('Creating AI Agent...', 'info');
        const pResp = await fetch(`${API_BASE}/api/personas`, {
            method:  'POST',
            headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
            body:    JSON.stringify({ name: personaName, bio: personaBio, keywords: txt.substring(0, 120) })
        });
        if (pResp.ok) {
            const pJson = await pResp.json();
            savedPersonaId = pJson.id || '';
            savedPersonaBio = personaBio;
            savedPersonaKeywords = txt.substring(0, 120);
            window._selectedPersonaId = savedPersonaId;
            window._personasCache = []; // invalidate cache so Persona Vault reloads
            console.log(`[dtLaunchFallback] Auto-created persona '${personaName}' → ${pJson.id}`);
        } else {
            console.warn('[dtLaunchFallback] Persona auto-save failed:', await pResp.text());
        }

        _restoreTwin();

        // ── Step 3: Create campaign with persona linked ─────────────────────
        // J-9 FIX: 600ms delay so Firestore write for the new persona settles
        // before campaign creation reads it for denormalisation.
        await new Promise(resolve => setTimeout(resolve, 600));

        saveCampaignAction({
            name:              campName,
            bio:               'CHILD_CAMPAIGN_OVERRIDE',
            keywords:          txt.substring(0, 120),
            campaign_focus:    txt.substring(0, 250),
            gl:                '',
            persona_id:        savedPersonaId,
            persona_bio:       savedPersonaBio,
            persona_keywords:  savedPersonaKeywords
        });

    } catch(err) {
        console.error('[dtLaunchFallback]', err);
        _restoreTwin();
        showToast(`Setup failed: ${err.message || 'Network error'}. Please try again.`, 'error');
    }
};

// Transition from View A to View D
window.dtFallbackToNaturalLanguage = function() {
    dtSwitchView('dt-view-d');
    setTimeout(() => document.getElementById('dt-intent-fallback')?.focus(), 100);
};


// =============================================================================
// V23 MULTI-CAMPAIGN: CHILD CAMPAIGN CREATION (STATE B)
// =============================================================================

// J-8 FIX: _pendingCards stores AI card data by index for deployPredictiveCard.
// Replaces the btoa() encoding scheme which crashes on emoji/non-ASCII characters
// that Gemini commonly generates in product names and market trend hooks.
window._pendingCards = {};

window.openChildCampaignModal = async function() {
    const aw = window.activeWallet || {};
    const ud = window.currentUserData || {};
    const allocated = Number(aw.allocated_credits || ud.allocated_credits || 0) || 0;
    const consumed  = Number(aw.consumed_credits  || ud.consumed_credits  || 0) || 0;
    const remaining = allocated - consumed;
    
    if (remaining <= 0) {
        showToast('Credits exhausted. Contact admin to reload.', 'error');
        return;
    }

    // Reset card store on each open
    window._pendingCards = {};

    const modal = document.getElementById('child-campaign-modal');
    if (modal) {
        showModal('child-campaign-modal');
        // V24.1.4: Initialize geo cascade for launch form (cc-geo-* elements)
        initGeoCascadeFor('cc-geo', null, '', '');
        const fallbackCont = document.getElementById('cc-custom-fallback-container');
        if (fallbackCont) fallbackCont.style.display = 'none'; // FIX T2: no .hidden CSS rule
        const cardsEl = document.getElementById('cc-recommendation-cards');
        if(cardsEl) {
            cardsEl.innerHTML = '<p style="text-align:center; color:#6b7280;">Loading market intelligence...</p>';
            cardsEl.style.display = 'block';
        }

        const rawProfile = await fetchTenantProfile();
        let html = '';
        if (rawProfile && rawProfile.recommended_campaigns && rawProfile.recommended_campaigns.length > 0) {
            rawProfile.recommended_campaigns.forEach((camp, idx) => {
                // J-8 FIX: Store data by index — no encoding at all.
                window._pendingCards[idx] = {
                    prod: camp.product_name      || '',
                    hook: camp.market_trend_hook || '',
                    adv:  camp.unfair_advantage  || ''
                };
                
                html += `
                <div id="c-card-${idx}" style="background: rgba(255,255,255,0.6); padding: 16px; border-radius: 12px; margin-bottom: 16px; border: 1px solid var(--glass-border); text-align: left;">
                    <div id="c-card-view-${idx}">
                        <h4 style="margin:0 0 6px 0; color:var(--primary); font-size:1.1rem;">${camp.product_name || 'Product'}</h4>
                        <p style="font-size:0.9rem; margin-bottom:12px; line-height: 1.4;"><strong style="color:#4f46e5;">Market Trend:</strong> ${camp.market_trend_hook || ''}<br><strong style="color:#4f46e5;">Advantage:</strong> ${camp.unfair_advantage || ''}</p>
                        <button class="primary-btn" style="width:100%; font-size:0.9rem; padding:8px;" onclick="window.editPredictiveCard(${idx})">Review &amp; Launch</button>
                    </div>
                    <div id="c-card-edit-${idx}" class="hidden">
                        <label style="font-size:0.8rem; color:var(--text-muted); display: block;">Product Focus</label>
                        <input type="text" id="c-prod-${idx}" class="fc-intent-input" style="height:36px; padding:8px; margin-bottom:8px; width: 100%; border: 1px solid #d1d5db; border-radius: 8px;" value="${(camp.product_name || '').replace(/"/g, '&quot;')}">
                        
                        <label style="font-size:0.8rem; color:var(--text-muted); display: block;">Market Opportunity</label>
                        <textarea id="c-hook-${idx}" class="fc-intent-input" style="min-height:60px; padding:8px; margin-bottom:8px; width: 100%; border: 1px solid #d1d5db; border-radius: 8px;">${(camp.market_trend_hook || '')}</textarea>
                        
                        <label style="font-size:0.8rem; color:var(--text-muted); display: block;">Unfair Advantage</label>
                        <textarea id="c-adv-${idx}" class="fc-intent-input" style="min-height:60px; padding:8px; margin-bottom:12px; width: 100%; border: 1px solid #d1d5db; border-radius: 8px;">${(camp.unfair_advantage || '')}</textarea>

                        <label style="font-size:0.8rem; color:var(--text-muted); display: block;">Target Location</label>
                        <input type="text" id="c-loc-${idx}" class="fc-intent-input" style="height:36px; padding:8px; margin-bottom:12px; width: 100%; border: 1px solid #d1d5db; border-radius: 8px;" placeholder="e.g. London, UK, Worldwide" value="${window._dtState?.extractedGl || ''}">
                        
                        <button class="primary-btn" style="width:100%; font-size:0.9rem; padding:8px; background:#10b981; border:none; border-radius: 20px; color:white; font-weight: 600; cursor: pointer;" onclick="window.deployPredictiveCard(${idx})">Deploy Campaign</button>
                    </div>
                </div>
                `;
            });
        } else {
            // == FIX T1: Manual Profile Fallback ==
            // When no recommended_campaigns (user has no website yet), check if
            // an admin manually pushed a tenant_profile with company_bio or KB.
            // Surface a single editable "Manual Twin" card so the user can
            // immediately launch a campaign without scanning a website URL.
            const manualBio = (rawProfile && (
                rawProfile.company_bio        ||
                rawProfile.company_description ||
                rawProfile.bio                ||
                (rawProfile.knowledge_base_text && rawProfile.knowledge_base_text[0])
            )) || '';

            if (manualBio) {
                const productHint  = (rawProfile && (rawProfile.company_name || rawProfile.name)) || 'Your Core Service';
                const locationHint = (rawProfile && rawProfile.detected_gl) || '';
                // J-8 FIX: Store in _pendingCards instead of btoa()
                window._pendingCards[0] = { prod: productHint, hook: manualBio.slice(0, 120), adv: '' };
                html = `<div style="background:linear-gradient(135deg,rgba(79,70,229,0.05),rgba(124,58,237,0.05));border:1px dashed rgba(79,70,229,0.3);border-radius:12px;padding:14px;margin-bottom:16px;text-align:left;"><div style="font-size:0.72rem;font-weight:700;text-transform:uppercase;letter-spacing:0.06em;color:var(--primary);margin-bottom:6px;">&#10022; Profile Detected (Manual Twin)</div><p style="font-size:0.88rem;color:var(--text-muted);margin-bottom:14px;line-height:1.5;">${manualBio.slice(0,180)}${manualBio.length>180?'\u2026':''}</p><button class="primary-btn" style="width:100%;font-size:0.9rem;padding:8px;" onclick="window.editPredictiveCard(0)">Customise &amp; Launch &#8594;</button></div><div id="c-card-0"><div id="c-card-view-0" class="hidden"></div><div id="c-card-edit-0"><label style="font-size:0.8rem;color:var(--text-muted);display:block;">Product / Service Focus</label><input type="text" id="c-prod-0" class="fc-intent-input" style="height:36px;padding:8px;margin-bottom:8px;width:100%;border:1px solid #d1d5db;border-radius:8px;" value="${productHint.replace(/"/g,'&quot;')}"><label style="font-size:0.8rem;color:var(--text-muted);display:block;">Market Opportunity / Pain Point</label><textarea id="c-hook-0" class="fc-intent-input" style="min-height:60px;padding:8px;margin-bottom:8px;width:100%;border:1px solid #d1d5db;border-radius:8px;">${manualBio.slice(0,200)}</textarea><label style="font-size:0.8rem;color:var(--text-muted);display:block;">Unfair Advantage</label><textarea id="c-adv-0" class="fc-intent-input" style="min-height:60px;padding:8px;margin-bottom:12px;width:100%;border:1px solid #d1d5db;border-radius:8px;"></textarea><label style="font-size:0.8rem;color:var(--text-muted);display:block;">Target Location</label><input type="text" id="c-loc-0" class="fc-intent-input" style="height:36px;padding:8px;margin-bottom:12px;width:100%;border:1px solid #d1d5db;border-radius:8px;" placeholder="e.g. Kerala, India, Worldwide" value="${locationHint}"><button class="primary-btn" style="width:100%;font-size:0.9rem;padding:8px;background:#10b981;border:none;border-radius:20px;color:white;font-weight:600;cursor:pointer;" onclick="window.deployPredictiveCard(0)">Deploy Campaign</button></div></div>`;
            } else {
                html = '<p style="text-align:center; color:#6b7280;">No predictive campaigns available. Use the custom fallback.</p>';
            }
        }
        if(cardsEl) cardsEl.innerHTML = html;
        if(document.getElementById('cc-name')) document.getElementById('cc-name').value = '';
    }
};

window.editPredictiveCard = function(idx) {
    document.getElementById('c-card-view-' + idx).classList.add('hidden');
    document.getElementById('c-card-edit-' + idx).classList.remove('hidden');
};


window.deployPredictiveCard = async function(idx) {
    // J-8 FIX: Read data from _pendingCards map (no btoa/origProd/origHook/origAdv args).
    const prod = (document.getElementById('c-prod-' + idx)?.value || '').trim();
    const hook = (document.getElementById('c-hook-' + idx)?.value || '').trim();
    const adv  = (document.getElementById('c-adv-' + idx)?.value || '').trim();
    const loc  = (document.getElementById('c-loc-' + idx)?.value || '').trim();

    const origCard = window._pendingCards[idx] || {};

    if (!loc) {
        showToast('Target Location is required.', 'error');
        return;
    }

    // ── Step 1: Auto-save the AI opportunity as a Persona ──────────────────
    // Compose a structured directive bio from the AI card fields
    const personaBio = [
        `[Who we help]: Businesses struggling with: ${hook || prod}`,
        `[The problem we solve]: ${hook || prod}`,
        `[Our unfair advantage / Unique Value]: ${adv || 'Our unique positioning in this market'}`
    ].join('\n');
    const personaName = `${prod} Strategy`;

    const deployBtn = document.querySelector(`#c-card-edit-${idx} button.primary-btn`);
    if (deployBtn) { deployBtn.disabled = true; deployBtn.textContent = '⚙️ Saving Agent...'; }

    let savedPersonaId = '';
    let savedPersonaBio = '';
    let savedPersonaKeywords = '';

    try {
        const user  = firebase.auth().currentUser;
        if (!user) { showToast('Session expired.', 'error'); return; }
        const token = await user.getIdToken(true);

        const pResp = await fetch(`${API_BASE}/api/personas`, {
            method:  'POST',
            headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
            body:    JSON.stringify({ name: personaName, bio: personaBio, keywords: prod })
        });
        if (pResp.ok) {
            const pJson = await pResp.json();
            savedPersonaId = pJson.id || '';
            savedPersonaBio = personaBio;
            savedPersonaKeywords = prod;
            window._selectedPersonaId = savedPersonaId;
            window._personasCache = []; // invalidate cache
            console.log(`[DEPLOY] Auto-created persona '${personaName}' → ${pJson.id}`);
        } else {
            console.warn('[DEPLOY] Persona auto-save failed:', await pResp.text());
        }
    } catch(pErr) {
        console.warn('[DEPLOY] Persona auto-save error (non-fatal):', pErr);
    }

    if (deployBtn) { deployBtn.textContent = '🚀 Launching...'; }

    // ── Step 2: Create the campaign with the new persona_id linked ──────────
    // J-8 FIX: Compare against _pendingCards (no btoa needed)
    const wasEdited = prod !== origCard.prod || hook !== origCard.hook || adv !== origCard.adv;

    closeModal('child-campaign-modal');

    // J-9 FIX: 600ms delay so Firestore write for the new persona settles
    // before campaign creation reads it for denormalisation.
    await new Promise(resolve => setTimeout(resolve, 600));

    saveCampaignAction({
        name:              prod,
        bio:               'CHILD_CAMPAIGN_OVERRIDE',
        keywords:          prod,
        campaign_focus:    prod,
        pain_point:        hook,
        unfair_advantage:  adv,
        gl:                '',
        location:          loc,
        human_edited:      wasEdited,
        target_angle_hook: hook,
        target_angle_adv:  adv,
        // Pass inline persona fields so denormalisation is guaranteed even if
        // Firestore eventual consistency causes the persona doc read to miss.
        persona_id:        savedPersonaId,
        persona_bio:       savedPersonaBio,
        persona_keywords:  savedPersonaKeywords
    });
};

// ── Campaign Builder: Persona dropdown state binding ─────────────────────────
// Called on <select> change. Drives ALL conditional rendering in the form.
window.onCcPersonaChange = function(personaId) {
    const preview    = document.getElementById('cc-persona-preview');
    const bioPreview = document.getElementById('cc-persona-bio-preview');
    const legacy     = document.getElementById('cc-legacy-fields');

    if (personaId) {
        // Lookup from cache — guaranteed populated before this fires
        const persona      = (window._personasCache || []).find(p => p.id === personaId);
        const bio          = (persona && persona.bio)      || '';
        const safeKeywords = (persona && persona.keywords) || '';
        const bioPreview150 = bio.length > 150 ? bio.slice(0, 150) + '\u2026' : (bio || '(No core directive set for this persona)');

        console.log('[CC] Persona selected:', personaId, '| bio:', bio.slice(0, 60), '| keywords:', safeKeywords);

        // ── Show directive preview card ───────────────────────────────────
        if (preview)    preview.style.display = 'block';
        if (bioPreview) bioPreview.textContent = bioPreview150;

        // ── Hide legacy ICP fields + their warning banner ─────────────────
        if (legacy) legacy.style.display = 'none';

        // Store for saveChildCampaign keyword merge
        window._ccActivePersonaKeywords = safeKeywords;

        // Auto-focus campaign name field
        setTimeout(() => document.getElementById('cc-name')?.focus(), 80);

    } else {
        // ── No persona selected — show legacy fields + warning ────────────
        console.log('[CC] Persona cleared — showing legacy fields');
        if (preview) preview.style.display = 'none';
        if (legacy)  legacy.style.display  = 'block';
        window._ccActivePersonaKeywords = '';
    }
};

window.showCcCustomFallback = function() {
    const r = document.getElementById('cc-recommendation-cards');
    if (r) r.style.display = 'none';
    const f = document.getElementById('cc-custom-fallback-container');
    if (f) f.style.display = 'block';

    // Pre-fill geography from DT state via cascade
    const extractedGl = window._dtState?.extractedGl || '';
    if (extractedGl) {
        initGeoCascadeFor('cc-geo', null, extractedGl, '');
    }

    // Reset form to clean state (persona-unselected)
    const sel = document.getElementById('cc-persona-select');
    if (sel) sel.value = '';
    onCcPersonaChange('');  // trigger show/hide

    // Populate dropdown with live personas
    populatePersonaDropdown('cc-persona-select');
};

window.saveChildCampaign = async function() {
    const personaSel   = document.getElementById('cc-persona-select');
    const nameEl       = document.getElementById('cc-name');
    const locEl        = document.getElementById('cc-location');   // hidden, synced by cascade
    const glEl         = document.getElementById('cc-gl');         // hidden, synced by cascade
    const extraKeysEl  = document.getElementById('cc-extra-keywords');
    // Legacy fields (visible only when no persona)
    const focusEl      = document.getElementById('cc-focus');
    const painEl       = document.getElementById('cc-pain');
    const advEl        = document.getElementById('cc-advantage');

    const selPid     = personaSel?.value    || '';
    const campName   = (nameEl?.value       || '').trim();
    const loc        = (locEl?.value        || '').trim();
    const gl         = (glEl?.value         || '').trim();
    const extraKeys  = (extraKeysEl?.value  || '').trim();

    // Build structured geo_hierarchy from cascade selects
    const ccContinent = document.getElementById('cc-geo-continent')?.value || '';
    const ccCountry   = document.getElementById('cc-geo-country')?.value   || '';
    const ccRegion    = document.getElementById('cc-geo-region')?.value    || '';

    // ── Validation ────────────────────────────────────────────────────────────
    if (!selPid) {
        showToast('Please select an AI Agent / Persona before launching.', 'error');
        personaSel?.focus();
        return;
    }
    if (!campName) {
        showToast('Campaign Name is required.', 'error');
        nameEl?.focus();
        return;
    }
    // V27.8.1: Continent OR Global is enough — country/state default to All
    if (!ccContinent) {
        showToast('Target Geography is required. Select Global or a continent (country/region optional — use All).', 'error');
        document.getElementById('cc-geo-continent')?.focus();
        return;
    }
    // Normalize empty country/region to All for structured storage
    const normCountry = ccCountry || 'All';
    const normRegion  = ccRegion  || 'All';

    window._selectedPersonaId = selPid;

    // ── Build keyword payload ─────────────────────────────────────────────────
    // _ccActivePersonaKeywords is set by onCcPersonaChange when user picks a persona.
    // Guaranteed non-null (empty string if persona has no keywords).
    const personaKeys    = window._ccActivePersonaKeywords || '';
    // Merge: persona base keywords + campaign-level extras
    const mergedKeywords = [personaKeys, extraKeys].filter(Boolean).join(', ');


    // Pain + advantage: use persona bio in persona mode, legacy fields in manual mode
    const pain = painEl?.value.trim() || '';
    const adv  = advEl?.value.trim()  || '';

    closeModal('child-campaign-modal');

    saveCampaignAction({
        name:              campName,
        bio:               'CHILD_CAMPAIGN_OVERRIDE',
        keywords:          mergedKeywords,
        campaign_focus:    campName,
        pain_point:        pain,
        unfair_advantage:  adv,
        gl:                gl,
        location:          loc || _compileGeoString(ccContinent, normCountry, normRegion),
        geo_hierarchy:     { continent: ccContinent, country: normCountry, region: normRegion }
    });
};


// =============================================================================
// V18 MULTI-TENANCY: BUSINESS PROFILE HUB
// =============================================================================

window.openBusinessProfile = async function() {
    const modal = document.getElementById('business-profile-modal');
    if (!modal) return;
    
    // Hide edit state, default to view
    document.getElementById('bp-edit-mode').classList.add('hidden');
    document.getElementById('bp-view-mode').classList.remove('hidden');
    document.getElementById('bp-bio-disp').textContent = 'Loading...';
    document.getElementById('bp-keys-disp').textContent = 'Loading...';
    modal.classList.remove('hidden');
    
    const tp = await fetchTenantProfile();
    if (tp) {
        document.getElementById('bp-bio-disp').textContent = tp.bio || 'Not Set';
        document.getElementById('bp-keys-disp').textContent = tp.keywords || 'Not Set';
        
        document.getElementById('bp-bio-edit').value = tp.bio || '';
        document.getElementById('bp-keys-edit').value = tp.keywords || '';
    }
};

window.saveBusinessProfile = async function() {
    const newBio = document.getElementById('bp-bio-edit').value.trim();
    const newKeys = document.getElementById('bp-keys-edit').value.trim();
    
    try {
        const user = window.firebase.auth().currentUser;
        const token = await user.getIdToken();
        const response = await fetch(`${API_BASE}/api/tenant_profiles`, {
            method: 'POST',
            headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
            body: JSON.stringify({
                bio: newBio,
                keywords: newKeys
            })
        });
        
        if (response.ok) {
            showToast('Business Profile Updated', 'success');
            document.getElementById('bp-edit-mode').classList.add('hidden');
            document.getElementById('bp-view-mode').classList.remove('hidden');
            
            document.getElementById('bp-bio-disp').textContent = newBio;
            document.getElementById('bp-keys-disp').textContent = newKeys;
        } else {
            showToast('Failed to update business profile', 'error');
        }
    } catch(e) {
        showToast('Error syncing profile', 'error');
    }
};

window.uploadKnowledgeBase = async function() {
    const fileInput = document.getElementById('bp-kb-upload');
    if (!fileInput || !fileInput.files || fileInput.files.length === 0) {
        showToast('Please select a file first.', 'warn');
        return;
    }
    
    const file = fileInput.files[0];
    const user = window.firebase.auth().currentUser;
    if (!user) return;
    
    const tenantId = window.currentUserData?.tenant_id || user.uid;
    const pathRef = `knowledge_bases/${tenantId}/${file.name}`;
    
    showToast(`Uploading ${file.name}...`, 'info');
    
    try {
        const storageRef = firebase.storage().ref();
        const fileRef = storageRef.child(pathRef);
        await fileRef.put(file);
        
        showToast(`${file.name} uploaded successfully. Extracting context...`, 'info');
        
        const token = await user.getIdToken();
        const response = await fetch(`${API_BASE}/api/tenant_profiles/extract-kb`, {
            method: 'POST',
            headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
            body: JSON.stringify({ filepath: pathRef })
        });
        
        const data = await response.json();
        if (response.ok) {
            showToast(`Extraction Complete: Knowledge Base Context Appended ✨`, 'success');
        } else {
            showToast(data.error || 'Failed to extract text from file', 'error');
        }
    } catch (e) {
        console.error('KB Upload Error:', e);
        showToast('Upload or extraction failed.', 'error');
    }
};


// =============================================================================
// REMOVED: Global body click delegation for btn-new-twin-* and btn-add-campaign-*.
//
// BUG ROOT CAUSE (Double-Fire): Both buttons have onclick="..." attributes directly
// in the HTML AND were also caught here via body delegation, causing every click
// to fire openNewCampaignModal() / openChildCampaignModal() TWICE. The second
// async call was resetting modal state while the first was still executing.
//
// Fix: The onclick attributes on the HTML buttons are sufficient. Body delegation
// for these specific buttons has been removed. The body listener below is kept
// only for genuinely dynamic elements that are rendered via innerHTML (no onclick).
// =============================================================================
document.body.addEventListener('click', function(e) {
    // Intentionally empty — kept for future dynamic element delegation only.
    // Do NOT re-add btn-new-twin-* or btn-add-campaign-* here.
});

// =============================================================================
// PERSONA VAULT — Full CRUD UI Engine
// =============================================================================

// In-memory cache of loaded personas for dropdown population
window._personasCache = [];
window._selectedPersonaId = '';

// ── loadPersonaVault ──────────────────────────────────────────────────────────
window.personasCurrentPage = 0;
const PERSONAS_PAGE_SIZE = 8;

window.loadPersonaVault = async function() {
    const grid = document.getElementById('persona-grid');
    if (!grid) return;
    grid.innerHTML = '<div style="text-align:center;color:var(--text-muted);padding:48px 20px;grid-column:1/-1;">&#8987; Loading&hellip;</div>';

    try {
        const user  = firebase.auth().currentUser;
        if (!user) return;
        const token = await user.getIdToken(true);
        const resp  = await fetch(`${API_BASE}/api/personas`, {
            headers: { 'Authorization': `Bearer ${token}` }
        });
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
        const json  = await resp.json();
        const list  = json.data || [];

        window._personasCache = list;
        renderPersonasGrid(list);
    } catch(err) {
        console.error('[Persona Vault]', err);
        if (grid) grid.innerHTML = `<div style="text-align:center;color:#ef4444;padding:32px;grid-column:1/-1;">Failed to load personas: ${_escapeHTML(err.message)}</div>`;
    }
};

function renderPersonasGrid(personas) {
    const grid = document.getElementById('persona-grid');
    const pagEl = document.getElementById('personas-pagination');
    if (!grid) return;

    if (personas.length === 0) {
        grid.innerHTML = `
            <div style="text-align:center; color:var(--text-muted); padding:64px 20px; grid-column:1/-1;">
                <div style="font-size:3rem; margin-bottom:12px;">&#127918;</div>
                <div style="font-size:1.05rem; font-weight:600; margin-bottom:8px;">No Personas Yet</div>
                <div style="font-size:0.85rem; margin-bottom:20px; max-width:360px; margin-left:auto; margin-right:auto;">
                    Create named AI agent personas with unique pitches and keywords.
                    Attach them to campaigns so each search uses the right voice.
                </div>
                <button class="primary-btn" onclick="openPersonaModal()" style="min-height:44px; padding:10px 28px;">
                    &#43; Create Your First Persona
                </button>
            </div>`;
        if (pagEl) pagEl.innerHTML = '';
        return;
    }

    const pageCount = Math.ceil(personas.length / PERSONAS_PAGE_SIZE) || 1;
    if (window.personasCurrentPage >= pageCount) window.personasCurrentPage = pageCount - 1;
    if (window.personasCurrentPage < 0) window.personasCurrentPage = 0;

    const startIndex = window.personasCurrentPage * PERSONAS_PAGE_SIZE;
    const pagePersonas = personas.slice(startIndex, startIndex + PERSONAS_PAGE_SIZE);

    grid.innerHTML = pagePersonas.map(p => _buildPersonaCard(p)).join('');

    if (!pagEl) return;
    if (pageCount <= 1) {
        pagEl.innerHTML = '';
        return;
    }

    let html = '';
    html += `<button class="sio-page-btn" ${window.personasCurrentPage === 0 ? 'disabled' : ''} onclick="changePersonasPage(${window.personasCurrentPage - 1})">&larr; Prev</button>`;
    for (let i = 0; i < pageCount; i++) {
        html += `<button class="sio-page-btn ${i === window.personasCurrentPage ? 'active' : ''}" onclick="changePersonasPage(${i})">${i + 1}</button>`;
    }
    html += `<button class="sio-page-btn" ${window.personasCurrentPage === pageCount - 1 ? 'disabled' : ''} onclick="changePersonasPage(${window.personasCurrentPage + 1})">Next &rarr;</button>`;
    pagEl.innerHTML = html;
}

window.changePersonasPage = function(pageIndex) {
    window.personasCurrentPage = pageIndex;
    if (window._personasCache) {
        renderPersonasGrid(window._personasCache);
    }
};

// ── _buildPersonaCard ─────────────────────────────────────────────────────────
function _buildPersonaCard(p) {
    const kwChips = (p.keywords || '').split(',')
        .map(k => k.trim()).filter(Boolean).slice(0, 5)
        .map(k => `<span style="display:inline-block; background:rgba(79,70,229,0.08); color:#4f46e5; font-size:0.7rem; font-weight:600; padding:3px 8px; border-radius:20px; margin:2px;">${_escapeHTML(k)}</span>`)
        .join('');
    const bioPreview = (p.bio || '').length > 120 ? p.bio.slice(0, 120) + '…' : (p.bio || '—');
    const safeId  = (p.id  || '').replace(/'/g, "\\'");
    const safeName = (p.name||'').replace(/'/g, "\\'");
    const safeBio  = (p.bio ||'').replace(/'/g, "\\'").replace(/\n/g, ' ');
    const safeKeys = (p.keywords||'').replace(/'/g, "\\'");

    return `
    <div style="background:#fff; border:1px solid #e5e7eb; border-radius:16px; padding:20px; display:flex; flex-direction:column; gap:12px; box-shadow:0 1px 4px rgba(0,0,0,0.06); transition:box-shadow 0.2s;" onmouseover="this.style.boxShadow='0 4px 16px rgba(79,70,229,0.12)'" onmouseout="this.style.boxShadow='0 1px 4px rgba(0,0,0,0.06)'">
        <div style="display:flex; justify-content:space-between; align-items:flex-start; gap:8px;">
            <div style="font-size:1rem; font-weight:700; color:#1e1b4b; line-height:1.3;">${_escapeHTML(p.name) || 'Unnamed Persona'}</div>
            <div style="display:flex; gap:8px; flex-shrink:0;">
                <button onclick="openPersonaModal('${safeId}','${safeName}','${safeBio}','${safeKeys}')" style="background:none; border:1px solid #d1d5db; border-radius:8px; padding:5px 10px; font-size:0.75rem; font-weight:600; color:#4f46e5; cursor:pointer; transition:all 0.15s;" onmouseover="this.style.background='#ede9fe'" onmouseout="this.style.background='none'">&#9998; Edit</button>
                <button onclick="deletePersona('${safeId}','${safeName}')" style="background:none; border:1px solid #fecaca; border-radius:8px; padding:5px 10px; font-size:0.75rem; font-weight:600; color:#ef4444; cursor:pointer; transition:all 0.15s;" onmouseover="this.style.background='#fef2f2'" onmouseout="this.style.background='none'">&#128465;</button>
            </div>
        </div>
        <div style="font-size:0.82rem; color:#6b7280; line-height:1.5;">${_escapeHTML(bioPreview)}</div>
        <div style="display:flex; flex-wrap:wrap; gap:4px; min-height:24px;">${kwChips || '<span style="font-size:0.72rem;color:#9ca3af;">No keywords set</span>'}</div>
        <div style="margin-top:auto; padding-top:8px; border-top:1px solid #f1f5f9; display:flex; justify-content:flex-end;">
            <button onclick="selectPersonaForCampaign('${safeId}','${safeName || 'Persona'}')" style="background:linear-gradient(135deg,#4f46e5,#7c3aed); color:#fff; border:none; border-radius:8px; padding:7px 14px; font-size:0.78rem; font-weight:600; cursor:pointer; transition:opacity 0.15s;" onmouseover="this.style.opacity='0.85'" onmouseout="this.style.opacity='1'">
                &#128640; Use in Campaign
            </button>
        </div>
    </div>`;
}

// ── openPersonaModal ──────────────────────────────────────────────────────────
// ── Tag engine internal state ────────────────────────────────────────────────
let _personaTags = [];

function _syncPersonaTagsToHidden() {
    const hidden = document.getElementById('persona-keywords');
    if (hidden) hidden.value = _personaTags.join(', ');
}

function _renderPersonaTags() {
    const container = document.getElementById('persona-tag-container');
    const input     = document.getElementById('persona-tag-input');
    if (!container || !input) return;

    // Remove all existing pills (leave the input in place)
    container.querySelectorAll('.persona-pill').forEach(p => p.remove());

    _personaTags.forEach((tag, idx) => {
        const isNegative = tag.toLowerCase().startsWith('not ');
        const pill = document.createElement('span');
        pill.className = 'persona-pill';
        pill.style.cssText = [
            'display:inline-flex', 'align-items:center', 'gap:5px',
            'padding:4px 10px', 'border-radius:20px',
            'font-size:0.78rem', 'font-weight:600', 'line-height:1', 'cursor:default',
            isNegative
                ? 'background:#fff1f0; color:#cf1322; border:1px solid #ffa39e;'
                : 'background:rgba(79,70,229,0.09); color:#4338ca; border:1px solid rgba(79,70,229,0.2);'
        ].join(';');
        pill.innerHTML = `${tag}<button tabindex="-1" onclick="_removePersonaTag(${idx})" style="background:none;border:none;cursor:pointer;padding:0 0 0 2px;color:inherit;opacity:0.6;font-size:0.85rem;line-height:1;" onmouseover="this.style.opacity='1'" onmouseout="this.style.opacity='0.6'">&#10005;</button>`;
        container.insertBefore(pill, input);
    });

    input.placeholder = _personaTags.length === 0 ? "e.g., 'looking for recommendations', 'software is too slow'..." : '';
    _syncPersonaTagsToHidden();
}

window._removePersonaTag = function(idx) {
    _personaTags.splice(idx, 1);
    _renderPersonaTags();
};

window.handlePersonaTagInput = function(e) {
    const input = e.target;
    const val   = (input.value || '').trim().replace(/,+$/, '').trim();

    if ((e.key === 'Enter' || e.key === ',') && val) {
        e.preventDefault();
        if (!_personaTags.includes(val)) {
            _personaTags.push(val);
            _renderPersonaTags();
        }
        input.value = '';
        input.style.width = '';
        return;
    }

    // Backspace on empty input removes last tag
    if (e.key === 'Backspace' && !input.value && _personaTags.length > 0) {
        _personaTags.pop();
        _renderPersonaTags();
    }
};

// ── openPersonaModal ──────────────────────────────────────────────────────────
const _BIO_TEMPLATE = `[Who we help]: ...
[The problem we solve]: ...
[Our unfair advantage / Unique Value]: ...`;

window.openPersonaModal = function(id='', name='', bio='', keywords='') {
    const overlay = document.getElementById('persona-modal-overlay');
    const title   = document.getElementById('persona-modal-title');
    const editId  = document.getElementById('persona-edit-id');
    const nameEl  = document.getElementById('persona-name');
    const bioEl   = document.getElementById('persona-bio');
    const tagInput= document.getElementById('persona-tag-input');
    if (!overlay) return;

    const isEdit = !!id;

    if (title)  title.textContent = isEdit ? 'Edit AI Agent' : 'Configure AI Agent';
    if (editId) editId.value      = id;
    if (nameEl) nameEl.value      = name;

    // Bio: use template for new, actual bio for edit (strip trailing ellipsis from card preview)
    if (bioEl)  bioEl.value = isEdit ? bio.replace(/\u2026$/, '').trimEnd() : _BIO_TEMPLATE;

    // Rebuild tag pills from existing keywords
    _personaTags = keywords
        ? keywords.split(',').map(k => k.trim()).filter(Boolean)
        : [];
    _renderPersonaTags();
    if (tagInput) { tagInput.value = ''; tagInput.style.width = ''; }

    overlay.style.display = 'flex';
    setTimeout(() => nameEl?.focus(), 80);
};

// ── closePersonaModal ─────────────────────────────────────────────────────────
window.closePersonaModal = function() {
    const overlay = document.getElementById('persona-modal-overlay');
    if (overlay) overlay.style.display = 'none';
    // Reset tag state
    _personaTags = [];
    _renderPersonaTags();
};

// ── savePersona ───────────────────────────────────────────────────────────────
window.savePersona = async function() {
    const editId  = document.getElementById('persona-edit-id')?.value || '';
    const name    = (document.getElementById('persona-name')?.value    || '').trim();
    const bio     = (document.getElementById('persona-bio')?.value     || '').trim();
    const keywords= (document.getElementById('persona-keywords')?.value|| '').trim();
    const saveBtn = document.getElementById('persona-save-btn');

    if (!name) { showToast('Agent Name / Strategy is required.', 'error');  return; }
    if (!bio || bio === _BIO_TEMPLATE) { showToast('Please fill in the Business Identifier.', 'error'); return; }

    if (saveBtn) {
        saveBtn.disabled = true;
        saveBtn.innerHTML = '&#9203; Deploying\u2026';
    }

    try {
        const user  = firebase.auth().currentUser;
        if (!user) { showToast('Session expired. Please sign in again.', 'error'); return; }
        const token = await user.getIdToken(true);

        const isEdit = !!editId;
        const url    = isEdit ? `${API_BASE}/api/personas/${editId}` : `${API_BASE}/api/personas`;
        const method = isEdit ? 'PUT' : 'POST';

        const resp = await fetch(url, {
            method,
            headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
            body: JSON.stringify({ name, bio, keywords, targeting_signals: _personaTags })
        });

        if (!resp.ok) {
            const e = await resp.json().catch(() => ({}));
            throw new Error(e.error || `HTTP ${resp.status}`);
        }

        const result = await resp.json();
        if (isEdit && result.linked_campaigns > 0) {
            showToast(`Agent updated. Cache refreshed for ${result.linked_campaigns} campaign(s).`, 'success');
        } else {
            showToast(isEdit ? 'Agent configuration saved.' : '&#9889; Agent deployed!', 'success');
        }

        closePersonaModal();
        loadPersonaVault();
    } catch(err) {
        console.error('[savePersona]', err);
        showToast('Deploy failed: ' + err.message, 'error');
    } finally {
        if (saveBtn) {
            saveBtn.disabled = false;
            saveBtn.innerHTML = '<span>&#9889;</span> Deploy Agent';
        }
    }
};


// ── deletePersona ─────────────────────────────────────────────────────────────
window.deletePersona = async function(id, name) {
    if (!id) return;
    if (!confirm(`Delete persona "${name}"?\nThis cannot be undone. Active campaigns using it will fall back to their own bio.`)) return;

    try {
        const user  = firebase.auth().currentUser;
        if (!user) return;
        const token = await user.getIdToken(true);
        const resp  = await fetch(`${API_BASE}/api/personas/${id}`, {
            method:  'DELETE',
            headers: { 'Authorization': `Bearer ${token}` }
        });
        const json = await resp.json();
        if (!resp.ok) {
            if (resp.status === 409) {
                showToast(`Cannot delete: in use by campaigns: ${json.campaigns?.join(', ')}`, 'error');
            } else {
                throw new Error(json.error || `HTTP ${resp.status}`);
            }
            return;
        }
        showToast(`Persona "${name}" deleted.`, 'success');
        loadPersonaVault();
    } catch(err) {
        console.error('[deletePersona]', err);
        showToast('Delete failed: ' + err.message, 'error');
    }
};

// ── selectPersonaForCampaign ──────────────────────────────────────────────────
// Called from persona card "Use in Campaign" — sets the global selection and
// switches to the campaign builder tab pre-loaded with this persona.
window.selectPersonaForCampaign = function(id, name) {
    window._selectedPersonaId = id;
    showToast(`Persona "${name}" selected. Open a campaign to use it.`, 'success');
    // Navigate to campaign builder
    switchTab('target');
};

// ── populatePersonaDropdown ───────────────────────────────────────────────────
// Call this when opening a modal that has a persona <select> element.
// Fetches from API once, then serves from _personasCache.
// CRITICAL: does NOT overwrite onchange — wires to onCcPersonaChange.
// J-11 FIX: Track which selects already have a change listener to prevent
// duplicate listeners accumulating each time the modal is re-opened.
// Without this guard, opening the modal 3× causes onCcPersonaChange to fire
// 3 times per change, causing persona preview to flicker and hide itself.
const _dropdownListenerAttached = new WeakSet();

window.populatePersonaDropdown = async function(selectElId) {
    const sel = document.getElementById(selectElId);
    if (!sel) return;

    // Reset to blank placeholder
    sel.innerHTML = '<option value="">\u2014 Select an AI Agent \u2014</option>';

    try {
        const user = firebase.auth().currentUser;
        if (!user) return;

        // Use cache if available, else fetch
        let list = window._personasCache;
        if (!list || list.length === 0) {
            const token = await user.getIdToken();
            const resp  = await fetch(`${API_BASE}/api/personas`, {
                headers: { 'Authorization': `Bearer ${token}` }
            });
            const json  = await resp.json();
            list = json.data || [];
            window._personasCache = list;
        }

        list.forEach(p => {
            const opt       = document.createElement('option');
            opt.value       = p.id;           // always p.id — matches onCcPersonaChange lookup
            opt.textContent = p.name;
            // J-17 FIX: Pre-select the persona that was chosen from the Persona Vault.
            // selectPersonaForCampaign() sets window._selectedPersonaId; we honour it here.
            if (p.id === window._selectedPersonaId) opt.selected = true;
            sel.appendChild(opt);
        });

        // J-11 FIX: Only attach the change listener once per DOM element.
        if (!_dropdownListenerAttached.has(sel)) {
            sel.addEventListener('change', function() {
                window._selectedPersonaId = sel.value;
                window.onCcPersonaChange(sel.value);
            });
            _dropdownListenerAttached.add(sel);
        }

        // J-17 FIX: If a persona was pre-selected from the vault, trigger the
        // conditional show/hide logic so the directive preview card appears immediately.
        if (sel.value) {
            window.onCcPersonaChange(sel.value);
        }

        console.log(`[populatePersonaDropdown] Loaded ${list.length} persona(s) into #${selectElId}`);
    } catch(err) {
        console.warn('[populatePersonaDropdown]', err);
        sel.innerHTML = '<option value="">Failed to load personas</option>';
    }
};


// =============================================================================
// L1 ROI DASHBOARD — Client Module
//
// loadROIDashboard(dateRange)
//   Calls GET /api/analytics/roi?date_range=N
//   Renders four hero cards with smooth counter animations.
//
// openUnitEconomicsModal()
//   Populates modal inputs from last fetched unit_economics.
//
// saveUnitEconomics()
//   PUTs to /api/analytics/unit-economics, then triggers immediate recalculate.
// =============================================================================

// Cache the last unit_economics from the API so the modal pre-fills correctly.
window._roiLastUE = null;

// Currency symbol map + formatter
function formatROICurrency(amount, currency = 'USD') {
    const symbols = { USD: '$', INR: '₹', GBP: '£', EUR: '€', AUD: 'A$', SGD: 'S$', AED: 'د.إ' };
    const sym = symbols[currency] || currency + ' ';
    if (amount >= 1_000_000) return sym + (amount / 1_000_000).toFixed(1) + 'M';
    if (amount >= 1_000)     return sym + (amount / 1_000).toFixed(1) + 'K';
    return sym + amount.toFixed(2);
}

// Smooth counter animation (easeOutExpo)
function animateCounter(el, targetVal, currency = 'USD', duration = 900) {
    if (!el) return;
    const start     = performance.now();
    const startVal  = 0;
    function step(now) {
        const elapsed = now - start;
        const progress = Math.min(elapsed / duration, 1);
        // easeOutExpo
        const eased = progress === 1 ? 1 : 1 - Math.pow(2, -10 * progress);
        const current = startVal + (targetVal - startVal) * eased;
        el.textContent = formatROICurrency(current, currency);
        if (progress < 1) requestAnimationFrame(step);
    }
    requestAnimationFrame(step);
}

// Show skeleton loader state on all four cards
function _roiShowSkeleton() {
    ['roi-ad-savings','roi-labor-savings','roi-total-offset','roi-pipeline-value'].forEach(id => {
        const el = document.getElementById(id);
        if (el) el.innerHTML = '<div class="roi-skeleton"></div>';
    });
}

window.loadROIDashboard = async function(dateRange = 30) {
    _roiShowSkeleton();
    try {
        const user  = firebase.auth().currentUser;
        if (!user) return;
        const token = await user.getIdToken();
        const resp  = await fetch(`${API_BASE}/api/analytics/roi?date_range=${dateRange}`, {
            headers: { 'Authorization': `Bearer ${token}` }
        });
        if (!resp.ok) {
            console.warn('[ROI] API error:', resp.status);
            ['roi-ad-savings','roi-labor-savings','roi-total-offset','roi-pipeline-value'].forEach(id => {
                const el = document.getElementById(id);
                if (el) el.textContent = '—';
            });
            return;
        }
        const payload = await resp.json();
        const m  = payload.metrics || {};
        const ue = payload.unit_economics || {};
        window._roiLastUE = ue;

        const curr = ue.currency || 'USD';
        const n    = m.n_approved || 0;

        // Animate card values
        animateCounter(document.getElementById('roi-ad-savings'),    m.ad_savings    || 0, curr);
        animateCounter(document.getElementById('roi-labor-savings'),  m.labor_savings  || 0, curr);
        animateCounter(document.getElementById('roi-total-offset'),   m.total_offset   || 0, curr);
        animateCounter(document.getElementById('roi-pipeline-value'), m.pipeline_value || 0, curr);

        // Update sub-labels
        const adSub = document.getElementById('roi-ad-sub');
        if (adSub) adSub.textContent = `${curr} ${ue.avg_cpl}/lead × ${n} approved`;

        const laborSub = document.getElementById('roi-labor-sub');
        if (laborSub) laborSub.textContent = `at ${curr} ${ue.sdr_hourly_rate}/hr SDR rate`;

        const approvedSub = document.getElementById('roi-approved-sub');
        if (approvedSub) approvedSub.textContent = `${n} approved lead${n !== 1 ? 's' : ''} in window`;

        const ratioLabel = document.getElementById('roi-ratio-label');
        if (ratioLabel) {
            if (m.roi_ratio > 0) {
                ratioLabel.textContent = `${m.roi_ratio}× ROI vs. Sideio cost`;
            } else {
                ratioLabel.textContent = 'ad + labor offset combined';
            }
        }

        // Pipeline value card — show prompt if no deal size set
        const pipelineSub = document.getElementById('roi-pipeline-sub');
        if (pipelineSub) {
            if (ue.avg_deal_size > 0) {
                const convPct = (ue.est_conversion_rate * 100).toFixed(1);
                pipelineSub.textContent = `at ${convPct}% conversion × ${formatROICurrency(ue.avg_deal_size, curr)} ADS`;
            } else {
                pipelineSub.textContent = '⚙️ Set avg deal size to unlock';
            }
        }

        const pipelineTrend = document.getElementById('roi-pipeline-trend');
        if (pipelineTrend) {
            const convPct = ((ue.est_conversion_rate || 0.02) * 100).toFixed(1);
            pipelineTrend.querySelector('span') && (pipelineTrend.lastChild.textContent = `at ${convPct}% conversion est.`);
        }

    } catch (err) {
        console.error('[ROI] loadROIDashboard error:', err);
    }
};

window.openUnitEconomicsModal = function() {
    const ue = window._roiLastUE || {};
    // Pre-fill inputs with current values (or leave blank to show placeholder)
    const setVal = (id, val, defaultVal) => {
        const el = document.getElementById(id);
        if (el && val !== undefined && val !== null && val !== defaultVal) el.value = val;
    };
    setVal('ue-cpl',       ue.avg_cpl,             50);
    setVal('ue-deal-size', ue.avg_deal_size,         0);
    setVal('ue-sdr-rate',  ue.sdr_hourly_rate,      15);
    setVal('ue-conv-rate', ue.est_conversion_rate != null
        ? +(ue.est_conversion_rate * 100).toFixed(2)
        : '', 2);

    const currSel = document.getElementById('ue-currency');
    if (currSel && ue.currency) currSel.value = ue.currency;

    const msg = document.getElementById('ue-save-msg');
    if (msg) msg.style.display = 'none';

    showModal('unit-economics-modal');
};

window.saveUnitEconomics = async function() {
    const cpl      = parseFloat(document.getElementById('ue-cpl')?.value)       || null;
    const deal     = parseFloat(document.getElementById('ue-deal-size')?.value)  || null;
    const sdr      = parseFloat(document.getElementById('ue-sdr-rate')?.value)   || null;
    const convPct  = parseFloat(document.getElementById('ue-conv-rate')?.value)  || null;
    const currency = document.getElementById('ue-currency')?.value              || 'USD';

    const payload = { currency };
    if (cpl    != null) payload.avg_cpl              = cpl;
    if (deal   != null) payload.avg_deal_size         = deal;
    if (sdr    != null) payload.sdr_hourly_rate       = sdr;
    if (convPct != null) payload.est_conversion_rate  = convPct / 100;

    try {
        const user  = firebase.auth().currentUser;
        if (!user) return;
        const token = await user.getIdToken();
        const resp  = await fetch(`${API_BASE}/api/analytics/unit-economics`, {
            method:  'PUT',
            headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
            body:    JSON.stringify(payload)
        });
        if (!resp.ok) throw new Error(await resp.text());

        const msg = document.getElementById('ue-save-msg');
        if (msg) { msg.style.display = 'block'; }

        // Trigger recalculate with current date range
        const range = document.getElementById('roi-range-select')?.value || 30;
        await loadROIDashboard(range);

        // Close modal after short delay so user sees the success message
        setTimeout(() => closeModal('unit-economics-modal'), 1200);
        showToast('Unit economics saved! ROI recalculated.', 'success');

    } catch(err) {
        console.error('[ROI] saveUnitEconomics error:', err);
        showToast('Failed to save unit economics. Please retry.', 'error');
    }
};


// =============================================================================
// ██╗███╗   ██╗██████╗  ██████╗ ██╗   ██╗███╗   ██╗██████╗
// ██║████╗  ██║██╔══██╗██╔═══██╗██║   ██║████╗  ██║██╔══██╗
// ██║██╔██╗ ██║██████╔╝██║   ██║██║   ██║██╔██╗ ██║██║  ██║
// ██║██║╚██╗██║██╔══██╗██║   ██║██║   ██║██║╚██╗██║██║  ██║
// ██║██║ ╚████║██████╔╝╚██████╔╝╚██████╔╝██║ ╚████║██████╔╝
// ╚═╝╚═╝  ╚═══╝╚═════╝  ╚═════╝  ╚═════╝ ╚═╝  ╚═══╝╚═════╝
// V23.5 — Inbound Sales Signal Radar
// =============================================================================

const INTENT_COLORS = {
    ACTIVE_SEEKING:   { bg: '#ecfdf5', border: '#10b981', badge: '#10b981', label: '🔥 Active Seeking'  },
    COMPETITOR_CHURN: { bg: '#fff7ed', border: '#f97316', badge: '#f97316', label: '🔀 Competitor Churn' },
    EXPRESSING_PAIN:  { bg: '#eff6ff', border: '#3b82f6', badge: '#3b82f6', label: '😣 Expressing Pain'  },
    TREND:            { bg: '#f5f3ff', border: '#8b5cf6', badge: '#8b5cf6', label: '📈 Trend Signal'     },
};

/**
 * Render the Inbound Radar summary banner on the Home tab.
 * Called once from loadMe() whenever payload.inbound_radar is present.
 */
function _renderInboundRadarBanner(radarData) {
    const container = document.getElementById('inbound-radar-banner');
    if (!container) return;

    const enabled       = radarData.enabled;
    const signalsWeek   = radarData.signals_this_week || 0;
    const lastRan       = radarData.last_ran_at ? new Date(radarData.last_ran_at).toLocaleDateString() : 'Never';
    const topKws        = (radarData.top_pain_keywords || []).slice(0, 5);

    container.style.display = 'block';
    container.innerHTML = `
        <div style="display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:10px;">
            <div style="display:flex; align-items:center; gap:10px;">
                <div style="width:10px; height:10px; border-radius:50%; background:${enabled ? '#10b981' : '#9ca3af'};
                     box-shadow: ${enabled ? '0 0 0 3px rgba(16,185,129,0.25)' : 'none'};
                     animation: ${enabled ? 'roiPulse 2s infinite' : 'none'};"></div>
                <div>
                    <span style="font-weight:700; font-size:0.85rem;">Inbound Radar</span>
                    <span style="color:#6b7280; font-size:0.78rem; margin-left:8px;">
                        ${enabled ? `${signalsWeek} signals this week · Last run: ${lastRan}` : 'Disabled — enable in Settings'}
                    </span>
                </div>
                ${topKws.length ? `
                    <div style="display:flex; gap:6px; flex-wrap:wrap;">
                        ${topKws.map(k => `<span style="background:#f1f5f9; border-radius:20px; padding:3px 10px; font-size:0.72rem; color:#475569; font-weight:600;">${k}</span>`).join('')}
                    </div>` : ''}
            </div>
            <div style="display:flex; gap:8px;">
                ${enabled ? `
                <button onclick="loadInboundSignals()" style="
                    background: linear-gradient(135deg,#4f46e5,#7c3aed);
                    color:#fff; border:none; border-radius:10px;
                    padding:8px 16px; font-size:0.8rem; font-weight:600;
                    cursor:pointer; transition:opacity 0.2s;"
                    onmouseover="this.style.opacity='0.85'" onmouseout="this.style.opacity='1'">
                    📡 View Signals
                </button>` : ''}
                <button onclick="toggleInboundRadar(${!enabled})" style="
                    background:#f8fafc; color:#374151; border:1px solid #e5e7eb;
                    border-radius:10px; padding:8px 14px; font-size:0.78rem;
                    font-weight:600; cursor:pointer; transition:all 0.2s;"
                    onmouseover="this.style.background='#f1f5f9'" onmouseout="this.style.background='#f8fafc'">
                    ${enabled ? '⏸ Disable' : '▶ Enable'} Radar
                </button>
            </div>
        </div>
    `;
}

/**
 * Load and display inbound signals in the modal/panel.
 */
async function loadInboundSignals(statusFilter = 'new') {
    const panel = document.getElementById('inbound-signals-panel');
    if (!panel) { showToast('Inbound panel not found in DOM', 'error'); return; }

    panel.style.display = 'block';
    panel.innerHTML = '<div style="text-align:center; padding:32px; color:#9ca3af;">⏳ Loading signals…</div>';

    try {
        const token = await firebase.auth().currentUser.getIdToken();
        const res   = await fetch(`${API_BASE}/api/inbound-signals?status=${statusFilter}&limit=20`, {
            headers: { 'Authorization': `Bearer ${token}` }
        });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const { signals } = await res.json();

        if (!signals || signals.length === 0) {
            panel.innerHTML = `
                <div style="text-align:center; padding:48px; color:#9ca3af;">
                    <div style="font-size:2.5rem; margin-bottom:12px;">📡</div>
                    <div style="font-weight:600; margin-bottom:6px;">No ${statusFilter} signals yet</div>
                    <div style="font-size:0.82rem;">The radar runs every 6 hours across the entire public web.</div>
                </div>`;
            return;
        }

        // Status filter tabs
        const tabs = ['new','reviewed','converted_to_lead','dismissed'];
        const tabsHtml = tabs.map(t => `
            <button onclick="loadInboundSignals('${t}')" style="
                background:${t === statusFilter ? 'var(--primary)' : '#f1f5f9'};
                color:${t === statusFilter ? '#fff' : '#374151'};
                border:none; border-radius:8px; padding:6px 14px;
                font-size:0.78rem; font-weight:600; cursor:pointer; transition:all 0.15s;">
                ${t.replace(/_/g,' ')}
            </button>`).join('');

        // Signal cards
        const cards = signals.map(sig => {
            const ic = INTENT_COLORS[sig.intent_label] || INTENT_COLORS.TREND;
            const score = Math.round((sig.intent_score || 0) * 100);
            const kws = (sig.pain_keywords || []).slice(0, 4).map(k =>
                `<span style="background:#f1f5f9; border-radius:12px; padding:2px 8px; font-size:0.7rem; color:#475569;">${_escapeHTML(k)}</span>`
            ).join(' ');

            return `
            <div style="background:${ic.bg}; border:1.5px solid ${ic.border}; border-radius:14px; padding:16px 18px; margin-bottom:12px;">
                <div style="display:flex; justify-content:space-between; align-items:flex-start; gap:12px; flex-wrap:wrap;">
                    <div style="flex:1; min-width:0;">
                        <div style="display:flex; align-items:center; gap:8px; margin-bottom:6px; flex-wrap:wrap;">
                            <span style="background:${ic.badge}; color:#fff; border-radius:20px; padding:3px 10px; font-size:0.7rem; font-weight:700;">${_escapeHTML(ic.label)}</span>
                            <span style="background:#fff; border:1px solid #e5e7eb; border-radius:20px; padding:2px 10px; font-size:0.7rem; font-weight:600; color:#374151;">
                                ${_escapeHTML(sig.source_platform || 'web')}
                            </span>
                            ${sig.company_name ? `<span style="font-size:0.78rem; font-weight:600; color:#1e293b;">🏢 ${_escapeHTML(sig.company_name)}</span>` : ''}
                        </div>
                        <div style="font-weight:600; font-size:0.92rem; margin-bottom:6px; color:#0f172a; line-height:1.4;">${_escapeHTML(sig.headline || '(no title)')}</div>
                        <div style="font-size:0.82rem; color:#475569; margin-bottom:8px; line-height:1.5;">${_escapeHTML((sig.snippet || '').substring(0,220))}…</div>
                        <div style="display:flex; gap:6px; flex-wrap:wrap; margin-bottom:8px;">${kws}</div>
                        <a href="${_safeHref(sig.source_url)}" target="_blank" rel="noopener noreferrer" style="font-size:0.75rem; color:var(--primary); text-decoration:none;">
                            🔗 View Source ↗
                        </a>
                    </div>
                    <div style="text-align:right; flex-shrink:0;">
                        <div style="font-size:1.8rem; font-weight:800; color:${ic.badge}; line-height:1;">${score}</div>
                        <div style="font-size:0.65rem; font-weight:700; text-transform:uppercase; color:#9ca3af; letter-spacing:0.06em;">Intent Score</div>
                        <div style="margin-top:10px; display:flex; flex-direction:column; gap:6px;">
                            <button onclick="updateSignalStatus('${sig.id}', 'converted_to_lead')"
                                style="background:linear-gradient(135deg,#10b981,#059669); color:#fff; border:none; border-radius:8px; padding:6px 14px; font-size:0.75rem; font-weight:700; cursor:pointer;">
                                + Add as Lead
                            </button>
                            <button onclick="updateSignalStatus('${sig.id}', 'dismissed')"
                                style="background:#f1f5f9; color:#6b7280; border:none; border-radius:8px; padding:5px 14px; font-size:0.75rem; cursor:pointer;">
                                Dismiss
                            </button>
                        </div>
                    </div>
                </div>
            </div>`;
        }).join('');

        panel.innerHTML = `
            <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:16px; flex-wrap:wrap; gap:10px;">
                <div style="font-size:0.72rem; font-weight:700; text-transform:uppercase; letter-spacing:0.08em; color:#6b7280;">
                    📡 Inbound Radar — ${signals.length} ${statusFilter.replace(/_/g,' ')} signal${signals.length !== 1 ? 's' : ''}
                </div>
                <div style="display:flex; gap:6px; flex-wrap:wrap;">${tabsHtml}</div>
            </div>
            ${cards}`;

    } catch (err) {
        panel.innerHTML = `<div style="color:#dc2626; padding:20px;">Failed to load signals: ${_escapeHTML(err.message)}</div>`;
        console.error('[Inbound Radar] loadInboundSignals error:', err);
    }
}

/**
 * Transition a signal's status (reviewed | dismissed | converted_to_lead).
 */
async function updateSignalStatus(signalDocId, newStatus) {
    try {
        const token = await firebase.auth().currentUser.getIdToken();
        const res = await fetch(`${API_BASE}/api/inbound-signals/${signalDocId}/status`, {
            method:  'PUT',
            headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
            body:    JSON.stringify({ status: newStatus }),
        });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const body = await res.json();

        if (newStatus === 'converted_to_lead' && body.lead_id) {
            showToast(`✅ Lead created! ID: ${body.lead_id.substring(0,8)}…`, 'success');
            // Reload leads feed to show the new lead
            if (typeof loadLeads === 'function') loadLeads();
        } else if (newStatus === 'dismissed') {
            showToast('Signal dismissed.', 'info');
        }
        // Refresh the signals panel
        const currentTab = document.querySelector('[data-radar-tab-active]')?.dataset?.radarTabActive || 'new';
        loadInboundSignals(currentTab);

    } catch (err) {
        showToast(`Failed to update signal: ${err.message}`, 'error');
        console.error('[Inbound Radar] updateSignalStatus error:', err);
    }
}

/**
 * Enable or disable the inbound radar for this tenant.
 * Writes users/{uid}.inbound_radar.enabled = true/false.
 */
async function toggleInboundRadar(enable) {
    try {
        const token = await firebase.auth().currentUser.getIdToken();
        // /api/me PUT accepts arbitrary profile updates — use it to toggle
        const res = await fetch(`${API_BASE}/api/me`, {
            method:  'PUT',
            headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
            body:    JSON.stringify({ inbound_radar_enabled: enable }),
        });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        showToast(enable ? '📡 Inbound Radar enabled!' : 'Radar disabled.', enable ? 'success' : 'info');
        // Reload /api/me to refresh banner
        loadMe();
    } catch (err) {
        showToast(`Failed to toggle radar: ${err.message}`, 'error');
    }
}

// ── V24.0: Competitive Intelligence ──────────────────────────────────
window.analyzeCompetitor = async function analyzeCompetitor() {
    const urlInput = document.getElementById('competitor-url-input');
    const resultsDiv = document.getElementById('competitor-results');
    const btn = document.getElementById('analyze-competitor-btn');
    const url = (urlInput?.value || '').trim();

    if (!url) { showToast('Enter a competitor URL', 'error'); return; }
    if (!url.startsWith('https://')) { showToast('URL must start with https://', 'error'); return; }

    btn.disabled = true;
    btn.textContent = 'Analyzing...';
    resultsDiv.style.display = 'none';

    try {
        const user = firebase.auth().currentUser;
        if (!user) { showToast('Not authenticated', 'error'); return; }
        const token = await user.getIdToken();

        // Get current tenant bio/keywords for context
        const meSnap = await firebase.firestore().collection('tenant_profiles').doc(user.uid).get();
        const me = meSnap.data() || {};

        const DT_URL = window._dtEngineUrl || 'https://digital-twin-engine-222247989819.asia-south1.run.app';
        const resp = await fetch(`${DT_URL}/api/analyze-competitor`, {
            method: 'POST',
            headers: {'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json'},
            body: JSON.stringify({
                competitor_url: url,
                tenant_bio: me.bio || '',
                tenant_keywords: me.keywords || ''
            })
        });

        if (!resp.ok) {
            const err = await resp.json().catch(() => ({}));
            throw new Error(err.error || `HTTP ${resp.status}`);
        }

        const data = await resp.json();

        // Render results
        const weaknesses = (data.competitor_weaknesses || []).map(w =>
            `<li>${_escapeHTML(w)}</li>`
        ).join('');
        const queries = (data.anti_competitor_queries || []).map(q =>
            `<li><code>${_escapeHTML(q)}</code></li>`
        ).join('');
        const signals = (data.churn_signals_to_watch || []).map(s =>
            `<li>${_escapeHTML(s)}</li>`
        ).join('');

        resultsDiv.innerHTML = `
            <div class="competitor-card">
                <h4>🎯 ${_escapeHTML(data.competitor_name || 'Unknown')}</h4>
                <p class="competitor-product">${_escapeHTML(data.competitor_product || '')}</p>
                <div class="competitor-section">
                    <h5>⚠️ Weaknesses</h5>
                    <ul>${weaknesses}</ul>
                </div>
                <div class="competitor-section">
                    <h5>🔍 Anti-Competitor Queries</h5>
                    <ul class="query-list">${queries}</ul>
                </div>
                <div class="competitor-section">
                    <h5>📡 Churn Signals to Watch</h5>
                    <ul>${signals}</ul>
                </div>
                <button type="button" class="sio-btn sio-btn-primary"
                    onclick="applyCompetitorQueries()"
                    aria-label="Apply competitor queries to campaign">
                    Apply Queries to Campaign
                </button>
            </div>
        `;
        resultsDiv.style.display = 'block';

        // Store data for later use
        window._lastCompetitorData = data;
        showToast('Competitor analysis complete', 'success');
    } catch (err) {
        console.error('[V24.0 CompetitorIntel] Analysis failed:', err);
        showToast(`Analysis failed: ${err.message}`, 'error');
    } finally {
        btn.disabled = false;
        btn.textContent = 'Analyze';
    }
};

window.applyCompetitorQueries = function applyCompetitorQueries() {
    const data = window._lastCompetitorData;
    if (!data) return;

    // Append anti-competitor queries to campaign keywords
    const keywordsInput = document.getElementById('edit-camp-keys') ||
                          document.getElementById('cc-extra-keywords');
    if (keywordsInput && data.anti_competitor_queries) {
        const existing = keywordsInput.value.trim();
        const newQueries = data.anti_competitor_queries.join(', ');
        keywordsInput.value = existing ? `${existing}, ${newQueries}` : newQueries;
    }

    showToast('Competitor queries applied to keywords', 'success');
};

// ══════════════════════════════════════════════════════════════════════════════
// V24.0: RESEARCH AGENTS MARKETPLACE
// ══════════════════════════════════════════════════════════════════════════════

window.showCreateAgentModal = function showCreateAgentModal() {
    // Populate persona dropdown from cached personas
    var select = document.getElementById('agent-persona');
    if (select && window._personasCache) {
        select.innerHTML = '<option value="">None</option>';
        window._personasCache.forEach(function(p) {
            select.innerHTML += '<option value="' + _escapeHTML(p.id) + '">' + _escapeHTML(p.name || 'Unnamed') + '</option>';
        });
    }
    var modal = document.getElementById('create-agent-modal');
    if (modal) modal.style.display = 'flex';
};

window.createAgent = async function createAgent() {
    var nameEl = document.getElementById('agent-name');
    var promptEl = document.getElementById('agent-prompt');
    var scheduleEl = document.getElementById('agent-schedule');
    var maxResultsEl = document.getElementById('agent-max-results');
    var personaEl = document.getElementById('agent-persona');

    var name = nameEl ? nameEl.value.trim() : '';
    var prompt = promptEl ? promptEl.value.trim() : '';
    var schedule = scheduleEl ? scheduleEl.value : 'weekly';
    var maxResults = parseInt(maxResultsEl ? maxResultsEl.value : '10', 10);
    var personaId = personaEl ? personaEl.value : '';

    if (!name) { showToast('Agent name is required', 'error'); return; }
    if (!prompt) { showToast('Research prompt is required', 'error'); return; }

    try {
        var user = firebase.auth().currentUser;
        if (!user) { showToast('Not authenticated', 'error'); return; }
        var token = await user.getIdToken();

        var resp = await fetch('/api/agents', {
            method: 'POST',
            headers: {'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json'},
            body: JSON.stringify({ name: name, prompt: prompt, schedule: schedule, max_results: maxResults, persona_id: personaId })
        });

        if (!resp.ok) {
            var err = await resp.json().catch(function() { return {}; });
            throw new Error(err.error || 'HTTP ' + resp.status);
        }

        closeModal('create-agent-modal');
        showToast('Research agent created!', 'success');
        loadAgents();
    } catch (err) {
        showToast('Failed: ' + err.message, 'error');
    }
};

window.loadAgents = async function loadAgents() {
    var container = document.getElementById('agents-list');
    if (!container) return;

    try {
        var user = firebase.auth().currentUser;
        if (!user) return;
        var token = await user.getIdToken();

        var resp = await fetch('/api/agents', {
            headers: {'Authorization': 'Bearer ' + token}
        });

        if (!resp.ok) throw new Error('HTTP ' + resp.status);
        var agents = await resp.json();

        if (!agents.length) {
            container.innerHTML = '<p style="font-size:0.85rem;color:#9ca3af;text-align:center;padding:16px;">No research agents yet. Create one to automate lead discovery.</p>';
            return;
        }

        container.innerHTML = agents.map(function(a) {
            var statusClass = a.status === 'active' ? 'agent-active' : 'agent-paused';
            var statusIcon = a.status === 'active' ? '\u{1F7E2}' : '\u23F8\uFE0F';
            var lastRan = a.last_ran_at ? new Date(a.last_ran_at).toLocaleDateString() : 'Never';
            var scheduleMap = {daily: 'Daily', biweekly: 'Twice/Week', weekly: 'Weekly'};
            var scheduleLabel = scheduleMap[a.schedule] || a.schedule;
            var promptText = a.prompt || '';
            var promptPreview = _escapeHTML(promptText.substring(0, 120)) + (promptText.length > 120 ? '...' : '');
            var toggleLabel = a.status === 'active' ? '\u23F8 Pause' : '\u25B6 Resume';
            var toggleTarget = a.status === 'active' ? 'paused' : 'active';

            return '<div class="agent-card ' + statusClass + '" data-agent-id="' + _escapeHTML(a.id) + '">' +
                '<div class="agent-card-header">' +
                    '<span class="agent-status">' + statusIcon + '</span>' +
                    '<strong class="agent-name">' + _escapeHTML(a.name) + '</strong>' +
                    '<span class="agent-schedule-badge">' + scheduleLabel + '</span>' +
                '</div>' +
                '<p class="agent-prompt-preview">' + promptPreview + '</p>' +
                '<div class="agent-stats">' +
                    '<span>\u{1F4CA} ' + (a.total_leads_found || 0) + ' leads found</span>' +
                    '<span>\u{1F550} Last: ' + lastRan + '</span>' +
                '</div>' +
                '<div class="agent-actions">' +
                    '<button style="padding:5px 12px;border-radius:8px;border:none;background:linear-gradient(135deg,#7c3aed,#4f46e5);color:#fff;font-family:Inter,sans-serif;font-size:0.75rem;font-weight:600;cursor:pointer;" onclick="runAgentNow(\'' + _escapeHTML(a.id) + '\')" aria-label="Run agent now">\u25B6 Run Now</button>' +
                    '<button style="padding:5px 12px;border-radius:8px;border:1px solid #d1d5db;background:#fff;color:#374151;font-family:Inter,sans-serif;font-size:0.75rem;font-weight:500;cursor:pointer;" onclick="toggleAgent(\'' + _escapeHTML(a.id) + '\', \'' + toggleTarget + '\')" aria-label="Toggle agent">' + toggleLabel + '</button>' +
                    '<button style="padding:5px 12px;border-radius:8px;border:1px solid rgba(255,59,48,0.2);background:rgba(255,59,48,0.08);color:#dc2626;font-family:Inter,sans-serif;font-size:0.75rem;font-weight:500;cursor:pointer;" onclick="deleteAgent(\'' + _escapeHTML(a.id) + '\')" aria-label="Delete agent">\u{1F5D1}</button>' +
                '</div>' +
            '</div>';
        }).join('');
    } catch (err) {
        container.innerHTML = '<p style="font-size:0.85rem;color:#9ca3af;text-align:center;padding:16px;">Failed to load agents.</p>';
        if (window.SIO_DEBUG) console.error('[V24.0 Agents]', err);
    }
};

window.runAgentNow = async function runAgentNow(agentId) {
    try {
        var user = firebase.auth().currentUser;
        if (!user) return;
        var token = await user.getIdToken();
        showToast('Running agent...', 'info');

        var resp = await fetch('/api/agents/' + encodeURIComponent(agentId) + '/run', {
            method: 'POST',
            headers: {'Authorization': 'Bearer ' + token}
        });

        var result = await resp.json();
        if (resp.ok) {
            showToast('Agent complete: ' + (result.leads_created || 0) + ' leads found', 'success');
            loadAgents();
        } else {
            throw new Error(result.error || 'Run failed');
        }
    } catch (err) {
        showToast('Agent run failed: ' + err.message, 'error');
    }
};

window.toggleAgent = async function toggleAgent(agentId, newStatus) {
    try {
        var user = firebase.auth().currentUser;
        if (!user) return;
        var token = await user.getIdToken();

        await fetch('/api/agents/' + encodeURIComponent(agentId), {
            method: 'PUT',
            headers: {'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json'},
            body: JSON.stringify({ status: newStatus })
        });

        showToast('Agent ' + newStatus, 'success');
        loadAgents();
    } catch (err) {
        showToast('Failed: ' + err.message, 'error');
    }
};

window.deleteAgent = async function deleteAgent(agentId) {
    if (!confirm('Delete this research agent?')) return;
    try {
        var user = firebase.auth().currentUser;
        if (!user) return;
        var token = await user.getIdToken();

        await fetch('/api/agents/' + encodeURIComponent(agentId), {
            method: 'DELETE',
            headers: {'Authorization': 'Bearer ' + token}
        });

        showToast('Agent deleted', 'success');
        loadAgents();
    } catch (err) {
        showToast('Failed: ' + err.message, 'error');
    }
};
