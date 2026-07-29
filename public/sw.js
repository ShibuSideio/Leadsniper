const CACHE_NAME = 'sideio-v27.9.1'; // V27.9.1: hard 90d content freshness feed+dispatch
const ASSETS_TO_CACHE = [
     '/',
     '/index.html',
     '/app.js?v=26.0.5',
     '/styles.css?v=26.0.4.1',
     '/manifest.json'
 ];


self.addEventListener('install', event => {
    self.skipWaiting(); // Force the waiting service worker to become the active service worker.
    event.waitUntil(
        caches.open(CACHE_NAME)
        .then(cache => cache.addAll(ASSETS_TO_CACHE))
    );
});

self.addEventListener('activate', event => {
    event.waitUntil(
        caches.keys().then(cacheNames => {
            return Promise.all(
                cacheNames.map(cacheName => {
                    if (cacheName !== CACHE_NAME) {
                        return caches.delete(cacheName);
                    }
                })
            );
        }).then(() => self.clients.claim()) // Immediately assert control.
    );
});

// Network-First Strategy (Guarantees fresh UI code deployed via Firebase Hosting)
self.addEventListener('fetch', event => {
    const url = new URL(event.request.url);
    const method = event.request.method;

    // ── CRITICAL: NetworkOnly for all /api/* state-mutating requests ──────────
    // POST, PUT, DELETE to /api/* MUST go straight to the network, bypassing
    // the cache entirely. A cached response for a campaign launch, onboarding
    // write, or credit settle is a data-integrity and financial bug.
    //
    // This fix resolves the "Digital Twin created" optimistic silent-drop:
    //   iOS Safari PWA in standalone mode re-uses a cached 200 response from
    //   the SW cache for repeat POST requests, making the frontend think the
    //   write succeeded while the backend never received it.
    if (url.pathname.startsWith('/api/') && (method === 'POST' || method === 'PUT' || method === 'DELETE' || method === 'PATCH')) {
        event.respondWith(
            fetch(event.request).catch(err => {
                // Surface network failures as a proper 503 so the frontend
                // catch() block shows an error toast instead of silently succeeding.
                return new Response(JSON.stringify({ error: 'Network unavailable', offline: true }), {
                    status: 503,
                    headers: { 'Content-Type': 'application/json' }
                });
            })
        );
        return;
    }

    // ── CRITICAL: Bypass cache for all Firebase / Google API traffic ──────────
    // Firestore WebChannel streams (long-poll XHR / WebSocket upgrades) cannot
    // be cloned into a Response object. Intercepting them causes:
    //   "Failed to convert value to 'Response'" + 30-second disconnect loops.
    // Pass these requests straight to the network — never touch the cache.
    // By returning without calling event.respondWith(), we let the browser handle them natively.
    if (
        url.hostname.includes('googleapis.com') ||
        url.hostname.includes('google.com')     ||
        url.hostname.includes('firestore')
    ) {
        return;
    }

    // All other requests: network-first, fall back to cache
    event.respondWith(
        fetch(event.request).catch(() => {
            return caches.match(event.request);
        })
    );
});

