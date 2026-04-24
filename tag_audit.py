"""
Tag audit crawler with auto consent and interaction simulation.

Features:
  - Auto accepts common consent banners (OneTrust, Cookiebot, Didomi, Quantcast, TrustArc, custom)
  - Simulates user interactions: scroll, hover, click, form fill, add-to-cart patterns
  - Captures every outbound tag request per page and per interaction
  - Classifies vendor, parses event name and parameters
  - Writes one CSV row per tag fire, with trigger context

Install:
    pip install playwright
    playwright install chromium

Run:
    python tag_audit.py https://www.example.com --max-pages 50 --output tags.csv
    python tag_audit.py https://shop.example.com --simulate full --headed
"""

import argparse
import csv
import json
import os
import re
from datetime import datetime
from urllib.parse import urlparse, parse_qs
from collections import deque
from playwright.sync_api import sync_playwright, Page, TimeoutError as PWTimeout


def log(msg: str) -> None:
    """Timestamped console log."""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


# ---------- Vendor detection ----------

VENDOR_RULES = [
    ("Meta Pixel",          r"facebook\.com/tr|connect\.facebook\.net"),
    ("Meta CAPI (client)",  r"graph\.facebook\.com/.*/events"),
    ("GA4",                 r"google-analytics\.com/g/collect|analytics\.google\.com/g/collect|region\d+\.google-analytics\.com/g/collect|analytics\.google\.com/collect|google-analytics\.com/mp/collect"),
    ("Universal Analytics", r"google-analytics\.com/collect|google-analytics\.com/j/collect"),
    ("Google Ads",          r"googleadservices\.com|googleads\.g\.doubleclick\.net|www\.google\.com\.au/ads|www\.google\.com/ads|www\.google\.com/pagead|www\.google\.com\.au/pagead"),
    ("Google Tag Manager",  r"googletagmanager\.com/gtm|googletagmanager\.com/gtag"),
    ("Google Signals",      r"stats\.g\.doubleclick\.net"),
    ("Floodlight / CM360",  r"fls\.doubleclick\.net|ad\.doubleclick\.net"),
    ("TikTok Pixel",        r"analytics\.tiktok\.com|business-api\.tiktok\.com"),
    ("LinkedIn Insight",    r"px\.ads\.linkedin\.com|snap\.licdn\.com"),
    ("Pinterest",           r"ct\.pinterest\.com|s\.pinimg\.com"),
    ("Reddit Pixel",        r"events\.redditmedia\.com|www\.redditstatic\.com/ads"),
    ("Snapchat Pixel",      r"tr\.snapchat\.com|sc-static\.net"),
    ("Amazon Ads",          r"amazon-adsystem\.com|aax\.amazon-adsystem\.com"),
    ("Adobe Analytics",     r"\.sc\.omtrdc\.net|2o7\.net|adobedc\.net"),
    ("Tealium",             r"tags\.tiqcdn\.com|collect\.tealiumiq\.com"),
    ("Segment",             r"api\.segment\.io|cdn\.segment\.com"),
    ("mParticle",           r"jssdk\.mparticle\.com|nativesdks\.mparticle\.com"),
    ("Hotjar",              r"static\.hotjar\.com|script\.hotjar\.com"),
    ("Criteo",              r"static\.criteo\.net|widget\.criteo\.com"),
    ("The Trade Desk",      r"insight\.adsrvr\.org|adsrvr\.org"),
    ("Microsoft / Bing UET",r"bat\.bing\.com"),
    ("Yahoo DOT",           r"sp\.analytics\.yahoo\.com"),
    # CRM / Personalisation
    ("Salesforce Personalisation (Evergage)", r"evergage\.com|evgnet\.com"),
    ("Salesforce Marketing Cloud",            r"exacttarget\.com|marketingcloudapis\.com|s7\.exacttarget\.com"),
    ("Adobe Target",        r"tt\.omtrdc\.net"),
    ("Optimizely",          r"cdn\.optimizely\.com|logx\.optimizely\.com"),
    ("Dynamic Yield",       r"dynamicyield\.com"),
    # Real User Monitoring
    ("Datadog RUM",         r"datadoghq\.com|datadoghq\.eu|datadog-rum|/dd-proxy/|browser-intake-datadoghq"),
    ("New Relic",           r"bam\.nr-data\.net|js-agent\.newrelic\.com"),
    ("Sentry",              r"sentry\.io|ingest\.sentry\.io"),
    # Voice of Customer / Surveys
    ("Qualtrics",           r"qualtrics\.com"),
    ("Medallia",            r"medallia\.com"),
    ("Foresee",             r"foresee\.com|foreseeresults\.com"),
    # Chat / CX
    ("Userlike Chat",       r"userlike-cdn-widgets|userlike-cdn-umm"),
    ("Intercom",            r"widget\.intercom\.io|api-iam\.intercom\.io"),
    ("Zendesk Chat",        r"zopim\.com|zdassets\.com"),
    ("LivePerson",          r"liveperson\.net"),
    ("Drift",               r"js\.driftt\.com|api\.drift\.com"),
    # Consent
    ("Usercentrics CMP",    r"usercentrics\.eu|app\.usercentrics|assets\.oneweb\.mercedes-benz\.com/plugin/cmm-cookie-banner"),
    ("OneTrust CMP",        r"cdn\.cookielaw\.org|onetrust\.com"),
    ("Cookiebot CMP",       r"consent\.cookiebot\.com|consentcdn\.cookiebot\.com"),
    ("Didomi CMP",          r"sdk\.privacy-center\.org|api\.privacy-center\.org"),
    ("TrustArc CMP",        r"consent\.trustarc\.com|trustarc\.com"),
    # Infrastructure
    ("Akamai",              r"akamai\.com|akamaihd\.net"),
]

EVENT_NAME_KEYS = ["ev", "event", "en", "eventName", "event_name", "t", "tid", "type"]


def classify_vendor(url: str) -> str:
    for vendor, pattern in VENDOR_RULES:
        if re.search(pattern, url, re.IGNORECASE):
            return vendor
    return ""


def extract_params(url: str, post_data):
    params = {}
    qs = parse_qs(urlparse(url).query, keep_blank_values=True)
    for k, v in qs.items():
        params[k] = v[0] if len(v) == 1 else v
    if post_data:
        try:
            body = json.loads(post_data)
            if isinstance(body, dict):
                for k, v in body.items():
                    params[f"body.{k}"] = v if isinstance(v, (str, int, float, bool)) else json.dumps(v)
        except (json.JSONDecodeError, TypeError):
            try:
                body_qs = parse_qs(post_data, keep_blank_values=True)
                for k, v in body_qs.items():
                    params[f"body.{k}"] = v[0] if len(v) == 1 else v
            except Exception:
                params["body.raw"] = post_data[:2000]
    return params


def guess_event_name(params: dict) -> str:
    for key in EVENT_NAME_KEYS:
        if key in params and params[key]:
            return str(params[key])
        body_key = f"body.{key}"
        if body_key in params and params[body_key]:
            return str(params[body_key])
    return ""


def same_host(url: str, root_host: str) -> bool:
    try:
        return urlparse(url).netloc.replace("www.", "") == root_host.replace("www.", "")
    except Exception:
        return False


# ---------- Consent auto-accept ----------

# Covers the major CMPs plus common custom patterns. Selectors tried in order, first match wins.
# Site-specific consent acceptance scripts. Each entry is a JS snippet that
# clicks the accept button and returns a truthy value on success, falsy on miss.
# Tried in order BEFORE generic selectors.
SITE_SPECIFIC_CONSENT_JS = [
    # Mercedes-Benz: cmm-cookie-banner with nested shadow roots
    ("mercedes-benz cmm-cookie-banner", """() => {
        const banner = document.querySelector('cmm-cookie-banner');
        if (!banner || !banner.shadowRoot) return null;
        const wb7 = banner.shadowRoot.querySelector('wb7-button[data-test="handle-accept-all-button"]');
        if (!wb7 || !wb7.shadowRoot) return null;
        const btn = wb7.shadowRoot.querySelector('button');
        if (!btn) return null;
        btn.click();
        return 'mercedes-benz cmm-cookie-banner';
    }"""),
    # Usercentrics v3 (standard web-component mount point)
    ("usercentrics uc-cmp", """() => {
        const hosts = document.querySelectorAll('#usercentrics-cmp-ui, #usercentrics-root, uc-cmp-ui');
        for (const host of hosts) {
            const root = host.shadowRoot || host;
            const btn = root.querySelector('button[data-testid="uc-accept-all-button"], button[data-action-name="accept-all"]');
            if (btn) { btn.click(); return 'usercentrics uc-cmp'; }
        }
        return null;
    }"""),
]


CONSENT_SELECTORS = [
    # Usercentrics (Mercedes-Benz and others). Must be first because it's common.
    "button[data-testid='uc-accept-all-button']",
    "button#uc-btn-accept-banner",
    "button[data-action-name='accept-all']",
    "button.uc-primary-button",
    "#usercentrics-root >>> button[data-testid='uc-accept-all-button']",
    "#usercentrics-cmp-ui >>> button[data-testid='accept-all-button']",
    # OneTrust
    "#onetrust-accept-btn-handler",
    "button#accept-recommended-btn-handler",
    # Cookiebot
    "#CybotCookiebotDialogBodyLevelButtonAccept",
    "#CybotCookiebotDialogBodyButtonAccept",
    # Didomi
    "button#didomi-notice-agree-button",
    "button.didomi-components-button--color",
    # Quantcast / TrustArc
    "button.qc-cmp2-summary-buttons > button[mode='primary']",
    ".qc-cmp2-summary-buttons button:nth-of-type(2)",
    "#truste-consent-button",
    # Iubenda
    "button.iubenda-cs-accept-btn",
    # Sourcepoint
    "button.sp_choice_type_11",
    "button[title='Accept All']",
    # TCF generic
    "button[aria-label*='Accept' i]",
    "button[aria-label*='Agree' i]",
    # Text-based fallbacks
    "button:has-text('Accept all')",
    "button:has-text('Accept All')",
    "button:has-text('Accept')",
    "button:has-text('I agree')",
    "button:has-text('Agree')",
    "button:has-text('Got it')",
    "button:has-text('Allow all')",
    "button:has-text('Allow All')",
]

# Try inside iframes too (many CMPs render in iframes)
CONSENT_IFRAME_HINTS = [
    "consent", "privacy", "cookie", "cmp", "trustarc", "onetrust", "didomi", "sourcepoint"
]


def accept_consent(page: Page, timeout_ms: int = 3000, debug: bool = False) -> str:
    """Try to accept consent. Returns the selector that worked, or empty string."""
    # Give the banner a moment to appear
    page.wait_for_timeout(min(timeout_ms, 2000))

    # First pass: site-specific scripts (fastest and most reliable)
    for name, js in SITE_SPECIFIC_CONSENT_JS:
        try:
            result = page.evaluate(js)
            if result:
                page.wait_for_timeout(600)
                return f"site-specific::{result}"
        except Exception:
            continue

    # Second pass: try every selector with 300ms timeout each
    for sel in CONSENT_SELECTORS:
        try:
            loc = page.locator(sel).first
            if loc.is_visible(timeout=300):
                loc.click(timeout=1000)
                page.wait_for_timeout(600)
                return sel
        except Exception:
            continue

    # Try inside iframes that look consent-related
    for frame in page.frames:
        url = (frame.url or "").lower()
        name = (frame.name or "").lower()
        if any(hint in url or hint in name for hint in CONSENT_IFRAME_HINTS):
            for sel in CONSENT_SELECTORS:
                try:
                    loc = frame.locator(sel).first
                    if loc.is_visible(timeout=300):
                        loc.click(timeout=1000)
                        page.wait_for_timeout(600)
                        return f"iframe::{sel}"
                except Exception:
                    continue

    # Fallback: shadow-DOM walk. Strict scoring: must contain an ACCEPT phrase,
    # must NOT contain a reject/settings phrase.
    try:
        js = """(debug) => {
            const ACCEPT_PHRASES = [
                'accept all', 'allow all', 'alle akzeptieren',
                'accept cookies', 'accept and continue',
                'i accept', 'i agree', 'agree and continue',
                'accept', 'agree', 'zustimmen', 'einverstanden'
            ];
            const REJECT_PHRASES = [
                'reject', 'deny', 'decline', 'disagree',
                'only necessary', 'necessary only', 'essential only',
                'settings', 'preferences', 'manage', 'customi',
                'ablehnen', 'einstellungen', 'mehr', 'details',
                'more info', 'more options', 'learn more', 'cookie policy',
                'privacy policy', 'close', 'dismiss'
            ];

            const getLabel = (el) => {
                const parts = [
                    (el.innerText || '').trim(),
                    (el.textContent || '').trim(),
                    el.getAttribute('aria-label') || '',
                    el.getAttribute('data-testid') || '',
                    el.getAttribute('title') || '',
                    el.getAttribute('id') || '',
                    el.getAttribute('name') || '',
                    el.className || ''
                ];
                return parts.filter(Boolean).join(' | ').toLowerCase();
            };

            const scoreButton = (label) => {
                // Reject phrases disqualify immediately
                for (const p of REJECT_PHRASES) if (label.includes(p)) return -1;
                // Return priority (lower index = higher priority)
                for (let i = 0; i < ACCEPT_PHRASES.length; i++) {
                    if (label.includes(ACCEPT_PHRASES[i])) return i;
                }
                return -1;
            };

            const walk = (root, acc) => {
                if (!root || !root.querySelectorAll) return;
                const els = root.querySelectorAll('button, [role="button"], a, input[type="button"], input[type="submit"]');
                for (const el of els) acc.push(el);
                const all = root.querySelectorAll('*');
                for (const el of all) if (el.shadowRoot) walk(el.shadowRoot, acc);
            };

            const candidates = [];
            walk(document, candidates);

            const scored = [];
            for (const el of candidates) {
                const r = el.getBoundingClientRect();
                if (r.width === 0 || r.height === 0) continue;
                const label = getLabel(el);
                const score = scoreButton(label);
                if (score === -1) continue;
                scored.push({el, score, label: label.slice(0, 80)});
            }

            scored.sort((a, b) => a.score - b.score);

            if (debug) {
                return {clicked: null, candidates: scored.slice(0, 15).map(c => ({score: c.score, label: c.label}))};
            }

            if (scored.length > 0) {
                const best = scored[0];
                best.el.click();
                return {clicked: best.label, candidates: null};
            }
            return {clicked: null, candidates: null};
        }"""
        result = page.evaluate(js, debug)

        if debug and result and result.get("candidates"):
            log("  consent debug: top candidates")
            for c in result["candidates"]:
                log(f"    score={c['score']}  {c['label']}")

        if result and result.get("clicked"):
            page.wait_for_timeout(600)
            return f"shadow-dom::{result['clicked'][:60]}"
    except Exception as e:
        if debug:
            log(f"  consent debug error: {e}")

    return ""


# ---------- Interaction simulation ----------

def simulate_scroll(page: Page):
    """Fast scroll to trigger lazy tags (scroll depth, viewport-triggered events)."""
    try:
        page.evaluate("""async () => {
            await new Promise(resolve => {
                let y = 0;
                const step = 1500;
                const timer = setInterval(() => {
                    window.scrollBy(0, step);
                    y += step;
                    if (y >= document.body.scrollHeight) {
                        clearInterval(timer);
                        resolve();
                    }
                }, 120);
            });
        }""")
        page.wait_for_timeout(400)
    except Exception:
        pass


def simulate_hover_nav(page: Page, max_hovers: int = 3):
    """Hover over top nav links to trigger engagement events."""
    try:
        nav_links = page.locator("nav a, header a").all()[:max_hovers]
        for link in nav_links:
            try:
                link.hover(timeout=1000)
                page.wait_for_timeout(300)
            except Exception:
                continue
    except Exception:
        pass


def simulate_cta_click(page: Page) -> str:
    """Click first obvious CTA (Add to cart, Buy, Subscribe, Sign up). Returns what was clicked."""
    cta_patterns = [
        "button:has-text('Add to cart')", "button:has-text('Add to Cart')",
        "button:has-text('Add to bag')", "button:has-text('Add to Bag')",
        "button:has-text('Buy now')", "button:has-text('Buy Now')",
        "a:has-text('Add to cart')", "a:has-text('Buy now')",
        "button[data-testid*='add-to-cart' i]",
        "button[class*='add-to-cart' i]",
        "button:has-text('Subscribe')",
        "button:has-text('Sign up')", "button:has-text('Sign Up')",
        "button:has-text('Get started')", "button:has-text('Get Started')",
    ]
    for pattern in cta_patterns:
        try:
            loc = page.locator(pattern).first
            loc.wait_for(state="visible", timeout=1500)
            loc.scroll_into_view_if_needed(timeout=1000)
            loc.click(timeout=2000)
            page.wait_for_timeout(1500)
            return pattern
        except Exception:
            continue
    return ""


def simulate_form_fill(page: Page) -> str:
    """Fill and submit the first visible newsletter or contact form."""
    try:
        email_input = page.locator("input[type='email']:visible").first
        email_input.wait_for(state="visible", timeout=1500)
        email_input.fill("audit.test@example.com", timeout=1500)
        page.wait_for_timeout(300)
        # Find associated submit button
        for sel in [
            "button[type='submit']:visible",
            "input[type='submit']:visible",
            "button:has-text('Subscribe'):visible",
            "button:has-text('Sign up'):visible",
        ]:
            try:
                page.locator(sel).first.click(timeout=1500)
                page.wait_for_timeout(1500)
                return "email_form_submit"
            except Exception:
                continue
        return "email_filled_no_submit"
    except Exception:
        return ""


def simulate_search(page: Page) -> str:
    """Use the site search if present."""
    for sel in ["input[type='search']:visible", "input[name*='search' i]:visible",
                "input[placeholder*='search' i]:visible"]:
        try:
            box = page.locator(sel).first
            box.wait_for(state="visible", timeout=1500)
            box.fill("test", timeout=1500)
            box.press("Enter", timeout=1500)
            page.wait_for_timeout(2000)
            return "search_submitted"
        except Exception:
            continue
    return ""


INTERACTION_REGISTRY = {
    "scroll":    simulate_scroll,
    "hover_nav": simulate_hover_nav,
    "cta_click": simulate_cta_click,
    "form_fill": simulate_form_fill,
    "search":    simulate_search,
}

SIMULATE_PROFILES = {
    "off":   [],
    "basic": ["scroll"],
    "full":  ["scroll", "hover_nav", "search", "cta_click", "form_fill"],
}


# ---------- Link discovery ----------

def extract_internal_links(page: Page, root_host: str) -> list:
    try:
        hrefs = page.eval_on_selector_all("a[href]", "els => els.map(e => e.href)")
    except Exception:
        return []
    seen, out = set(), []
    for href in hrefs:
        if not href or href.startswith(("mailto:", "tel:", "javascript:")):
            continue
        clean = href.split("#")[0]
        if same_host(clean, root_host) and clean not in seen:
            seen.add(clean)
            out.append(clean)
    return out


# ---------- Crawl ----------

CSV_FIELDS = ["captured_at", "page_url", "trigger", "vendor", "event_name", "method",
              "host", "resource_type", "request_url", "params_json"]


def load_processed_pages(output_csv: str) -> set:
    """Read existing CSV and return set of page_url values already captured."""
    processed = set()
    if not os.path.exists(output_csv):
        return processed
    try:
        with open(output_csv, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("page_url"):
                    processed.add(row["page_url"])
    except Exception as e:
        log(f"Could not read existing CSV for resume: {e}")
    return processed


def open_csv_writer(output_csv: str):
    """Open CSV for append. Write header only if file is new/empty."""
    new_file = not os.path.exists(output_csv) or os.path.getsize(output_csv) == 0
    f = open(output_csv, "a", newline="", encoding="utf-8")
    writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
    if new_file:
        writer.writeheader()
        f.flush()
    return f, writer


def crawl(start_url, max_pages, output_csv, headless=True, wait_ms=3000,
          simulate_profile="basic", auto_consent=True, resume=True,
          capture_all=False, debug_consent=False):

    root_host = urlparse(start_url).netloc
    root_domain = ".".join(root_host.replace("www.", "").split(".")[-2:])
    interactions = SIMULATE_PROFILES.get(simulate_profile, [])

    # Resume support: skip URLs already in the CSV
    already_done = load_processed_pages(output_csv) if resume else set()
    if already_done:
        log(f"Resume: {len(already_done)} pages already in {output_csv}, will skip those")

    visited = set()                      # URLs we've already popped from queue this run
    queued = {start_url}                 # URLs ever added to queue (dedup)
    queue = deque([start_url])
    new_pages_crawled = 0
    total_rows_written = 0

    csv_file, csv_writer = open_csv_writer(output_csv)

    # Optional debug CSV capturing every external request (classified or not)
    debug_file = None
    debug_writer = None
    debug_csv = None
    if capture_all:
        debug_csv = output_csv.replace(".csv", "_allreq.csv")
        if not debug_csv.endswith(".csv"):
            debug_csv = output_csv + "_allreq.csv"
        new_debug = not os.path.exists(debug_csv) or os.path.getsize(debug_csv) == 0
        debug_file = open(debug_csv, "a", newline="", encoding="utf-8")
        debug_fields = ["captured_at", "page_url", "vendor", "host", "method",
                        "resource_type", "request_url"]
        debug_writer = csv.DictWriter(debug_file, fieldnames=debug_fields)
        if new_debug:
            debug_writer.writeheader()
            debug_file.flush()
        log(f"Debug mode on, unclassified external requests will go to {debug_csv}")

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=headless)
            context = browser.new_context(
                viewport={"width": 1440, "height": 900},
                user_agent="Mozilla/5.0 (TagAuditBot/1.0)",
                locale="en-AU",
            )

            # Speed: block heavy resources that never carry tags
            # Images, fonts, video, audio are ~60-80% of page weight and irrelevant to tagging.
            # CSS stays on because consent banners and interactive elements depend on it.
            BLOCKED_TYPES = {"image", "font", "media"}
            def _block_heavy(route, request):
                if request.resource_type in BLOCKED_TYPES:
                    route.abort()
                else:
                    route.continue_()
            if not capture_all:
                # Only block when not debugging. In --capture-all we want to see everything.
                context.route("**/*", _block_heavy)

            consent_accepted_on = {}  # host -> selector

            while queue and new_pages_crawled < max_pages:
                url = queue.popleft()
                if url in visited:
                    continue
                visited.add(url)

                is_already_recorded = url in already_done

                if is_already_recorded:
                    # We've captured this page's tags before. Still load it to
                    # discover links, but don't re-record and don't count toward max.
                    log(f"[skip-recorded] {url} (discovering links only)")
                else:
                    new_pages_crawled += 1
                    log(f"[{new_pages_crawled}/{max_pages}] {url}")

                page = context.new_page()
                current_trigger = {"value": "page_load"}
                page_rows = []  # buffer tag fires for this page
                debug_rows = []  # buffer all external requests this page

                if not is_already_recorded:
                    def on_request(req, page_url=url, trigger_ref=current_trigger,
                                   buf=page_rows, dbg=debug_rows):
                        vendor = classify_vendor(req.url)
                        req_host = urlparse(req.url).netloc
                        is_external = root_domain not in req_host
                        if vendor:
                            try:
                                post_data = req.post_data
                            except Exception:
                                post_data = None
                            params = extract_params(req.url, post_data)
                            buf.append({
                                "captured_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                "page_url": page_url,
                                "trigger": trigger_ref["value"],
                                "vendor": vendor,
                                "event_name": guess_event_name(params),
                                "method": req.method,
                                "host": req_host,
                                "resource_type": req.resource_type,
                                "request_url": req.url,
                                "params_json": json.dumps(params, default=str)[:8000],
                            })
                        # Debug capture: anything external, classified or not
                        if capture_all and (is_external or not vendor):
                            dbg.append({
                                "captured_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                "page_url": page_url,
                                "vendor": vendor or "",
                                "host": req_host,
                                "method": req.method,
                                "resource_type": req.resource_type,
                                "request_url": req.url[:2000],
                            })
                    page.on("request", on_request)

                # Load the page
                try:
                    page.goto(url, wait_until="domcontentloaded", timeout=30000)
                except PWTimeout:
                    log("  load timeout, continuing")
                except Exception as e:
                    log(f"  load error: {e}")
                    page.close()
                    continue

                # Consent handling (only if we're actually recording)
                if auto_consent and not is_already_recorded:
                    host = urlparse(url).netloc
                    if host not in consent_accepted_on:
                        if debug_consent:
                            # First, dump candidates without clicking
                            accept_consent(page, timeout_ms=2500, debug=True)
                        clicked = accept_consent(page, timeout_ms=2500)
                        if clicked:
                            log(f"  consent accepted via: {clicked}")
                            consent_accepted_on[host] = clicked
                            current_trigger["value"] = "post_consent"
                            page.wait_for_timeout(800)
                        else:
                            log("  consent: no matching button found")

                if is_already_recorded:
                    # Quick load: minimal wait just for link discovery
                    page.wait_for_timeout(1500)
                else:
                    # Just a simple wait for tags to fire. Request hook captures
                    # everything regardless of load state.
                    page.wait_for_timeout(wait_ms)

                    # Simulate interactions (only on new pages)
                    for action in interactions:
                        fn = INTERACTION_REGISTRY.get(action)
                        if not fn:
                            continue
                        current_trigger["value"] = action
                        try:
                            result = fn(page)
                            if result:
                                log(f"  {action}: {result}")
                            page.wait_for_timeout(800)
                        except Exception as e:
                            log(f"  {action} failed: {e}")

                    current_trigger["value"] = "post_interaction"

                # Harvest links for queue
                try:
                    for link in extract_internal_links(page, root_host):
                        if link not in visited and link not in queued:
                            queue.append(link)
                            queued.add(link)
                except Exception:
                    pass

                page.close()

                # Flush this page's rows to CSV immediately (only for new pages)
                if page_rows:
                    csv_writer.writerows(page_rows)
                    csv_file.flush()
                    try:
                        os.fsync(csv_file.fileno())
                    except Exception:
                        pass
                    total_rows_written += len(page_rows)
                    log(f"  captured {len(page_rows)} tag fires (total this run: {total_rows_written})")

                # Flush debug rows
                if capture_all and debug_rows and debug_writer is not None:
                    debug_writer.writerows(debug_rows)
                    debug_file.flush()
                    log(f"  debug captured {len(debug_rows)} external requests")

            browser.close()
    finally:
        csv_file.close()
        if debug_file:
            debug_file.close()

    log(f"Done. New pages this run: {new_pages_crawled}. Rows this run: {total_rows_written}. CSV: {output_csv}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("start_url")
    parser.add_argument("--max-pages", type=int, default=50)
    parser.add_argument("--output", default="tag_audit.csv")
    parser.add_argument("--wait-ms", type=int, default=2000,
                        help="Wait after page load for tags to fire, default 2000ms")
    parser.add_argument("--headed", action="store_true")
    parser.add_argument("--simulate", choices=list(SIMULATE_PROFILES.keys()), default="basic")
    parser.add_argument("--no-consent", action="store_true", help="Disable auto consent accept")
    parser.add_argument("--no-resume", action="store_true",
                        help="Do not skip pages already present in the CSV")
    parser.add_argument("--capture-all", action="store_true",
                        help="Write every external request (classified or not) to a debug CSV")
    parser.add_argument("--debug-consent", action="store_true",
                        help="Log candidate consent buttons and their scores before clicking")
    args = parser.parse_args()

    crawl(
        start_url=args.start_url,
        max_pages=args.max_pages,
        output_csv=args.output,
        headless=not args.headed,
        wait_ms=args.wait_ms,
        simulate_profile=args.simulate,
        auto_consent=not args.no_consent,
        resume=not args.no_resume,
        capture_all=args.capture_all,
        debug_consent=args.debug_consent,
    )