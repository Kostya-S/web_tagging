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
import re
from urllib.parse import urlparse, parse_qs
from collections import deque
from playwright.sync_api import sync_playwright, Page, TimeoutError as PWTimeout


# ---------- Vendor detection ----------

VENDOR_RULES = [
    ("Meta Pixel",          r"facebook\.com/tr|connect\.facebook\.net"),
    ("Meta CAPI (client)",  r"graph\.facebook\.com/.*/events"),
    ("GA4",                 r"google-analytics\.com/g/collect|analytics\.google\.com/g/collect"),
    ("Universal Analytics", r"google-analytics\.com/collect|google-analytics\.com/j/collect"),
    ("Google Ads",          r"googleadservices\.com|googleads\.g\.doubleclick\.net"),
    ("Google Tag Manager",  r"googletagmanager\.com/gtm|googletagmanager\.com/gtag"),
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
    ("The Trade Desk",      r"insight\.adsrvr\.org"),
    ("Bing / Microsoft",    r"bat\.bing\.com"),
    ("Yahoo DOT",           r"sp\.analytics\.yahoo\.com"),
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
CONSENT_SELECTORS = [
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
    # Usercentrics
    "button[data-testid='uc-accept-all-button']",
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


def accept_consent(page: Page, timeout_ms: int = 4000) -> str:
    """Try to accept consent. Returns the selector that worked, or empty string."""
    # Try main frame selectors
    for sel in CONSENT_SELECTORS:
        try:
            loc = page.locator(sel).first
            loc.wait_for(state="visible", timeout=timeout_ms)
            loc.click(timeout=1500)
            page.wait_for_timeout(800)
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
                    loc.wait_for(state="visible", timeout=1500)
                    loc.click(timeout=1500)
                    page.wait_for_timeout(800)
                    return f"iframe::{sel}"
                except Exception:
                    continue
    return ""


# ---------- Interaction simulation ----------

def simulate_scroll(page: Page):
    """Slow scroll to trigger lazy tags (scroll depth, viewport-triggered events)."""
    try:
        page.evaluate("""async () => {
            await new Promise(resolve => {
                let y = 0;
                const step = 400;
                const timer = setInterval(() => {
                    window.scrollBy(0, step);
                    y += step;
                    if (y >= document.body.scrollHeight) {
                        clearInterval(timer);
                        resolve();
                    }
                }, 250);
            });
        }""")
        page.wait_for_timeout(800)
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

def crawl(start_url, max_pages, output_csv, headless=True, wait_ms=3000,
          simulate_profile="basic", auto_consent=True):

    root_host = urlparse(start_url).netloc
    visited = set()
    queue = deque([start_url])
    rows = []
    consent_accepted_on = {}  # host -> selector

    interactions = SIMULATE_PROFILES.get(simulate_profile, [])

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context(
            viewport={"width": 1440, "height": 900},
            user_agent="Mozilla/5.0 (TagAuditBot/1.0)",
            locale="en-AU",
        )

        while queue and len(visited) < max_pages:
            url = queue.popleft()
            if url in visited:
                continue
            visited.add(url)
            print(f"[{len(visited)}/{max_pages}] {url}")

            page = context.new_page()
            current_trigger = {"value": "page_load"}  # mutable via closure

            def on_request(req, page_url=url, trigger_ref=current_trigger):
                vendor = classify_vendor(req.url)
                if not vendor:
                    return
                try:
                    post_data = req.post_data
                except Exception:
                    post_data = None
                params = extract_params(req.url, post_data)
                rows.append({
                    "page_url": page_url,
                    "trigger": trigger_ref["value"],
                    "vendor": vendor,
                    "event_name": guess_event_name(params),
                    "method": req.method,
                    "host": urlparse(req.url).netloc,
                    "resource_type": req.resource_type,
                    "request_url": req.url,
                    "params_json": json.dumps(params, default=str)[:8000],
                })

            page.on("request", on_request)

            # Load the page
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=30000)
            except PWTimeout:
                print(f"  load timeout, continuing")
            except Exception as e:
                print(f"  load error: {e}")
                page.close()
                continue

            # Consent handling
            if auto_consent:
                host = urlparse(url).netloc
                if host not in consent_accepted_on:
                    clicked = accept_consent(page)
                    if clicked:
                        print(f"  consent accepted via: {clicked}")
                        consent_accepted_on[host] = clicked
                        current_trigger["value"] = "post_consent"
                        page.wait_for_timeout(1500)  # tags often re-fire after consent

            # Wait for initial tags to settle
            try:
                page.wait_for_load_state("networkidle", timeout=8000)
            except PWTimeout:
                pass
            page.wait_for_timeout(wait_ms)

            # Simulate interactions
            for action in interactions:
                fn = INTERACTION_REGISTRY.get(action)
                if not fn:
                    continue
                current_trigger["value"] = action
                try:
                    result = fn(page)
                    if result:
                        print(f"  {action}: {result}")
                    page.wait_for_timeout(1500)  # let tags fire after interaction
                except Exception as e:
                    print(f"  {action} failed: {e}")

            current_trigger["value"] = "post_interaction"

            # Harvest links for queue
            try:
                for link in extract_internal_links(page, root_host):
                    if link not in visited and link not in queue:
                        queue.append(link)
            except Exception:
                pass

            page.close()

        browser.close()

    fieldnames = ["page_url", "trigger", "vendor", "event_name", "method", "host",
                  "resource_type", "request_url", "params_json"]
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"\nDone. Pages: {len(visited)}. Tag fires: {len(rows)}. CSV: {output_csv}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("start_url")
    parser.add_argument("--max-pages", type=int, default=50)
    parser.add_argument("--output", default="tag_audit.csv")
    parser.add_argument("--wait-ms", type=int, default=3000)
    parser.add_argument("--headed", action="store_true")
    parser.add_argument("--simulate", choices=list(SIMULATE_PROFILES.keys()), default="basic")
    parser.add_argument("--no-consent", action="store_true", help="Disable auto consent accept")
    args = parser.parse_args()

    crawl(
        start_url=args.start_url,
        max_pages=args.max_pages,
        output_csv=args.output,
        headless=not args.headed,
        wait_ms=args.wait_ms,
        simulate_profile=args.simulate,
        auto_consent=not args.no_consent,
    )
