"""
Google search script: for each keyword in keywords_eecglobal.csv, search on Google and click
the result linking to eecglobal.com. One window, one session.
Optional: set ANTICAPTCHA_API_KEY to solve robot verification via Anti-Captcha.
"""

import atexit
import csv
import logging
import os
import random
import shutil
import signal
import sys
import tempfile
import time
import urllib.parse
from dataclasses import dataclass
from datetime import datetime
from enum import Enum, auto
from pathlib import Path

from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth

from anticaptcha_solver import solve_google_recaptcha

# Global flag for graceful shutdown
_shutdown_requested = False


def _signal_handler(signum, frame):
    """Handle SIGINT/SIGTERM for graceful shutdown."""
    global _shutdown_requested
    logger.info("Shutdown requested (signal %d). Finishing current keyword...", signum)
    _shutdown_requested = True

# ---------------------------------------------------------------------------
# Logger — configured in _setup_logging()
# ---------------------------------------------------------------------------
logger = logging.getLogger("google_clicker")

# ---------------------------------------------------------------------------
# Browser profile config
# ---------------------------------------------------------------------------
# Pool of common user agents (Windows/Mac Chrome variants)
USER_AGENT_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
]

# Pool of common viewport sizes
VIEWPORT_POOL = [
    {"width": 1920, "height": 1080},
    {"width": 1366, "height": 768},
    {"width": 1536, "height": 864},
    {"width": 1440, "height": 900},
    {"width": 1680, "height": 1050},
    {"width": 1280, "height": 720},
    {"width": 1600, "height": 900},
    {"width": 1280, "height": 800},
]

# Optional: locale and timezone for the browser (None = use system default)
LOCALE = None  # e.g. "en-US"
TIMEZONE_ID = None  # e.g. "America/New_York"

# ---------------------------------------------------------------------------
DEFAULT_KEYWORDS_CSV = Path(__file__).resolve().parent / "keywords_eecglobal.csv"
RESULTS_LOG_CSV = Path(__file__).resolve().parent / "results_log_eecglobal.csv"
# When all keywords are done, result log is moved here so the next run starts fresh
RESULTS_ARCHIVE_DIR = Path(__file__).resolve().parent / "Report_eecglobal"
VERIFICATION_WAIT_TIMEOUT_MS = 120_000
# Default timeout for all Playwright operations (prevents indefinite hangs).
# Long stalls can also be caused by: Chrome/GPU freeze (timeouts may not fire),
# or Anti-Captcha API taking very long; consider an external watchdog (e.g. restart after N hours).
DEFAULT_OPERATION_TIMEOUT_MS = 90_000
# Retry Anti-Captcha this many times on failure (e.g. ERROR_FAILED_LOADING_WIDGET)
ANTICAPTCHA_MAX_ATTEMPTS = 3
ANTICAPTCHA_RETRY_DELAY_SEC = (5, 10)  # min, max seconds between attempts
# Max solve cycles: solve → check page → if still captcha solve again.
SORRY_PAGE_MAX_SOLVE_CYCLES = 3
# Short wait after each token submit to let the page update or redirect
POST_SUBMIT_WAIT_SEC = (5, 8)
# When no results + captcha: after submitting token, wait up to this long and check every minute
POST_CAPTCHA_WAIT_MAX_SEC = 15 * 60
CAPTCHA_CHECK_INTERVAL_SEC = 60
# One-off wait for result selector right after token submit
ONE_OFF_WAIT_FOR_RESULTS_SEC = 90
ONE_OFF_CHECK_INTERVAL_SEC = 5
TARGET_DOMAIN = "eecglobal.com"
MAX_RESULT_PAGES = 50

# Delays: between result pages and between keywords
DELAY_BETWEEN_PAGES_SEC = (3.0, 5.0)  # min, max seconds between pages
DELAY_BETWEEN_KEYWORDS_SEC = (30, 60)  # min, max seconds between keywords

# Non-target click settings
NON_TARGET_CLICK_PROBABILITY = 0.25  # 25% chance of clicking a random non-target result
NON_TARGET_DWELL_SEC = (3, 8)  # how long to stay on the non-target page

LOG_HEADER = ["keyword", "page_num", "position", "timestamp"]

# Anti-Captcha: env wins; if unset, this fallback is used (set to "" to disable).
ANTICAPTCHA_API_KEY_FALLBACK = "64bd9cd5c306974febf3847e0dab53c4"


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------
class SearchOutcome(Enum):
    """Outcome signals for search/captcha operations."""
    REOPEN_NEEDED = auto()


# ---------------------------------------------------------------------------
# Setup helpers
# ---------------------------------------------------------------------------
def _setup_logging() -> None:
    """Configure logging with console and file handlers."""
    logger.setLevel(logging.DEBUG)

    # Console handler — INFO level
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter("%(asctime)s  %(levelname)-7s  %(message)s"))
    logger.addHandler(console)

    # File handler — DEBUG level for full detail
    log_file = Path(__file__).resolve().parent / "google_clicker_eecglobal.log"
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s  %(levelname)-7s  %(message)s")
    )
    logger.addHandler(file_handler)


def _pick_random_ua() -> str:
    """Select a random user agent from the pool."""
    return random.choice(USER_AGENT_POOL)


def _pick_random_viewport() -> dict:
    """Select a random viewport size from the pool."""
    return random.choice(VIEWPORT_POOL)


def _create_temp_profile() -> Path:
    """Create a temporary browser profile directory and register cleanup on exit."""
    tmp_dir = Path(tempfile.mkdtemp(prefix="gclicker_profile_"))

    def _cleanup():
        try:
            shutil.rmtree(tmp_dir, ignore_errors=True)
        except Exception:
            pass

    atexit.register(_cleanup)
    return tmp_dir


def get_anticaptcha_api_key() -> str:
    """Return Anti-Captcha API key: env ANTICAPTCHA_API_KEY if set, else hardcoded fallback."""
    return (os.environ.get("ANTICAPTCHA_API_KEY", "") or ANTICAPTCHA_API_KEY_FALLBACK).strip()


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------
@dataclass
class SearchResult:
    title: str
    url: str
    snippet: str = ""


# ---------------------------------------------------------------------------
# CSV helpers
# ---------------------------------------------------------------------------
def log_result(keyword: str, page_num: int | str, position: int | str) -> None:
    """Append one row to results_log_eecglobal.csv. Creates file with header if needed."""
    path = RESULTS_LOG_CSV
    write_header = not path.exists() or path.stat().st_size == 0
    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow(LOG_HEADER)
        writer.writerow([keyword, page_num, position, datetime.now().isoformat()])


def load_keywords(csv_path: Path | str) -> list[str]:
    """Load keywords from CSV (one per line or one column)."""
    path = Path(csv_path)
    if not path.exists():
        return []
    keywords: list[str] = []
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        for row in reader:
            if not row:
                continue
            cell = row[0].strip() if row[0] else ""
            if cell and cell.lower() != "keyword":
                keywords.append(cell)
    return keywords


def move_result_log_to_archive() -> None:
    """When all keywords are done, move results_log_eecglobal.csv to Report_eecglobal/ with a timestamp so next run starts fresh."""
    path = RESULTS_LOG_CSV
    if not path.exists() or path.stat().st_size == 0:
        return
    RESULTS_ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    archive_name = f"results_log_eecglobal_{timestamp}.csv"
    dest = RESULTS_ARCHIVE_DIR / archive_name
    try:
        shutil.move(str(path), str(dest))
        logger.info("Moved result log to %s (next run will start fresh)", dest)
    except Exception as e:
        logger.warning("Could not move result log to archive: %s", e)


def get_completed_keywords() -> set[str]:
    """Return the set of keyword strings already logged in results_log_eecglobal.csv (for resume by name)."""
    path = RESULTS_LOG_CSV
    if not path.exists() or path.stat().st_size == 0:
        return set()
    completed: set[str] = set()
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        first = True
        for row in reader:
            if not row:
                continue
            if first and row and str(row[0]).strip().lower() == "keyword":
                first = False
                continue
            completed.add(str(row[0]).strip())
    return completed


# ---------------------------------------------------------------------------
# Captcha / verification handling
# ---------------------------------------------------------------------------
def handle_sorry_page(page, context) -> bool | SearchOutcome:
    """
    When on a Google captcha page: send to Anti-Captcha, submit token, then check the page.
    If the page is still a captcha, submit again. Repeat up to SORRY_PAGE_MAX_SOLVE_CYCLES times.
    Returns True when we're past the captcha; SearchOutcome.REOPEN_NEEDED on failure.
    """
    logger.warning('Robot verification page detected ("I\'m not a robot").')
    api_key = get_anticaptcha_api_key()
    if not api_key:
        logger.error("ANTICAPTCHA_API_KEY not set.")
        try:
            context.close()
        except Exception:
            pass
        return SearchOutcome.REOPEN_NEEDED

    for cycle in range(1, SORRY_PAGE_MAX_SOLVE_CYCLES + 1):
        logger.info(
            "Solve cycle %d/%d: sending page to Anti-Captcha...",
            cycle,
            SORRY_PAGE_MAX_SOLVE_CYCLES,
        )
        solved = False
        for attempt in range(1, ANTICAPTCHA_MAX_ATTEMPTS + 1):
            logger.debug(
                "Anti-Captcha attempt %d/%d...", attempt, ANTICAPTCHA_MAX_ATTEMPTS
            )
            if solve_google_recaptcha(page, api_key):
                solved = True
                break
            if attempt < ANTICAPTCHA_MAX_ATTEMPTS:
                delay = random.uniform(*ANTICAPTCHA_RETRY_DELAY_SEC)
                logger.info("Attempt failed. Retrying in %.0fs...", delay)
                time.sleep(delay)
        if not solved:
            logger.error(
                "Anti-Captcha failed to solve captcha for cycle %d (no token submitted).",
                cycle,
            )
            if cycle < SORRY_PAGE_MAX_SOLVE_CYCLES:
                logger.info("Trying next cycle...")
                continue
            logger.error(
                "Anti-Captcha failed after all cycles. Closing browser and reopening."
            )
            try:
                context.close()
            except Exception:
                pass
            return SearchOutcome.REOPEN_NEEDED

        # Token was submitted; wait and check the page
        wait_sec = random.uniform(*POST_SUBMIT_WAIT_SEC)
        logger.info("Token submitted. Waiting %.0fs for page to update...", wait_sec)
        time.sleep(wait_sec)

        is_captcha, reason = _robust_is_captcha_page_with_reason(page)
        if not is_captcha:
            logger.info("Verification passed (%s). Continuing.", reason)
            return True
        if cycle < SORRY_PAGE_MAX_SOLVE_CYCLES:
            logger.info("Page is still a captcha (%s). Submitting again...", reason)

    logger.error(
        "Still on captcha after %d cycles. Closing browser and reopening...",
        SORRY_PAGE_MAX_SOLVE_CYCLES,
    )
    try:
        context.close()
    except Exception:
        pass
    return SearchOutcome.REOPEN_NEEDED


def handle_captcha_when_no_results(page, context) -> bool | SearchOutcome:
    """
    Called when we have no results (after search or after next page). Solve captcha once,
    then wait up to 15 minutes, checking every 1 minute.
    Returns True to proceed, SearchOutcome.REOPEN_NEEDED on failure.
    """
    api_key = get_anticaptcha_api_key()
    if not api_key:
        logger.error("Anti-Captcha API key not set. Cannot solve captcha.")
        try:
            context.close()
        except Exception:
            pass
        return SearchOutcome.REOPEN_NEEDED

    logger.info("No results and robust check: captcha page. Sending to Anti-Captcha...")
    for attempt in range(1, ANTICAPTCHA_MAX_ATTEMPTS + 1):
        if solve_google_recaptcha(page, api_key):
            break
        if attempt < ANTICAPTCHA_MAX_ATTEMPTS:
            delay = random.uniform(*ANTICAPTCHA_RETRY_DELAY_SEC)
            logger.info("Attempt failed. Retrying in %.0fs...", delay)
            time.sleep(delay)
    else:
        logger.error("Anti-Captcha failed to solve. Closing browser and reopening...")
        try:
            context.close()
        except Exception:
            pass
        return SearchOutcome.REOPEN_NEEDED

    url_after_submit = page.url or ""
    deadline = time.monotonic() + POST_CAPTCHA_WAIT_MAX_SEC
    check_count = 0

    # One-off wait: check for result selector every few seconds
    logger.info(
        "Token submitted. One-off wait for result links (check every %ds for up to %ds)...",
        ONE_OFF_CHECK_INTERVAL_SEC,
        ONE_OFF_WAIT_FOR_RESULTS_SEC,
    )
    one_off_deadline = time.monotonic() + ONE_OFF_WAIT_FOR_RESULTS_SEC
    while time.monotonic() < one_off_deadline:
        if _page_has_result_links(page):
            logger.info(
                "[Detection] result links appeared (one-off wait). Proceeding."
            )
            return True
        # Also check if we've left the captcha entirely (e.g. error page, redirect)
        if not _robust_is_captcha_page(page):
            logger.info(
                "[Detection] no longer on captcha page (one-off wait). Proceeding."
            )
            return True
        time.sleep(ONE_OFF_CHECK_INTERVAL_SEC)
    logger.info(
        "One-off wait ended. Will check every %ds for up to %d min...",
        CAPTCHA_CHECK_INTERVAL_SEC,
        POST_CAPTCHA_WAIT_MAX_SEC // 60,
    )

    while time.monotonic() < deadline:
        time.sleep(CAPTCHA_CHECK_INTERVAL_SEC)
        check_count += 1
        current_url = page.url or ""
        is_captcha, reason = _robust_is_captcha_page_with_reason(page)

        if not is_captcha:
            logger.info("[Detection] not captcha: %s. Proceeding.", reason)
            return True

        logger.info(
            "[Detection] still captcha: %s. (%d min elapsed)", reason, check_count
        )
        if current_url != url_after_submit:
            logger.info(
                "Different URL and still captcha (new captcha). Submitting to resolve..."
            )
            for attempt in range(1, ANTICAPTCHA_MAX_ATTEMPTS + 1):
                if solve_google_recaptcha(page, api_key):
                    url_after_submit = page.url or ""
                    logger.info(
                        "New captcha token submitted. Resuming wait (check every %ds)...",
                        CAPTCHA_CHECK_INTERVAL_SEC,
                    )
                    break
                if attempt < ANTICAPTCHA_MAX_ATTEMPTS:
                    time.sleep(random.uniform(*ANTICAPTCHA_RETRY_DELAY_SEC))
            else:
                logger.warning("Failed to solve new captcha. Continuing to wait...")
                url_after_submit = current_url

    is_captcha_final, reason_final = _robust_is_captcha_page_with_reason(page)
    if not is_captcha_final:
        logger.info("[Detection] not captcha: %s. Proceeding.", reason_final)
        return True
    logger.error(
        "[Detection] still captcha: %s. Timeout (15 min). Closing browser and reopening...",
        reason_final,
    )
    try:
        context.close()
    except Exception:
        pass
    return SearchOutcome.REOPEN_NEEDED


def handle_google_verification(page, context):
    """If on consent or robot verification page, handle it. Returns handle_sorry_page result if called."""
    time.sleep(random.uniform(2, 4))
    if "consent.google" in page.url:
        logger.info("Cookie consent page. Clicking 'Accept all'...")
        try:
            page.get_by_role("button", name="Accept all").click(timeout=4000)
            time.sleep(random.uniform(1.5, 3))
            logger.info("Consent accepted.")
        except Exception:
            logger.debug("Could not find Accept button (page may have changed).")
    if "sorry" in page.url or _page_looks_like_verification(page):
        return handle_sorry_page(page, context)


def ensure_google_ready(page, context):
    """Navigate to Google and handle consent/sorry once. Returns SearchOutcome.REOPEN_NEEDED if caller should reopen."""
    logger.info("Loading Google homepage...")
    try:
        page.goto(
            "https://www.google.com/?nfpr=1",
            wait_until="domcontentloaded",
            timeout=20000,
        )
    except Exception as e:
        logger.error("Failed to load Google: %s. Need browser reopen.", e)
        try:
            context.close()
        except Exception:
            pass
        return SearchOutcome.REOPEN_NEEDED
    result = handle_google_verification(page, context)
    if result is SearchOutcome.REOPEN_NEEDED:
        return SearchOutcome.REOPEN_NEEDED
    logger.info("Google ready.")


# ---------------------------------------------------------------------------
# Page detection helpers
# ---------------------------------------------------------------------------
def _page_looks_like_verification(page) -> bool:
    """True if the current page looks like Google's robot verification (URL or content)."""
    url = page.url or ""
    if "sorry" in url:
        return True
    try:
        content = page.content()
        return (
            "unusual traffic" in content.lower()
            or "not a robot" in content.lower()
            or "really you sending the requests" in content.lower()
            or "recaptcha" in content.lower()
            or "g-recaptcha" in content
        )
    except Exception:
        return False


def _page_has_result_links(page) -> bool:
    """True if #search or #rso contains at least one external (non-Google) result link."""
    for area in ["#search", "#rso"]:
        try:
            links = page.locator(f"{area} a[href^='http']").all()
            for link in links[:15]:
                try:
                    href = link.get_attribute("href")
                    if href and not href.startswith("https://www.google."):
                        return True
                except Exception:
                    continue
        except Exception:
            continue
    return False


def _robust_is_captcha_page_with_reason(page) -> tuple[bool, str]:
    """Robust captcha detection with reason string. Returns (is_captcha, reason)."""
    try:
        if _page_has_result_links(page):
            return (False, "result links found in #search/#rso")
        try:
            if page.locator('iframe[src*="recaptcha"]').first.is_visible(timeout=400):
                return (True, "reCAPTCHA iframe visible")
        except Exception:
            pass
        try:
            if page.locator(
                'textarea[name="g-recaptcha-response"]'
            ).first.is_visible(timeout=400):
                return (True, "g-recaptcha-response textarea visible")
        except Exception:
            pass
        url = page.url or ""
        if "sorry" in url:
            time.sleep(0.5)
            if _page_has_result_links(page):
                return (False, "result links found (URL still sorry)")
            return (True, "URL contains 'sorry' and no result links")
        content = page.content()
        if (
            "unusual traffic" in content.lower()
            or "not a robot" in content.lower()
            or "really you sending the requests" in content.lower()
        ):
            return (True, "verification text in page content")
        return (False, "no captcha signals")
    except Exception:
        return (_page_looks_like_verification(page), "fallback check (exception)")


def _robust_is_captcha_page(page) -> bool:
    """Robust captcha detection. Returns True only when clearly on a captcha page."""
    is_captcha, _ = _robust_is_captcha_page_with_reason(page)
    return is_captcha


def _is_target_link_an_ad(page, link_locator) -> bool:
    """Return True if the given link (target domain link) is inside an ad/sponsored block."""
    try:
        return link_locator.evaluate("""
            (el) => {
                const block = el.closest('div.g');
                if (!block) return false;
                const text = (block.innerText || '').toLowerCase();
                return /\\b(ad|sponsored)\\b/.test(text);
            }
        """)
    except Exception:
        return False


# ---------------------------------------------------------------------------
# SERP result extraction
# ---------------------------------------------------------------------------
def _extract_results_on_page(
    page, target_domain: str
) -> tuple[list[SearchResult], int | None]:
    """Extract result links on current page in SERP order; return (results_list, 1-based position of target_domain or None)."""
    results_list: list[SearchResult] = []
    seen_urls: set[str] = set()

    # Prefer organic result blocks (div.g) so order = actual result order on page
    for block_selector in ["#rso div.g", "#search div.g"]:
        try:
            blocks = page.locator(block_selector).all()
            for block in blocks:
                try:
                    link = block.locator("a[href^='http']").first
                    href = link.get_attribute("href") if link else None
                    if (
                        not href
                        or href.startswith("https://www.google.")
                        or href in seen_urls
                    ):
                        continue
                    seen_urls.add(href)
                    title = ""
                    try:
                        title = (link.inner_text() or "").strip()[:200]
                    except Exception:
                        pass
                    results_list.append(
                        SearchResult(title=title, url=href, snippet="")
                    )
                except Exception:
                    continue
            if results_list:
                break
        except Exception:
            continue

    # Fallback: collect all external links in DOM order
    if not results_list:
        for area in ["#search", "#rso"]:
            try:
                links = page.locator(f"{area} a[href^='http']").all()
            except Exception:
                continue
            for link in links:
                try:
                    href = link.get_attribute("href")
                    if not href or href.startswith("https://www.google."):
                        continue
                    if href in seen_urls:
                        continue
                    seen_urls.add(href)
                    title = ""
                    try:
                        title = (link.inner_text() or "").strip()[:200]
                    except Exception:
                        pass
                    results_list.append(
                        SearchResult(title=title, url=href, snippet="")
                    )
                except Exception:
                    continue
            if results_list:
                break

    # Position = 1-based index of the result containing target_domain
    position_on_page: int | None = None
    for i, r in enumerate(results_list, 1):
        if target_domain in r.url:
            position_on_page = i
            break
    return results_list, position_on_page


# ---------------------------------------------------------------------------
# Human-like behavior helpers
# ---------------------------------------------------------------------------
def _scroll_page_naturally(page, duration_sec: float) -> None:
    """Scroll the page with natural, variable behavior to emulate reading.

    - Random scroll depth (60–100% of page height)
    - Variable step sizes
    - Occasional scroll-back-up (~30% chance)
    - Random pauses mid-scroll (~20% chance per step)
    """
    try:
        total_height = page.evaluate("document.documentElement.scrollHeight")
        viewport_height = page.evaluate("window.innerHeight")
        max_scroll = total_height - viewport_height
        if max_scroll <= 0:
            return

        # Random target depth: 60–100% of scrollable area
        target_scroll = max_scroll * random.uniform(0.6, 1.0)
        scrolled = 0.0

        steps = max(4, min(20, int(duration_sec * 2.5)))
        step_time_base = duration_sec / steps

        for i in range(steps):
            if scrolled >= target_scroll:
                break

            # Occasional scroll-back-up (~30% chance, not on first or last step)
            if 1 < i < steps - 1 and random.random() < 0.3:
                scroll_back = random.randint(100, 300)
                page.evaluate(f"window.scrollBy(0, -{scroll_back})")
                time.sleep(random.uniform(0.3, 0.8))

            # Variable step size
            remaining = target_scroll - scrolled
            avg_step = remaining / max(1, steps - i)
            step_amount = avg_step * random.uniform(0.5, 1.5)
            step_amount = min(step_amount, remaining)

            page.evaluate(f"window.scrollBy(0, {step_amount})")
            scrolled += step_amount

            # Random pause (~20% chance) to emulate reading
            if random.random() < 0.2:
                time.sleep(random.uniform(0.5, 2.0))
            else:
                time.sleep(step_time_base * random.uniform(0.6, 1.4))

    except Exception:
        pass


def _click_random_non_target(page, results_list: list[SearchResult], target_domain: str) -> None:
    """Click a random non-target result, dwell on it, then navigate back.

    Called with NON_TARGET_CLICK_PROBABILITY chance to make sessions look more natural.
    """
    # Collect non-target, non-Google results
    non_targets = [
        r for r in results_list
        if target_domain not in r.url and "google." not in r.url
    ]
    if not non_targets:
        logger.debug("No non-target results to click.")
        return

    chosen = random.choice(non_targets)
    logger.info("Non-target click: visiting %s", chosen.url[:80])

    try:
        # Use double-quoted attribute selector to safely match URLs with special chars
        link = page.locator(f"a[href=\"{chosen.url}\"]").first
        if not link.is_visible(timeout=2000) and chosen.title:
            # Fallback: find link by its title text
            link = page.locator("a[href^='http']").filter(has_text=chosen.title[:40]).first
        link.scroll_into_view_if_needed(timeout=5000)
        time.sleep(random.uniform(0.3, 0.8))
        link.click(timeout=5000)

        # Dwell on the non-target page
        dwell = random.uniform(*NON_TARGET_DWELL_SEC)
        logger.debug("Dwelling on non-target page for %.1fs...", dwell)
        time.sleep(dwell * 0.3)
        # Do some scrolling on the page
        _scroll_page_naturally(page, dwell * 0.7)

        # Go back to the SERP
        page.go_back(wait_until="domcontentloaded", timeout=15000)
        time.sleep(random.uniform(1, 3))
        logger.debug("Returned to SERP after non-target click.")
    except Exception as e:
        logger.debug("Non-target click failed: %s", e)
        try:
            page.go_back(wait_until="domcontentloaded", timeout=10000)
            time.sleep(random.uniform(1, 2))
        except Exception:
            pass


def _goto_next_results_page(page, current_page_one_based: int) -> bool:
    """Navigate to the next Google results page."""
    try:
        parsed = urllib.parse.urlparse(page.url)
        qs = urllib.parse.parse_qs(parsed.query)
        qs["start"] = [str(current_page_one_based * 10)]
        new_query = urllib.parse.urlencode(qs, doseq=True)
        next_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}?{new_query}"
        page.goto(next_url, wait_until="domcontentloaded", timeout=20000)
        time.sleep(random.uniform(1.5, 3))
        page.wait_for_selector("#search, #rso, [role='main']", timeout=25_000)
        time.sleep(random.uniform(1, 2))
        return True
    except Exception:
        return False


def _type_query_like_human(page, query: str) -> None:
    """Type the search query with variable delay. Includes a single typo correction."""
    correction_done = False
    i = 0
    can_correct_after = 4
    must_leave_after = len(query) - 2
    correction_at = (
        random.randint(can_correct_after, max(can_correct_after, must_leave_after))
        if must_leave_after >= can_correct_after
        else -1
    )

    while i < len(query):
        if not correction_done and correction_at >= 0 and i == correction_at:
            n = random.randint(2, min(5, i))
            for _ in range(n):
                page.keyboard.press("Backspace")
            time.sleep(random.uniform(0.1, 0.25))
            for j in range(n):
                page.keyboard.type(query[i - n + j], delay=random.randint(70, 160))
                time.sleep(random.uniform(0.02, 0.06))
            correction_done = True
        page.keyboard.type(query[i], delay=random.randint(80, 180))
        time.sleep(random.uniform(0.02, 0.06))
        i += 1


# ---------------------------------------------------------------------------
# Core search logic
# ---------------------------------------------------------------------------
def run_one_search(
    page, context, query: str, target_domain: str
) -> tuple[int, int] | SearchOutcome | None:
    """Search for query, paginate results, find and click target_domain.

    Returns (page_num, position_on_page) on success, SearchOutcome.REOPEN_NEEDED,
    or None if not found.
    """
    search_selectors = [
        'textarea[name="q"]',
        'input[name="q"]',
        '[aria-label="Search"]',
    ]
    search_box = None
    for sel in search_selectors:
        try:
            search_box = page.locator(sel).first
            if search_box.is_visible(timeout=2000):
                break
        except Exception:
            continue
    try:
        if not search_box or not search_box.is_visible():
            return None
    except Exception:
        return None

    search_box.click()
    time.sleep(random.uniform(0.4, 0.9))
    _type_query_like_human(page, query)
    time.sleep(random.uniform(0.3, 0.7))
    page.keyboard.press("Enter")
    try:
        page.wait_for_url(
            lambda url: (
                "google.com/search" in url
                or "sorry" in url
                or "consent.google" in url
            ),
            timeout=20_000,
        )
    except Exception:
        pass
    time.sleep(random.uniform(1, 2))

    results_selector = "#search, #rso, [role='main']"
    if "sorry" in page.url or _page_looks_like_verification(page):
        logger.warning("Robot verification appeared after search. Handling...")
        result = handle_sorry_page(page, context)
        if result is SearchOutcome.REOPEN_NEEDED:
            return SearchOutcome.REOPEN_NEEDED
        if result is not True:
            return None
        time.sleep(random.uniform(1, 2))
    if "consent.google" in page.url:
        logger.info("Cookie consent appeared. Accepting...")
        try:
            page.get_by_role("button", name="Accept all").click(timeout=4000)
            time.sleep(random.uniform(1.5, 3))
        except Exception:
            pass
    max_verification_retries = 3
    for _ in range(max_verification_retries):
        try:
            page.wait_for_selector(results_selector, timeout=25_000)
        except Exception:
            if _page_looks_like_verification(page):
                logger.warning(
                    "Results did not load; verification page detected. Handling..."
                )
                result = handle_sorry_page(page, context)
                if result is SearchOutcome.REOPEN_NEEDED:
                    return SearchOutcome.REOPEN_NEEDED
                if result is not True:
                    return None
                time.sleep(random.uniform(1, 2))
                continue
            return None
        time.sleep(random.uniform(1, 2))
        # Even if selector is present, page might be verification
        if _page_looks_like_verification(page):
            logger.warning("Verification content detected on page. Solving...")
            result = handle_sorry_page(page, context)
            if result is SearchOutcome.REOPEN_NEEDED:
                return SearchOutcome.REOPEN_NEEDED
            if result is not True:
                return None
            time.sleep(random.uniform(1, 2))
            continue
        break
    else:
        return None
    time.sleep(random.uniform(1, 2))

    for page_num in range(1, MAX_RESULT_PAGES + 1):
        results_list, position_on_page = _extract_results_on_page(page, target_domain)
        if not results_list:
            if _robust_is_captcha_page(page):
                result = handle_captcha_when_no_results(page, context)
                if result is SearchOutcome.REOPEN_NEEDED:
                    return SearchOutcome.REOPEN_NEEDED
                time.sleep(random.uniform(1, 2))
                results_list, position_on_page = _extract_results_on_page(
                    page, target_domain
                )
            if not results_list:
                break
        if position_on_page is not None:
            # Optionally click a non-target result first to look more natural
            if random.random() < NON_TARGET_CLICK_PROBABILITY:
                _click_random_non_target(page, results_list, target_domain)
                # Verify we're back on the SERP after the non-target detour
                current_url = page.url or ""
                if "google." not in current_url:
                    logger.warning(
                        "Not back on Google SERP after non-target click. Skipping target click on this page."
                    )
                    continue

            # Click the target (skip if it is an ad)
            link_to_click = page.locator(f"a[href*='{target_domain}']").first
            if _is_target_link_an_ad(page, link_to_click):
                logger.info("Target result is an ad, skipping (will try next page if any).")
            else:
                try:
                    link_to_click.scroll_into_view_if_needed(timeout=5000)
                    time.sleep(random.uniform(0.2, 0.5))
                    link_to_click.click(timeout=5000)
                    time.sleep(random.uniform(2, 4))
                    return (page_num, position_on_page)
                except Exception:
                    pass

        if page_num < MAX_RESULT_PAGES:
            delay_sec = random.uniform(*DELAY_BETWEEN_PAGES_SEC)
            _scroll_page_naturally(page, delay_sec)
            if not _goto_next_results_page(page, page_num):
                return None

    return None


# ---------------------------------------------------------------------------
# Browser launch
# ---------------------------------------------------------------------------
def _launch_browser(p, profile_dir: Path, launch_options: dict, stealth: Stealth):
    """Launch persistent context and return (context, page)."""
    try:
        context = p.chromium.launch_persistent_context(str(profile_dir), **launch_options)
        # Prevent indefinite hangs: cap all operations (content(), wait_for_selector, etc.)
        context.set_default_timeout(DEFAULT_OPERATION_TIMEOUT_MS)
        context.set_default_navigation_timeout(DEFAULT_OPERATION_TIMEOUT_MS)
        stealth.apply_stealth_sync(context)
        page = context.new_page()
        return context, page
    except Exception as e:
        logger.error("Failed to launch browser: %s", e)
        raise


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    _setup_logging()

    keywords = load_keywords(DEFAULT_KEYWORDS_CSV)
    if not keywords:
        logger.error("No keywords found in keywords_eecglobal.csv")
        return

    completed = get_completed_keywords()
    keywords_to_do = [kw for kw in keywords if kw not in completed]
    if not keywords_to_do:
        logger.info("All keywords already processed (results_log_eecglobal.csv is up to date).")
        return

    logger.info("Loaded %d keywords from keywords_eecglobal.csv", len(keywords))
    if completed:
        logger.info(
            "Resuming: %d already done, %d remaining.",
            len(completed),
            len(keywords_to_do),
        )
    logger.info("Target: click result with %s", TARGET_DOMAIN)
    if get_anticaptcha_api_key():
        logger.info("Anti-Captcha: API key set (auto-solve enabled).")
    else:
        logger.warning(
            "Anti-Captcha: no API key (you will need to solve verification manually)."
        )

    # Create a fresh temp profile for this run
    profile_dir = _create_temp_profile()
    # Randomize browser fingerprint
    user_agent = _pick_random_ua()
    viewport = _pick_random_viewport()
    logger.info("Browser profile: %s", profile_dir)
    logger.info("User-Agent: %s", user_agent)
    logger.info("Viewport: %dx%d", viewport["width"], viewport["height"])

    stealth = Stealth()
    launch_options: dict = {
        "channel": "chrome",
        "headless": False,
        "viewport": viewport,
        "args": ["--no-sandbox"],
        "user_agent": user_agent,
    }
    if LOCALE is not None:
        launch_options["locale"] = LOCALE
    if TIMEZONE_ID is not None:
        launch_options["timezone_id"] = TIMEZONE_ID

    logger.info("[1/2] Launching browser...")
    with sync_playwright() as p:
        context, page = _launch_browser(p, profile_dir, launch_options, stealth)
        logger.info("[2/2] Loading Google...")
        while True:
            need_reopen = ensure_google_ready(page, context)
            if need_reopen is not SearchOutcome.REOPEN_NEEDED:
                break
            logger.info("Reopening browser...")
            context, page = _launch_browser(p, profile_dir, launch_options, stealth)
            logger.info("[2/2] Loading Google...")

        total_keywords = len(completed) + len(keywords_to_do)
        for idx, query in enumerate(keywords_to_do):
            # Check for shutdown request before processing each keyword
            if _shutdown_requested:
                logger.info("Shutdown requested. Exiting after current keyword.")
                break
            keyword_num = len(completed) + idx + 1
            logger.info(
                "---- Keyword %d/%d ----", keyword_num, total_keywords
            )
            logger.info(
                "Searching: %s", query[:60] + ("..." if len(query) > 60 else "")
            )
            result = run_one_search(page, context, query, TARGET_DOMAIN)
            search_used = query
            if result is SearchOutcome.REOPEN_NEEDED:
                logger.info("Reopening browser and retrying keyword...")
                context, page = _launch_browser(
                    p, profile_dir, launch_options, stealth
                )
                logger.info("Loading Google...")
                while True:
                    need_reopen = ensure_google_ready(page, context)
                    if need_reopen is not SearchOutcome.REOPEN_NEEDED:
                        break
                    context, page = _launch_browser(
                        p, profile_dir, launch_options, stealth
                    )
                result = run_one_search(page, context, query, TARGET_DOMAIN)
                search_used = query
                # If the retry also needs a reopen, restore the browser now
                if result is SearchOutcome.REOPEN_NEEDED:
                    logger.warning("Retry also failed. Reopening browser for next keyword...")
                    context, page = _launch_browser(
                        p, profile_dir, launch_options, stealth
                    )
                    while True:
                        need_reopen = ensure_google_ready(page, context)
                        if need_reopen is not SearchOutcome.REOPEN_NEEDED:
                            break
                        context, page = _launch_browser(
                            p, profile_dir, launch_options, stealth
                        )
            if (
                result is not None
                and result is not SearchOutcome.REOPEN_NEEDED
            ):
                page_num, pos = result
                logger.info(
                    "Clicked page %d, result #%d (%s)", page_num, pos, TARGET_DOMAIN
                )
                log_result(search_used, page_num, pos)
            else:
                logger.info("Not found (skipping to next keyword)")
                log_result(query, "", "not_found")

            if idx < len(keywords_to_do) - 1:
                delay = random.uniform(*DELAY_BETWEEN_KEYWORDS_SEC)
                logger.info("Waiting %.0fs before next keyword...", delay)
                time.sleep(delay)
                try:
                    page.goto(
                        "https://www.google.com/?nfpr=1",
                        wait_until="domcontentloaded",
                        timeout=20000,
                    )
                except Exception as e:
                    logger.warning("Failed to navigate to Google: %s. Reopening browser...", e)
                    context, page = _launch_browser(
                        p, profile_dir, launch_options, stealth
                    )
                    while True:
                        need_reopen = ensure_google_ready(page, context)
                        if need_reopen is not SearchOutcome.REOPEN_NEEDED:
                            break
                        context, page = _launch_browser(
                            p, profile_dir, launch_options, stealth
                        )
                    continue
                ver = handle_google_verification(page, context)
                if ver is SearchOutcome.REOPEN_NEEDED:
                    logger.info("Reopening browser...")
                    context, page = _launch_browser(
                        p, profile_dir, launch_options, stealth
                    )
                    while True:
                        need_reopen = ensure_google_ready(page, context)
                        if need_reopen is not SearchOutcome.REOPEN_NEEDED:
                            break
                        context, page = _launch_browser(
                            p, profile_dir, launch_options, stealth
                        )

        logger.info("Done. All keywords processed.")
        move_result_log_to_archive()
        context.close()


if __name__ == "__main__":
    main()
