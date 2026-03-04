"""
Unified Google search clicker: reads keywords_unified.csv (keyword + target_domain per row),
searches each on Google, and clicks the corresponding target result.

Runs two browser instances in parallel (configurable via NUM_WORKERS env var), each
with a unique fingerprint.  Resumes from where it left off via results_log_unified.csv.

Optional: set ANTICAPTCHA_API_KEY to solve robot verification via Anti-Captcha.
"""

import atexit
import csv
import logging
import os
import random
import shutil
import signal
import tempfile
import threading
import time
import traceback
import urllib.parse
from dataclasses import dataclass
from datetime import datetime
from enum import Enum, auto
from pathlib import Path

from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth

from anticaptcha_solver import solve_google_recaptcha

# ---------------------------------------------------------------------------
# Global shutdown event — set by signal handler, checked throughout
# ---------------------------------------------------------------------------
_shutdown_event = threading.Event()


def _signal_handler(signum=None, frame=None):
    """Handle SIGINT/SIGTERM: kill browsers and force-stop immediately."""
    print("\nCtrl+C received — killing browsers and stopping...", flush=True)
    _shutdown_event.set()
    import psutil
    curr_proc = psutil.Process()
    for child in curr_proc.children(recursive=True):
        try:
            name = child.name().lower()
            if "chrome" in name or "chromium" in name or "node" in name:
                child.kill()
        except psutil.NoSuchProcess:
            pass
    os._exit(1)


def _shutdown_requested() -> bool:
    return _shutdown_event.is_set()


def interruptible_sleep(seconds: float, interval: float = 0.5) -> bool:
    """Sleep for `seconds`, waking every `interval` to check for shutdown.
    Returns True if interrupted (shutdown requested), False if full sleep completed."""
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        remaining = deadline - time.monotonic()
        _shutdown_event.wait(timeout=min(interval, remaining))
        if _shutdown_event.is_set():
            return True
    return False


# ---------------------------------------------------------------------------
# Logger — configured in _setup_logging()
# ---------------------------------------------------------------------------
logger = logging.getLogger("google_clicker")

# ---------------------------------------------------------------------------
# Browser profile config
# ---------------------------------------------------------------------------
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

LOCALE = None
TIMEZONE_ID = None

# ---------------------------------------------------------------------------
# File paths
# ---------------------------------------------------------------------------
DEFAULT_KEYWORDS_CSV = Path(__file__).resolve().parent / "keywords_unified.csv"
RESULTS_LOG_CSV = Path(__file__).resolve().parent / "results_log_unified.csv"
RESULTS_ARCHIVE_DIR = Path(__file__).resolve().parent / "Report_unified"

# ---------------------------------------------------------------------------
# Timeouts & delays
# ---------------------------------------------------------------------------
VERIFICATION_WAIT_TIMEOUT_MS = 120_000
DEFAULT_OPERATION_TIMEOUT_MS = 90_000
ANTICAPTCHA_MAX_ATTEMPTS = 3
ANTICAPTCHA_RETRY_DELAY_SEC = (5, 10)
SORRY_PAGE_MAX_SOLVE_CYCLES = 3
POST_SUBMIT_WAIT_SEC = (5, 8)
POST_CAPTCHA_WAIT_MAX_SEC = 15 * 60
CAPTCHA_CHECK_INTERVAL_SEC = 60
ONE_OFF_WAIT_FOR_RESULTS_SEC = 90
ONE_OFF_CHECK_INTERVAL_SEC = 5
MAX_RESULT_PAGES = 50

DELAY_BETWEEN_PAGES_SEC = (3.0, 5.0)
DELAY_BETWEEN_KEYWORDS_SEC = (30, 60)

# Non-target click settings
NON_TARGET_CLICK_PROBABILITY = 0.25
NON_TARGET_DWELL_SEC = (3, 8)

# Target page dwell settings (NEW: dwell on target after click)
TARGET_DWELL_SEC = (8, 25)

# Worker configuration
NUM_WORKERS = int(os.environ.get("NUM_WORKERS", "2"))
WORKER_STAGGER_DELAY_SEC = (15, 45)

LOG_HEADER = ["keyword", "target_domain", "page_num", "position", "timestamp"]

# Anti-Captcha API key
ANTICAPTCHA_API_KEY_FALLBACK = "64bd9cd5c306974febf3847e0dab53c4"

# Thread-safe CSV lock
_log_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------
class SearchOutcome(Enum):
    """Outcome signals for search/captcha operations."""
    REOPEN_NEEDED = auto()


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
class _WorkerFormatter(logging.Formatter):
    """Formatter that includes an optional [Wn] prefix."""

    def format(self, record):
        prefix = getattr(record, "prefix", "")
        if prefix:
            record.msg = f"{prefix}  {record.msg}"
        return super().format(record)


def _setup_logging() -> None:
    """Configure logging with console and file handlers."""
    logger.setLevel(logging.DEBUG)

    fmt = _WorkerFormatter("%(asctime)s  %(levelname)-7s  %(message)s")

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(fmt)
    logger.addHandler(console)

    log_file = Path(__file__).resolve().parent / "google_clicker_unified.log"
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)


def _make_worker_logger(worker_id: int) -> logging.LoggerAdapter:
    """Create a logger adapter with [Wn] prefix."""
    return logging.LoggerAdapter(logger, {"prefix": f"[W{worker_id}]"})


# ---------------------------------------------------------------------------
# Setup helpers
# ---------------------------------------------------------------------------
def _pick_random_ua() -> str:
    return random.choice(USER_AGENT_POOL)


def _pick_random_viewport() -> dict:
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
# CSV helpers  (thread-safe)
# ---------------------------------------------------------------------------
def log_result(keyword: str, domain: str, page_num: int | str, position: int | str) -> None:
    """Append one row to results_log_unified.csv. Thread-safe."""
    with _log_lock:
        path = RESULTS_LOG_CSV
        write_header = not path.exists() or path.stat().st_size == 0
        with path.open("a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            if write_header:
                writer.writerow(LOG_HEADER)
            writer.writerow([keyword, domain, page_num, position, datetime.now().isoformat()])


def load_keywords(csv_path: Path | str) -> list[tuple[str, str]]:
    """Load keywords from CSV. Returns list of (keyword, target_domain) tuples."""
    path = Path(csv_path)
    if not path.exists():
        return []
    keywords: list[tuple[str, str]] = []
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header_skipped = False
        for row in reader:
            if not row:
                continue
            kw = row[0].strip() if row[0] else ""
            if not header_skipped and kw.lower() == "keyword":
                header_skipped = True
                continue
            domain = row[1].strip() if len(row) > 1 and row[1] else ""
            if kw and domain:
                keywords.append((kw, domain))
            elif kw and not domain:
                logger.warning("Skipping keyword without target_domain: %s", kw[:60])
    return keywords


def move_result_log_to_archive() -> None:
    """Move results_log_unified.csv to Report_unified/ with timestamp."""
    path = RESULTS_LOG_CSV
    if not path.exists() or path.stat().st_size == 0:
        return
    RESULTS_ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    archive_name = f"results_log_unified_{timestamp}.csv"
    dest = RESULTS_ARCHIVE_DIR / archive_name
    try:
        shutil.move(str(path), str(dest))
        logger.info("Moved result log to %s (next run will start fresh)", dest)
    except Exception as e:
        logger.warning("Could not move result log to archive: %s", e)


def get_completed_keywords() -> set[tuple[str, str]]:
    """Return set of (keyword, target_domain) pairs already logged."""
    path = RESULTS_LOG_CSV
    if not path.exists() or path.stat().st_size == 0:
        return set()
    completed: set[tuple[str, str]] = set()
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        first = True
        for row in reader:
            if not row:
                continue
            if first and str(row[0]).strip().lower() == "keyword":
                first = False
                continue
            kw = str(row[0]).strip()
            domain = str(row[1]).strip() if len(row) > 1 else ""
            if kw and domain:
                completed.add((kw, domain))
    return completed


# ---------------------------------------------------------------------------
# Captcha / verification handling
# ---------------------------------------------------------------------------
def handle_sorry_page(page, context, wlog) -> bool | SearchOutcome:
    """
    When on a Google captcha page: send to Anti-Captcha, submit token, then check.
    Returns True when past captcha; SearchOutcome.REOPEN_NEEDED on failure.
    """
    wlog.warning("CAPTCHA detected. Solving via Anti-Captcha...")
    api_key = get_anticaptcha_api_key()
    if not api_key:
        wlog.error("ANTICAPTCHA_API_KEY not set.")
        try:
            context.close()
        except Exception:
            pass
        return SearchOutcome.REOPEN_NEEDED

    for cycle in range(1, SORRY_PAGE_MAX_SOLVE_CYCLES + 1):
        solved = False
        for attempt in range(1, ANTICAPTCHA_MAX_ATTEMPTS + 1):
            if solve_google_recaptcha(page, api_key):
                solved = True
                break
            if attempt < ANTICAPTCHA_MAX_ATTEMPTS:
                interruptible_sleep(random.uniform(*ANTICAPTCHA_RETRY_DELAY_SEC))
        if not solved:
            wlog.error("ERROR: Anti-Captcha failed (cycle %d/%d).", cycle, SORRY_PAGE_MAX_SOLVE_CYCLES)
            if cycle < SORRY_PAGE_MAX_SOLVE_CYCLES:
                continue
            wlog.error("ERROR: Anti-Captcha failed after all cycles. Reopening browser.")
            try:
                context.close()
            except Exception:
                pass
            return SearchOutcome.REOPEN_NEEDED

        wait_sec = random.uniform(*POST_SUBMIT_WAIT_SEC)
        if interruptible_sleep(wait_sec):
            return SearchOutcome.REOPEN_NEEDED

        is_captcha, _ = _robust_is_captcha_page_with_reason(page)
        if not is_captcha:
            wlog.info("CAPTCHA solved. Continuing.")
            return True
        if cycle < SORRY_PAGE_MAX_SOLVE_CYCLES:
            pass  # retry silently

    wlog.error("ERROR: CAPTCHA unsolvable after all retries. Reopening browser...")
    try:
        context.close()
    except Exception:
        pass
    return SearchOutcome.REOPEN_NEEDED


def handle_captcha_when_no_results(page, context, wlog) -> bool | SearchOutcome:
    """Solve captcha when no results appear. Wait up to 15 min checking every 1 min."""
    api_key = get_anticaptcha_api_key()
    if not api_key:
        wlog.error("Anti-Captcha API key not set. Cannot solve captcha.")
        try:
            context.close()
        except Exception:
            pass
        return SearchOutcome.REOPEN_NEEDED

    wlog.warning("CAPTCHA detected (no results). Solving via Anti-Captcha...")
    for attempt in range(1, ANTICAPTCHA_MAX_ATTEMPTS + 1):
        if solve_google_recaptcha(page, api_key):
            break
        if attempt < ANTICAPTCHA_MAX_ATTEMPTS:
            interruptible_sleep(random.uniform(*ANTICAPTCHA_RETRY_DELAY_SEC))
    else:
        wlog.error("ERROR: Anti-Captcha failed. Reopening browser...")
        try:
            context.close()
        except Exception:
            pass
        return SearchOutcome.REOPEN_NEEDED

    url_after_submit = page.url or ""
    deadline = time.monotonic() + POST_CAPTCHA_WAIT_MAX_SEC
    check_count = 0

    one_off_deadline = time.monotonic() + ONE_OFF_WAIT_FOR_RESULTS_SEC
    while time.monotonic() < one_off_deadline:
        if _page_has_result_links(page):
            return True
        if not _robust_is_captcha_page(page):
            return True
        if interruptible_sleep(ONE_OFF_CHECK_INTERVAL_SEC):
            return SearchOutcome.REOPEN_NEEDED

    while time.monotonic() < deadline:
        if interruptible_sleep(CAPTCHA_CHECK_INTERVAL_SEC):
            return SearchOutcome.REOPEN_NEEDED
        check_count += 1
        current_url = page.url or ""
        is_captcha, reason = _robust_is_captcha_page_with_reason(page)

        if not is_captcha:
            wlog.info("CAPTCHA resolved. Continuing.")
            return True

        wlog.warning("CAPTCHA still active (%d min elapsed).", check_count)
        if current_url != url_after_submit:
            for attempt in range(1, ANTICAPTCHA_MAX_ATTEMPTS + 1):
                if solve_google_recaptcha(page, api_key):
                    url_after_submit = page.url or ""
                    break
                if attempt < ANTICAPTCHA_MAX_ATTEMPTS:
                    interruptible_sleep(random.uniform(*ANTICAPTCHA_RETRY_DELAY_SEC))

    is_captcha_final, _ = _robust_is_captcha_page_with_reason(page)
    if not is_captcha_final:
        return True
    wlog.error("ERROR: CAPTCHA timeout (15 min). Reopening browser...")
    try:
        context.close()
    except Exception:
        pass
    return SearchOutcome.REOPEN_NEEDED


def handle_google_verification(page, context, wlog):
    """If on consent or robot verification page, handle it."""
    time.sleep(random.uniform(2, 4))
    if "consent.google" in page.url:
        try:
            page.get_by_role("button", name="Accept all").click(timeout=4000)
            time.sleep(random.uniform(1.5, 3))
        except Exception:
            pass
    if "sorry" in page.url or _page_looks_like_verification(page):
        return handle_sorry_page(page, context, wlog)


def ensure_google_ready(page, context, wlog):
    """Navigate to Google and handle consent/sorry once."""
    try:
        page.goto(
            "https://www.google.com/?nfpr=1",
            wait_until="domcontentloaded",
            timeout=20000,
        )
    except Exception as e:
        wlog.error("ERROR: Failed to load Google: %s. Reopening browser.", e)
        try:
            context.close()
        except Exception:
            pass
        return SearchOutcome.REOPEN_NEEDED
    result = handle_google_verification(page, context, wlog)
    if result is SearchOutcome.REOPEN_NEEDED:
        return SearchOutcome.REOPEN_NEEDED



# ---------------------------------------------------------------------------
# Page detection helpers
# ---------------------------------------------------------------------------
def _page_looks_like_verification(page) -> bool:
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
    is_captcha, _ = _robust_is_captcha_page_with_reason(page)
    return is_captcha


def _is_target_link_an_ad(page, link_locator) -> bool:
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
    results_list: list[SearchResult] = []
    seen_urls: set[str] = set()

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
                    results_list.append(SearchResult(title=title, url=href, snippet=""))
                except Exception:
                    continue
            if results_list:
                break
        except Exception:
            continue

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
                    results_list.append(SearchResult(title=title, url=href, snippet=""))
                except Exception:
                    continue
            if results_list:
                break

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
    try:
        total_height = page.evaluate("document.documentElement.scrollHeight")
        viewport_height = page.evaluate("window.innerHeight")
        max_scroll = total_height - viewport_height
        if max_scroll <= 0:
            return

        target_scroll = max_scroll * random.uniform(0.6, 1.0)
        scrolled = 0.0
        steps = max(4, min(20, int(duration_sec * 2.5)))
        step_time_base = duration_sec / steps

        for i in range(steps):
            if scrolled >= target_scroll:
                break
            if 1 < i < steps - 1 and random.random() < 0.3:
                scroll_back = random.randint(100, 300)
                page.evaluate(f"window.scrollBy(0, -{scroll_back})")
                time.sleep(random.uniform(0.3, 0.8))

            remaining = target_scroll - scrolled
            avg_step = remaining / max(1, steps - i)
            step_amount = avg_step * random.uniform(0.5, 1.5)
            step_amount = min(step_amount, remaining)

            page.evaluate(f"window.scrollBy(0, {step_amount})")
            scrolled += step_amount

            if random.random() < 0.2:
                time.sleep(random.uniform(0.5, 2.0))
            else:
                time.sleep(step_time_base * random.uniform(0.6, 1.4))
    except Exception:
        pass


def _click_random_non_target(page, results_list: list[SearchResult], target_domain: str, wlog) -> None:
    non_targets = [
        r for r in results_list
        if target_domain not in r.url and "google." not in r.url
    ]
    if not non_targets:
        wlog.debug("No non-target results to click.")
        return

    chosen = random.choice(non_targets)
    wlog.info("Non-target click: visiting %s", chosen.url[:80])

    try:
        link = page.locator(f"a[href=\"{chosen.url}\"]").first
        if not link.is_visible(timeout=2000) and chosen.title:
            link = page.locator("a[href^='http']").filter(has_text=chosen.title[:40]).first
        link.scroll_into_view_if_needed(timeout=5000)
        time.sleep(random.uniform(0.3, 0.8))
        link.click(timeout=5000)

        dwell = random.uniform(*NON_TARGET_DWELL_SEC)
        wlog.debug("Dwelling on non-target page for %.1fs...", dwell)
        time.sleep(dwell * 0.3)
        _scroll_page_naturally(page, dwell * 0.7)

        page.go_back(wait_until="domcontentloaded", timeout=15000)
        time.sleep(random.uniform(1, 3))
        wlog.debug("Returned to SERP after non-target click.")
    except Exception as e:
        wlog.debug("Non-target click failed: %s", e)
        try:
            page.go_back(wait_until="domcontentloaded", timeout=10000)
            time.sleep(random.uniform(1, 2))
        except Exception:
            pass


def _dwell_on_target_page(page, wlog) -> None:
    """Dwell on the target page with natural scrolling to avoid bounce detection."""
    dwell = random.uniform(*TARGET_DWELL_SEC)
    wlog.debug("Dwelling on target page for %.1fs...", dwell)
    time.sleep(dwell * 0.3)
    _scroll_page_naturally(page, dwell * 0.7)


def _goto_next_results_page(page, current_page_one_based: int) -> bool:
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
    page, context, query: str, target_domain: str, wlog
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
        wlog.warning("Robot verification appeared after search. Handling...")
        result = handle_sorry_page(page, context, wlog)
        if result is SearchOutcome.REOPEN_NEEDED:
            return SearchOutcome.REOPEN_NEEDED
        if result is not True:
            return None
        time.sleep(random.uniform(1, 2))
    if "consent.google" in page.url:
        wlog.info("Cookie consent appeared. Accepting...")
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
                wlog.warning("Results did not load; verification page detected. Handling...")
                result = handle_sorry_page(page, context, wlog)
                if result is SearchOutcome.REOPEN_NEEDED:
                    return SearchOutcome.REOPEN_NEEDED
                if result is not True:
                    return None
                time.sleep(random.uniform(1, 2))
                continue
            return None
        time.sleep(random.uniform(1, 2))
        if _page_looks_like_verification(page):
            wlog.warning("Verification content detected on page. Solving...")
            result = handle_sorry_page(page, context, wlog)
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
                result = handle_captcha_when_no_results(page, context, wlog)
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
                _click_random_non_target(page, results_list, target_domain, wlog)
                current_url = page.url or ""
                if "google." not in current_url:
                    wlog.warning(
                        "Not back on Google SERP after non-target click. Skipping target click."
                    )
                    continue

            # Click the target (skip if it is an ad)
            link_to_click = page.locator(f"a[href*='{target_domain}']").first
            if _is_target_link_an_ad(page, link_to_click):
                wlog.info("Target result is an ad, skipping (will try next page if any).")
            else:
                try:
                    link_to_click.scroll_into_view_if_needed(timeout=5000)
                    time.sleep(random.uniform(0.2, 0.5))
                    link_to_click.click(timeout=5000)
                    time.sleep(random.uniform(2, 4))
                    # Dwell on target page (NEW: read the page like a real user)
                    _dwell_on_target_page(page, wlog)
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
        context.set_default_timeout(DEFAULT_OPERATION_TIMEOUT_MS)
        context.set_default_navigation_timeout(DEFAULT_OPERATION_TIMEOUT_MS)
        stealth.apply_stealth_sync(context)
        page = context.new_page()
        return context, page
    except Exception as e:
        logger.error("Failed to launch browser: %s", e)
        raise


# ---------------------------------------------------------------------------
# Worker function (runs in its own thread)
# ---------------------------------------------------------------------------
def worker(keywords_slice: list[tuple[str, str]], worker_id: int) -> None:
    """Run one browser session over a slice of (keyword, domain) pairs."""
    wlog = _make_worker_logger(worker_id)

    if not keywords_slice:
        wlog.info("No keywords assigned. Exiting.")
        return

    # Stagger startup for workers beyond the first
    if worker_id > 1:
        delay = random.uniform(*WORKER_STAGGER_DELAY_SEC)
        if interruptible_sleep(delay):
            return

    wlog.info("[W%d] Started — %d keywords to process.", worker_id, len(keywords_slice))

    profile_dir = _create_temp_profile()
    user_agent = _pick_random_ua()
    viewport = _pick_random_viewport()

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

    try:
        with sync_playwright() as p:
            context, page = _launch_browser(p, profile_dir, launch_options, stealth)

            while True:
                need_reopen = ensure_google_ready(page, context, wlog)
                if need_reopen is not SearchOutcome.REOPEN_NEEDED:
                    break
                context, page = _launch_browser(p, profile_dir, launch_options, stealth)

            for idx, (query, domain) in enumerate(keywords_slice):
                if _shutdown_requested():
                    wlog.info("Shutdown requested. Exiting after current keyword.")
                    break

                wlog.info(
                    "[%d/%d] %s → %s",
                    idx + 1, len(keywords_slice),
                    query[:60] + ("..." if len(query) > 60 else ""),
                    domain,
                )

                result = run_one_search(page, context, query, domain, wlog)
                search_used = query

                if result is SearchOutcome.REOPEN_NEEDED:
                    wlog.warning("Browser error — retrying keyword...")  
                    context, page = _launch_browser(p, profile_dir, launch_options, stealth)
                    while True:
                        need_reopen = ensure_google_ready(page, context, wlog)
                        if need_reopen is not SearchOutcome.REOPEN_NEEDED:
                            break
                        context, page = _launch_browser(p, profile_dir, launch_options, stealth)
                    result = run_one_search(page, context, query, domain, wlog)
                    search_used = query
                    if result is SearchOutcome.REOPEN_NEEDED:
                        wlog.error("ERROR: Retry also failed. Skipping keyword.")
                        context, page = _launch_browser(p, profile_dir, launch_options, stealth)
                        while True:
                            need_reopen = ensure_google_ready(page, context, wlog)
                            if need_reopen is not SearchOutcome.REOPEN_NEEDED:
                                break
                            context, page = _launch_browser(p, profile_dir, launch_options, stealth)

                if result is not None and result is not SearchOutcome.REOPEN_NEEDED:
                    page_num, pos = result
                    wlog.info("  ✓ Clicked  page=%d  pos=%d  [%s]", page_num, pos, domain)
                    log_result(search_used, domain, page_num, pos)
                else:
                    wlog.info("  ✗ Not found")
                    log_result(query, domain, "", "not_found")

                if idx < len(keywords_slice) - 1:
                    delay = random.uniform(*DELAY_BETWEEN_KEYWORDS_SEC)
                    if interruptible_sleep(delay):
                        break
                    try:
                        page.goto(
                            "https://www.google.com/?nfpr=1",
                            wait_until="domcontentloaded",
                            timeout=20000,
                        )
                    except Exception as e:
                        wlog.error("ERROR: Failed to navigate back to Google: %s. Reopening.", e)
                        context, page = _launch_browser(p, profile_dir, launch_options, stealth)
                        while True:
                            need_reopen = ensure_google_ready(page, context, wlog)
                            if need_reopen is not SearchOutcome.REOPEN_NEEDED:
                                break
                            context, page = _launch_browser(p, profile_dir, launch_options, stealth)
                        continue
                    ver = handle_google_verification(page, context, wlog)
                    if ver is SearchOutcome.REOPEN_NEEDED:
                        wlog.info("Reopening browser...")
                        context, page = _launch_browser(p, profile_dir, launch_options, stealth)
                        while True:
                            need_reopen = ensure_google_ready(page, context, wlog)
                            if need_reopen is not SearchOutcome.REOPEN_NEEDED:
                                break
                            context, page = _launch_browser(p, profile_dir, launch_options, stealth)

            wlog.info("All assigned keywords processed.")
            try:
                context.close()
            except Exception:
                pass

    except Exception as e:
        wlog.error("Worker crashed: %s", e)
        wlog.debug("Traceback: %s", traceback.format_exc())


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    _setup_logging()

    keywords = load_keywords(DEFAULT_KEYWORDS_CSV)
    if not keywords:
        logger.error("No keywords found in %s", DEFAULT_KEYWORDS_CSV)
        return

    completed = get_completed_keywords()
    keywords_to_do = [(kw, d) for kw, d in keywords if (kw, d) not in completed]
    if not keywords_to_do:
        logger.info("All keywords already processed (results_log_unified.csv is up to date).")
        return

    logger.info("Loaded %d keywords from %s", len(keywords), DEFAULT_KEYWORDS_CSV.name)
    if completed:
        logger.info(
            "Resuming: %d already done, %d remaining.",
            len(completed), len(keywords_to_do),
        )
    if get_anticaptcha_api_key():
        logger.info("Anti-Captcha: API key set (auto-solve enabled).")
    else:
        logger.warning("Anti-Captcha: no API key (you will need to solve verification manually).")

    # Randomize keyword order per run to avoid sequential pattern detection
    random.shuffle(keywords_to_do)
    logger.info("Keyword order randomized for this run.")

    # Split keywords across workers
    num_workers = min(NUM_WORKERS, len(keywords_to_do))
    chunk_size = len(keywords_to_do) // num_workers
    chunks: list[list[tuple[str, str]]] = []
    for i in range(num_workers):
        start = i * chunk_size
        end = start + chunk_size if i < num_workers - 1 else len(keywords_to_do)
        chunks.append(keywords_to_do[start:end])

    logger.info(
        "Splitting %d keywords across %d workers: %s",
        len(keywords_to_do),
        num_workers,
        [len(c) for c in chunks],
    )

    threads: list[threading.Thread] = []
    for worker_id, chunk in enumerate(chunks, 1):
        t = threading.Thread(target=worker, args=(chunk, worker_id), daemon=True)
        threads.append(t)

    for t in threads:
        t.start()

    # Wait for threads to finish, using a timeout so the main thread
    # can wake up to process SIGINT/SIGTERM handlers properly.
    try:
        while any(t.is_alive() for t in threads):
            if _shutdown_requested():
                break
            for t in threads:
                t.join(timeout=0.5)
    except KeyboardInterrupt:
        _signal_handler()

    if not _shutdown_requested():
        logger.info("Done. All workers finished.")
        move_result_log_to_archive()


if __name__ == "__main__":
    main()
