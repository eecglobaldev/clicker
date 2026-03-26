"""
Google Search clicker v2: searches keywords on Google and clicks target domain results.

Reads keywords_unified.csv (keyword + target_domain per row), runs two browser
instances in parallel, and clicks the corresponding organic (non-ad) target result.

Designed for 24/7 unattended operation with robust error recovery, human-like
behavior, and resource management.

Environment variables:
  ANTICAPTCHA_API_KEY  - Anti-Captcha API key (required for captcha solving)
  NUM_WORKERS          - Number of parallel browser workers (default: 2)
"""

import atexit
import csv
import glob
import json
import logging
import os
import random
import shutil
import signal
import socket
import subprocess
import tempfile
import threading
import time
import traceback
import urllib.parse
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from logging.handlers import RotatingFileHandler
from pathlib import Path

import psutil
from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth

from anticaptcha_solver import solve_google_recaptcha

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent


@dataclass(frozen=True)
class Config:
    """All tunables in one place."""

    # File paths
    keywords_csv: Path = BASE_DIR / "keywords_unified.csv"
    results_log_csv: Path = BASE_DIR / "results_log_unified.csv"
    results_archive_dir: Path = BASE_DIR / "Report_unified"
    log_file: Path = BASE_DIR / "google_clicker_unified.log"

    # Workers
    num_workers: int = 2
    worker_stagger_delay: tuple[float, float] = (15.0, 45.0)

    # Timeouts
    keyword_timeout_sec: float = 600.0  # 10 min per keyword max
    default_operation_timeout_ms: int = 90_000
    verification_wait_timeout_ms: int = 120_000

    # Captcha
    anticaptcha_api_key: str = "64bd9cd5c306974febf3847e0dab53c4"
    anticaptcha_max_attempts: int = 3
    anticaptcha_retry_delay: tuple[float, float] = (5.0, 10.0)
    sorry_page_max_solve_cycles: int = 3
    post_submit_wait: tuple[float, float] = (5.0, 8.0)
    post_captcha_wait_max_sec: float = 900.0
    captcha_check_interval_sec: float = 60.0
    one_off_wait_for_results_sec: float = 90.0
    one_off_check_interval_sec: float = 5.0
    captcha_max_per_cycle: int = 30
    captcha_consecutive_threshold: int = 5
    captcha_cooldown_sec: float = 1800.0  # 30 min

    # Delays
    delay_between_pages: tuple[float, float] = (3.0, 5.0)
    delay_between_keywords: tuple[float, float] = (30.0, 60.0)

    # Human behavior
    non_target_click_probability: float = 0.25
    non_target_dwell: tuple[float, float] = (3.0, 8.0)
    target_dwell: tuple[float, float] = (8.0, 25.0)
    serp_browse_probability: float = 0.3
    session_break_min_keywords: int = 10
    session_break_max_keywords: int = 30
    session_break_duration: tuple[float, float] = (120.0, 300.0)  # 2-5 min

    # Browser
    max_result_pages: int = 50
    max_relaunch_attempts: int = 10
    relaunch_backoff_sec: float = 30.0
    profile_refresh_interval: int = 50  # keywords

    # Resource management
    memory_limit_mb: int = 4096
    memory_check_interval: int = 10  # keywords
    disk_min_free_mb: int = 100
    archive_max_files: int = 100

    # Cycle
    cycle_complete_delay_sec: float = 60.0

    @staticmethod
    def from_env() -> "Config":
        return Config(
            num_workers=int(os.environ.get("NUM_WORKERS", "2")),
            anticaptcha_api_key=(os.environ.get("ANTICAPTCHA_API_KEY", "") or "64bd9cd5c306974febf3847e0dab53c4").strip(),
        )


# ---------------------------------------------------------------------------
# Global shutdown event
# ---------------------------------------------------------------------------
_shutdown_event = threading.Event()


def _kill_all_child_browsers():
    try:
        curr_proc = psutil.Process()
        for child in curr_proc.children(recursive=True):
            try:
                name = child.name().lower()
                if "chrome" in name or "chromium" in name or "node" in name:
                    child.kill()
            except psutil.NoSuchProcess:
                pass
    except Exception:
        pass


def _signal_handler(_signum=None, _frame=None):
    print("\nSignal received — killing browsers and stopping...", flush=True)
    _shutdown_event.set()
    _kill_all_child_browsers()


def _shutdown_requested() -> bool:
    return _shutdown_event.is_set()


def interruptible_sleep(seconds: float, interval: float = 0.5) -> bool:
    """Sleep for `seconds`, checking for shutdown every `interval`.
    Returns True if interrupted (shutdown requested)."""
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        remaining = deadline - time.monotonic()
        _shutdown_event.wait(timeout=min(interval, remaining))
        if _shutdown_event.is_set():
            return True
    return False


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logger = logging.getLogger("google_clicker")

LOG_HEADER = ["keyword", "target_domain", "page_num", "position", "timestamp"]


class _WorkerFormatter(logging.Formatter):
    def format(self, record):
        prefix = getattr(record, "prefix", "")
        if prefix:
            record.msg = f"{prefix}  {record.msg}"
        return super().format(record)


def _setup_logging(config: Config) -> None:
    if logger.handlers:
        return
    logger.setLevel(logging.DEBUG)

    fmt = _WorkerFormatter("%(asctime)s  %(levelname)-7s  %(message)s")

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(fmt)
    logger.addHandler(console)

    file_handler = RotatingFileHandler(
        config.log_file, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)


def _make_worker_logger(worker_id: int) -> logging.LoggerAdapter:
    return logging.LoggerAdapter(logger, {"prefix": f"[W{worker_id}]"})


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
CHROME_SUPPRESS_FLAGS = [
    "--no-sandbox",
    "--no-first-run",
    "--no-default-browser-check",
    "--disable-features=ChromeWhatsNewUI",
    "--disable-session-crashed-bubble",
    "--disable-infobars",
    "--disable-restore-session-state",
    "--noerrdialogs",
    "--disable-component-update",
    "--disable-backgrounding-occluded-windows",
    "--disable-renderer-backgrounding",
    "--disable-background-timer-throttling",
    "--disable-ipc-flooding-protection",
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

# Consent button names for multiple locales
CONSENT_BUTTON_NAMES = [
    "Accept all",
    "Alle akzeptieren",
    "Tout accepter",
    "Aceptar todo",
    "Accetta tutto",
    "Aceitar tudo",
]

# Non-organic container selectors to exclude from results
NON_ORGANIC_SELECTORS = [
    "[data-initq]",  # People Also Ask
    ".kp-blk",  # Knowledge Panel
    ".related-question-pair",
    "g-scrolling-carousel",
    ".cu-container",
]

# Thread-safe CSV lock
_log_lock = threading.Lock()

# Captcha circuit breaker (shared across workers)
_captcha_solve_count = 0
_captcha_solve_lock = threading.Lock()


# ---------------------------------------------------------------------------
# User agent detection
# ---------------------------------------------------------------------------
def _detect_chrome_version() -> str | None:
    """Detect installed Chrome major version."""
    for cmd in ["google-chrome", "chromium-browser", "chromium", "google-chrome-stable"]:
        try:
            result = subprocess.run(
                [cmd, "--version"], capture_output=True, text=True, timeout=5
            )
            if result.returncode == 0:
                parts = result.stdout.strip().split()
                for part in parts:
                    if "." in part and part[0].isdigit():
                        return part.split(".")[0]
        except Exception:
            continue
    return None


def _build_user_agent_pool() -> list[str]:
    """Build user agent pool based on installed Chrome version."""
    detected = _detect_chrome_version()
    if detected:
        try:
            major = int(detected)
            versions = [major - 1, major, major]
        except ValueError:
            versions = [133, 134, 134]
    else:
        versions = [133, 134, 134]

    agents = []
    platforms = [
        "Windows NT 10.0; Win64; x64",
        "Macintosh; Intel Mac OS X 10_15_7",
        "X11; Linux x86_64",
    ]
    for v in versions:
        for plat in platforms:
            agents.append(
                f"Mozilla/5.0 ({plat}) AppleWebKit/537.36 "
                f"(KHTML, like Gecko) Chrome/{v}.0.0.0 Safari/537.36"
            )
    return agents


USER_AGENT_POOL: list[str] = []  # populated in main()


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------
@dataclass
class SearchResult:
    title: str
    url: str
    snippet: str = ""


class SearchOutcome(Enum):
    REOPEN_NEEDED = auto()
    TIMEOUT = auto()
    NOT_FOUND = auto()


@dataclass
class SerpScan:
    results: list[SearchResult] = field(default_factory=list)
    target_found: bool = False
    target_position: int | None = None
    target_is_ad: bool = False
    has_results: bool = False
    captcha_detected: bool = False


# ---------------------------------------------------------------------------
# Browser profile management
# ---------------------------------------------------------------------------
def _create_temp_profile() -> Path:
    """Create a temp browser profile with Chrome Preferences to suppress extra windows."""
    tmp_dir = Path(tempfile.mkdtemp(prefix="gclicker_profile_"))

    # Write Chrome Preferences to suppress restore/welcome/default-browser
    default_dir = tmp_dir / "Default"
    default_dir.mkdir(exist_ok=True)
    prefs = {
        "profile": {"exit_type": "Normal", "exited_cleanly": True},
        "session": {"restore_on_startup": 4, "startup_urls": []},
        "browser": {"check_default_browser": False, "has_seen_welcome_page": True},
        "whats_new": {"last_version": 999},
    }
    (default_dir / "Preferences").write_text(json.dumps(prefs))

    def _cleanup():
        try:
            shutil.rmtree(tmp_dir, ignore_errors=True)
        except Exception:
            pass

    atexit.register(_cleanup)
    return tmp_dir


def _cleanup_stale_profiles() -> None:
    """Remove leftover gclicker_profile_* dirs from /tmp on startup."""
    total_size = 0
    for d in glob.glob("/tmp/gclicker_profile_*"):
        try:
            for dirpath, _dirnames, filenames in os.walk(d):
                for f in filenames:
                    try:
                        total_size += os.path.getsize(os.path.join(dirpath, f))
                    except OSError:
                        pass
            shutil.rmtree(d, ignore_errors=True)
        except Exception:
            pass
    if total_size > 0:
        logger.info("Cleaned up %.1f MB of stale browser profiles.", total_size / (1024 * 1024))


# ---------------------------------------------------------------------------
# CSV I/O (thread-safe)
# ---------------------------------------------------------------------------
def _check_disk_space(config: Config) -> bool:
    """Return True if enough disk space is available."""
    try:
        usage = shutil.disk_usage(BASE_DIR)
        free_mb = usage.free / (1024 * 1024)
        if free_mb < config.disk_min_free_mb:
            logger.critical("Disk space critically low: %.0f MB free", free_mb)
            return False
    except Exception:
        pass
    return True


def log_result(keyword: str, domain: str, page_num: int | str, position: int | str, config: Config) -> None:
    """Append one row to results CSV. Thread-safe with fsync."""
    with _log_lock:
        if not _check_disk_space(config):
            return
        path = config.results_log_csv
        write_header = not path.exists() or path.stat().st_size == 0
        with path.open("a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            if write_header:
                writer.writerow(LOG_HEADER)
            writer.writerow([keyword, domain, page_num, position, datetime.now().isoformat()])
            f.flush()
            os.fsync(f.fileno())


def load_keywords(csv_path: Path) -> list[tuple[str, str]]:
    """Load (keyword, target_domain) pairs from CSV."""
    if not csv_path.exists():
        return []
    try:
        keywords: list[tuple[str, str]] = []
        with csv_path.open(newline="", encoding="utf-8") as f:
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
        return keywords
    except Exception as e:
        logger.error("Failed to read keywords CSV %s: %s", csv_path, e)
        return []


def get_completed_keywords(config: Config) -> set[tuple[str, str]]:
    """Return set of already-completed (keyword, domain) pairs. Thread-safe."""
    with _log_lock:
        path = config.results_log_csv
        if not path.exists() or path.stat().st_size == 0:
            return set()
        try:
            completed: set[tuple[str, str]] = set()
            with path.open(newline="", encoding="utf-8") as f:
                reader = csv.reader(f)
                first = True
                for row in reader:
                    try:
                        if not row or len(row) < 2:
                            continue
                        if first and str(row[0]).strip().lower() == "keyword":
                            first = False
                            continue
                        kw = str(row[0]).strip()
                        domain = str(row[1]).strip()
                        if kw and domain:
                            completed.add((kw, domain))
                    except Exception:
                        continue  # skip corrupted rows
            return completed
        except Exception as e:
            logger.warning("Could not read results log %s: %s", path, e)
            return set()


def move_result_log_to_archive(config: Config) -> None:
    """Move results log to archive directory. Thread-safe."""
    with _log_lock:
        path = config.results_log_csv
        if not path.exists() or path.stat().st_size == 0:
            return
        try:
            config.results_archive_dir.mkdir(parents=True, exist_ok=True)
            timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            dest = config.results_archive_dir / f"results_log_unified_{timestamp}.csv"
            shutil.move(str(path), str(dest))
            logger.info("Archived result log to %s", dest)
        except Exception as e:
            logger.warning("Could not archive result log: %s. Deleting to allow fresh cycle.", e)
            try:
                path.unlink()
            except Exception as e2:
                logger.error("Could not delete result log: %s", e2)


def _prune_archives(config: Config) -> None:
    """Keep only the last N archive files."""
    try:
        archives = sorted(config.results_archive_dir.glob("results_log_unified_*.csv"))
        if len(archives) > config.archive_max_files:
            for old in archives[: -config.archive_max_files]:
                try:
                    old.unlink()
                except Exception:
                    pass
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Captcha circuit breaker
# ---------------------------------------------------------------------------
def _increment_captcha_count(config: Config) -> bool:
    """Increment global captcha count. Returns True if over budget."""
    global _captcha_solve_count
    with _captcha_solve_lock:
        _captcha_solve_count += 1
        return _captcha_solve_count > config.captcha_max_per_cycle


def _reset_captcha_count() -> None:
    global _captcha_solve_count
    with _captcha_solve_lock:
        _captcha_solve_count = 0


def _check_anticaptcha_balance(api_key: str) -> float | None:
    """Check Anti-Captcha account balance. Returns balance or None on error."""
    try:
        import requests
        resp = requests.post(
            "https://api.anti-captcha.com/getBalance",
            json={"clientKey": api_key},
            timeout=10,
        )
        data = resp.json()
        if data.get("errorId", 1) == 0:
            return data.get("balance", 0.0)
    except Exception:
        pass
    return None


# ---------------------------------------------------------------------------
# Health checks
# ---------------------------------------------------------------------------
def _browser_is_healthy(page, wlog) -> bool:
    """Check if browser page is still responsive."""
    try:
        result = page.evaluate("1 + 1", timeout=5000)
        return result == 2
    except Exception:
        wlog.warning("Browser health check failed.")
        return False


def _wait_for_network(wlog, timeout_sec: float = 300, check_interval: float = 10) -> bool:
    """Block until network is available. Returns True if available, False if shutdown."""
    deadline = time.monotonic() + timeout_sec
    while time.monotonic() < deadline:
        if _shutdown_requested():
            return False
        try:
            socket.create_connection(("8.8.8.8", 53), timeout=5).close()
            return True
        except OSError:
            wlog.warning("Network unavailable. Retrying in %ds...", int(check_interval))
            if interruptible_sleep(check_interval):
                return False
    wlog.error("Network unavailable after %ds.", int(timeout_sec))
    return False


def _check_memory(config: Config, wlog) -> bool:
    """Return True if memory usage is dangerously high."""
    try:
        proc = psutil.Process()
        rss_mb = proc.memory_info().rss / (1024 * 1024)
        children_mb = sum(
            c.memory_info().rss / (1024 * 1024)
            for c in proc.children(recursive=True)
            if c.is_running()
        )
        total_mb = rss_mb + children_mb
        if total_mb > config.memory_limit_mb:
            wlog.warning("Memory usage %.0f MB exceeds limit %d MB", total_mb, config.memory_limit_mb)
            return True
    except Exception:
        pass
    return False


# ---------------------------------------------------------------------------
# Page detection helpers
# ---------------------------------------------------------------------------
def _page_looks_like_verification(page) -> bool:
    url = page.url or ""
    if "sorry" in url:
        return True
    try:
        content = page.content()
        cl = content.lower()
        return (
            "unusual traffic" in cl
            or "not a robot" in cl
            or "really you sending the requests" in cl
            or "recaptcha" in cl
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


def _is_captcha_page(page) -> tuple[bool, str]:
    """Check if current page is a captcha page. Returns (is_captcha, reason)."""
    try:
        if _page_has_result_links(page):
            return (False, "result links found")
        try:
            if page.locator('iframe[src*="recaptcha"]').first.is_visible(timeout=400):
                return (True, "reCAPTCHA iframe visible")
        except Exception:
            pass
        try:
            if page.locator('textarea[name="g-recaptcha-response"]').first.is_visible(timeout=400):
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
        cl = content.lower()
        if "unusual traffic" in cl or "not a robot" in cl or "really you sending the requests" in cl:
            return (True, "verification text in page content")
        return (False, "no captcha signals")
    except Exception:
        return (_page_looks_like_verification(page), "fallback check")


def _is_captcha(page) -> bool:
    is_cap, _ = _is_captcha_page(page)
    return is_cap


def _is_end_of_results(page) -> bool:
    """Detect Google's 'omitted similar results' or 'no results' message."""
    try:
        for sel in ["#botstuff", "#bottomstuff", "#search"]:
            try:
                text = page.locator(sel).inner_text(timeout=2000).lower()
                if "omitted" in text or "did not match any documents" in text:
                    return True
            except Exception:
                continue
    except Exception:
        pass
    return False


# ---------------------------------------------------------------------------
# Ad detection (4-layer check)
# ---------------------------------------------------------------------------
_AD_CHECK_JS = """
(el) => {
    // Layer 1: ad container ancestors
    const adContainers = ['#tads', '#bottomads', 'div[data-text-ad]',
                          'div.commercial-unit-desktop-top', 'div.cu-container', 'div.uEierd'];
    for (const sel of adContainers) {
        if (el.closest(sel)) return true;
    }
    // Layer 2: walk up DOM checking for sponsored/ad text
    let node = el;
    for (let i = 0; i < 10 && node; i++) {
        if (node.nodeType === 1) {
            const text = (node.innerText || '').toLowerCase();
            if (/\\bsponsored\\b/.test(text)) {
                // Check if it's a short label, not deep content
                const spans = node.querySelectorAll('span');
                for (const s of spans) {
                    const st = s.textContent.trim().toLowerCase();
                    if (st === 'sponsored' || st === 'ad') return true;
                }
            }
        }
        node = node.parentElement;
    }
    // Layer 3: ad URL patterns
    const href = el.href || el.getAttribute('href') || '';
    if (/googleadservices\\.com|\\/aclk\\?|\\/pagead\\/|googlesyndication\\.com/.test(href)) return true;
    // Layer 4: data attributes
    const block = el.closest('div.g') || el.closest('div[data-hveid]');
    if (block) {
        if (block.hasAttribute('data-rw') || block.hasAttribute('data-pcu')) return true;
    }
    if (el.hasAttribute('data-rw') || el.hasAttribute('data-pcu')) return true;
    return false;
}
"""


def _is_ad_result(page, link_locator) -> bool:
    """Check if a search result link is an ad using 4-layer detection."""
    try:
        return link_locator.evaluate(_AD_CHECK_JS)
    except Exception:
        return False


def _is_in_non_organic_container(page, link_locator) -> bool:
    """Check if link is inside a non-organic container (PAA, knowledge panel, etc.)."""
    try:
        return link_locator.evaluate("""
            (el) => {
                const selectors = ['[data-initq]', '.kp-blk', '.related-question-pair',
                                   'g-scrolling-carousel', '.cu-container'];
                for (const sel of selectors) {
                    if (el.closest(sel)) return true;
                }
                return false;
            }
        """)
    except Exception:
        return False


# ---------------------------------------------------------------------------
# SERP result extraction
# ---------------------------------------------------------------------------
_BLOCK_SELECTORS = [
    "#rso div.g",
    "#search div.g",
    "#rso div.MjjYud > div > div > div.g",
    "div[data-sokoban-container] div.g",
]


def _extract_organic_results(page, target_domain: str) -> SerpScan:
    """Extract organic (non-ad) results from current SERP page."""
    scan = SerpScan()
    seen_urls: set[str] = set()
    results: list[SearchResult] = []

    # Try structured block selectors first
    for block_selector in _BLOCK_SELECTORS:
        try:
            blocks = page.locator(block_selector).all()
            for block in blocks:
                try:
                    link = block.locator("a[href^='http']").first
                    href = link.get_attribute("href") if link else None
                    if not href or href.startswith("https://www.google.") or href in seen_urls:
                        continue
                    # Skip ads and non-organic containers
                    if _is_ad_result(page, link):
                        continue
                    if _is_in_non_organic_container(page, link):
                        continue
                    seen_urls.add(href)
                    title = ""
                    try:
                        title = (link.inner_text() or "").strip()[:200]
                    except Exception:
                        pass
                    results.append(SearchResult(title=title, url=href))
                except Exception:
                    continue
            if results:
                break
        except Exception:
            continue

    # Fallback: grab all links in search area (excluding known non-organic)
    if not results:
        for area in ["#search", "#rso"]:
            try:
                links = page.locator(f"{area} a[href^='http']").all()
            except Exception:
                continue
            for link in links:
                try:
                    href = link.get_attribute("href")
                    if not href or href.startswith("https://www.google.") or href in seen_urls:
                        continue
                    if _is_ad_result(page, link):
                        continue
                    if _is_in_non_organic_container(page, link):
                        continue
                    seen_urls.add(href)
                    title = ""
                    try:
                        title = (link.inner_text() or "").strip()[:200]
                    except Exception:
                        pass
                    results.append(SearchResult(title=title, url=href))
                except Exception:
                    continue
            if results:
                break

    scan.results = results
    scan.has_results = len(results) > 0

    # Find target position (organic only)
    for i, r in enumerate(results, 1):
        if target_domain in r.url:
            scan.target_found = True
            scan.target_position = i
            scan.target_is_ad = False
            break

    # If target not in organic results, check if it exists as an ad
    if not scan.target_found:
        try:
            ad_links = page.locator(f"a[href*='{target_domain}']").all()
            for ad_link in ad_links:
                if _is_ad_result(page, ad_link):
                    scan.target_found = True
                    scan.target_is_ad = True
                    break
        except Exception:
            pass

    # Check for captcha
    if not scan.has_results:
        scan.captcha_detected = _is_captcha(page)

    return scan


# ---------------------------------------------------------------------------
# Captcha handling
# ---------------------------------------------------------------------------
def _handle_sorry_page(page, context, config: Config, wlog) -> bool | SearchOutcome:
    """Handle Google captcha page. Returns True on success, REOPEN_NEEDED on failure."""
    wlog.warning("CAPTCHA detected. Solving via Anti-Captcha...")
    api_key = config.anticaptcha_api_key
    if not api_key:
        wlog.error("ANTICAPTCHA_API_KEY not set.")
        try:
            context.close()
        except Exception:
            pass
        return SearchOutcome.REOPEN_NEEDED

    for cycle in range(1, config.sorry_page_max_solve_cycles + 1):
        solved = False
        for attempt in range(1, config.anticaptcha_max_attempts + 1):
            if _increment_captcha_count(config):
                wlog.warning("Captcha budget exhausted for this cycle. Skipping.")
                return SearchOutcome.REOPEN_NEEDED

            if solve_google_recaptcha(page, api_key):
                solved = True
                break
            if attempt < config.anticaptcha_max_attempts:
                interruptible_sleep(random.uniform(*config.anticaptcha_retry_delay))

        if not solved:
            wlog.error("Anti-Captcha failed (cycle %d/%d).", cycle, config.sorry_page_max_solve_cycles)
            if cycle < config.sorry_page_max_solve_cycles:
                continue
            wlog.error("All captcha cycles failed. Reopening browser.")
            try:
                context.close()
            except Exception:
                pass
            return SearchOutcome.REOPEN_NEEDED

        wait_sec = random.uniform(*config.post_submit_wait)
        if interruptible_sleep(wait_sec):
            return SearchOutcome.REOPEN_NEEDED

        is_cap, _ = _is_captcha_page(page)
        if not is_cap:
            wlog.info("CAPTCHA solved.")
            return True
        if cycle < config.sorry_page_max_solve_cycles:
            pass  # retry

    wlog.error("CAPTCHA unsolvable after all retries. Reopening browser.")
    try:
        context.close()
    except Exception:
        pass
    return SearchOutcome.REOPEN_NEEDED


def _handle_captcha_no_results(page, context, config: Config, wlog) -> bool | SearchOutcome:
    """Handle captcha when no results appeared. Returns True on success."""
    api_key = config.anticaptcha_api_key
    if not api_key:
        wlog.error("Anti-Captcha API key not set.")
        try:
            context.close()
        except Exception:
            pass
        return SearchOutcome.REOPEN_NEEDED

    wlog.warning("CAPTCHA detected (no results). Solving...")
    for attempt in range(1, config.anticaptcha_max_attempts + 1):
        if _increment_captcha_count(config):
            wlog.warning("Captcha budget exhausted. Skipping.")
            return SearchOutcome.REOPEN_NEEDED

        if solve_google_recaptcha(page, api_key):
            break
        if attempt < config.anticaptcha_max_attempts:
            interruptible_sleep(random.uniform(*config.anticaptcha_retry_delay))
    else:
        wlog.error("Anti-Captcha failed. Reopening browser.")
        try:
            context.close()
        except Exception:
            pass
        return SearchOutcome.REOPEN_NEEDED

    url_after = page.url or ""
    # Quick check: results might appear immediately
    one_off_deadline = time.monotonic() + config.one_off_wait_for_results_sec
    while time.monotonic() < one_off_deadline:
        if _page_has_result_links(page):
            return True
        if not _is_captcha(page):
            return True
        if interruptible_sleep(config.one_off_check_interval_sec):
            return SearchOutcome.REOPEN_NEEDED

    # Long poll
    deadline = time.monotonic() + config.post_captcha_wait_max_sec
    check_count = 0
    while time.monotonic() < deadline:
        if interruptible_sleep(config.captcha_check_interval_sec):
            return SearchOutcome.REOPEN_NEEDED
        check_count += 1

        is_cap, _ = _is_captcha_page(page)
        if not is_cap:
            wlog.info("CAPTCHA resolved.")
            return True

        wlog.warning("CAPTCHA still active (%d min).", check_count)
        current_url = page.url or ""
        if current_url != url_after:
            for retry in range(1, config.anticaptcha_max_attempts + 1):
                if _increment_captcha_count(config):
                    break
                if solve_google_recaptcha(page, api_key):
                    url_after = page.url or ""
                    break
                if retry < config.anticaptcha_max_attempts:
                    interruptible_sleep(random.uniform(*config.anticaptcha_retry_delay))

    is_cap_final, _ = _is_captcha_page(page)
    if not is_cap_final:
        return True
    wlog.error("CAPTCHA timeout. Reopening browser.")
    try:
        context.close()
    except Exception:
        pass
    return SearchOutcome.REOPEN_NEEDED


def _handle_consent(page, wlog) -> None:
    """Accept Google cookie consent if present."""
    if "consent.google" not in page.url:
        return
    for btn_name in CONSENT_BUTTON_NAMES:
        try:
            btn = page.get_by_role("button", name=btn_name)
            if btn.is_visible(timeout=1000):
                btn.click(timeout=4000)
                interruptible_sleep(random.uniform(1.5, 3))
                return
        except Exception:
            continue
    # Fallback selector
    try:
        btn = page.locator('form[action*="consent"] button').first
        if btn.is_visible(timeout=1000):
            btn.click(timeout=4000)
            interruptible_sleep(random.uniform(1.5, 3))
    except Exception:
        pass


def _handle_verification(page, context, config: Config, wlog):
    """Handle consent or robot verification. Returns REOPEN_NEEDED or None."""
    interruptible_sleep(random.uniform(2, 4))
    _handle_consent(page, wlog)
    if "sorry" in page.url or _page_looks_like_verification(page):
        return _handle_sorry_page(page, context, config, wlog)
    return None


def _ensure_google_ready(page, context, config: Config, wlog):
    """Navigate to Google and handle consent/captcha. Returns REOPEN_NEEDED or None."""
    try:
        page.goto(
            "https://www.google.com",
            wait_until="domcontentloaded",
            timeout=20000,
        )
    except Exception as e:
        wlog.error("Failed to load Google: %s", e)
        try:
            context.close()
        except Exception:
            pass
        return SearchOutcome.REOPEN_NEEDED
    result = _handle_verification(page, context, config, wlog)
    if result is SearchOutcome.REOPEN_NEEDED:
        return SearchOutcome.REOPEN_NEEDED
    return None


# ---------------------------------------------------------------------------
# Human-like behavior
# ---------------------------------------------------------------------------
def _time_of_day_multiplier() -> float:
    """Return a delay multiplier based on time of day."""
    hour = datetime.now().hour
    if 0 <= hour < 6:
        return random.uniform(1.3, 1.8)
    elif 6 <= hour < 9:
        return random.uniform(1.0, 1.3)
    elif 9 <= hour < 17:
        return random.uniform(0.9, 1.1)
    elif 17 <= hour < 22:
        return random.uniform(1.0, 1.2)
    else:
        return random.uniform(1.2, 1.5)


def _move_mouse_to_element(page, locator) -> None:
    """Move mouse naturally toward an element before clicking."""
    try:
        box = locator.bounding_box(timeout=3000)
        if not box:
            return
        vp = page.viewport_size
        if not vp:
            return
        start_x = random.randint(100, max(101, vp["width"] - 100))
        start_y = random.randint(100, max(101, vp["height"] - 100))
        target_x = box["x"] + box["width"] / 2 + random.uniform(-5, 5)
        target_y = box["y"] + box["height"] / 2 + random.uniform(-3, 3)

        page.mouse.move(start_x, start_y)
        time.sleep(random.uniform(0.05, 0.15))

        steps = random.randint(3, 6)
        for i in range(1, steps + 1):
            t = i / steps
            x = start_x + (target_x - start_x) * t + random.uniform(-2, 2)
            y = start_y + (target_y - start_y) * t + random.uniform(-2, 2)
            page.mouse.move(x, y)
            time.sleep(random.uniform(0.02, 0.08))
    except Exception:
        pass


def _type_query_like_human(page, query: str) -> None:
    """Type a search query character-by-character with natural timing and corrections."""
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


def _scroll_page_naturally(page, duration_sec: float) -> None:
    """Scroll the page naturally over the given duration."""
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
            # Occasional backward scroll
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


def _browse_serp(page, config: Config, wlog) -> None:
    """Simulate browsing the SERP before clicking — reading results like a human."""
    wlog.debug("Browsing SERP naturally before clicking...")
    duration = random.uniform(5.0, 15.0)
    _scroll_page_naturally(page, duration)
    # Occasionally scroll back to top
    if random.random() < 0.4:
        try:
            page.evaluate("window.scrollTo(0, 0)")
            time.sleep(random.uniform(0.5, 1.5))
        except Exception:
            pass


def _click_random_non_target(page, results: list[SearchResult], target_domain: str, config: Config, wlog) -> None:
    """Click a random non-target result, dwell, then go back."""
    non_targets = [r for r in results if target_domain not in r.url and "google." not in r.url]
    if not non_targets:
        return

    chosen = random.choice(non_targets)
    wlog.info("Non-target click: %s", chosen.url[:80])

    try:
        css_safe_url = chosen.url.split("#")[0].split("?")[0]
        link = page.locator(f"a[href*='{css_safe_url}']").first
        if not link.is_visible(timeout=2000) and chosen.title:
            link = page.locator("a[href^='http']").filter(has_text=chosen.title[:40]).first

        _move_mouse_to_element(page, link)
        link.scroll_into_view_if_needed(timeout=5000)
        interruptible_sleep(random.uniform(0.3, 0.8))
        link.click(timeout=5000)

        dwell = random.uniform(*config.non_target_dwell)
        wlog.debug("Dwelling on non-target for %.1fs...", dwell)
        interruptible_sleep(dwell * 0.3)
        _scroll_page_naturally(page, dwell * 0.7)

        page.go_back(wait_until="domcontentloaded", timeout=15000)
        interruptible_sleep(random.uniform(1, 3))
    except Exception as e:
        wlog.debug("Non-target click failed: %s", e)
        try:
            page.go_back(wait_until="domcontentloaded", timeout=10000)
            interruptible_sleep(random.uniform(1, 2))
        except Exception:
            pass


def _dwell_on_target(page, config: Config, wlog) -> None:
    """Dwell on target page with natural scrolling."""
    dwell = random.uniform(*config.target_dwell)
    wlog.debug("Dwelling on target for %.1fs...", dwell)
    interruptible_sleep(dwell * 0.3)
    _scroll_page_naturally(page, dwell * 0.7)


# ---------------------------------------------------------------------------
# Navigation helpers
# ---------------------------------------------------------------------------
def _find_search_box(page):
    """Find and return the Google search box locator, or None."""
    for sel in ['textarea[name="q"]', 'input[name="q"]', '[aria-label="Search"]']:
        try:
            box = page.locator(sel).first
            if box.is_visible(timeout=2000):
                return box
        except Exception:
            continue
    return None


def _goto_next_results_page(page, current_page_one_based: int) -> bool:
    """Navigate to the next Google results page."""
    try:
        parsed = urllib.parse.urlparse(page.url)
        qs = urllib.parse.parse_qs(parsed.query)
        qs["start"] = [str(current_page_one_based * 10)]
        new_query = urllib.parse.urlencode(qs, doseq=True)
        next_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}?{new_query}"
        page.goto(next_url, wait_until="domcontentloaded", timeout=20000)
        interruptible_sleep(random.uniform(1.5, 3))
        page.wait_for_selector("#search, #rso, [role='main']", timeout=25_000)
        interruptible_sleep(random.uniform(1, 2))
        return True
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Search pipeline
# ---------------------------------------------------------------------------
# Layout watchdog: consecutive zero-result keywords
_zero_result_counter = 0
_zero_result_lock = threading.Lock()
ZERO_RESULT_ALARM_THRESHOLD = 5


def _track_zero_results(had_results: bool, wlog) -> None:
    global _zero_result_counter
    with _zero_result_lock:
        if had_results:
            _zero_result_counter = 0
        else:
            _zero_result_counter += 1
            if _zero_result_counter >= ZERO_RESULT_ALARM_THRESHOLD:
                wlog.critical(
                    "ALERT: %d consecutive keywords found ZERO results. "
                    "Google SERP layout may have changed. Check selectors.",
                    _zero_result_counter,
                )


def run_one_search(
    page, context, query: str, target_domain: str, config: Config, wlog, deadline: float
) -> tuple[int, int] | SearchOutcome | None:
    """Search for query, find and click target_domain in organic results.

    Returns (page_num, position) on success, SearchOutcome on error, None if not found.
    """
    # Step 1: Find search box
    search_box = _find_search_box(page)
    if not search_box:
        return None

    # Step 2: Move mouse to search box and type
    _move_mouse_to_element(page, search_box)
    search_box.click()
    interruptible_sleep(random.uniform(0.4, 0.9))
    _type_query_like_human(page, query)
    interruptible_sleep(random.uniform(0.3, 0.7))

    # Step 3: Submit
    page.keyboard.press("Enter")
    try:
        page.wait_for_url(
            lambda url: "google.com/search" in url or "sorry" in url or "consent.google" in url,
            timeout=20_000,
        )
    except Exception:
        pass
    interruptible_sleep(random.uniform(1, 2))

    # Step 4: Handle post-search captcha/consent
    if "sorry" in page.url or _page_looks_like_verification(page):
        wlog.warning("Robot verification after search. Handling...")
        result = _handle_sorry_page(page, context, config, wlog)
        if result is SearchOutcome.REOPEN_NEEDED:
            return SearchOutcome.REOPEN_NEEDED
        if result is not True:
            return None
        interruptible_sleep(random.uniform(1, 2))

    _handle_consent(page, wlog)

    # Step 5: Wait for results with verification retry
    results_selector = "#search, #rso, [role='main']"
    max_verification_retries = 3
    for _ in range(max_verification_retries):
        try:
            page.wait_for_selector(results_selector, timeout=25_000)
        except Exception:
            if _page_looks_like_verification(page):
                wlog.warning("Verification page detected. Handling...")
                result = _handle_sorry_page(page, context, config, wlog)
                if result is SearchOutcome.REOPEN_NEEDED:
                    return SearchOutcome.REOPEN_NEEDED
                if result is not True:
                    return None
                interruptible_sleep(random.uniform(1, 2))
                continue
            return None

        interruptible_sleep(random.uniform(1, 2))
        if _page_looks_like_verification(page):
            wlog.warning("Verification content detected. Solving...")
            result = _handle_sorry_page(page, context, config, wlog)
            if result is SearchOutcome.REOPEN_NEEDED:
                return SearchOutcome.REOPEN_NEEDED
            if result is not True:
                return None
            interruptible_sleep(random.uniform(1, 2))
            continue
        break
    else:
        return None

    interruptible_sleep(random.uniform(1, 2))
    had_any_results = False

    # Step 6: Paginate through results
    for page_num in range(1, config.max_result_pages + 1):
        # Check deadline
        if time.monotonic() > deadline:
            wlog.warning("Keyword deadline exceeded at page %d.", page_num)
            return SearchOutcome.TIMEOUT

        # Extract organic results
        scan = _extract_organic_results(page, target_domain)

        # Handle captcha if no results
        if not scan.has_results and scan.captcha_detected:
            result = _handle_captcha_no_results(page, context, config, wlog)
            if result is SearchOutcome.REOPEN_NEEDED:
                return SearchOutcome.REOPEN_NEEDED
            interruptible_sleep(random.uniform(1, 2))
            scan = _extract_organic_results(page, target_domain)

        if scan.has_results:
            had_any_results = True

        if not scan.has_results:
            if _is_end_of_results(page):
                wlog.debug("End of results detected at page %d.", page_num)
            break

        if scan.target_found:
            if scan.target_is_ad:
                wlog.info("Target only found as ad on page %d, trying next page.", page_num)
            else:
                # Optional: browse SERP before clicking
                if random.random() < config.serp_browse_probability:
                    _browse_serp(page, config, wlog)

                # Optional: click a non-target result first
                if random.random() < config.non_target_click_probability:
                    _click_random_non_target(page, scan.results, target_domain, config, wlog)

                    # Re-scan SERP after back-navigation (DOM may have changed)
                    current_url = page.url or ""
                    if "google." not in current_url:
                        wlog.warning("Not back on SERP after non-target click. Re-navigating...")
                        try:
                            page.goto(
                                f"https://www.google.com/search?q={urllib.parse.quote(query)}"
                                f"&start={(page_num - 1) * 10}",
                                wait_until="domcontentloaded",
                                timeout=20000,
                            )
                            interruptible_sleep(random.uniform(1, 2))
                        except Exception:
                            continue

                    scan = _extract_organic_results(page, target_domain)
                    if not scan.target_found or scan.target_is_ad:
                        wlog.warning("Target lost from SERP after non-target click.")
                        if page_num < config.max_result_pages:
                            continue
                        break

                # Find the organic target link (skip ad copies)
                try:
                    all_target_links = page.locator(f"a[href*='{target_domain}']").all()
                    link_to_click = None
                    for candidate in all_target_links:
                        if not _is_ad_result(page, candidate):
                            link_to_click = candidate
                            break

                    if link_to_click is None:
                        wlog.info("Target only found as ad links, trying next page.")
                    else:
                        _move_mouse_to_element(page, link_to_click)
                        link_to_click.scroll_into_view_if_needed(timeout=5000)
                        interruptible_sleep(random.uniform(0.2, 0.5))
                        link_to_click.click(timeout=5000)
                        interruptible_sleep(random.uniform(2, 4))
                        _dwell_on_target(page, config, wlog)
                        _track_zero_results(True, wlog)
                        return (page_num, scan.target_position or 0)
                except Exception as e:
                    wlog.debug("Target click failed: %s", e)

        # Navigate to next page
        if page_num < config.max_result_pages:
            delay_sec = random.uniform(*config.delay_between_pages)
            _scroll_page_naturally(page, delay_sec)
            if not _goto_next_results_page(page, page_num):
                break

    _track_zero_results(had_any_results, wlog)
    return None


# ---------------------------------------------------------------------------
# Browser management
# ---------------------------------------------------------------------------
def _build_launch_options(config: Config) -> dict:
    """Build Playwright launch options with all Chrome suppression flags."""
    viewport = random.choice(VIEWPORT_POOL)
    user_agent = random.choice(USER_AGENT_POOL) if USER_AGENT_POOL else (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36"
    )
    return {
        "channel": "chrome",
        "headless": False,
        "viewport": viewport,
        "args": CHROME_SUPPRESS_FLAGS,
        "user_agent": user_agent,
    }


def _launch_browser(p, profile_dir: Path, launch_options: dict, stealth: Stealth, config: Config):
    """Launch persistent context with exactly one page. Returns (context, page)."""
    try:
        context = p.chromium.launch_persistent_context(str(profile_dir), **launch_options)
        context.set_default_timeout(config.default_operation_timeout_ms)
        context.set_default_navigation_timeout(config.default_operation_timeout_ms)
        stealth.apply_stealth_sync(context)

        # Wait for Chrome to finish startup (extra windows appear asynchronously)
        time.sleep(2.0)

        # Get or create the main page
        page = context.pages[0] if context.pages else context.new_page()

        # Close all extra pages (round 1)
        for pg in context.pages:
            if pg != page:
                try:
                    pg.close()
                except Exception:
                    pass

        # Brief wait for any stragglers, then close again (round 2)
        time.sleep(0.5)
        for pg in context.pages:
            if pg != page:
                try:
                    pg.close()
                except Exception:
                    pass

        # Auto-close any future popups/new tabs
        context.on("page", lambda new_page: new_page.close())

        return context, page
    except Exception as e:
        logger.error("Failed to launch browser: %s", e)
        raise


def _safe_relaunch(p, context, profile_dir, launch_options, stealth, config, wlog):
    """Close old context and launch a fresh browser."""
    try:
        if context is not None:
            context.close()
    except Exception:
        pass
    return _launch_browser(p, profile_dir, launch_options, stealth, config)


def _relaunch_and_ready(p, context, profile_dir, launch_options, stealth, config: Config, wlog):
    """Relaunch browser and ensure Google is ready. Returns (context, page)."""
    # Check network first
    if not _wait_for_network(wlog):
        raise RuntimeError("Shutdown requested or network unavailable")

    for attempt in range(1, config.max_relaunch_attempts + 1):
        try:
            context, page = _safe_relaunch(p, context, profile_dir, launch_options, stealth, config, wlog)
        except Exception as e:
            wlog.error("Browser launch failed (attempt %d/%d): %s", attempt, config.max_relaunch_attempts, e)
            if attempt < config.max_relaunch_attempts:
                if interruptible_sleep(config.relaunch_backoff_sec):
                    raise RuntimeError("Shutdown requested during relaunch backoff")
                continue
            raise RuntimeError(f"Failed to launch browser after {config.max_relaunch_attempts} attempts") from e

        need_reopen = _ensure_google_ready(page, context, config, wlog)
        if need_reopen is not SearchOutcome.REOPEN_NEEDED:
            return context, page

        wlog.warning("Google not ready (attempt %d/%d), retrying...", attempt, config.max_relaunch_attempts)
        if attempt < config.max_relaunch_attempts:
            if interruptible_sleep(config.relaunch_backoff_sec):
                raise RuntimeError("Shutdown requested during relaunch backoff")

    raise RuntimeError(f"Failed to reach Google after {config.max_relaunch_attempts} attempts")


# ---------------------------------------------------------------------------
# Worker
# ---------------------------------------------------------------------------
def worker(keywords_slice: list[tuple[str, str]], worker_id: int, config: Config) -> None:
    """Run one browser session over a slice of (keyword, domain) pairs."""
    wlog = _make_worker_logger(worker_id)

    if not keywords_slice:
        wlog.info("No keywords assigned. Exiting.")
        return

    # Stagger startup
    if worker_id > 1:
        delay = random.uniform(*config.worker_stagger_delay)
        if interruptible_sleep(delay):
            return

    wlog.info("Started — %d keywords to process.", len(keywords_slice))

    profile_dir = _create_temp_profile()
    launch_options = _build_launch_options(config)
    stealth = Stealth()

    # Schedule next session break
    next_break_at = random.randint(config.session_break_min_keywords, config.session_break_max_keywords)
    consecutive_captchas = 0

    try:
        with sync_playwright() as p:
            context, page = _relaunch_and_ready(
                p, None, profile_dir, launch_options, stealth, config, wlog
            )

            for idx, (query, domain) in enumerate(keywords_slice):
                if _shutdown_requested():
                    wlog.info("Shutdown requested. Exiting.")
                    break

                wlog.info("[%d/%d] %s → %s", idx + 1, len(keywords_slice),
                          query[:60] + ("..." if len(query) > 60 else ""), domain)

                had_captcha = False

                try:
                    # Per-keyword deadline
                    keyword_deadline = time.monotonic() + config.keyword_timeout_sec

                    result = run_one_search(page, context, query, domain, config, wlog, keyword_deadline)

                    if result is SearchOutcome.REOPEN_NEEDED:
                        wlog.warning("Browser error — retrying keyword...")
                        had_captcha = True
                        context, page = _relaunch_and_ready(
                            p, context, profile_dir, launch_options, stealth, config, wlog
                        )
                        keyword_deadline = time.monotonic() + config.keyword_timeout_sec
                        result = run_one_search(page, context, query, domain, config, wlog, keyword_deadline)
                        if result is SearchOutcome.REOPEN_NEEDED:
                            wlog.error("Retry also failed. Skipping keyword.")
                            context, page = _relaunch_and_ready(
                                p, context, profile_dir, launch_options, stealth, config, wlog
                            )

                    if result is SearchOutcome.TIMEOUT:
                        wlog.warning("Keyword timed out. Skipping.")
                        log_result(query, domain, "", "timeout", config)
                    elif result is not None and not isinstance(result, SearchOutcome):
                        page_num, pos = result
                        wlog.info("  ✓ Clicked  page=%d  pos=%d  [%s]", page_num, pos, domain)
                        log_result(query, domain, page_num, pos, config)
                    else:
                        wlog.info("  ✗ Not found")
                        log_result(query, domain, "", "not_found", config)

                except Exception as e:
                    wlog.error("Unhandled error on keyword '%s': %s", query[:60], e)
                    wlog.debug("Traceback: %s", traceback.format_exc())
                    log_result(query, domain, "", "error", config)
                    try:
                        context, page = _relaunch_and_ready(
                            p, context, profile_dir, launch_options, stealth, config, wlog
                        )
                    except Exception:
                        wlog.error("Cannot recover from error. Exiting worker.")
                        remaining = len(keywords_slice) - (idx + 1)
                        if remaining > 0:
                            wlog.error("%d keywords NOT processed (next cycle).", remaining)
                        break

                # Captcha cooldown tracking
                if had_captcha:
                    consecutive_captchas += 1
                    if consecutive_captchas >= config.captcha_consecutive_threshold:
                        wlog.warning(
                            "IP appears flagged (%d consecutive captchas). Cooling down for %d min...",
                            consecutive_captchas, int(config.captcha_cooldown_sec // 60),
                        )
                        if interruptible_sleep(config.captcha_cooldown_sec):
                            break
                        consecutive_captchas = 0
                        # Refresh profile to reset fingerprint
                        try:
                            context.close()
                        except Exception:
                            pass
                        shutil.rmtree(profile_dir, ignore_errors=True)
                        profile_dir = _create_temp_profile()
                        launch_options = _build_launch_options(config)
                        context, page = _relaunch_and_ready(
                            p, None, profile_dir, launch_options, stealth, config, wlog
                        )
                else:
                    consecutive_captchas = 0

                # Health checks & maintenance between keywords
                if idx < len(keywords_slice) - 1:
                    # Memory check
                    if (idx + 1) % config.memory_check_interval == 0:
                        if _check_memory(config, wlog):
                            wlog.info("High memory — refreshing browser profile.")
                            try:
                                context.close()
                            except Exception:
                                pass
                            shutil.rmtree(profile_dir, ignore_errors=True)
                            profile_dir = _create_temp_profile()
                            launch_options = _build_launch_options(config)
                            context, page = _relaunch_and_ready(
                                p, None, profile_dir, launch_options, stealth, config, wlog
                            )

                    # Profile refresh (prevent state accumulation)
                    elif (idx + 1) % config.profile_refresh_interval == 0:
                        wlog.info("Refreshing browser profile (every %d keywords).", config.profile_refresh_interval)
                        try:
                            context.close()
                        except Exception:
                            pass
                        shutil.rmtree(profile_dir, ignore_errors=True)
                        profile_dir = _create_temp_profile()
                        launch_options = _build_launch_options(config)
                        context, page = _relaunch_and_ready(
                            p, None, profile_dir, launch_options, stealth, config, wlog
                        )

                    # Session break (human-like pauses)
                    elif (idx + 1) >= next_break_at:
                        break_duration = random.uniform(*config.session_break_duration)
                        wlog.info("Session break: pausing for %.0fs...", break_duration)
                        if interruptible_sleep(break_duration):
                            break
                        next_break_at = (idx + 1) + random.randint(
                            config.session_break_min_keywords, config.session_break_max_keywords
                        )

                    # Browser health check
                    if not _browser_is_healthy(page, wlog):
                        wlog.warning("Browser unhealthy. Relaunching...")
                        context, page = _relaunch_and_ready(
                            p, context, profile_dir, launch_options, stealth, config, wlog
                        )
                    else:
                        # Normal delay between keywords
                        delay = random.uniform(*config.delay_between_keywords) * _time_of_day_multiplier()
                        if interruptible_sleep(delay):
                            break

                        # Navigate back to Google for next keyword
                        try:
                            page.goto("https://www.google.com", wait_until="domcontentloaded", timeout=20000)
                        except Exception as e:
                            wlog.error("Failed to navigate to Google: %s. Reopening.", e)
                            context, page = _relaunch_and_ready(
                                p, context, profile_dir, launch_options, stealth, config, wlog
                            )
                            continue
                        ver = _handle_verification(page, context, config, wlog)
                        if ver is SearchOutcome.REOPEN_NEEDED:
                            context, page = _relaunch_and_ready(
                                p, context, profile_dir, launch_options, stealth, config, wlog
                            )

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
    atexit.register(_kill_all_child_browsers)

    config = Config.from_env()
    _setup_logging(config)
    _cleanup_stale_profiles()

    # Build user agent pool based on installed Chrome
    global USER_AGENT_POOL
    USER_AGENT_POOL = _build_user_agent_pool()
    logger.info("User agent pool: %d agents (Chrome versions from installed browser).", len(USER_AGENT_POOL))

    # Check Anti-Captcha balance on startup
    if config.anticaptcha_api_key:
        balance = _check_anticaptcha_balance(config.anticaptcha_api_key)
        if balance is not None:
            logger.info("Anti-Captcha balance: $%.2f", balance)
            if balance < 2.0:
                logger.warning("Anti-Captcha balance is low ($%.2f). Consider topping up.", balance)
        else:
            logger.warning("Could not check Anti-Captcha balance.")
    else:
        logger.warning("ANTICAPTCHA_API_KEY not set. Captcha solving disabled.")

    cycle = 0

    while not _shutdown_requested():
        cycle += 1
        _reset_captcha_count()

        # Load keywords
        keywords = load_keywords(config.keywords_csv)
        if not keywords:
            logger.error("No keywords found in %s. Retrying in 60s...", config.keywords_csv)
            if interruptible_sleep(60):
                break
            continue

        # Determine remaining work
        completed = get_completed_keywords(config)
        keywords_to_do = [(kw, d) for kw, d in keywords if (kw, d) not in completed]

        if not keywords_to_do:
            logger.info("All %d keywords processed. Archiving and starting fresh cycle.", len(keywords))
            move_result_log_to_archive(config)
            _prune_archives(config)
            if interruptible_sleep(config.cycle_complete_delay_sec):
                break
            continue

        logger.info("=== Cycle %d: %d remaining of %d total ===", cycle, len(keywords_to_do), len(keywords))
        if completed:
            logger.info("Resuming: %d already done.", len(completed))

        # Randomize keyword order
        random.shuffle(keywords_to_do)

        # Split keywords across workers
        num_workers = min(config.num_workers, len(keywords_to_do))
        chunk_size = len(keywords_to_do) // num_workers
        chunks: list[list[tuple[str, str]]] = []
        for i in range(num_workers):
            start = i * chunk_size
            end = start + chunk_size if i < num_workers - 1 else len(keywords_to_do)
            chunks.append(keywords_to_do[start:end])

        logger.info("Splitting %d keywords across %d workers: %s",
                     len(keywords_to_do), num_workers, [len(c) for c in chunks])

        threads: list[threading.Thread] = []
        for wid, chunk in enumerate(chunks, 1):
            t = threading.Thread(target=worker, args=(chunk, wid, config), daemon=True)
            threads.append(t)

        for t in threads:
            t.start()

        # Wait for threads
        try:
            while any(t.is_alive() for t in threads):
                if _shutdown_requested():
                    break
                for t in threads:
                    t.join(timeout=0.5)
        except KeyboardInterrupt:
            _signal_handler()

        if _shutdown_requested():
            break

        # Check completion
        completed_now = get_completed_keywords(config)
        all_keywords_set = set(keywords)
        if all_keywords_set.issubset(completed_now):
            logger.info("Cycle %d complete. All %d keywords processed. Archiving.", cycle, len(keywords))
            move_result_log_to_archive(config)
            _prune_archives(config)
            if interruptible_sleep(config.cycle_complete_delay_sec):
                break
        else:
            remaining = len(all_keywords_set - completed_now)
            logger.info("Cycle %d finished. %d keywords remain for next iteration.", cycle, remaining)

    logger.info("Shutdown complete.")


if __name__ == "__main__":
    main()
