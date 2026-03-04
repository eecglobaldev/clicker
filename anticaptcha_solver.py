"""
Solve Google reCAPTCHA (robot verification) using Anti-Captcha API.
Uses RecaptchaV2EnterpriseTaskProxyless per https://anti-captcha.com/apidoc/task-types/RecaptchaV2EnterpriseTaskProxyless
"""

import logging
import re
import time
from typing import Any

# Known site key used on Google's sorry page (fallback if extraction fails)
GOOGLE_SORRY_SITEKEY = "6LfwHekUAAAAAPBMRMUvw_bQySVjvV4W1oW7gJKp"

# Timeout for Anti-Captcha to solve a single task (seconds).
# The SDK default is 300s (5 min); we increase to 600s (10 min) for hard captchas.
ANTICAPTCHA_SOLVE_TIMEOUT_SEC = 600


def _print_solver_error(solver: Any, api_key: str) -> None:
    """Log the Anti-Captcha error on failure."""
    err_code = getattr(solver, "error_code", "") or "" if solver else ""
    err_str = getattr(solver, "err_string", "") or "" if solver else ""
    print(f"    [Anti-Captcha] ERROR: error_code={repr(err_code)} err_string={repr(err_str)} — no token (check key/balance).")


def solve_google_recaptcha(page, api_key: str) -> bool:
    """
    If current page shows Google reCAPTCHA (sorry URL or verification content), extract params,
    get token from Anti-Captcha, inject token and submit. Returns True if solved and submitted.
    """
    if not api_key:
        return False
    url = page.url or ""
    try:
        content = page.content()
    except Exception:
        content = ""
    is_verification_page = (
        "sorry" in url
        or "unusual traffic" in content.lower()
        or "not a robot" in content.lower()
        or "g-recaptcha" in content
    )
    if not is_verification_page:
        return False

    try:
        time.sleep(2)
        html = page.content()
        sitekey = _extract_sitekey(html)
        enterprise_payload = _extract_enterprise_payload(html)
        data_s = _extract_data_s(html)
        if not sitekey and "google.com" in page.url:
            sitekey = GOOGLE_SORRY_SITEKEY
        if not sitekey:
            return False

        website_url = page.url.split("?")[0] if "?" in page.url else page.url
        if not website_url.startswith("http"):
            website_url = "https://www.google.com/sorry/index"
        token, last_solver = _get_token_from_anticaptcha(
            api_key=api_key,
            website_url=website_url,
            website_key=sitekey,
            enterprise_payload=enterprise_payload,
            data_s=data_s,
        )
        if not token:
            _print_solver_error(last_solver, api_key)
            return False
        ok = _inject_token_and_submit(page, token)
        return ok
    except Exception as e:
        logging.getLogger("google_clicker").error("[Anti-Captcha] Error: %s", e)
        return False


def _extract_sitekey(html: str) -> str | None:
    """Extract reCAPTCHA site key from page HTML."""
    # data-sitekey="..."
    m = re.search(r'data-sitekey=["\']([^"\']+)["\']', html, re.I)
    if m:
        return m.group(1).strip()
    # "sitekey":"..." or sitekey: "..."
    m = re.search(r'["\']?sitekey["\']?\s*[:=]\s*["\']([^"\']+)["\']', html, re.I)
    if m:
        return m.group(1).strip()
    # 'k':'...' or "k":"..." (Google sometimes uses this)
    m = re.search(r'["\']k["\']\s*:\s*["\']([^"\']+)["\']', html)
    if m:
        return m.group(1).strip()
    # 6L... pattern (reCAPTCHA keys start with 6L)
    m = re.search(r'["\'](6L[a-zA-Z0-9_-]{38,50})["\']', html)
    if m:
        return m.group(1).strip()
    return None


def _extract_enterprise_payload(html: str) -> dict[str, Any] | None:
    """Extract enterprise render params (e.g. 's' token) for RecaptchaV2Enterprise."""
    # grecaptcha.enterprise.render(..., { ..., s: "..." })
    m = re.search(r'["\']s["\']\s*:\s*["\']([^"\']+)["\']', html)
    if m:
        return {"s": m.group(1).strip()}
    return None


def _extract_data_s(html: str) -> str | None:
    """Extract data-s value for Google reCAPTCHA (RecaptchaV2TaskProxyless)."""
    m = re.search(r'data-s=["\']([^"\']+)["\']', html, re.I)
    if m:
        return m.group(1).strip()
    return None


def _fetch_task_result_raw(api_key: str, task_id: int) -> dict | None:
    """Call getTaskResult and return the raw JSON for debugging."""
    try:
        import requests
        r = requests.post(
            "https://api.anti-captcha.com/getTaskResult",
            json={"clientKey": api_key, "taskId": task_id},
            headers={"Content-Type": "application/json", "Accept": "application/json"},
            timeout=30,
        )
        return r.json()
    except Exception as e:
        print("    [Anti-Captcha] Could not fetch raw task result:", e)
        return None


ANTICAPTCHA_API_URL = "https://api.anti-captcha.com"
_REQUEST_TIMEOUT = 12  # seconds per individual HTTP call (short to catch dropped packets fast)


def _ac_post(endpoint: str, payload: dict) -> dict:
    """POST to Anti-Captcha API with explicit timeout and retries for flaky networks."""
    import requests
    url = f"{ANTICAPTCHA_API_URL}/{endpoint}"
    last_err = None
    for attempt in range(1, 4):
        try:
            r = requests.post(
                url,
                json=payload,
                timeout=_REQUEST_TIMEOUT,
                headers={"Content-Type": "application/json", "Accept": "application/json"},
            )
            r.raise_for_status()
            return r.json()
        except requests.exceptions.RequestException as e:
            last_err = e
            time.sleep(1)  # brief pause before retry
    raise last_err


def _create_task(api_key: str, task: dict) -> int | None:
    """Submit a task to Anti-Captcha. Returns task_id on success, None on failure."""
    log = logging.getLogger("google_clicker")
    try:
        resp = _ac_post("createTask", {"clientKey": api_key, "task": task})
        if resp.get("errorId", 1) != 0:
            log.error("[Anti-Captcha] createTask error — %s: %s",
                      resp.get("errorCode", "?"), resp.get("errorDescription", "?"))
            return None
        task_id = resp.get("taskId")
        log.info("[Anti-Captcha] Task created (id=%s). Waiting up to %ds...",
                 task_id, ANTICAPTCHA_SOLVE_TIMEOUT_SEC)
        return task_id
    except Exception as e:
        log.error("[Anti-Captcha] createTask request failed: %s", e)
        return None


def _poll_task(api_key: str, task_id: int, timeout_sec: int = ANTICAPTCHA_SOLVE_TIMEOUT_SEC) -> str | None:
    """Poll getTaskResult until solved, failed, or timed out. Returns gRecaptchaResponse token."""
    log = logging.getLogger("google_clicker")
    deadline = time.monotonic() + timeout_sec
    time.sleep(3)  # recommended initial wait before first poll
    poll_interval = 5

    while time.monotonic() < deadline:
        time.sleep(poll_interval)
        try:
            resp = _ac_post("getTaskResult", {"clientKey": api_key, "taskId": task_id})
        except Exception as e:
            log.warning("[Anti-Captcha] getTaskResult request failed: %s", e)
            continue

        if resp.get("errorId", 0) != 0:
            log.error("[Anti-Captcha] Task error — %s: %s",
                      resp.get("errorCode", "?"), resp.get("errorDescription", "?"))
            return None

        status = resp.get("status", "")
        if status == "ready":
            token = (resp.get("solution") or {}).get("gRecaptchaResponse")
            if token:
                log.info("[Anti-Captcha] Token received.")
                return token
            log.error("[Anti-Captcha] Status ready but no token in solution.")
            return None
        # status == "processing" — keep waiting

    log.error("[Anti-Captcha] Timed out after %ds waiting for task %s.", timeout_sec, task_id)
    return None


def _get_token_from_anticaptcha(
    api_key: str,
    website_url: str,
    website_key: str,
    enterprise_payload: dict | None = None,
    data_s: str | None = None,
) -> tuple[str | None, Any]:
    """Get gRecaptchaResponse token from Anti-Captcha REST API. Returns (token, None)."""
    log = logging.getLogger("google_clicker")

    # Try RecaptchaV2Enterprise first (recommended for Google)
    enterprise_task: dict = {
        "type": "RecaptchaV2EnterpriseTaskProxyless",
        "websiteURL": website_url,
        "websiteKey": website_key,
    }
    if enterprise_payload:
        enterprise_task["enterprisePayload"] = enterprise_payload

    task_id = _create_task(api_key, enterprise_task)
    if task_id is not None:
        token = _poll_task(api_key, task_id)
        if token:
            return (token, None)
        log.warning("[Anti-Captcha] Enterprise task failed. Trying V2 fallback...")

    # Fallback: RecaptchaV2 proxyless with optional data-s
    v2_task: dict = {
        "type": "RecaptchaV2TaskProxyless",
        "websiteURL": website_url,
        "websiteKey": website_key,
    }
    if data_s:
        v2_task["recaptchaDataSValue"] = data_s

    task_id = _create_task(api_key, v2_task)
    if task_id is not None:
        token = _poll_task(api_key, task_id)
        if token:
            return (token, None)

    return (None, None)


def _inject_token_and_submit(page, token: str) -> bool:
    """Inject g-recaptcha-response token into the page and submit the form."""
    try:
        submitted = page.evaluate(
            """(token) => {
                const textarea = document.querySelector('textarea[name="g-recaptcha-response"]') 
                    || document.getElementById('g-recaptcha-response')
                    || document.querySelector('textarea[name="g-recaptcha-response"]');
                if (!textarea) {
                    const iframes = document.querySelectorAll('iframe');
                    for (const f of iframes) {
                        try {
                            const doc = f.contentDocument || f.contentWindow?.document;
                            if (doc) {
                                const ta = doc.querySelector('textarea');
                                if (ta) { ta.value = token; ta.innerHTML = token; }
                            }
                        } catch(e) {}
                    }
                    return false;
                }
                textarea.innerHTML = token;
                textarea.value = token;
                textarea.dispatchEvent(new Event('input', { bubbles: true }));
                const form = textarea.closest('form');
                if (form) {
                    const callbackName = document.querySelector('[data-callback]')?.getAttribute('data-callback');
                    if (callbackName && typeof window[callbackName] === 'function') {
                        try { window[callbackName](token); } catch(e) {}
                    }
                    form.submit();
                    return true;
                }
                const btn = document.querySelector('input[type="submit"]') || document.querySelector('button[type="submit"]') || document.querySelector('input[value="Submit"]');
                if (btn) { btn.click(); return true; }
                return false;
            }""",
            token,
        )
        if submitted:
            # Wait for the page to actually navigate after form submit
            try:
                page.wait_for_load_state("domcontentloaded", timeout=15000)
            except Exception:
                # Fallback: fixed wait if load state times out
                time.sleep(4)
        return bool(submitted)
    except Exception:
        return False
