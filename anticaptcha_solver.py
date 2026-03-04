"""
Solve Google reCAPTCHA (robot verification) using Anti-Captcha API.
Uses RecaptchaV2EnterpriseTaskProxyless per https://anti-captcha.com/apidoc/task-types/RecaptchaV2EnterpriseTaskProxyless
"""

import json
import re
import time
from typing import Any

# Known site key used on Google's sorry page (fallback if extraction fails)
GOOGLE_SORRY_SITEKEY = "6LfwHekUAAAAAPBMRMUvw_bQySVjvV4W1oW7gJKp"

# Timeout for Anti-Captcha to solve a single task (seconds).
# The SDK default is 300s (5 min); we increase to 600s (10 min) for hard captchas.
ANTICAPTCHA_SOLVE_TIMEOUT_SEC = 600


def _print_solver_error(solver: Any, api_key: str) -> None:
    """Print the exact error from the solver and the raw API response when available."""
    if solver is None:
        print("    [Anti-Captcha] No solver instance (createTask may have failed early).")
        return
    err_code = getattr(solver, "error_code", "") or ""
    err_str = getattr(solver, "err_string", "") or ""
    task_id = getattr(solver, "task_id", 0) or 0
    print("    [Anti-Captcha] Solver error_code:", repr(err_code))
    print("    [Anti-Captcha] Solver err_string:", repr(err_str))
    if task_id:
        raw = _fetch_task_result_raw(api_key, task_id)
        if raw is not None:
            print("    [Anti-Captcha] Raw getTaskResult response:", json.dumps(raw, indent=2))
    if not err_code and not err_str:
        print("    [Anti-Captcha] (No error_code/err_string set; token was empty or 0.)")
    print("    [Anti-Captcha] Failed: no token from API (check key, balance, or try again).")


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
        print("    [Anti-Captcha] Reading verification page...")
        time.sleep(2)
        html = page.content()
        sitekey = _extract_sitekey(html)
        enterprise_payload = _extract_enterprise_payload(html)
        data_s = _extract_data_s(html)
        if not sitekey and "google.com" in page.url:
            sitekey = GOOGLE_SORRY_SITEKEY
            print("    [Anti-Captcha] Using fallback site key for Google.")
        if not sitekey:
            print("    [Anti-Captcha] Failed: could not find reCAPTCHA site key on page.")
            return False
        print(f"    [Anti-Captcha] Site key found: {sitekey[:20]}...")

        website_url = page.url.split("?")[0] if "?" in page.url else page.url
        if not website_url.startswith("http"):
            website_url = "https://www.google.com/sorry/index"
        print(f"    [Anti-Captcha] Sending task to Anti-Captcha API (timeout {ANTICAPTCHA_SOLVE_TIMEOUT_SEC}s)...")
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
        print("    [Anti-Captcha] Token received. Submitting form...")

        ok = _inject_token_and_submit(page, token)
        if ok:
            print("    [Anti-Captcha] Form submitted.")
        else:
            print("    [Anti-Captcha] Could not find form/textarea to submit token.")
        return ok
    except Exception as e:
        print(f"    [Anti-Captcha] Error: {e}")
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


def _solve_with_timeout(solver, task_payload: dict) -> str | None:
    """Create task and wait with our custom timeout instead of the SDK default 300s."""
    if solver.create_task(task_payload) != 1:
        solver.log("could not create task")
        solver.log(solver.err_string)
        return None
    solver.log("created task with id " + str(solver.task_id))
    time.sleep(3)
    task_result = solver.wait_for_result(ANTICAPTCHA_SOLVE_TIMEOUT_SEC)
    if task_result == 0:
        return None
    return task_result["solution"]["gRecaptchaResponse"]


def _get_token_from_anticaptcha(
    api_key: str,
    website_url: str,
    website_key: str,
    enterprise_payload: dict | None = None,
    data_s: str | None = None,
) -> tuple[str | None, Any]:
    """Get gRecaptchaResponse token from Anti-Captcha. Returns (token, last_solver)."""
    last_solver = None
    # Try RecaptchaV2Enterprise first (recommended for Google)
    try:
        from anticaptchaofficial.recaptchav2enterpriseproxyless import recaptchaV2EnterpriseProxyless
        solver = recaptchaV2EnterpriseProxyless()
        solver.set_verbose(0)
        solver.set_key(api_key)
        solver.set_website_url(website_url)
        solver.set_website_key(website_key)
        if enterprise_payload:
            solver.set_enterprise_payload(enterprise_payload)
        last_solver = solver
        task_payload = {
            "clientKey": solver.client_key,
            "task": {
                "type": "RecaptchaV2EnterpriseTaskProxyless",
                "websiteURL": website_url,
                "websiteKey": website_key,
                "enterprisePayload": enterprise_payload,
            },
            "softId": solver.soft_id,
        }
        token = _solve_with_timeout(solver, task_payload)
        if token:
            return (token, solver)
    except Exception as e:
        print("    [Anti-Captcha] Enterprise solver exception:", e)

    # Fallback: RecaptchaV2 (non-Enterprise) with data-s for Google
    try:
        from anticaptchaofficial.recaptchav2proxyless import recaptchaV2Proxyless
        solver = recaptchaV2Proxyless()
        solver.set_verbose(0)
        solver.set_key(api_key)
        solver.set_website_url(website_url)
        solver.set_website_key(website_key)
        if data_s:
            solver.set_data_s(data_s)
        last_solver = solver
        task_data = {
            "type": "RecaptchaV2TaskProxyless",
            "websiteURL": website_url,
            "websiteKey": website_key,
        }
        if data_s:
            task_data["recaptchaDataSValue"] = data_s
        task_payload = {
            "clientKey": solver.client_key,
            "task": task_data,
            "softId": solver.soft_id,
        }
        token = _solve_with_timeout(solver, task_payload)
        if token:
            return (token, solver)
    except Exception as e:
        print("    [Anti-Captcha] V2 proxyless solver exception:", e)

    return (None, last_solver)


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
