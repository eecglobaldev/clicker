import signal, time, os, sys
import psutil

def _signal_handler(signum, frame):
    print("\nCtrl+C received in Python — killing browsers and stopping...", flush=True)
    curr_proc = psutil.Process()
    for child in curr_proc.children(recursive=True):
        try:
            name = child.name().lower()
            if "chrome" in name or "chromium" in name:
                child.kill()
        except psutil.NoSuchProcess:
            pass
    os._exit(0)

signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)

from playwright.sync_api import sync_playwright
try:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()
        page.goto('https://google.com')
        print("Browser open. Press Ctrl+C in terminal...")
        time.sleep(60)
except Exception as e:
    print("Exception", e)
