#!/usr/bin/env python3
"""
tanuki_gex_screener.py
======================
Reads the TanukiTrade Options Screener, parses all tickers,
and ranks them by highest positive "First GEX" value.

REQUIREMENTS:
pip install selenium webdriver-manager pandas tabulate python-dotenv

SETUP:
1. Set credentials + screener options in .env:

   TANUKI_EMAIL=your@email.com
   TANUKI_TVUSER=yourTradingViewUsername
   TANUKI_HEADLESS=false

   # ── Screener options (all optional) ──
   TANUKI_PRESET=           # blank = default view | neg_gamma_squeeze | high_iv | net_gex
   TANUKI_FILTER_IVR_MIN=   # e.g. 50   → only show IVR >= 50
   TANUKI_FILTER_IVR_MAX=   # e.g. 100
   TANUKI_FILTER_IVX_5D_MIN=  # e.g. 15  → IVx 5d change >= 15%
   TANUKI_FILTER_NET_GEX=   # positive | negative | blank = all
   TANUKI_TOP_N=20
   TANUKI_MIN_GEX=0.0
   TANUKI_VERIFY_TIMEOUT=60

2. Chrome must be installed.
3. Run: python tanuki_gex_screener.py
"""

from __future__ import annotations

import os
import time
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv(usecwd=True))

import pandas as pd
from tabulate import tabulate

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("tanuki_screener")

TZ = ZoneInfo("America/New_York")

LOGIN_URL    = "https://app.tanukitrade.com/accounts/login/"
VERIFY_URL   = "https://app.tanukitrade.com/accounts/verify/"
SCREENER_URL = "https://app.tanukitrade.com/webapp/options-screener/"

# ── Credentials ───────────────────────────────────────────────────────────────
EMAIL  = os.getenv("TANUKI_EMAIL", "")
TVUSER = os.getenv("TANUKI_TVUSER", "")

# ── Screener config ───────────────────────────────────────────────────────────
TOP_N              = int(os.getenv("TANUKI_TOP_N", "20"))
MIN_GEX            = float(os.getenv("TANUKI_MIN_GEX", "0.0"))
HEADLESS           = os.getenv("TANUKI_HEADLESS", "false").lower() == "true"
PAGE_LOAD_TIMEOUT  = int(os.getenv("TANUKI_TIMEOUT", "40"))
VERIFY_TIMEOUT     = int(os.getenv("TANUKI_VERIFY_TIMEOUT", "60"))
DEBUG              = os.getenv("TANUKI_DEBUG", "false").lower() == "true"

# ── Preset & filter options ───────────────────────────────────────────────────
# PRESET: blank = default | "neg_gamma_squeeze" | "high_iv" | "net_gex"
PRESET             = os.getenv("TANUKI_PRESET", "").strip().lower()

# Numeric range filters applied AFTER scraping (client-side, AND logic)
FILTER_IVR_MIN     = os.getenv("TANUKI_FILTER_IVR_MIN", "").strip()
FILTER_IVR_MAX     = os.getenv("TANUKI_FILTER_IVR_MAX", "").strip()
FILTER_IVX_5D_MIN  = os.getenv("TANUKI_FILTER_IVX_5D_MIN", "").strip()
# "positive" | "negative" | "" = all
FILTER_NET_GEX     = os.getenv("TANUKI_FILTER_NET_GEX", "").strip().lower()

# ── Preset keyword map (matches visible button text on screener page) ─────────
PRESET_KEYWORDS = {
    "neg_gamma_squeeze": ("negative gamma", "neg gamma", "gamma squeeze", "put support"),
    "high_iv":           ("high iv", "iv spike", "high ivx", "vol spike", "expanding vol"),
    "net_gex":           ("net gex", "gex rank", "highest gex", "positive gex"),
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_gex_value(raw: str) -> float:
    """Convert GEX strings like '$1.23B', '-$456M', '2.1T' -> float in billions."""
    raw = raw.strip().replace(",", "").replace("$", "").replace("+", "").upper()
    if not raw or raw in ("-", "N/A", "--", ""):
        return float("nan")
    multipliers = {"T": 1_000, "B": 1, "M": 0.001, "K": 0.000_001}
    for suffix, mult in multipliers.items():
        if raw.endswith(suffix):
            try:
                return float(raw[:-1]) * mult
            except ValueError:
                return float("nan")
    try:
        return float(raw)
    except ValueError:
        return float("nan")


def _parse_float_col(val: str) -> float:
    """Parse a generic numeric screener cell (handles %, $, commas)."""
    try:
        return float(
            str(val).strip()
            .replace(",", "").replace("$", "")
            .replace("%", "").replace("+", "")
        )
    except (ValueError, TypeError):
        return float("nan")


def build_driver() -> webdriver.Chrome:
    opts = Options()
    if HEADLESS:
        opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1400,900")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    if not HEADLESS:
        opts.add_experimental_option("detach", True)
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=opts)
    driver.execute_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    )
    return driver


def _dump_page(driver: webdriver.Chrome, label: str) -> None:
    fname = f"debug_{label}.html"
    with open(fname, "w", encoding="utf-8") as f:
        f.write(driver.page_source)
    log.info("Page source saved to %s", fname)
    log.info("Current URL : %s", driver.current_url)
    log.info("Page title  : %s", driver.title)
    inputs = driver.find_elements(By.CSS_SELECTOR, "input")
    log.info("Input fields (%d):", len(inputs))
    for i, inp in enumerate(inputs):
        log.info(
            "  [%d] type=%-12s name=%-20s id=%-20s placeholder=%s",
            i,
            inp.get_attribute("type") or "",
            inp.get_attribute("name") or "",
            inp.get_attribute("id") or "",
            inp.get_attribute("placeholder") or "",
        )
    btns = driver.find_elements(By.CSS_SELECTOR, "button")
    log.info("Buttons (%d):", len(btns))
    for i, b in enumerate(btns):
        log.info(
            "  [%d] text=%-30s type=%s visible=%s",
            i, b.text.strip()[:30],
            b.get_attribute("type") or "",
            b.is_displayed(),
        )


def _click_submit_button(driver: webdriver.Chrome, fallback_field=None) -> None:
    keywords = ("sign in", "log in", "login", "submit", "next", "continue", "enter")
    for selector in [
        "button[type='submit']", "input[type='submit']",
        "button[type='button']", "button",
    ]:
        for btn in driver.find_elements(By.CSS_SELECTOR, selector):
            if not btn.is_displayed():
                continue
            if any(k in btn.text.strip().lower() for k in keywords):
                log.info("Clicking button: '%s'", btn.text.strip())
                btn.click()
                return
    log.info("No labelled button found — pressing Enter.")
    target = fallback_field or driver.switch_to.active_element
    target.send_keys(Keys.RETURN)


# ── Login ─────────────────────────────────────────────────────────────────────

def login(driver: webdriver.Chrome) -> bool:
    if not EMAIL or not TVUSER:
        log.error("TANUKI_EMAIL or TANUKI_TVUSER not set in .env")
        log.error("  TANUKI_EMAIL=your@email.com")
        log.error("  TANUKI_TVUSER=yourTradingViewUsername")
        return False

    log.info("Opening login page ...")
    driver.get(LOGIN_URL)
    wait = WebDriverWait(driver, PAGE_LOAD_TIMEOUT)
    time.sleep(2)

    if DEBUG:
        _dump_page(driver, "01_login_page")

    try:
        # Step 1: email
        try:
            email_field = wait.until(EC.visibility_of_element_located((
                By.CSS_SELECTOR,
                "input[type='email'], input[name='email'], "
                "input[id='id_email'], input[placeholder*='mail']",
            )))
        except TimeoutException:
            log.error("Email input not found.")
            _dump_page(driver, "01_no_email_field")
            return False

        email_field.clear()
        email_field.send_keys(EMAIL)
        log.info("Email entered: %s", EMAIL)
        time.sleep(0.5)

        # Step 2: TradingView username (name=tv_user, id=id_tv_user confirmed from logs)
        tvuser_field = None
        for sel in [
            "input[name='tv_user']", "input[id='id_tv_user']",
            "input[id*='tv_user']", "input[id*='tvuser']",
            "input[placeholder*='TradingView']", "input[placeholder*='username']",
        ]:
            fields = [f for f in driver.find_elements(By.CSS_SELECTOR, sel) if f.is_displayed()]
            if fields:
                tvuser_field = fields[0]
                log.info("TradingView field found via: %s", sel)
                break

        if tvuser_field is None:
            candidates = [
                f for f in driver.find_elements(
                    By.CSS_SELECTOR,
                    "input[type='text'], input:not([type='email'])"
                    ":not([type='hidden']):not([type='checkbox'])"
                    ":not([type='radio']):not([type='button'])",
                ) if f.is_displayed()
            ]
            if candidates:
                tvuser_field = candidates[0]
                log.info(
                    "TradingView field via fallback — id=%s placeholder=%s",
                    tvuser_field.get_attribute("id") or "",
                    tvuser_field.get_attribute("placeholder") or "",
                )

        if tvuser_field is None:
            log.error("TradingView username field not found.")
            _dump_page(driver, "02_no_tvuser_field")
            return False

        tvuser_field.clear()
        tvuser_field.send_keys(TVUSER)
        log.info("TradingView username entered.")
        time.sleep(0.5)

        if DEBUG:
            _dump_page(driver, "02_before_submit")

        # Step 3: Sign In
        _click_submit_button(driver, tvuser_field)
        time.sleep(2)

        # Step 4: handle /accounts/verify/ if present
        if VERIFY_URL in driver.current_url:
            log.info("Redirected to verify page: %s", driver.current_url)
            if DEBUG:
                _dump_page(driver, "03_verify_page")
            if not HEADLESS:
                log.info(
                    ">>> ACTION REQUIRED: Complete email/OTP verification in the "
                    "browser window. Script will continue automatically once verified "
                    "(up to %ds). <<<", VERIFY_TIMEOUT
                )
            try:
                WebDriverWait(driver, VERIFY_TIMEOUT).until(
                    lambda d: VERIFY_URL not in d.current_url
                )
                log.info("Verification complete — URL: %s", driver.current_url)
            except TimeoutException:
                log.error("Timed out waiting for verification (%ds).", VERIFY_TIMEOUT)
                _dump_page(driver, "03_verify_timeout")
                return False

        if "login" in driver.current_url.lower():
            log.error("Still on login page — check credentials.")
            _dump_page(driver, "04_login_stuck")
            return False

        log.info("Login successful — URL: %s", driver.current_url)
        return True

    except Exception as e:
        log.error("Unexpected login error: %s", e)
        _dump_page(driver, "99_login_error")
        return False


# ── Preset selection ──────────────────────────────────────────────────────────

def apply_preset(driver: webdriver.Chrome) -> None:
    """
    Click the matching preset/filter button on the screener page.
    Uses PRESET_KEYWORDS to find the button by partial text match.
    Logs all available buttons if no match found.
    """
    if not PRESET:
        log.info("No preset configured — using default screener view.")
        return

    keywords = PRESET_KEYWORDS.get(PRESET)
    if not keywords:
        log.warning(
            "Unknown TANUKI_PRESET='%s'. Valid options: %s",
            PRESET, list(PRESET_KEYWORDS.keys()),
        )
        return

    log.info("Applying preset: '%s' (keywords: %s)", PRESET, keywords)
    time.sleep(2)

    all_btns = driver.find_elements(
        By.CSS_SELECTOR,
        "button, [role='tab'], [role='button'], "
        "[class*='preset'], [class*='filter'], [class*='chip'], [class*='badge']",
    )

    matched = None
    available = []
    for btn in all_btns:
        if not btn.is_displayed():
            continue
        text = btn.text.strip().lower()
        if not text:
            continue
        available.append(text)
        if any(k in text for k in keywords):
            matched = btn
            break

    if matched:
        log.info("Clicking preset button: '%s'", matched.text.strip())
        try:
            driver.execute_script("arguments[0].scrollIntoView(true);", matched)
            time.sleep(0.3)
            matched.click()
            time.sleep(3)
            log.info("Preset applied successfully.")
        except Exception as e:
            log.warning("Could not click preset button: %s", e)
    else:
        log.warning(
            "Preset '%s' not found on page. Available visible buttons: %s",
            PRESET, available[:20],
        )
        if DEBUG:
            _dump_page(driver, "06_preset_not_found")


# ── Screener ──────────────────────────────────────────────────────────────────

def load_screener(driver: webdriver.Chrome) -> bool:
    log.info("Loading options screener ...")
    driver.get(SCREENER_URL)
    wait = WebDriverWait(driver, PAGE_LOAD_TIMEOUT)
    time.sleep(3)

    if "screener" not in driver.current_url.lower():
        log.warning(
            "Did not land on screener (at: %s) — retrying after 10s ...",
            driver.current_url,
        )
        time.sleep(10)
        driver.get(SCREENER_URL)
        time.sleep(5)

    if DEBUG:
        _dump_page(driver, "05_screener_page")

    for sel in [
        "table tbody tr",
        "div[class*='screener'] tr",
        "div[class*='table'] tr",
        "tr[class*='row']",
        "[class*='ScreenerRow']",
        "[class*='screener-row']",
    ]:
        try:
            wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, sel)))
            log.info("Table detected via selector: %s", sel)
            break
        except TimeoutException:
            continue
    else:
        log.warning("No table selector matched — proceeding after 5s.")
        time.sleep(5)
        return False

    time.sleep(3)

    # Apply preset via UI click after table is visible
    apply_preset(driver)

    time.sleep(5)  # let JS re-render rows after preset change
    return True


def parse_screener_table(driver: webdriver.Chrome) -> pd.DataFrame:
    rows_data = []
    try:
        header_cells = driver.find_elements(By.CSS_SELECTOR, "thead th, thead td")
        headers = [h.text.strip() for h in header_cells]
        log.info("Headers (%d): %s", len(headers), headers)

        if not headers:
            first_row = driver.find_elements(
                By.CSS_SELECTOR, "tr:first-child td, tr:first-child th"
            )
            headers = [c.text.strip() for c in first_row]

        body_rows = driver.find_elements(By.CSS_SELECTOR, "tbody tr")
        log.info("Data rows found: %d", len(body_rows))

        for row in body_rows:
            cells = row.find_elements(By.CSS_SELECTOR, "td")
            if not cells:
                continue
            row_data = [c.text.strip() for c in cells]
            padded = (row_data + [""] * len(headers))[: len(headers)]
            rows_data.append(dict(zip(headers, padded)))

    except Exception as e:
        log.error("Table parse error: %s", e)

    if not rows_data:
        log.warning("No rows parsed. Set TANUKI_DEBUG=true to save page source.")

    return pd.DataFrame(rows_data)


# ── Client-side filtering ─────────────────────────────────────────────────────

def apply_client_filters(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply numeric and directional filters defined in .env after scraping.
    All filters are additive (AND logic).
    """
    original_len = len(df)
    applied = []

    # IVR range filter
    ivr_col = next(
        (c for c in df.columns if "ivr" in c.lower() or "iv rank" in c.lower()), None
    )
    if ivr_col:
        if FILTER_IVR_MIN:
            try:
                mn = float(FILTER_IVR_MIN)
                df = df[df[ivr_col].apply(_parse_float_col) >= mn]
                applied.append(f"IVR >= {mn}")
            except ValueError:
                pass
        if FILTER_IVR_MAX:
            try:
                mx = float(FILTER_IVR_MAX)
                df = df[df[ivr_col].apply(_parse_float_col) <= mx]
                applied.append(f"IVR <= {mx}")
            except ValueError:
                pass
    elif FILTER_IVR_MIN or FILTER_IVR_MAX:
        log.warning("IVR filter set but no IVR column found in table. Skipping.")

    # IVx 5d change filter
    ivx5_col = next(
        (c for c in df.columns if "5d" in c.lower() and "iv" in c.lower()), None
    )
    if ivx5_col:
        if FILTER_IVX_5D_MIN:
            try:
                mn = float(FILTER_IVX_5D_MIN)
                df = df[df[ivx5_col].apply(_parse_float_col) >= mn]
                applied.append(f"IVx 5d Change >= {mn}%")
            except ValueError:
                pass
    elif FILTER_IVX_5D_MIN:
        log.warning("IVx 5d filter set but no matching column found. Skipping.")

    # Net GEX directional filter
    net_gex_col = next(
        (c for c in df.columns if "net gex" in c.lower() or c.lower() == "net_gex"), None
    )
    if net_gex_col and FILTER_NET_GEX in ("positive", "negative"):
        df = df.copy()
        df["_net_gex_float"] = df[net_gex_col].apply(_parse_gex_value)
        if FILTER_NET_GEX == "positive":
            df = df[df["_net_gex_float"] >= 0]
            applied.append("Net GEX >= 0 (positive)")
        else:
            df = df[df["_net_gex_float"] < 0]
            applied.append("Net GEX < 0 (negative)")
        df = df.drop(columns=["_net_gex_float"])
    elif FILTER_NET_GEX and FILTER_NET_GEX not in ("positive", "negative"):
        log.warning(
            "TANUKI_FILTER_NET_GEX='%s' is invalid. Use 'positive' or 'negative'.",
            FILTER_NET_GEX,
        )

    if applied:
        log.info(
            "Client-side filters applied: %s → %d/%d rows remain.",
            " | ".join(applied), len(df), original_len,
        )
    else:
        log.info("No client-side filters applied.")

    return df.reset_index(drop=True)


# ── GEX ranking ───────────────────────────────────────────────────────────────

def find_gex_column(df: pd.DataFrame) -> str | None:
    for col in df.columns:
        if any(c in col.lower() for c in ["first gex", "1st gex", "gex1", "net gex", "gex"]):
            return col
    return None


def find_ticker_column(df: pd.DataFrame) -> str | None:
    for col in df.columns:
        if col.lower() in ("ticker", "symbol", "name"):
            return col
    return df.columns[0] if len(df.columns) > 0 else None


def rank_by_gex(df: pd.DataFrame, gex_col: str, ticker_col: str) -> pd.DataFrame:
    df = df.copy()
    df["_gex_float"] = df[gex_col].apply(_parse_gex_value)
    positive = df[df["_gex_float"] >= MIN_GEX].sort_values("_gex_float", ascending=False)
    if TOP_N > 0:
        positive = positive.head(TOP_N).copy()
    positive["First GEX (B)"] = positive["_gex_float"].round(3)
    return positive.drop(columns=["_gex_float"])


def save_results(df: pd.DataFrame) -> str:
    ts = datetime.now(TZ).strftime("%Y%m%d_%H%M%S")
    preset_tag = f"_{PRESET}" if PRESET else ""
    filename = f"gex_screener{preset_tag}_{ts}.csv"
    df.to_csv(filename, index=False)
    log.info("Saved: %s", filename)
    return filename


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    log.info("=" * 60)
    log.info("TanukiTrade GEX Screener — Top Positive First GEX Tickers")
    log.info("=" * 60)
    log.info(
        "HEADLESS=%s | DEBUG=%s | TOP_N=%d | MIN_GEX=%.2f | PRESET=%s",
        HEADLESS, DEBUG, TOP_N, MIN_GEX, PRESET or "default",
    )
    if any([FILTER_IVR_MIN, FILTER_IVR_MAX, FILTER_IVX_5D_MIN, FILTER_NET_GEX]):
        log.info(
            "Filters: IVR=[%s, %s] | IVx5d>=%s | NetGEX=%s",
            FILTER_IVR_MIN or "—", FILTER_IVR_MAX or "—",
            FILTER_IVX_5D_MIN or "—", FILTER_NET_GEX or "all",
        )

    driver = build_driver()
    try:
        if not login(driver):
            log.error("Login failed — exiting.")
            if not HEADLESS:
                input("Press Enter to exit (browser stays open) ...")
            return

        loaded = load_screener(driver)
        if not loaded:
            log.error("Screener failed to load.")
            if not HEADLESS:
                input("Browser open — inspect and press Enter to exit ...")
            return

        df = parse_screener_table(driver)

        if df.empty:
            log.error("No data scraped from screener.")
            if not HEADLESS:
                input("Browser open — press Enter to exit after inspecting ...")
            return

        log.info("Raw table: %d rows x %d cols", *df.shape)

        # Apply client-side filters (IVR range, IVx 5d, Net GEX direction)
        df = apply_client_filters(df)

        gex_col    = find_gex_column(df)
        ticker_col = find_ticker_column(df)

        if not gex_col:
            log.error("No GEX column found. Columns: %s", list(df.columns))
            print(df.head(10).to_string())
            return

        log.info("GEX column='%s' | Ticker column='%s'", gex_col, ticker_col)
        ranked = rank_by_gex(df, gex_col, ticker_col)

        if ranked.empty:
            log.warning("No tickers remaining after filters (MIN_GEX=%.2f).", MIN_GEX)
            return

        # Build display columns: ticker + GEX + any IV/skew/delta extras
        display_cols = [ticker_col, gex_col, "First GEX (B)"]
        display_cols = [c for c in display_cols if c in ranked.columns]
        for extra in ["IVx", "IVRank", "IV Rank", "IVR", "Skew", "Exp Move", "5d", "Net GEX"]:
            for col in ranked.columns:
                if extra.lower() in col.lower() and col not in display_cols:
                    display_cols.append(col)
                    break

        preset_label = f" [{PRESET.upper().replace('_', ' ')}]" if PRESET else ""
        print()
        print(
            f"  TOP {len(ranked)} TICKERS{preset_label} — HIGHEST POSITIVE FIRST GEX"
            f"  (min >= {MIN_GEX}B)"
        )
        print("  " + "=" * 60)
        print(tabulate(
            ranked[display_cols].reset_index(drop=True),
            headers="keys",
            tablefmt="rounded_outline",
            showindex=True,
        ))
        print()
        save_results(ranked)

    finally:
        if HEADLESS:
            driver.quit()
        else:
            log.info("Browser kept open — close it manually when done.")


if __name__ == "__main__":
    main()
