# app/budget.py
from __future__ import annotations

import os
import re
from datetime import datetime, timezone
from typing import Tuple

import pandas as pd
from playwright.sync_api import sync_playwright


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _clean_value(x: str) -> str:
    s = (x or "").replace("\xa0", " ").strip()
    s = s.replace("MRU", "").replace("%", "").strip()
    # supprimer espaces milliers : "1 779 041,93" -> "1779041,93"
    s = re.sub(r"\s+", "", s)
    # virgule -> point
    s = s.replace(",", ".")
    return s


def scrape_budget() -> Tuple[pd.DataFrame, dict]:
    url = os.environ.get("BUDGET_URL", "https://services.tresor.mr/budget")
    timeout_ms = int(os.environ.get("HTTP_TIMEOUT_SECONDS", "30")) * 1000

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)

        # ✅ attendre le remplissage réel PrimeNG
        page.wait_for_selector(
            "tbody.p-datatable-tbody tr.ng-star-inserted",
            timeout=timeout_ms
        )

        # En-têtes
        headers = page.eval_on_selector_all(
            "thead tr th",
            "ths => ths.map(th => th.innerText.trim()).filter(t => t.length > 0)"
        )

        # Lignes
        rows = page.eval_on_selector_all(
            "tbody.p-datatable-tbody tr.ng-star-inserted",
            """trs => trs.map(tr =>
                Array.from(tr.querySelectorAll('td'))
                     .map(td => td.innerText.trim())
            )"""
        )

        browser.close()

    if not rows:
        raise RuntimeError("Budget table rendered but no rows extracted")

    # fallback si headers vides
    if not headers:
        headers = [f"col_{i+1}" for i in range(len(rows[0]))]

    df = pd.DataFrame(rows, columns=headers[:len(rows[0])])

    # Nettoyage & normalisation Q1.4
    for col in df.columns:
        s = df[col].astype(str).map(_clean_value)
        df[col] = pd.to_numeric(s, errors="ignore")

    df["source_url"] = url
    df["scraped_at"] = _utc_now_iso()

    kpi = {
        "rows": int(len(df)),
        "missing_values": int(df.isna().sum().sum()),
    }
    return df, kpi
