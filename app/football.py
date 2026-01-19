# app/football.py
from __future__ import annotations

import os
import re
from datetime import datetime, timezone
from typing import Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup


DATE_RE = re.compile(r"^\s*(\d{2}/\d{2}/\d{4})\s*$")
SCORE_RE = re.compile(r"(\d+)\s*-\s*(\d+)")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _norm_date_ddmmyyyy_to_iso(d: str) -> str | None:
    d = (d or "").strip()
    if not d:
        return None
    dt = pd.to_datetime(d, dayfirst=True, errors="coerce")
    if pd.isna(dt):
        return None
    return dt.strftime("%Y-%m-%d")


def _parse_score_from_match_card(card) -> tuple[int | None, int | None, str]:
    score_box = card.select_one('[data-testid="live-score-element"]')
    if not score_box:
        return None, None, "SCHEDULED"

    # Prendre uniquement les spans numériques
    nums = []
    for sp in score_box.select("span"):
        t = sp.get_text(strip=True)
        if t.isdigit():
            nums.append(int(t))

    if len(nums) >= 2:
        return nums[0], nums[1], "PLAYED"

    # fallback texte
    txt = score_box.get_text(" ", strip=True)
    m = SCORE_RE.search(txt)
    if m:
        return int(m.group(1)), int(m.group(2)), "PLAYED"

    return None, None, "SCHEDULED"



def _extract_teams(card) -> tuple[str | None, str | None]:
    """
    Ton DOM montre:
    - home: [data-testid="team-name-badge"] .text-right
    - away: [data-testid="team-name-badge"] .text-left
    """
    home_el = card.select_one('[data-testid="team-name-badge"] .text-right')
    away_el = card.select_one('[data-testid="team-name-badge"] .text-left')

    home = home_el.get_text(strip=True) if home_el else None
    away = away_el.get_text(strip=True) if away_el else None
    return home, away


def scrape_football() -> Tuple[pd.DataFrame, dict]:
    url = os.environ["FOOTBALL_URL"]
    timeout = int(os.environ.get("HTTP_TIMEOUT_SECONDS", "20"))
    headers = {
        "User-Agent": os.environ.get("USER_AGENT", "TP4-DataOps-SID31"),
        "Accept": "text/html,*/*",
        "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
    }

    r = requests.get(url, timeout=timeout, headers=headers)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")

    # On parcourt la page dans l'ordre du DOM pour propager la "date courante"
    current_date_iso: str | None = None
    out_rows: list[dict] = []

    # On prend un conteneur large; puis on balaie ses descendants textuels
    # Astuce: on parcourt les tags et on détecte:
    # - une date "dd/mm/yyyy" => mise à jour current_date_iso
    # - un match-card => extraction match avec date courante
    root = soup.body or soup

    for el in root.descendants:
        if not getattr(el, "name", None):
            continue

        # 1) détecter une date affichée comme texte seul
        # (souvent un titre/label entre groupes de matchs)
        if el.name in ("div", "span", "h3", "h4", "p"):
            t = el.get_text(strip=True)
            if t and DATE_RE.match(t):
                current_date_iso = _norm_date_ddmmyyyy_to_iso(t)
                continue

        # 2) match card
        if el.has_attr("data-testid") and el["data-testid"] == "match-card":
            home, away = _extract_teams(el)
            if not home or not away:
                # si structure varie, on skip (robuste)
                continue

            home_score, away_score, status = _parse_score_from_match_card(el)

            out_rows.append(
                {
                    "match_date": current_date_iso,
                    "home_team": home,
                    "away_team": away,
                    "home_score": home_score,
                    "away_score": away_score,
                    "status": status,
                    "source_url": url,
                    "scraped_at": _utc_now_iso(),
                }
            )

    df = pd.DataFrame(out_rows)

    # Nettoyage Q2.3
    if not df.empty:
        df["match_date"] = df["match_date"].astype("string")

        # scores nullable
        df["home_score"] = pd.to_numeric(df["home_score"], errors="coerce").astype("Int64")
        df["away_score"] = pd.to_numeric(df["away_score"], errors="coerce").astype("Int64")

        # Dédupliquer
        df = df.drop_duplicates(subset=["match_date", "home_team", "away_team"], keep="first")

    kpi = {
        "rows": int(len(df)),
        "missing_values": int(df.isna().sum().sum()) if not df.empty else 0,
    }
    return df, kpi
