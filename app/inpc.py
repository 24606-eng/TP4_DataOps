# app/inpc.py
from __future__ import annotations

import os
import re
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Tuple

import camelot
import pandas as pd
import requests


TABLE2_PATTERNS = [
    re.compile(r"\btableau\s*2\b", re.IGNORECASE),
    re.compile(r"\btab\.\s*2\b", re.IGNORECASE),
]


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _download_pdf(pdf_url: str, out_dir: Path) -> Path:
    timeout = int(os.environ.get("HTTP_TIMEOUT_SECONDS", "20"))
    headers = {"User-Agent": os.environ.get("USER_AGENT", "TP4-DataOps-SID31")}

    r = requests.get(pdf_url, timeout=timeout, headers=headers)
    r.raise_for_status()

    pdf_path = out_dir / "inpc.pdf"
    pdf_path.write_bytes(r.content)
    return pdf_path


def _normalize_colname(s: str) -> str:
    s = (s or "").strip().lower()
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"[^a-z0-9]+", "_", s).strip("_")
    return s or "col"


def _normalize_number_cell(x: str) -> str:
    """
    Normalise:
    - espaces milliers: "1 234,56" -> "1234.56"
    - virgule décimale -> point
    - NBSP
    - % supprimé
    """
    s = str(x) if x is not None else ""
    s = s.replace("\n", " ").replace("\xa0", " ").strip()
    s = s.replace("%", "")
    s = s.replace(" ", "")          # supprime séparateur de milliers
    s = s.replace(",", ".")         # décimal
    return s


def _looks_like_header_row(row: pd.Series) -> bool:
    """
    Heuristique: une ligne "header" contient surtout du texte non numérique.
    """
    vals = [str(v).strip() for v in row.tolist()]
    if all(v == "" for v in vals):
        return False
    # si au moins 2 cellules non vides et peu de chiffres -> header probable
    non_empty = [v for v in vals if v != ""]
    if len(non_empty) < 2:
        return False
    digit_ratio = sum(any(ch.isdigit() for ch in v) for v in non_empty) / len(non_empty)
    return digit_ratio < 0.5


def _drop_repeated_headers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Supprime les entêtes répétés dans le corps du tableau:
    - lignes identiques aux noms de colonnes
    - lignes qui contiennent les mêmes libellés que l'entête
    """
    if df.empty:
        return df

    header_vals = [str(c).strip().lower() for c in df.columns.tolist()]

    def is_repeat(row: pd.Series) -> bool:
        vals = [str(v).strip().lower() for v in row.tolist()]
        # identique exactement
        if vals == header_vals:
            return True
        # ou très proche (beaucoup de cellules égales)
        same = sum(1 for a, b in zip(vals, header_vals) if a == b and a != "")
        return same >= max(2, len(header_vals) // 2)

    mask = df.apply(is_repeat, axis=1)
    return df.loc[~mask].reset_index(drop=True)


def _coerce_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convertit en numérique quand pertinent, sinon laisse en string.
    """
    for col in df.columns:
        # tenter conversion si la colonne ressemble à numérique
        s = df[col].astype(str).str.strip()
        # proportion de valeurs qui ressemblent à un nombre
        numeric_like = s.str.match(r"^-?\d+(\.\d+)?$").mean()
        if numeric_like >= 0.6:  # seuil
            df[col] = pd.to_numeric(s, errors="coerce")
    return df


def extract_inpc_table2(out_dir: str) -> Tuple[pd.DataFrame, dict]:
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    pdf_url = os.environ.get(
        "INPC_PDF_URL",
        "https://ansade.mr/wp-content/uploads/2026/01/Note-INPC-decembre-2025_FR_VF.pdf",
    )

    pdf_path = _download_pdf(pdf_url, out_path)

    # Camelot: parfois stream marche mieux sur tableaux "lignes"
    tables = camelot.read_pdf(str(pdf_path), pages="all", flavor="stream")

    if len(tables) == 0:
        raise RuntimeError("No tables found by Camelot in INPC PDF")

    # 1) choisir UNIQUEMENT le Tableau 2
    chosen = None
    for t in tables:
        raw = t.df.copy()
        joined = " ".join(raw.astype(str).fillna("").values.flatten().tolist())
        if any(p.search(joined) for p in TABLE2_PATTERNS):
            chosen = raw
            break

    # fallback si Camelot ne capture pas le titre "Tableau 2"
    if chosen is None:
        # en TP, Tableau 2 est souvent le 2e tableau détecté
        chosen = tables[1].df.copy() if len(tables) >= 2 else tables[0].df.copy()

    df = chosen.copy()
    df = df.replace("\n", " ", regex=True)

    # 2) supprimer lignes vides
    df = df.applymap(lambda x: str(x).strip())
    df = df.loc[~(df == "").all(axis=1)].reset_index(drop=True)

    # 3) fixer l'entête: si 1ère ligne ressemble à header, la prendre
    if df.shape[0] >= 2 and _looks_like_header_row(df.iloc[0]):
        df.columns = df.iloc[0].tolist()
        df = df.drop(index=0).reset_index(drop=True)

    # 4) normaliser noms de colonnes (Q3.4)
    df.columns = [_normalize_colname(str(c)) for c in df.columns]

    # 5) normaliser nombres (Q3.4)
    for col in df.columns:
        df[col] = df[col].map(_normalize_number_cell)

    # 6) supprimer entêtes répétés (Q3.4)
    df = _drop_repeated_headers(df)

    # 7) types numériques quand pertinent (Q3.4)
    df = _coerce_numeric_columns(df)

    # 8) traçabilité
    df["source_url"] = pdf_url
    df["scraped_at"] = _utc_now_iso()

    kpi = {"rows": int(len(df)), "missing_values": int(df.isna().sum().sum())}
    return df, kpi

def clean_inpc_table2(
    raw_csv_path: str | Path = "shared_out/inpc_table2.csv",
    clean_csv_path: str | Path = "shared_out/inpc_table2_clean.csv",
) -> str:
    raw_csv_path = Path(raw_csv_path)
    clean_csv_path = Path(clean_csv_path)

    df = pd.read_csv(raw_csv_path)

    # 1) enlever lignes vides / titres
    df = df.dropna(how="all")
    df = df[~df["0"].astype(str).str.contains(r"Tableau2|Fonctions", na=False, regex=True)]

    # 2) renommer colonnes
    cols = {
        "0": "code",
        "1": "fonction",
        "2": "poids",
        "3": "dec_24",
        "4": "sept_25",
        "5": "oct_25",
        "7": "dec_25",
        "8": "var_1m",
        "9": "var_3m",
        "10": "var_1an",
        "11": "var_12m",
    }
    df = df.rename(columns=cols)

    # 3) garder seulement les lignes principales (code = 2 chiffres)
    df["code"] = df["code"].astype(str).str.strip()
    df = df[df["code"].str.match(r"^\d{2}$", na=False)]

    # 4) nettoyer texte
    df["fonction"] = df["fonction"].astype(str).str.replace(r"\s+", " ", regex=True).str.strip()

    # 5) gérer valeurs collées (ex: 122.6124.4125.0)
    def split_joined_numbers(x):
        if pd.isna(x):
            return x
        s = str(x).strip()
        # déjà clean
        if re.fullmatch(r"[-+]?[\d.,]+", s) and s.count(".") <= 1:
            return s
        nums = re.findall(r"\d+(?:\.\d+)?", s)
        return nums

    def last_num(v):
        if isinstance(v, list) and len(v) > 0:
            return v[-1]
        return v

    for c in ["dec_24", "sept_25", "oct_25", "dec_25"]:
        if c in df.columns:
            df[c] = df[c].apply(split_joined_numbers).apply(last_num)

    # 6) conversion numérique
    for c in ["poids", "dec_24", "sept_25", "oct_25", "dec_25"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # 7) colonnes finales
    keep = [
        "code", "fonction", "poids",
        "dec_24", "sept_25", "oct_25", "dec_25",
        "var_1m", "var_3m", "var_1an", "var_12m",
        "source_url", "scraped_at",
    ]
    keep = [c for c in keep if c in df.columns]
    df = df[keep].reset_index(drop=True)

    clean_csv_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(clean_csv_path, index=False, encoding="utf-8")

    return str(clean_csv_path)