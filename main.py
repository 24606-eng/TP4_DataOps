from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

from app.budget import scrape_budget
from app.football import scrape_football
from app.inpc import extract_inpc_table2



def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def ensure_out_dir() -> Path:
    out_dir = Path(os.environ.get("OUTPUT_DIR", "/out"))
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir


def write_run_report(out_dir: Path, lines: list[str]) -> None:
    (out_dir / "run_report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    out_dir = ensure_out_dir()

    kpi = {
        "scraped_at": utc_now_iso(),
        "budget": {"status": "FAIL", "rows": 0, "missing_values": 0},
        "football": {"status": "FAIL", "rows": 0, "missing_values": 0},
        "inpc": {"status": "TODO", "rows": 0, "missing_values": 0},
    }

    report = [
        "# TP4 — Run report",
        f"- scraped_at: {kpi['scraped_at']}",
    ]

    # ========= FOOTBALL =========
    try:
        df_foot, kpi_foot = scrape_football()
        out_csv = out_dir / "football_results.csv"
        df_foot.to_csv(out_csv, index=False)

        kpi["football"].update(
            status="OK",
            rows=kpi_foot["rows"],
            missing_values=kpi_foot["missing_values"],
        )
        report.append(f"- Football: OK ({kpi_foot['rows']} rows)")
    except Exception as e:
        report.append(f"- Football: FAIL ({e})")

    # ========= BUDGET =========
    try:
        df_budget, kpi_budget = scrape_budget()
        out_csv = out_dir / "budget_execution.csv"
        df_budget.to_csv(out_csv, index=False)

        kpi["budget"].update(
            status="OK",
            rows=kpi_budget["rows"],
            missing_values=kpi_budget["missing_values"],
        )
        report.append(f"- Budget: OK ({kpi_budget['rows']} rows)")
    except Exception as e:
        report.append(f"- Budget: FAIL ({e})")


    #### inpc
        # ========= INPC =========
    try:
        df_inpc, kpi_inpc = extract_inpc_table2(str(out_dir))
        out_csv = out_dir / "inpc_table2.csv"
        df_inpc.to_csv(out_csv, index=False)
        

        kpi["inpc"].update(
            status="OK",
            rows=kpi_inpc["rows"],
            missing_values=kpi_inpc["missing_values"],
        )
        report.append(f"- INPC: OK ({kpi_inpc['rows']} rows)")
    except Exception as e:
        report.append(f"- INPC: FAIL ({e})")
    # ========================
        

    # ========= OUTPUTS =========
    (out_dir / "kpi.json").write_text(
        json.dumps(kpi, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    write_run_report(out_dir, report)

    print("[DONE] Pipeline finished (football + budget)")


if __name__ == "__main__":
    main()
