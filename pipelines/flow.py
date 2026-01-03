

"""flow.py

Orchestration entrypoint for Day-1 ingestion.

Runs:
  1) Extract recalls (DOT Socrata)
  2) Extract complaints (NHTSA) [optional if model/year not provided]
  3) Load raw tables in Postgres with idempotent upserts

This stays intentionally lightweight (no Prefect yet). The goal is:
- one command you can run locally and from CI
- clear logs and failure behavior
"""

from __future__ import annotations

import os
import sys
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional

# Allow running as a script: `python pipelines/flow.py`
# by adding the repo root to sys.path so `import pipelines.*` works.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pipelines.extract import extract_recalls, extract_complaints
from pipelines.load import load_recalls, load_complaints

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")


def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name)
    return v if v is not None and v.strip() != "" else default


def _require(name: str) -> str:
    v = _env(name)
    if v is None:
        raise ValueError(f"Missing required env var: {name}")
    return v


def run_ingest() -> Dict[str, Any]:
    """Run extract+load for raw layer. Returns a small run report."""
    make = _env("RECALLWATCH_MAKE", "TESLA")
    start = _env("RECALLWATCH_START", "2024-01-01")
    end = _env("RECALLWATCH_END", "2024-12-31")

    report: Dict[str, Any] = {
        "started_at": datetime.utcnow().isoformat() + "Z",
        "make": make,
        "start_date": start,
        "end_date": end,
        "recalls_extracted": 0,
        "recalls_loaded": 0,
        "complaints_extracted": 0,
        "complaints_loaded": 0,
        "complaints_skipped": False,
        "errors": [],
    }

    # -------------------------
    # Recalls
    # -------------------------
    logger.info("Step 1/2: Extract+Load recalls | make=%s window=%s..%s", make, start, end)
    recalls = extract_recalls(make=make, start_date=start, end_date=end)
    report["recalls_extracted"] = len(recalls)
    report["recalls_loaded"] = load_recalls(recalls)

    # -------------------------
    # Complaints (optional)
    # -------------------------
    model = _env("RECALLWATCH_MODEL")
    model_year = _env("RECALLWATCH_MODEL_YEAR")

    if not model or not model_year:
        report["complaints_skipped"] = True
        logger.info(
            "Step 2/2: Complaints skipped (set RECALLWATCH_MODEL and RECALLWATCH_MODEL_YEAR to enable)."
        )
        report["finished_at"] = datetime.utcnow().isoformat() + "Z"
        return report

    logger.info(
        "Step 2/2: Extract+Load complaints | make=%s model=%s year=%s", make, model, model_year
    )
    complaints = extract_complaints(make=make, start_date=start, end_date=end)
    report["complaints_extracted"] = len(complaints)
    report["complaints_loaded"] = load_complaints(complaints)

    report["finished_at"] = datetime.utcnow().isoformat() + "Z"
    return report


def _print_report(report: Dict[str, Any]) -> None:
    print("\n=== RecallWatch Ingest Report ===")
    print(f"make: {report.get('make')}")
    print(f"window: {report.get('start_date')} .. {report.get('end_date')}")
    print(f"started_at: {report.get('started_at')}")
    print(f"finished_at: {report.get('finished_at')}")
    print("-")
    print(f"recalls: extracted={report.get('recalls_extracted')} loaded={report.get('recalls_loaded')}")
    if report.get("complaints_skipped"):
        print("complaints: skipped (set RECALLWATCH_MODEL and RECALLWATCH_MODEL_YEAR)")
    else:
        print(
            f"complaints: extracted={report.get('complaints_extracted')} loaded={report.get('complaints_loaded')}"
        )


if __name__ == "__main__":
    try:
        rep = run_ingest()
        # Ensure finished_at exists even if we returned early
        rep.setdefault("finished_at", datetime.utcnow().isoformat() + "Z")
        _print_report(rep)
    except Exception as e:
        logger.exception("Ingest failed")
        print(f"\nERROR: {e}")
        sys.exit(1)