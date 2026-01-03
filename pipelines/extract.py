"""
extract.py

Responsible for extracting raw vehicle quality data from public APIs
(Socrata / NHTSA ODI datasets).

This module:
- fetches raw data
- handles pagination and retries
- returns raw records (no transformation)

It does NOT:
- clean data
- deduplicate
- write to the database
"""

from __future__ import annotations

import time
import logging
from typing import List, Dict, Optional

import requests

# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------

SOCRATA_BASE_URL = "https://data.transportation.gov/resource"
RECALLS_DATASET_ID = "86zz-ue7u"
COMPLAINTS_DATASET_ID = "htum-kus7"

PAGE_SIZE = 1000
REQUEST_TIMEOUT = 30
RATE_LIMIT_SLEEP = 0.2  # seconds between requests

# ---------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

# ---------------------------------------------------------------------
# Core helpers
# ---------------------------------------------------------------------

def _fetch_socrata(
    dataset_id: str,
    where_clause: Optional[str] = None,
) -> List[Dict]:
    """
    Generic Socrata fetcher with pagination.

    Parameters
    ----------
    dataset_id : str
        Socrata dataset identifier.
    where_clause : str, optional
        SoQL WHERE clause (without 'WHERE').

    Returns
    -------
    List[Dict]
        Raw records returned by the API.
    """
    records: List[Dict] = []
    offset = 0

    while True:
        params = {
            "$limit": PAGE_SIZE,
            "$offset": offset,
        }

        if where_clause:
            params["$where"] = where_clause

        url = f"{SOCRATA_BASE_URL}/{dataset_id}.json"

        logger.info("Fetching %s | offset=%s", dataset_id, offset)

        response = requests.get(
            url,
            params=params,
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()

        page = response.json()

        if not page:
            logger.info("No more records. Stopping pagination.")
            break

        records.extend(page)
        offset += PAGE_SIZE

        time.sleep(RATE_LIMIT_SLEEP)

    logger.info("Fetched %s total records from %s", len(records), dataset_id)
    return records

# ---------------------------------------------------------------------
# Public extractors
# ---------------------------------------------------------------------

def extract_recalls(
    make: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> List[Dict]:
    """
    Extract recall records for a given make and date range.

    Parameters
    ----------
    make : str
        Vehicle make (e.g. 'TESLA').
    start_date : str, optional
        ISO date string (YYYY-MM-DD).
    end_date : str, optional
        ISO date string (YYYY-MM-DD).

    Returns
    -------
    List[Dict]
        Raw recall records.
    """
    filters = [f"upper(make) = '{make.upper()}'"]

    if start_date:
        filters.append(f"report_received_date >= '{start_date}'")
    if end_date:
        filters.append(f"report_received_date <= '{end_date}'")

    where_clause = " AND ".join(filters)

    return _fetch_socrata(
        dataset_id=RECALLS_DATASET_ID,
        where_clause=where_clause,
    )


def extract_complaints(
    make: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> List[Dict]:
    """
    Extract complaint records for a given make and date range.

    Parameters
    ----------
    make : str
        Vehicle make (e.g. 'TESLA').
    start_date : str, optional
        ISO date string (YYYY-MM-DD).
    end_date : str, optional
        ISO date string (YYYY-MM-DD).

    Returns
    -------
    List[Dict]
        Raw complaint records.
    """
    filters = [f"upper(make) = '{make.upper()}'"]

    if start_date:
        filters.append(f"date_received >= '{start_date}'")
    if end_date:
        filters.append(f"date_received <= '{end_date}'")

    where_clause = " AND ".join(filters)

    return _fetch_socrata(
        dataset_id=COMPLAINTS_DATASET_ID,
        where_clause=where_clause,
    )