from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4


def utc_timestamp_compact() -> str:
    """
    Return a compact UTC timestamp suitable for IDs.

    Example:
    20260416T031522Z
    """
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def generate_session_load_id() -> str:
    """
    Generate an ID for the current ingestion session / app execution.
    """
    return f"session_{utc_timestamp_compact()}_{uuid4().hex[:8]}"


def generate_load_id() -> str:
    """
    Generate an ID for one processed file / load unit.
    """
    return f"load_{utc_timestamp_compact()}_{uuid4().hex[:8]}"