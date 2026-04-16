from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from etl.utils.file_hash import calculate_file_hash
from etl.utils.ids import generate_load_id, generate_session_load_id


def add_load_metadata(
    df: pd.DataFrame,
    file_path: str | Path,
    read_debug: dict[str, Any],
    session_load_id: str | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """
    Add technical metadata columns required for warehouse loading.

    Columns added:
    - load_id
    - session_load_id
    - file_name
    - file_hash
    - loaded_at
    - source_sheet
    - header_row_detected

    Notes
    -----
    - source_row_number is already added earlier in read_excel.py
    - loaded_at is stored as UTC-naive timestamp for easier downstream handling
    """
    result_df = df.copy()
    file_path = Path(file_path)

    resolved_session_load_id = session_load_id or generate_session_load_id()
    load_id = generate_load_id()
    file_hash = calculate_file_hash(file_path)
    loaded_at = pd.Timestamp.utcnow().tz_localize(None)

    header_detection = read_debug.get("header_detection", {})
    selected_row_index = header_detection.get("selected_row_index")

    result_df["load_id"] = load_id
    result_df["session_load_id"] = resolved_session_load_id
    result_df["file_name"] = file_path.name
    result_df["file_hash"] = file_hash
    result_df["loaded_at"] = loaded_at
    result_df["source_sheet"] = str(read_debug.get("sheet_name"))
    result_df["header_row_detected"] = (
        int(selected_row_index) if selected_row_index is not None else pd.NA
    )

    if "source_row_number" in result_df.columns:
        result_df["source_row_number"] = pd.to_numeric(
            result_df["source_row_number"],
            errors="coerce",
        ).astype("Int64")

    result_df["header_row_detected"] = pd.to_numeric(
        result_df["header_row_detected"],
        errors="coerce",
    ).astype("Int64")

    debug_info = {
        "load_id": load_id,
        "session_load_id": resolved_session_load_id,
        "file_name": file_path.name,
        "file_hash": file_hash,
        "loaded_at": str(loaded_at),
        "source_sheet": str(read_debug.get("sheet_name")),
        "header_row_detected": selected_row_index,
    }

    return result_df, debug_info