from __future__ import annotations

import re
import unicodedata
from pathlib import Path
from typing import Any

import pandas as pd
import yaml


DEFAULT_SCAN_ROWS = 15
MIN_HEADER_SCORE = 3.0


def normalize_text(value: Any) -> str:
    """
    Normalize a cell value so headers can be compared robustly.

    Rules:
    - convert to string
    - strip whitespace
    - lowercase
    - remove accents
    - replace line breaks with spaces
    - remove extra spaces
    - keep letters/numbers/spaces/#/()
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""

    text = str(value).strip().lower()

    if not text:
        return ""

    text = text.replace("\n", " ").replace("\r", " ")
    text = unicodedata.normalize("NFKD", text)
    text = "".join(char for char in text if not unicodedata.combining(char))
    text = re.sub(r"[^a-z0-9#()/%\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()

    return text


def load_column_aliases(mapping_path: str | Path) -> dict[str, list[str]]:
    """
    Load aliases from etl/config/column_mapping.yml

    Expected structure:
    column_aliases:
      sale_id:
        - "# de venta"
        - "nro de venta"
    """
    mapping_path = Path(mapping_path)

    with mapping_path.open("r", encoding="utf-8") as file:
        config = yaml.safe_load(file) or {}

    aliases = config.get("column_aliases", {})

    normalized_aliases: dict[str, list[str]] = {}
    for standard_col, alias_list in aliases.items():
        normalized_aliases[standard_col] = [normalize_text(alias) for alias in alias_list]

    return normalized_aliases


def build_alias_lookup(aliases_by_standard_col: dict[str, list[str]]) -> set[str]:
    """
    Flatten all aliases into a set for quick membership checks.
    """
    alias_lookup: set[str] = set()

    for alias_list in aliases_by_standard_col.values():
        alias_lookup.update(alias_list)

    return alias_lookup


def is_effectively_empty(value: Any) -> bool:
    """
    True if the value should be treated as empty.
    """
    if value is None:
        return True

    if isinstance(value, float) and pd.isna(value):
        return True

    return normalize_text(value) == ""


def row_to_normalized_values(row: pd.Series) -> list[str]:
    """
    Convert a raw row into normalized non-empty text values.
    """
    values: list[str] = []

    for value in row.tolist():
        normalized = normalize_text(value)
        if normalized:
            values.append(normalized)

    return values


def score_header_row(
    row_values: list[str],
    next_row_values: list[str],
    alias_lookup: set[str],
) -> float:
    """
    Score how likely a row is to be the header.

    Main signal:
    - exact match against known aliases

    Secondary signals:
    - enough non-empty text cells
    - mostly text-like values
    - next row looks more like data than header
    """
    if not row_values:
        return 0.0

    score = 0.0

    non_empty_count = len(row_values)
    alias_matches = sum(1 for value in row_values if value in alias_lookup)

    mostly_numeric_count = sum(
        1 for value in row_values if re.fullmatch(r"[\d.,/-]+", value)
    )

    # Strongest signal: known aliases
    score += alias_matches * 3.0

    # Helpful signal: several populated cells
    if non_empty_count >= 3:
        score += 1.0
    if non_empty_count >= 5:
        score += 1.0

    # Penalize rows that look like data or noise
    score -= mostly_numeric_count * 1.5

    # If next row looks more like data, this row is more likely the header
    if next_row_values:
        next_numeric_like = sum(
            1 for value in next_row_values if re.fullmatch(r"[\d.,/-]+", value)
        )
        if next_numeric_like >= 1:
            score += 0.5

    return score


def detect_header(
    raw_df: pd.DataFrame,
    mapping_path: str | Path = "etl/config/column_mapping.yml",
    max_rows_to_scan: int = DEFAULT_SCAN_ROWS,
    min_header_score: float = MIN_HEADER_SCORE,
) -> tuple[int, dict[str, Any]]:
    """
    Detect the header row in a dataframe read WITHOUT header.

    Parameters
    ----------
    raw_df : pd.DataFrame
        DataFrame loaded with header=None
    mapping_path : str | Path
        Path to column_mapping.yml
    max_rows_to_scan : int
        Number of top rows to inspect
    min_header_score : float
        Minimum score required to accept a header row

    Returns
    -------
    tuple[int, dict[str, Any]]
        header_row_index,
        debug_info

    Raises
    ------
    ValueError
        If no row reaches the minimum score
    """
    if raw_df.empty:
        raise ValueError("The input file is empty. Header cannot be detected.")

    aliases_by_standard_col = load_column_aliases(mapping_path)
    alias_lookup = build_alias_lookup(aliases_by_standard_col)

    rows_to_scan = min(len(raw_df), max_rows_to_scan)

    candidates: list[dict[str, Any]] = []

    for row_idx in range(rows_to_scan):
        row = raw_df.iloc[row_idx]
        next_row = raw_df.iloc[row_idx + 1] if row_idx + 1 < len(raw_df) else pd.Series()

        row_values = row_to_normalized_values(row)
        next_row_values = row_to_normalized_values(next_row)

        row_score = score_header_row(
            row_values=row_values,
            next_row_values=next_row_values,
            alias_lookup=alias_lookup,
        )

        alias_matches = [value for value in row_values if value in alias_lookup]

        candidates.append(
            {
                "row_index": row_idx,
                "score": row_score,
                "row_values": row_values,
                "alias_matches": alias_matches,
            }
        )

    best_candidate = max(candidates, key=lambda item: item["score"])

    if best_candidate["score"] < min_header_score:
        raise ValueError(
            "Header row could not be detected confidently. "
            f"Best candidate: row {best_candidate['row_index']} "
            f"with score {best_candidate['score']:.2f}. "
            f"Row values: {best_candidate['row_values']}"
        )

    debug_info = {
        "selected_row_index": best_candidate["row_index"],
        "selected_score": best_candidate["score"],
        "selected_alias_matches": best_candidate["alias_matches"],
        "candidates": candidates,
    }

    return best_candidate["row_index"], debug_info