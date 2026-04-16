from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from etl.ingest.detect_header import normalize_text


TECHNICAL_COLUMNS = {
    "source_row_number",
}


def load_column_mapping(mapping_path: str | Path) -> dict[str, list[str]]:
    """
    Load mapping configuration from YAML.
    """
    mapping_path = Path(mapping_path)

    with mapping_path.open("r", encoding="utf-8") as file:
        config = yaml.safe_load(file) or {}

    return config.get("column_aliases", {})


def build_reverse_mapping(column_mapping: dict[str, list[str]]) -> dict[str, str]:
    """
    Convert:
        {sale_id: ["# de venta", "ventas # de venta"]}
    into:
        {"# de venta": "sale_id", "ventas # de venta": "sale_id"}

    Rule:
    - repeated normalized aliases are allowed only if they map to the same target
    - if the same normalized alias maps to different targets, fail loudly
    """
    reverse_mapping: dict[str, str] = {}

    for standard_col, aliases in column_mapping.items():
        for alias in aliases:
            normalized = normalize_text(alias)

            if normalized in reverse_mapping:
                existing_target = reverse_mapping[normalized]

                if existing_target != standard_col:
                    raise ValueError(
                        f"Conflicting alias detected in column_mapping.yml: '{alias}' "
                        f"normalized as '{normalized}' is already mapped to "
                        f"'{existing_target}', cannot also map to '{standard_col}'."
                    )

                continue

            reverse_mapping[normalized] = standard_col

    return reverse_mapping


def detect_duplicate_target_columns(
    original_columns: list[Any],
    mapped_columns: list[str],
) -> dict[str, list[str]]:
    """
    Detect when multiple source columns map to the same target column name.

    Returns:
        {
            "buyer_state": ["Compradores Estado", "Destino comprador"]
        }
    """
    target_to_sources: dict[str, list[str]] = {}

    for original_col, mapped_col in zip(original_columns, mapped_columns):
        if mapped_col not in target_to_sources:
            target_to_sources[mapped_col] = []

        target_to_sources[mapped_col].append(str(original_col))

    duplicates = {
        target: sources
        for target, sources in target_to_sources.items()
        if len(sources) > 1 and target not in TECHNICAL_COLUMNS
    }

    return duplicates


def map_columns(
    df: pd.DataFrame,
    mapping_path: str | Path = "etl/config/column_mapping.yml",
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """
    Map raw source columns to standardized internal schema.

    Rules:
    - technical columns are preserved as-is
    - known source columns are renamed using YAML aliases
    - unknown columns are preserved as-is and reported
    - if multiple source columns map to the same target column, fail loudly
    """
    column_mapping = load_column_mapping(mapping_path)
    reverse_mapping = build_reverse_mapping(column_mapping)

    original_columns = list(df.columns)
    new_column_names: list[str] = []
    mapped_columns_detail: dict[str, str] = {}
    unmapped_columns: list[str] = []
    ignored_technical_columns: list[str] = []

    for col in original_columns:
        col_str = str(col)

        if col_str in TECHNICAL_COLUMNS:
            ignored_technical_columns.append(col_str)
            new_column_names.append(col_str)
            continue

        normalized_col = normalize_text(col_str)

        if normalized_col in reverse_mapping:
            target_name = reverse_mapping[normalized_col]
            new_column_names.append(target_name)
            mapped_columns_detail[col_str] = target_name
        else:
            new_column_names.append(col_str)
            unmapped_columns.append(col_str)

    duplicate_target_columns = detect_duplicate_target_columns(
        original_columns=original_columns,
        mapped_columns=new_column_names,
    )

    if duplicate_target_columns:
        raise ValueError(
            "Column mapping produced duplicate target columns. "
            f"Review column_mapping.yml. Details: {duplicate_target_columns}"
        )

    mapped_df = df.copy()
    mapped_df.columns = new_column_names

    debug_info = {
        "mapped_columns": mapped_columns_detail,
        "unmapped_columns": unmapped_columns,
        "ignored_technical_columns": ignored_technical_columns,
        "duplicate_target_columns": duplicate_target_columns,
        "final_columns": list(mapped_df.columns),
    }

    if unmapped_columns:
        print("⚠️ WARNING: Unmapped columns detected:")
        for col in unmapped_columns:
            print(f"   - {col}")

    return mapped_df, debug_info