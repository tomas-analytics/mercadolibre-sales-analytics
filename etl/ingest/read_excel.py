from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from etl.ingest.detect_header import detect_header
from etl.ingest.map_columns import map_columns


def drop_fully_empty_rows_and_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove rows and columns that are completely empty.
    """
    return df.dropna(axis=0, how="all").dropna(axis=1, how="all")


def value_to_label(value: Any) -> str:
    """
    Convert a header cell value into a clean label.
    """
    if value is None:
        return ""

    if isinstance(value, float) and pd.isna(value):
        return ""

    return str(value).strip()


def build_category_labels(category_row: pd.Series) -> list[str]:
    """
    Build category labels using forward-fill across the row.

    Example:
    [Ventas, null, null, Compradores, null]
    ->
    [Ventas, Ventas, Ventas, Compradores, Compradores]
    """
    labels: list[str] = []
    current_label = ""

    for value in category_row.tolist():
        cleaned = value_to_label(value)

        if cleaned:
            current_label = cleaned

        labels.append(current_label)

    return labels


def make_unique_headers(headers: list[str]) -> list[str]:
    """
    Ensure duplicated headers become unique by adding suffixes.

    Example:
    ["estado", "estado", "estado"]
    ->
    ["estado", "estado__2", "estado__3"]
    """
    seen: dict[str, int] = {}
    unique_headers: list[str] = []

    for header in headers:
        base_header = header if header else "unnamed_column"

        if base_header not in seen:
            seen[base_header] = 1
            unique_headers.append(base_header)
            continue

        seen[base_header] += 1
        unique_headers.append(f"{base_header}__{seen[base_header]}")

    return unique_headers


def build_multilevel_headers(
    raw_df: pd.DataFrame,
    header_row_index: int,
) -> tuple[list[str], dict[str, Any]]:
    """
    Build source headers using:
    - category row = header_row_index - 1
    - field row = header_row_index

    Example:
    category: "Ventas"
    field: "Estado"
    ->
    "Ventas Estado"
    """
    header_row = raw_df.iloc[header_row_index]
    header_labels = [value_to_label(value) for value in header_row.tolist()]

    category_row_used = False
    category_labels = [""] * len(header_labels)

    if header_row_index > 0:
        candidate_category_row = raw_df.iloc[header_row_index - 1]
        candidate_category_labels = build_category_labels(candidate_category_row)

        if any(label for label in candidate_category_labels):
            category_labels = candidate_category_labels
            category_row_used = True

    combined_headers: list[str] = []

    for category_label, header_label in zip(category_labels, header_labels):
        if category_label and header_label:
            combined_headers.append(f"{category_label} {header_label}".strip())
        elif header_label:
            combined_headers.append(header_label)
        elif category_label:
            combined_headers.append(category_label)
        else:
            combined_headers.append("")

    unique_headers = make_unique_headers(combined_headers)

    debug_info = {
        "category_row_used": category_row_used,
        "category_row_index": header_row_index - 1 if category_row_used else None,
        "raw_header_labels": header_labels,
        "category_labels": category_labels,
        "combined_headers": combined_headers,
        "unique_headers": unique_headers,
    }

    return unique_headers, debug_info


def build_dataframe_from_detected_header(
    raw_df: pd.DataFrame,
    header_row_index: int,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """
    Build a clean dataframe using the detected header row and, when available,
    the category row above it.
    """
    source_headers, header_build_debug = build_multilevel_headers(
        raw_df=raw_df,
        header_row_index=header_row_index,
    )

    data_df = raw_df.iloc[header_row_index + 1 :].copy()
    data_df.columns = source_headers
    data_df = data_df.reset_index(drop=True)
    data_df = drop_fully_empty_rows_and_columns(data_df)

    return data_df, header_build_debug


def add_source_row_number(
    df: pd.DataFrame,
    header_row_index: int,
) -> pd.DataFrame:
    """
    Add the original row number from the source file.
    """
    result_df = df.copy()

    result_df["source_row_number"] = range(
        header_row_index + 2,
        header_row_index + 2 + len(result_df),
    )

    return result_df


def read_excel_file(
    file_path: str | Path,
    sheet_name: str | int = 0,
    mapping_path: str | Path = "etl/config/column_mapping.yml",
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """
    Read an Excel file, detect the header row automatically,
    rebuild the dataframe with the detected header,
    map columns to the internal standard schema,
    and return both the dataframe and debug info.
    """
    file_path = Path(file_path)

    raw_df = pd.read_excel(
        file_path,
        sheet_name=sheet_name,
        header=None,
        dtype=object,
    )

    header_row_index, header_debug = detect_header(
        raw_df=raw_df,
        mapping_path=mapping_path,
    )

    structured_df, header_build_debug = build_dataframe_from_detected_header(
        raw_df=raw_df,
        header_row_index=header_row_index,
    )

    structured_df = add_source_row_number(
        df=structured_df,
        header_row_index=header_row_index,
    )

    mapped_df, mapping_debug = map_columns(
        df=structured_df,
        mapping_path=mapping_path,
    )

    debug_info = {
        "file_path": str(file_path),
        "sheet_name": sheet_name,
        "header_detection": header_debug,
        "header_build": header_build_debug,
        "column_mapping": mapping_debug,
        "raw_shape": raw_df.shape,
        "structured_shape": structured_df.shape,
        "mapped_shape": mapped_df.shape,
    }

    return mapped_df, debug_info