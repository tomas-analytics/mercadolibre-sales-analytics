from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from etl.ingest.read_excel import read_excel_file
from etl.ingest.standardize_types import standardize_types
from etl.ingest.validate_schema import validate_schema


def build_output_paths(file_path: str | Path, output_dir: str | Path) -> dict[str, Path]:
    """
    Build local output paths for processed files.
    """
    file_path = Path(file_path)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    base_name = file_path.stem

    return {
        "csv": output_dir / f"{base_name}_processed.csv",
        "parquet": output_dir / f"{base_name}_processed.parquet",
    }


def save_processed_output(
    df: pd.DataFrame,
    file_path: str | Path,
    output_dir: str | Path = "data/processed",
) -> dict[str, str]:
    """
    Save processed dataframe locally.

    Saves:
    - CSV for easy manual inspection
    - Parquet for analytical/technical use
    """
    paths = build_output_paths(file_path=file_path, output_dir=output_dir)

    df.to_csv(paths["csv"], index=False, encoding="utf-8-sig")

    parquet_saved = False
    parquet_error = None

    try:
        df.to_parquet(paths["parquet"], index=False)
        parquet_saved = True
    except Exception as exc:
        parquet_error = str(exc)

    return {
        "csv_path": str(paths["csv"]),
        "parquet_path": str(paths["parquet"]),
        "parquet_saved": parquet_saved,
        "parquet_error": parquet_error,
    }


def run_ingestion_pipeline(
    file_path: str | Path,
    sheet_name: str | int = 0,
    mapping_path: str | Path = "etl/config/column_mapping.yml",
    required_columns_path: str | Path = "etl/config/required_columns.yml",
    schema_definition_path: str | Path = "etl/config/schema_definition.yml",
    save_output: bool = True,
    output_dir: str | Path = "data/processed",
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """
    Execute the ingestion pipeline end-to-end.

    Steps:
    1. Read Excel without assuming header
    2. Detect header row
    3. Rebuild dataframe from detected header
    4. Map source columns to internal standard columns
    5. Validate schema
    6. Standardize data types
    7. Optionally save processed output locally
    """
    file_path = Path(file_path)

    mapped_df, read_debug = read_excel_file(
        file_path=file_path,
        sheet_name=sheet_name,
        mapping_path=mapping_path,
    )

    validation_debug = validate_schema(
        df=mapped_df,
        required_columns_path=required_columns_path,
        schema_definition_path=schema_definition_path,
        fail_on_missing_required=True,
    )

    standardized_df, standardization_debug = standardize_types(
        df=mapped_df,
        schema_definition_path=schema_definition_path,
    )

    output_debug = None
    if save_output:
        output_debug = save_processed_output(
            df=standardized_df,
            file_path=file_path,
            output_dir=output_dir,
        )

    pipeline_debug = {
        "file_path": str(file_path),
        "sheet_name": sheet_name,
        "read_step": read_debug,
        "validation_step": validation_debug,
        "standardization_step": standardization_debug,
        "output_step": output_debug,
        "final_shape": standardized_df.shape,
        "final_columns": list(standardized_df.columns),
    }

    return standardized_df, pipeline_debug


def print_pipeline_summary(pipeline_debug: dict[str, Any]) -> None:
    """
    Print a compact execution summary for local testing/debugging.
    """
    print("\n" + "=" * 60)
    print("INGESTION PIPELINE SUMMARY")
    print("=" * 60)

    print(f"File: {pipeline_debug['file_path']}")
    print(f"Sheet: {pipeline_debug['sheet_name']}")
    print(f"Final shape: {pipeline_debug['final_shape']}")

    header_info = pipeline_debug["read_step"]["header_detection"]
    print(
        f"Detected header row: {header_info['selected_row_index']} "
        f"(score={header_info['selected_score']:.2f})"
    )

    mapping_info = pipeline_debug["read_step"]["column_mapping"]
    mapped_count = len(mapping_info["mapped_columns"])
    unmapped_count = len(mapping_info["unmapped_columns"])

    print(f"Mapped columns: {mapped_count}")
    print(f"Unmapped columns: {unmapped_count}")

    if mapping_info["unmapped_columns"]:
        print("Unmapped columns detected:")
        for column in mapping_info["unmapped_columns"]:
            print(f"  - {column}")

    validation_info = pipeline_debug["validation_step"]
    print(f"Schema valid: {validation_info['is_valid']}")

    if validation_info["missing_required_columns"]:
        print("Missing required columns:")
        for column in validation_info["missing_required_columns"]:
            print(f"  - {column}")

    if validation_info["extra_columns"]:
        print("Extra columns not present in schema_definition.yml:")
        for column in validation_info["extra_columns"]:
            print(f"  - {column}")

    standardization_info = pipeline_debug["standardization_step"]
    if standardization_info["conversion_errors"]:
        print("Conversion errors:")
        for column, error in standardization_info["conversion_errors"].items():
            print(f"  - {column}: {error}")

    output_info = pipeline_debug.get("output_step")
    if output_info:
        print(f"CSV saved to: {output_info['csv_path']}")
        if output_info["parquet_saved"]:
            print(f"Parquet saved to: {output_info['parquet_path']}")
        else:
            print("Parquet was not saved.")
            if output_info["parquet_error"]:
                print(f"Parquet error: {output_info['parquet_error']}")

    print("=" * 60 + "\n")


if __name__ == "__main__":
    SAMPLE_FILE_PATH = "20260412_Ventas_AR_Mercado_Libre_y_Mercado_Shops_2026-04-12_17-10hs_171360540.xlsx"

    final_df, pipeline_debug = run_ingestion_pipeline(
        file_path=SAMPLE_FILE_PATH,
        sheet_name=0,
        save_output=True,
        output_dir="data/processed",
    )

    print_pipeline_summary(pipeline_debug)
    print(final_df.head())