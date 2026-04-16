from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from etl.ingest.add_load_metadata import add_load_metadata
from etl.ingest.load_to_bigquery import load_dataframe_to_bigquery_raw
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
) -> dict[str, str | bool | None]:
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
    session_load_id: str | None = None,
    load_bigquery: bool = False,
    gcp_project_id: str | None = None,
    bigquery_dataset_id: str = "raw",
    bigquery_target_table_name: str = "mercadolibre_sales",
    bigquery_staging_table_name: str = "mercadolibre_sales_load",
    bigquery_location: str = "US",
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """
    Execute the ingestion pipeline end-to-end.

    Steps:
    1. Read Excel without assuming header
    2. Detect header row
    3. Rebuild dataframe from detected header
    4. Map source columns to internal standard columns
    5. Validate required/schema consistency
    6. Standardize business column data types
    7. Add technical load metadata
    8. Optionally save processed output locally
    9. Optionally load into BigQuery raw layer
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

    enriched_df, metadata_debug = add_load_metadata(
        df=standardized_df,
        file_path=file_path,
        read_debug=read_debug,
        session_load_id=session_load_id,
    )

    output_debug = None
    if save_output:
        output_debug = save_processed_output(
            df=enriched_df,
            file_path=file_path,
            output_dir=output_dir,
        )

    bigquery_debug = None
    if load_bigquery:
        if not gcp_project_id:
            raise ValueError("gcp_project_id is required when load_bigquery=True")

        bigquery_debug = load_dataframe_to_bigquery_raw(
            df=enriched_df,
            project_id=gcp_project_id,
            dataset_id=bigquery_dataset_id,
            target_table_name=bigquery_target_table_name,
            staging_table_name=bigquery_staging_table_name,
            schema_definition_path=schema_definition_path,
            unique_key="sale_id",
            location=bigquery_location,
        )

    pipeline_debug = {
        "file_path": str(file_path),
        "sheet_name": sheet_name,
        "read_step": read_debug,
        "validation_step": validation_debug,
        "standardization_step": standardization_debug,
        "metadata_step": metadata_debug,
        "output_step": output_debug,
        "bigquery_step": bigquery_debug,
        "final_shape": enriched_df.shape,
        "final_columns": list(enriched_df.columns),
    }

    return enriched_df, pipeline_debug


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

    metadata_info = pipeline_debug["metadata_step"]
    print(f"Load ID: {metadata_info['load_id']}")
    print(f"Session load ID: {metadata_info['session_load_id']}")
    print(f"File hash: {metadata_info['file_hash']}")

    output_info = pipeline_debug.get("output_step")
    if output_info:
        print(f"CSV saved to: {output_info['csv_path']}")
        if output_info["parquet_saved"]:
            print(f"Parquet saved to: {output_info['parquet_path']}")
        else:
            print("Parquet was not saved.")
            if output_info["parquet_error"]:
                print(f"Parquet error: {output_info['parquet_error']}")

    bigquery_info = pipeline_debug.get("bigquery_step")
    if bigquery_info:
        print(f"BigQuery target table: {bigquery_info['target_table']}")
        print(f"Rows in file: {bigquery_info['rows_in_file']}")
        print(
            f"Duplicate rows inside file: "
            f"{bigquery_info['duplicate_rows_inside_file']}"
        )
        print(
            f"Rows after internal dedup: "
            f"{bigquery_info['rows_after_internal_dedup']}"
        )
        print(f"Rows already existing: {bigquery_info['rows_already_existing']}")
        print(f"Rows inserted: {bigquery_info['rows_inserted']}")

    print("=" * 60 + "\n")


if __name__ == "__main__":
    SAMPLE_FILE_PATH = (
        "data/raw/"
        "20260412_Ventas_AR_Mercado_Libre_y_Mercado_Shops_"
        "2026-04-12_17-10hs_171360540.xlsx"
    )

    final_df, pipeline_debug = run_ingestion_pipeline(
        file_path=SAMPLE_FILE_PATH,
        sheet_name=0,
        save_output=True,
        output_dir="data/processed",
        load_bigquery=True,
        gcp_project_id="mercadolibre-sales-analytics",
        bigquery_dataset_id="raw",
        bigquery_target_table_name="mercadolibre_sales",
        bigquery_staging_table_name="mercadolibre_sales_load",
        bigquery_location="US",
    )

    print_pipeline_summary(pipeline_debug)
    print(final_df.head())