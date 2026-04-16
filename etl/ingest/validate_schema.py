from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import yaml


def load_required_columns(required_columns_path: str | Path) -> list[str]:
    """
    Load required columns from YAML.

    Expected structure:
    required_columns:
      - sale_id
      - sale_date
      - sale_status
      - quantity
      - total_amount_ars
    """
    required_columns_path = Path(required_columns_path)

    with required_columns_path.open("r", encoding="utf-8") as file:
        config = yaml.safe_load(file) or {}

    return config.get("required_columns", [])


def load_schema_definition(schema_definition_path: str | Path) -> dict[str, str]:
    """
    Load schema definition from YAML.

    Expected structure:
    schema:
      sale_id: string
      sale_date: date
      quantity: int64
    """
    schema_definition_path = Path(schema_definition_path)

    with schema_definition_path.open("r", encoding="utf-8") as file:
        config = yaml.safe_load(file) or {}

    return config.get("schema", {})


def validate_schema(
    df: pd.DataFrame,
    required_columns_path: str | Path = "etl/config/required_columns.yml",
    schema_definition_path: str | Path = "etl/config/schema_definition.yml",
    fail_on_missing_required: bool = True,
) -> dict[str, Any]:
    """
    Validate dataframe columns against project configuration.

    Checks:
    - missing required columns
    - missing schema columns
    - extra columns not present in schema definition
    """
    required_columns = load_required_columns(required_columns_path)
    schema_definition = load_schema_definition(schema_definition_path)

    df_columns = list(df.columns)
    df_columns_set = set(df_columns)

    required_columns_set = set(required_columns)
    schema_columns_set = set(schema_definition.keys())

    missing_required_columns = sorted(required_columns_set - df_columns_set)
    missing_schema_columns = sorted(schema_columns_set - df_columns_set)
    extra_columns = sorted(df_columns_set - schema_columns_set)

    is_valid = len(missing_required_columns) == 0

    validation_result = {
        "is_valid": is_valid,
        "dataframe_columns": df_columns,
        "required_columns": required_columns,
        "schema_columns": sorted(schema_columns_set),
        "missing_required_columns": missing_required_columns,
        "missing_schema_columns": missing_schema_columns,
        "extra_columns": extra_columns,
    }

    if missing_required_columns and fail_on_missing_required:
        raise ValueError(
            "Schema validation failed. Missing required columns: "
            f"{missing_required_columns}"
        )

    return validation_result