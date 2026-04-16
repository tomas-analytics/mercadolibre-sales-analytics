from __future__ import annotations

import re
import unicodedata
from pathlib import Path
from typing import Any

import pandas as pd
import yaml


SPANISH_MONTHS = {
    "enero": 1,
    "febrero": 2,
    "marzo": 3,
    "abril": 4,
    "mayo": 5,
    "junio": 6,
    "julio": 7,
    "agosto": 8,
    "septiembre": 9,
    "setiembre": 9,
    "octubre": 10,
    "noviembre": 11,
    "diciembre": 12,
}


def load_schema_definition(schema_definition_path: str | Path) -> dict[str, str]:
    schema_definition_path = Path(schema_definition_path)

    with schema_definition_path.open("r", encoding="utf-8") as file:
        config = yaml.safe_load(file) or {}

    return config.get("schema", {})


def strip_accents(text: str) -> str:
    text = unicodedata.normalize("NFKD", text)
    return "".join(char for char in text if not unicodedata.combining(char))


def normalize_scalar_text(value: Any) -> str:
    text = str(value).strip().lower()
    text = strip_accents(text)
    text = re.sub(r"\s+", " ", text)
    return text


def parse_spanish_datetime_scalar(
    value: Any,
    reference_year: int | None = None,
) -> pd.Timestamp | pd.NaT:
    if value is None:
        return pd.NaT

    if isinstance(value, float) and pd.isna(value):
        return pd.NaT

    if isinstance(value, pd.Timestamp):
        return value

    try:
        if pd.isna(value):
            return pd.NaT
    except TypeError:
        pass

    if hasattr(value, "to_pydatetime"):
        try:
            return pd.Timestamp(value)
        except Exception:
            pass

    text = str(value).strip()

    if not text:
        return pd.NaT

    normalized = normalize_scalar_text(text)

    full_match = re.match(
        r"^(\d{1,2}) de ([a-z]+) de (\d{4})(?: (\d{1,2}):(\d{2}) ?hs\.?)?$",
        normalized,
    )
    if full_match:
        day, month_name, year, hour, minute = full_match.groups()
        month_number = SPANISH_MONTHS.get(month_name)

        if month_number is not None:
            return pd.Timestamp(
                int(year),
                month_number,
                int(day),
                int(hour or 0),
                int(minute or 0),
            )

    short_match = re.match(
        r"^(\d{1,2}) de ([a-z]+)(?: \| (\d{1,2}):(\d{2}))?$",
        normalized,
    )
    if short_match:
        day, month_name, hour, minute = short_match.groups()
        month_number = SPANISH_MONTHS.get(month_name)

        if month_number is not None:
            inferred_year = reference_year or pd.Timestamp.today().year

            return pd.Timestamp(
                int(inferred_year),
                month_number,
                int(day),
                int(hour or 0),
                int(minute or 0),
            )

    return pd.to_datetime(value, errors="coerce")


def clean_numeric_string(value: Any) -> Any:
    if value is None:
        return value

    if isinstance(value, float) and pd.isna(value):
        return value

    if isinstance(value, (int, float)):
        return value

    text = str(value).strip()

    if not text:
        return None

    text = text.replace("$", "").replace("ARS", "").replace("ars", "").strip()

    if "," in text and "." in text:
        text = text.replace(".", "").replace(",", ".")
    elif "," in text:
        text = text.replace(",", ".")

    text = text.replace(" ", "")

    return text


def standardize_string(series: pd.Series) -> pd.Series:
    return series.astype("string").str.strip()


def standardize_int64(series: pd.Series) -> pd.Series:
    cleaned = series.map(clean_numeric_string)
    numeric_series = pd.to_numeric(cleaned, errors="coerce")
    return numeric_series.astype("Int64")


def standardize_float64(series: pd.Series) -> pd.Series:
    cleaned = series.map(clean_numeric_string)
    return pd.to_numeric(cleaned, errors="coerce").astype("float64")


def standardize_date(series: pd.Series) -> pd.Series:
    parsed = series.map(parse_spanish_datetime_scalar)
    return parsed.map(lambda value: value.date() if pd.notna(value) else pd.NaT)


def standardize_datetime(
    series: pd.Series,
    reference_year_series: pd.Series | None = None,
) -> pd.Series:
    if reference_year_series is None:
        parsed = series.map(parse_spanish_datetime_scalar)
    else:
        parsed = pd.Series(
            [
                parse_spanish_datetime_scalar(
                    value,
                    reference_year=(int(year) if pd.notna(year) else None),
                )
                for value, year in zip(series, reference_year_series)
            ],
            index=series.index,
        )

    return pd.to_datetime(parsed, errors="coerce")


def standardize_column(
    df: pd.DataFrame,
    column_name: str,
    target_type: str,
) -> pd.Series:
    series = df[column_name]

    if target_type == "string":
        return standardize_string(series)

    if target_type == "int64":
        return standardize_int64(series)

    if target_type == "float64":
        return standardize_float64(series)

    if target_type == "date":
        return standardize_date(series)

    if target_type == "datetime":
        reference_year_series = None

        if column_name in {
            "shipped_at",
            "delivered_at",
            "return_shipped_at",
            "return_delivered_at",
            "return_reviewed_at",
        } and "sale_date" in df.columns:
            parsed_sale_date = pd.Series(
                [parse_spanish_datetime_scalar(value) for value in df["sale_date"]],
                index=df.index,
            )
            reference_year_series = parsed_sale_date.dt.year

        return standardize_datetime(
            series,
            reference_year_series=reference_year_series,
        )

    raise ValueError(f"Unsupported target type in schema_definition.yml: {target_type}")


def standardize_types(
    df: pd.DataFrame,
    schema_definition_path: str | Path = "etl/config/schema_definition.yml",
) -> tuple[pd.DataFrame, dict[str, Any]]:
    schema_definition = load_schema_definition(schema_definition_path)

    standardized_df = df.copy()

    converted_columns: dict[str, str] = {}
    skipped_columns: list[str] = []
    conversion_errors: dict[str, str] = {}

    for column_name, target_type in schema_definition.items():
        if column_name not in standardized_df.columns:
            skipped_columns.append(column_name)
            continue

        try:
            standardized_df[column_name] = standardize_column(
                standardized_df,
                column_name=column_name,
                target_type=target_type,
            )
            converted_columns[column_name] = target_type
        except Exception as exc:
            conversion_errors[column_name] = str(exc)

    debug_info = {
        "converted_columns": converted_columns,
        "skipped_columns": skipped_columns,
        "conversion_errors": conversion_errors,
        "result_dtypes": {
            column: str(dtype) for column, dtype in standardized_df.dtypes.items()
        },
    }

    if conversion_errors:
        raise ValueError(
            "Type standardization failed for one or more columns: "
            f"{conversion_errors}"
        )

    return standardized_df, debug_info