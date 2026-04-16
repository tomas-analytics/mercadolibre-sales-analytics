from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import yaml
from google.cloud import bigquery


def load_schema_definition(schema_definition_path: str | Path) -> dict[str, str]:
    """
    Load schema definition from YAML.

    Expected structure:
    schema:
      sale_id: string
      sale_date: date
      ...
    """
    schema_definition_path = Path(schema_definition_path)

    with schema_definition_path.open("r", encoding="utf-8") as file_obj:
        config = yaml.safe_load(file_obj) or {}

    return config.get("schema", {})


def map_yaml_type_to_bigquery(yaml_type: str) -> str:
    """
    Map project YAML types to BigQuery field types.
    """
    mapping = {
        "string": "STRING",
        "int64": "INT64",
        "integer": "INT64",
        "float64": "FLOAT64",
        "float": "FLOAT64",
        "boolean": "BOOL",
        "bool": "BOOL",
        "date": "DATE",
        "datetime": "TIMESTAMP",
        "timestamp": "TIMESTAMP",
    }

    normalized = str(yaml_type).strip().lower()

    if normalized not in mapping:
        raise ValueError(f"Unsupported YAML type for BigQuery mapping: {yaml_type}")

    return mapping[normalized]


def build_bigquery_schema_from_yaml(
    schema_definition_path: str | Path,
    dataframe_columns: list[str],
) -> list[bigquery.SchemaField]:
    """
    Build BigQuery schema fields from schema_definition.yml,
    preserving the dataframe column order.
    """
    schema_definition = load_schema_definition(schema_definition_path)

    schema_fields: list[bigquery.SchemaField] = []

    for column_name in dataframe_columns:
        if column_name not in schema_definition:
            raise ValueError(
                f"Column '{column_name}' not found in schema_definition.yml"
            )

        yaml_type = schema_definition[column_name]
        bigquery_type = map_yaml_type_to_bigquery(yaml_type)

        schema_fields.append(
            bigquery.SchemaField(
                name=column_name,
                field_type=bigquery_type,
                mode="NULLABLE",
            )
        )

    return schema_fields


def prepare_dataframe_for_bigquery(
    df: pd.DataFrame,
    schema_definition_path: str | Path,
) -> pd.DataFrame:
    """
    Prepare dataframe types for a BigQuery load.

    Important:
    - DATE columns are converted to Python date objects
    - TIMESTAMP columns are converted to pandas datetime64[ns]
    - INT64 columns are kept nullable-compatible where possible
    """
    schema_definition = load_schema_definition(schema_definition_path)
    result_df = df.copy()

    for column_name in result_df.columns:
        target_type = schema_definition.get(column_name)

        if target_type is None:
            raise ValueError(
                f"Column '{column_name}' not found in schema_definition.yml"
            )

        if target_type == "date":
            result_df[column_name] = pd.to_datetime(
                result_df[column_name],
                errors="coerce",
            ).dt.date

        elif target_type == "datetime":
            result_df[column_name] = pd.to_datetime(
                result_df[column_name],
                errors="coerce",
            )

        elif target_type in {"int64", "integer"}:
            result_df[column_name] = pd.to_numeric(
                result_df[column_name],
                errors="coerce",
            ).astype("Int64")

        elif target_type in {"float64", "float"}:
            result_df[column_name] = pd.to_numeric(
                result_df[column_name],
                errors="coerce",
            ).astype("float64")

        elif target_type in {"string"}:
            result_df[column_name] = result_df[column_name].astype("string")

    return result_df


def ensure_dataset_exists(
    client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    location: str,
) -> None:
    """
    Create dataset if it does not exist.
    """
    dataset_fqn = f"{project_id}.{dataset_id}"
    dataset = bigquery.Dataset(dataset_fqn)
    dataset.location = location
    client.create_dataset(dataset, exists_ok=True)


def build_create_table_ddl(
    table_fqn: str,
    schema_fields: list[bigquery.SchemaField],
    partition_column: str | None = None,
    cluster_columns: list[str] | None = None,
) -> str:
    """
    Build CREATE TABLE IF NOT EXISTS DDL for BigQuery.
    """
    column_definitions = ",\n  ".join(
        f"`{field.name}` {field.field_type}" for field in schema_fields
    )

    ddl = f"""
    CREATE TABLE IF NOT EXISTS `{table_fqn}` (
      {column_definitions}
    )
    """

    if partition_column:
        ddl += f"\nPARTITION BY DATE(`{partition_column}`)"

    if cluster_columns:
        cluster_sql = ", ".join(f"`{col}`" for col in cluster_columns)
        ddl += f"\nCLUSTER BY {cluster_sql}"

    return ddl.strip()


def ensure_table_exists(
    client: bigquery.Client,
    table_fqn: str,
    schema_fields: list[bigquery.SchemaField],
    partition_column: str | None = None,
    cluster_columns: list[str] | None = None,
) -> None:
    """
    Create table if it does not exist.
    """
    ddl = build_create_table_ddl(
        table_fqn=table_fqn,
        schema_fields=schema_fields,
        partition_column=partition_column,
        cluster_columns=cluster_columns,
    )
    client.query(ddl).result()


def load_dataframe_to_table(
    client: bigquery.Client,
    df: pd.DataFrame,
    table_fqn: str,
    schema_fields: list[bigquery.SchemaField],
    write_disposition: str,
) -> bigquery.LoadJob:
    """
    Load DataFrame into BigQuery table.
    """
    job_config = bigquery.LoadJobConfig(
        schema=schema_fields,
        write_disposition=write_disposition,
    )

    load_job = client.load_table_from_dataframe(
        dataframe=df,
        destination=table_fqn,
        job_config=job_config,
    )
    load_job.result()
    return load_job


def build_merge_sql(
    target_table_fqn: str,
    staging_table_fqn: str,
    columns: list[str],
    unique_key: str,
) -> str:
    """
    Build insert-only MERGE statement.
    """
    if unique_key not in columns:
        raise ValueError(f"Unique key '{unique_key}' not found in columns")

    insert_columns_sql = ",\n        ".join(f"`{col}`" for col in columns)
    insert_values_sql = ",\n        ".join(f"source.`{col}`" for col in columns)

    return f"""
MERGE `{target_table_fqn}` AS target
USING (
    SELECT *
    FROM `{staging_table_fqn}`
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY `{unique_key}`
        ORDER BY `loaded_at` DESC, `source_row_number` DESC
    ) = 1
) AS source
ON target.`{unique_key}` = source.`{unique_key}`
WHEN NOT MATCHED THEN
  INSERT (
        {insert_columns_sql}
  )
  VALUES (
        {insert_values_sql}
  )
""".strip()


def count_existing_sale_ids(
    client: bigquery.Client,
    target_table_fqn: str,
    staging_table_fqn: str,
    unique_key: str,
) -> int:
    """
    Count rows from staging already present in target by unique key.
    """
    sql = f"""
SELECT COUNT(*) AS row_count
FROM (
    SELECT *
    FROM `{staging_table_fqn}`
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY `{unique_key}`
        ORDER BY `loaded_at` DESC, `source_row_number` DESC
    ) = 1
) AS source
INNER JOIN `{target_table_fqn}` AS target
    ON source.`{unique_key}` = target.`{unique_key}`
"""

    result = client.query(sql).result()
    row = next(result)
    return int(row["row_count"])


def count_rows_by_file_hash(
    client: bigquery.Client,
    table_fqn: str,
    file_hash: str,
) -> int:
    """
    Count rows in target table for a given file hash.
    """
    sql = f"""
SELECT COUNT(*) AS row_count
FROM `{table_fqn}`
WHERE file_hash = @file_hash
"""

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("file_hash", "STRING", file_hash),
        ]
    )

    result = client.query(sql, job_config=job_config).result()
    row = next(result)
    return int(row["row_count"])


def merge_staging_into_target(
    client: bigquery.Client,
    target_table_fqn: str,
    staging_table_fqn: str,
    columns: list[str],
    unique_key: str,
) -> bigquery.QueryJob:
    """
    Execute MERGE from staging into target.
    """
    merge_sql = build_merge_sql(
        target_table_fqn=target_table_fqn,
        staging_table_fqn=staging_table_fqn,
        columns=columns,
        unique_key=unique_key,
    )
    merge_job = client.query(merge_sql)
    merge_job.result()
    return merge_job


def load_dataframe_to_bigquery_raw(
    df: pd.DataFrame,
    project_id: str,
    dataset_id: str,
    target_table_name: str = "mercadolibre_sales",
    staging_table_name: str = "mercadolibre_sales_load",
    schema_definition_path: str | Path = "etl/config/schema_definition.yml",
    unique_key: str = "sale_id",
    location: str = "US",
) -> dict[str, Any]:
    """
    Load a processed dataframe into BigQuery staging and MERGE into raw target.

    Behavior:
    - removes duplicate sale_id rows inside the incoming dataframe
    - overwrites staging on each execution
    - inserts only new sale_id rows into target
    """
    if df.empty:
        raise ValueError("Cannot load an empty dataframe to BigQuery")

    if unique_key not in df.columns:
        raise ValueError(f"Unique key '{unique_key}' is missing from dataframe")

    if "file_hash" not in df.columns:
        raise ValueError("Column 'file_hash' is required for load auditing")

    deduped_df = df.drop_duplicates(subset=[unique_key], keep="first").copy()
    duplicate_rows_inside_file = len(df) - len(deduped_df)

    prepared_df = prepare_dataframe_for_bigquery(
        df=deduped_df,
        schema_definition_path=schema_definition_path,
    )

    client = bigquery.Client(project=project_id, location=location)

    target_table_fqn = f"{project_id}.{dataset_id}.{target_table_name}"
    staging_table_fqn = f"{project_id}.{dataset_id}.{staging_table_name}"

    ensure_dataset_exists(
        client=client,
        project_id=project_id,
        dataset_id=dataset_id,
        location=location,
    )

    schema_fields = build_bigquery_schema_from_yaml(
        schema_definition_path=schema_definition_path,
        dataframe_columns=list(prepared_df.columns),
    )

    ensure_table_exists(
        client=client,
        table_fqn=target_table_fqn,
        schema_fields=schema_fields,
        partition_column="loaded_at",
        cluster_columns=["sale_id", "sale_date"],
    )

    ensure_table_exists(
        client=client,
        table_fqn=staging_table_fqn,
        schema_fields=schema_fields,
        partition_column="loaded_at",
        cluster_columns=["sale_id", "sale_date"],
    )

    load_dataframe_to_table(
        client=client,
        df=prepared_df,
        table_fqn=staging_table_fqn,
        schema_fields=schema_fields,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    rows_in_file = len(df)
    rows_after_internal_dedup = len(prepared_df)

    rows_already_existing = count_existing_sale_ids(
        client=client,
        target_table_fqn=target_table_fqn,
        staging_table_fqn=staging_table_fqn,
        unique_key=unique_key,
    )

    merge_job = merge_staging_into_target(
        client=client,
        target_table_fqn=target_table_fqn,
        staging_table_fqn=staging_table_fqn,
        columns=list(prepared_df.columns),
        unique_key=unique_key,
    )

    rows_inserted = rows_after_internal_dedup - rows_already_existing

    file_hash = str(prepared_df["file_hash"].iloc[0])
    rows_now_present_for_hash = count_rows_by_file_hash(
        client=client,
        table_fqn=target_table_fqn,
        file_hash=file_hash,
    )

    return {
        "project_id": project_id,
        "dataset_id": dataset_id,
        "target_table": target_table_fqn,
        "staging_table": staging_table_fqn,
        "rows_in_file": rows_in_file,
        "duplicate_rows_inside_file": duplicate_rows_inside_file,
        "rows_after_internal_dedup": rows_after_internal_dedup,
        "rows_already_existing": rows_already_existing,
        "rows_inserted": rows_inserted,
        "rows_now_present_for_file_hash": rows_now_present_for_hash,
        "unique_key": unique_key,
        "file_hash": file_hash,
        "merge_job_id": merge_job.job_id,
    }