import pandas as pd
import duckdb
import os
from dagster import (
    asset, AssetIn, Output, AssetExecutionContext,
    TableSchema, TableColumn, MaterializeResult, 
    Definitions, with_source_code_references, 
    AssetSpec, MetadataValue
)

# --------- Configuration ---------
DATA_DIR = '../data/raw'
DB_PATH = '../data/instacart.db'

# --------- Build Assets ---------
# --------- Ingestion and Data Prep ---------
@asset(
    group_name="initial_preparation",
    tags={"asset_type": "pandas", "data_source": "downloaded-csv"},
    code_version="0.0.1",
    compute_kind="pandas",
    description="Loads CSV files from the raw data directory into pandas DataFrames."
)
def load_csv_files(context: AssetExecutionContext) -> dict:
    dataframes = {}
    for filename in os.listdir(DATA_DIR):
        if filename.endswith('.csv'):
            df_name = filename.split('.')[0]
            file_path = os.path.join(DATA_DIR, filename)
            dataframes[df_name] = pd.read_csv(file_path)
            context.log.info(f"Loaded {filename} with shape {dataframes[df_name].shape}")
    
    context.add_output_metadata({
        "num_files_loaded": len(dataframes),
        "file_names": MetadataValue.json(list(dataframes.keys()))
    })
    return dataframes

@asset(
    ins={'dataframes': AssetIn('load_csv_files')},
    group_name="initial_preparation",
    tags={"asset_type": "duckdb"},
    code_version="0.0.1",
    compute_kind="duckdb",
    description="Creates DuckDB tables from pandas DataFrames."
)
def create_tables(context: AssetExecutionContext, dataframes: dict) -> str:
    conn = duckdb.connect(DB_PATH)
    for table_name, df in dataframes.items():
        conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df")
        context.log.info(f"Created table {table_name} with {len(df)} rows")
    conn.close()
    return "DuckDB tables created successfully"

@asset(
    ins={'tables_created': AssetIn('create_tables')},
    group_name="initial_preparation",
    tags={"asset_type": "duckdb"},
    compute_kind="duckdb",
    description="Validates the created DuckDB tables by counting rows."
)
def validate_tables(context: AssetExecutionContext, tables_created: str) -> dict:
    conn = duckdb.connect(DB_PATH)
    tables = conn.execute("SHOW TABLES").fetchall()
    validation_results = {}
    for table in tables:
        table_name = table[0]
        row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        validation_results[table_name] = row_count
        context.log.info(f"Table {table_name} has {row_count} rows")
    conn.close()
    
    context.add_output_metadata({
        "num_tables": len(validation_results),
        "table_row_counts": MetadataValue.json(validation_results)
    })
    return validation_results