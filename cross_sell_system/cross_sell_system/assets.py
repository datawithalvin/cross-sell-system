import pandas as pd
import duckdb
import os
from dagster import (
    asset, multi_asset, AssetOut, 
    TableSchema, TableColumn, MaterializeResult, 
    Definitions, with_source_code_references, 
    AssetSpec, AssetIn, Output
)

# --------- konfigurasi ---------

# --------- build assets ---------
# --------- ingestion dan data prep ---------
@asset(
    group_name="initial_preparation",
    tags={"asset_type":"pandas", 
          "data_source":"downloaded-csv"},
    code_version="0.0.1",
    compute_kind="pandas"
)
def load_csv_files():
    data_dir = '../data/raw'
    dataframes = {}
    for filename in os.listdir(data_dir):
        if filename.endswith('.csv'):
            df_name = filename.split('.')[0]
            dataframes[df_name] = pd.read_csv(os.path.join(data_dir, filename))
    return dataframes

@asset(
    ins={'dataframes': AssetIn('load_csv_files')},
    group_name="initial_preparation",
    tags={"asset_type":"duckdb"},
    code_version="0.0.1",
    compute_kind="duckdb"
    )
def create_tables(dataframes):
    conn = duckdb.connect('../data/instacart.db')
    for table_name, df in dataframes.items():
        conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df")
    conn.close()
    return "DuckDB tables created successfully"