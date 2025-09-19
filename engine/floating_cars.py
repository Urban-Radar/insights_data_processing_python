from pathlib import Path

from duckdb import DuckDBPyConnection
from duckdb.duckdb import DuckDBPyRelation
from pyarrow import dataset
from pyarrow._dataset import Dataset


def build_mobility_database(duck_con: DuckDBPyConnection, floating_car_data_source: Path, storage_format: str):
    floating_car_dataset: Dataset = dataset.dataset(floating_car_data_source, format=storage_format)

    floating_car_table: DuckDBPyRelation = duck_con.sql("SELECT * FROM floating_car_dataset")
    floating_car_table.to_df().info()
    return floating_car_table

