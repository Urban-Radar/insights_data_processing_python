from pathlib import Path

from duckdb import DuckDBPyConnection
from duckdb.duckdb import DuckDBPyRelation
from pyarrow import dataset
from pyarrow._dataset import Dataset


def build_mobility_database(duck_con: DuckDBPyConnection, floating_car_data_source: Path, storage_format: str):
    floating_car_dataset: Dataset = dataset.dataset(floating_car_data_source, format=storage_format)

    floating_car_table: DuckDBPyRelation = duck_con.sql("SELECT unique_id,"
                                                        "CAST(timestamp as timestamp) AS timestamp,"
                                                        "latitude, longitude, speed, heading, vehicle_type, road_type, "
                                                        "road_speed_limit "
                                                        "FROM floating_car_dataset")
    print(floating_car_table)
    return floating_car_table

