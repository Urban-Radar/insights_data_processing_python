from pathlib import Path

from duckdb import DuckDBPyConnection
from duckdb.duckdb import DuckDBPyRelation
from pyarrow import dataset
from pyarrow._dataset import Dataset


def build_mobility_database(duck_con: DuckDBPyConnection, floating_car_data_source: Path, storage_format: str):
    floating_car_dataset: Dataset = dataset.dataset(floating_car_data_source, format=storage_format)
    print(floating_car_dataset.head(10))

    floating_car_table: DuckDBPyRelation = duck_con.sql("SELECT * FROM floating_car_dataset")
    print(floating_car_table.to_df().info())



# bms_schema <- schema(latitude = float(),
#                      longitude = float(),
#                      speed = int32(),
#                      # heading = int32(),
#                      vehicle_type = string(),
#                      # road_type = string(),
#                      road_speed_limit = int16(),
#                      # hour = int8(),
#                      unique_id = string(),
#                      timestamp = timestamp(unit = "ns", timezone = "GMT"),
#                      country = string(),
#                      year = int32(),
#                      month = int32(),
#                      day = int32())
# bms_con <- arrow::open_dataset(Sys.getenv("BMS_dir"), schema = bms_schema)
#
#
# bms_con <- tbl(duck_con, sql(str_glue("SELECT * FROM read_parquet('{file.path(Sys.getenv('BMS_dir'),'*','*','*','*','*.parquet')}', hive_partitioning = true)")))