import os
from pathlib import Path
from typing import Union, List

import duckdb
import pandas
from duckdb import DuckDBPyConnection

from engine.geometry import build_area_of_interest
from engine.floating_cars import build_mobility_database
from engine.static import DataStorage

DATA_ROOT_DIR: Path = Path(os.getcwd()).parent / "data"

PROJECT_NAME: str = "LogEHub"

GEOMETRY_PATH: Path = DATA_ROOT_DIR / "geometry" / PROJECT_NAME
DEFAULT_AREA_OF_INTEREST_FILENAME: str = "LR.geojson"

FLOATING_CAR_DATA_PATH: Path = DATA_ROOT_DIR / "raw-data" / "BMS" / "probe" / "country=ES"

PROJECT_DEPLOYMENT_PATH: Path = DATA_ROOT_DIR / "blocks" / PROJECT_NAME

DUCKDB_PATH: Path = Path(os.getcwd()) / "duckdb" / "temp.db"

class LasRozas:

    def __init__(self, extra_args: List[str]):

        self.area_of_interest_path: Path = GEOMETRY_PATH / DEFAULT_AREA_OF_INTEREST_FILENAME
        if len(extra_args) == 1:
            self.area_of_interest_filename = GEOMETRY_PATH / extra_args[0]

        self.floating_car_data_path: Path = FLOATING_CAR_DATA_PATH
        if len(extra_args) >= 3:
            self.floating_car_data_path = self.floating_car_data_path / f"year={extra_args[-3]}" / f"month={extra_args[-2]}" / f"day={extra_args[-1]}"

        self.duck_con: DuckDBPyConnection = duckdb.connect(DUCKDB_PATH)
        self.area_of_interest: Union[DuckDBPyConnection|None] = None
        self.area_of_interest_bounding_box: pandas.DataFrame = pandas.DataFrame()
        self.bridgestone_mobility_database: Union[DuckDBPyConnection|None]  = None


    def full_pipeline(self) -> None:

        self.area_of_interest, self.area_of_interest_bounding_box = build_area_of_interest(self.duck_con,
                                                                                           self.area_of_interest_path)

        self.bridgestone_mobility_database = build_mobility_database(self.duck_con, self.floating_car_data_path,
                                                                     DataStorage.parquet.value)


