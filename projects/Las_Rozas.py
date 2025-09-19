import os
from enum import Enum
from pathlib import Path
from typing import Union, List, Any, Dict

import duckdb
import pandas
from duckdb import DuckDBPyConnection
from duckdb.duckdb import DuckDBPyRelation

from engine.io_with_insights import export_to_environment
from engine.trips_calculator import filter_data_relevant_to_perimeter, extract_and_label_segments, extract_trips_with_stops, \
    label_trip_points, enrich_trips, derive_od_datasets, prepare_trips_for_map_matching, infer_road_use, \
    infer_road_congestion

from engine.geometry import build_area_of_interest
from engine.floating_cars import build_mobility_database
from engine.static import DataStorage, Environment, Vehicle

DATA_ROOT_DIR: Path = Path(os.getcwd()).parent / "data"

PROJECT_NAME: str = "LogEHub"

GEOMETRY_PATH: Path = DATA_ROOT_DIR / "geometry" / PROJECT_NAME
DEFAULT_AREA_OF_INTEREST_FILENAME: str = "LR.geojson"

FLOATING_CAR_DATA_PATH: Path = DATA_ROOT_DIR / "raw-data" / "BMS" / "probe" / "country=ES"

PROJECT_DEPLOYMENT_PATH: Path = DATA_ROOT_DIR / "blocks" / PROJECT_NAME

DUCKDB_PATH: Path = Path(os.getcwd()) / "duckdb" / "temp.db"

class Fields(Enum):
    unique_id = "unique_id"
    timestamp = "timestamp"
    latitude = "latitude"
    longitude = "longitude"
    speed = "speed"
    heading = "heading"
    vehicle_type = "vehicle_type"
    road_type = "road_type"
    road_speed_limit = "road_speed_limit"
    hour = "hour"


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
        self.area_of_interest_bounding_box: Dict[str, float] = dict()
        self.bridgestone_mobility_database: Union[DuckDBPyRelation|None]  = None


    def full_pipeline(self) -> None:

        self.load_data()

        self.infer_origin_destination()
        self.export_origin_destination(Environment.DEV)
        self.export_origin_destination(Environment.PROD)

        self.infer_road_use_and_congestion()
        self.export_road_use_and_congestion(Environment.DEV)
        self.export_road_use_and_congestion(Environment.PROD)

    def load_data(self):
        self.area_of_interest, self.area_of_interest_bounding_box = build_area_of_interest(self.duck_con,
                                                                                           self.area_of_interest_path)

        self.bridgestone_mobility_database = build_mobility_database(self.duck_con, self.floating_car_data_path,
                                                                     DataStorage.parquet.value)

    def infer_origin_destination(self):

        extra_filters: Dict[str, List[str]] = {
            Fields.vehicle_type.value: [Vehicle.truck.value, Vehicle.lcv.value]
        }
        session_ids: DuckDBPyRelation = filter_data_relevant_to_perimeter(self.bridgestone_mobility_database,
                                                                          self.area_of_interest_bounding_box,
                                                                          extra_filters)
        extract_and_label_segments(session_ids)
        extract_trips_with_stops()
        label_trip_points()
        enrich_trips()
        derive_od_datasets()

    def export_origin_destination(self, environment: Environment):
        data: Any = None
        export_to_environment(data, environment)

    def export_road_use_and_congestion(self, environment: Environment):
        data: Any = None
        export_to_environment(data, environment)

    def infer_road_use_and_congestion(self):
        prepare_trips_for_map_matching()
        infer_road_use()
        infer_road_congestion()


