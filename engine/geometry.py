import os
from pathlib import Path
from typing import Tuple, Dict
import pandas
from duckdb import DuckDBPyRelation, DuckDBPyConnection


def build_area_of_interest(duck_con: DuckDBPyConnection, geometry_path: Path) -> Tuple[DuckDBPyRelation, Dict[str, float]]:

    duck_con.execute("LOAD spatial; LOAD json;")
    duck_con.execute("SET preserve_insertion_order=false")

    if not os.path.isfile(geometry_path):
        raise Exception(f"No such file: {geometry_path}")

    area_of_interest_epsg4258: DuckDBPyRelation = duck_con.sql(f"SELECT * FROM ST_Read('{geometry_path}')")

    area_of_interest_epsg4326: DuckDBPyRelation = duck_con.sql("SELECT st_transform(geom,'EPSG:4258','EPSG:4326') "
                                                               "AS area_of_interest_geometry "
                                                               "FROM area_of_interest_epsg4258")

    area_of_interest_bounding_box: Dict[str, float] =  duck_con.sql("SELECT st_extent(area_of_interest_geometry) "
                                                                    "AS area_of_interest_bounding_box "
                                                                    "FROM area_of_interest_epsg4326").to_df().iloc[0,0]

    return area_of_interest_epsg4326, area_of_interest_bounding_box


