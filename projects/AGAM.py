import os
from pathlib import Path
import duckdb
from duckdb import DuckDBPyConnection

from engine.geometry import build_area_of_interest

DUCKDB_PATH = Path(os.getcwd()) / "duckdb" / "temp.db"
GEOMETRY_PATH = Path(os.getcwd()).parent / "data" / "geometry" / "AGAM"
DEFAULT_AREA_OF_INTEREST: str = "Périmètre Etude PL FMN_Urban Radar_juillet 2024.shp"

class AGAM:

    def full_pipeline(self, area_of_interest: str) -> None:

        duck_con: DuckDBPyConnection = duckdb.connect(DUCKDB_PATH)
        duck_con.sql("LOAD spatial;")

        shapefile_path: Path = GEOMETRY_PATH / (area_of_interest if area_of_interest else DEFAULT_AREA_OF_INTEREST)
        area_of_interest, area_of_interest_bounding_box = build_area_of_interest(duck_con, shapefile_path)