from enum import Enum


class Project(Enum):
    AGAM = "AGAM"
    LasRozas = "LasRozas"

class Task(Enum):
    load_data = "load_data"
    full_pipeline = "full_pipeline"

class Environment(Enum):
    DEV = "DEV"
    PROD = "PROD"

class DataStorage(Enum):
    parquet = "parquet"
    feather = "feather"

class Vehicle(Enum):
    truck = "truck"
    lcv = "lcv"

class DayPhase(Enum):
    day_only = "day_only"
    night_only = "night_only"
    all = "all"

LATITUDE: str = "latitude"
LONGITUDE: str = "longitude"
TIMESTAMP: str = "timestamp"
xmin, xmax, ymin, ymax = 'min_x', 'max_x', 'min_y', 'max_y'