from enum import Enum


class Project(Enum):
    AGAM = "AGAM"
    LasRozas = "LasRozas"


class Task(Enum):
    load_data = "load_data"
    full_pipeline = "full_pipeline"


class DataStorage(Enum):
    parquet = "parquet"
    feather = "feather"