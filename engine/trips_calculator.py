from typing import Dict, List

from duckdb.duckdb import DuckDBPyRelation, DuckDBPyConnection

from engine.static import Vehicle, DayPhase, LONGITUDE, LATITUDE, TIMESTAMP, xmax, ymin, ymax, xmin

speed_threshold: float = 5
stop_time_threshold: float = 3*60

def filter_data_relevant_to_perimeter(floating_car_data: DuckDBPyRelation,
                                      area_of_interest: Dict[str, float],
                                      extra_filters: Dict[str, List[str]])\
        -> DuckDBPyRelation :

    interesting_vehicles: DuckDBPyRelation = floating_car_data
    for field in extra_filters:
        if field in floating_car_data.columns:
            interesting_vehicles = interesting_vehicles.filter(f"{field} IN ('{"','".join(extra_filters[field])}')")

    interesting_vehicles = (interesting_vehicles.filter(f"{LONGITUDE} > {area_of_interest[xmin]}")
                            .filter(f"{LONGITUDE} < {area_of_interest[xmax]}")
                            .filter(f"{LATITUDE} > {area_of_interest[ymin]}")
                            .filter(f"{LATITUDE} < {area_of_interest[ymax]}"))

    return interesting_vehicles


def extract_and_label_segments(duck_con: DuckDBPyConnection, vectors_in_area_of_interest: DuckDBPyRelation) \
        -> DuckDBPyRelation:

    vectors_by_vehicle_and_time: DuckDBPyRelation = duck_con.sql(f"SELECT unique_id, {TIMESTAMP} as timestamp_start, "
                                               f" LEAD({TIMESTAMP}) OVER (PARTITION BY unique_id ORDER BY {TIMESTAMP}) as timestamp_end,"
                                               f" {LATITUDE}, {LONGITUDE}, vehicle_type, speed, road_speed_limit,"
                                               " age(timestamp_end, timestamp_start) AS time_delta,"
                                               f" st_distance_sphere(st_point({LATITUDE}, {LONGITUDE}), "
                                                        f"st_point("
                                                                 f"LEAD({LATITUDE}) OVER (PARTITION BY unique_id ORDER BY {TIMESTAMP}) , "
                                                                 f"LEAD({LONGITUDE}) OVER (PARTITION BY unique_id ORDER BY {TIMESTAMP})))"
                                                        " AS distance_delta,"
                                               f" distance_delta * 3.6 / epoch(time_delta) as instant_speed"
                                               " FROM vectors_in_area_of_interest "
                                               " ORDER BY timestamp")
    vectors_by_vehicle_and_time = vectors_by_vehicle_and_time.filter("timestamp_end NOT NULL")
    print(vectors_by_vehicle_and_time)
    return vectors_by_vehicle_and_time

def extract_trips_with_stops(connection: DuckDBPyConnection, vectors_by_vehicle_and_time):
     vectors_by_vehicle_and_time = connection.sql(f"SELECT * , "
                                                          f"CASE WHEN (speed < {speed_threshold} OR instant_speed < {speed_threshold}) AND epoch(time_delta) > {stop_time_threshold} "
                                                          f"THEN 1 ELSE 0 END AS 'is_stop', "
                                                          f"CASE WHEN is_stop = 1 AND LAG(is_stop) OVER (PARTITION BY unique_id ORDER BY timestamp_start) = 0 "
                                                          f"THEN 1 ELSE 0 END AS 'is_new_stop' "
                                                          f"FROM vectors_by_vehicle_and_time")
     print(connection.sql(f"SELECT count(unique_id), count(time_delta), sum(is_stop), sum(is_new_stop) FROM vectors_by_vehicle_and_time WHERE is_stop = 1"))

def label_trip_points():
    label_trip_points_by_category(Vehicle.truck, DayPhase.all)
    label_trip_points_by_category(Vehicle.lcv, DayPhase.day_only)
    label_trip_points_by_category(Vehicle.lcv, DayPhase.night_only)

def label_trip_points_by_category(vehicle: Vehicle, day_phase: DayPhase):
    pass


def enrich_trips():
    pass

def derive_od_datasets():
    pass


def infer_trip_segments():
    pass


def run_map_matching_for_trip_segments():
    pass


def prepare_trips_for_map_matching():
    infer_trip_segments()
    run_map_matching_for_trip_segments()


def infer_road_use():
    estimate_road_counts_in_time_window() #TODO Handle variants


def estimate_road_counts_in_time_window():
    pass


def infer_road_congestion():
    pass


