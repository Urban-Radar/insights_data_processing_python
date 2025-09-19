from typing import Dict, List

from duckdb import DuckDBPyRelation
from duckdb.duckdb import DuckDBPyConnection

from engine.static import Vehicle, DayPhase, LONGITUDE, LATITUDE, xmax, ymin, ymax, xmin


def filter_data_relevant_to_perimeter(floating_car_data: DuckDBPyRelation,
                                      area_of_interest: Dict[str, float],
                                      extra_filters: Dict[str, List[str]])\
        -> DuckDBPyRelation :

    print(floating_car_data.shape)
    interesting_vehicles: DuckDBPyRelation = floating_car_data
    for field in extra_filters:
        if field in floating_car_data.columns:
            interesting_vehicles = interesting_vehicles.filter(f"{field} IN ('{"','".join(extra_filters[field])}')")

    interesting_vehicles = (interesting_vehicles.filter(f"{LONGITUDE} > {area_of_interest[xmin]}")
                            .filter(f"{LONGITUDE} < {area_of_interest[xmax]}")
                            .filter(f"{LATITUDE} > {area_of_interest[ymin]}")
                            .filter(f"{LATITUDE} < {area_of_interest[ymax]}"))

    return interesting_vehicles

# ```{r}
# basic_filter <- rlang::list2(
#   expr(vehicle_type %in% c("lcv","truck")),
#   expr(country == "ES"),
#   expr(month %in% c('09','10','11') & year %in% c(2024)), # First batch of spain data
# )
# ```
#
# Identify trip/session ids within area of interest. This step takes a few minutes to compute.
#
# ```{r}
# #Find session ids that intersect (area of interest)
# session_ids <- bms_con %>%
#   filter(
#     !!!basic_filter,
#     between(longitude, !!AOI_bb$min_x, !!AOI_bb$max_x),
#     between(latitude, !!AOI_bb$min_y, !!AOI_bb$max_y)
#   ) %>%
#   filter(st_intersects(st_point(longitude,latitude), sql("(SELECT AOI_geom FROM AOI_duck)"))) %>%
#   distinct(unique_id) %>% compute()
#
# ```

def extract_and_label_segments(ids):
    pass

def extract_trips_with_stops():
    pass

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


