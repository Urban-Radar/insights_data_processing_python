from engine.static import Vehicle, DayPhase


def extract_session_ids():
    pass

def extract_and_label_segments():
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


