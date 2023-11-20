import logging
import partridge as ptg

# capture logs in notebook
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logging.debug("test")

# load a GTFS of AC Transit
# path = 'gtfs.zip'
path = 'kr_gtfs.zip'
_date, service_ids = ptg.read_busiest_date(path)
view = {'trips.txt': {'service_id': service_ids}}
feed = ptg.load_feed(path, view)

import geopandas as gpd
import pyproj
from shapely.geometry import Point

# convert all known stops in the schedule to shapes in a GeoDataFrame
gdf = gpd.GeoDataFrame(
    {"stop_id": feed.stops.stop_id.tolist()},
    geometry=[
        Point(lon, lat)
        for lat, lon in zip(
            feed.stops.stop_lat,
            feed.stops.stop_lon)
    ]
)
gdf = gdf.set_index("stop_id")

# CRS 수정
gdf.crs = 'epsg:4326'

# re-cast to meter-based projection to allow for distance calculations
centroid = gdf.iloc[0].geometry.centroid
aeqd_crs = pyproj.CRS(
    proj='aeqd',
    ellps='WGS84',
    datum='WGS84',

    lat_0=centroid.y,
    lon_0=centroid.x
)

gdf = gdf.to_crs(crs=aeqd_crs)

# let's use this example origin and destination
# to find the time it would take to go from one to another
# from_stop_name = "Santa Clara Av & Mozart St"
# to_stop_name = "10th Avenue SB"
from_stop_name = "가천대"
to_stop_name = "태평"

# QA: we know the best way to connect these two is the 51A -> 1T
# if we depart at 8:30 AM, schedule should suggest:
#     take 51A 8:37 - 8:49
#     make walk connection
#     take 1T 8:56 - 9:03
# total travel time: 26 minutes

# look at all trips from that stop that are after the depart time
departure_secs = 8.5 * 60 * 60

# get all information, including the stop ids, for the start and end nodes
from_stop = feed.stops[feed.stops.stop_name == from_stop_name].head(1).squeeze()
to_stop = feed.stops[feed.stops.stop_name == to_stop_name].head(1).squeeze()

# for check
to_stop2 = feed.stops[feed.stops.stop_name == to_stop_name]

# extract just the stop ids
from_stop_id = from_stop.stop_id
to_stop_id = to_stop.stop_id

from copy import copy
from typing import Any
from typing import Dict
from typing import List

# assume all xfers are 3 minutes
TRANSFER_COST = (3 * 60)

def get_trip_ids_for_stop(feed, stop_id: str, departure_time: int):
    """Takes a stop and departure time and get associated trip ids."""
    mask_1 = feed.stop_times.stop_id == stop_id
    mask_2 = feed.stop_times.departure_time >= departure_time

    # extract the list of qualifying trip ids
    potential_trips = feed.stop_times[mask_1 & mask_2].trip_id.unique().tolist()

    return potential_trips


# def stop_times_for_kth_trip(
#     from_stop_id: str,
#     stop_ids: List[str],
#     time_to_stops_orig: Dict[str, Any],
# ) -> Dict[str, Any]:
#     # prevent upstream mutation of dictionary
#     time_to_stops = copy(time_to_stops_orig)
#     stop_ids = list(stop_ids)
#     potential_trips_num = 0

#     for i, ref_stop_id in enumerate(stop_ids):
#         # how long it took to get to the stop so far (0 for start node)
#         # baseline_cost = time_to_stops[ref_stop_id]
#         baseline_cost, baseline_transfers = time_to_stops[ref_stop_id]

#         # get list of all trips associated with this stop
#         potential_trips = get_trip_ids_for_stop(feed, ref_stop_id, departure_secs)
#         potential_trips_num += int(len(potential_trips))
        
#         for potential_trip in potential_trips:

#             # get all the stop time arrivals for that trip
#             stop_times_sub = feed.stop_times[feed.stop_times.trip_id == potential_trip]
#             stop_times_sub = stop_times_sub.sort_values(by="stop_sequence")

#             # get the "hop on" point
#             from_her_subset = stop_times_sub[stop_times_sub.stop_id == ref_stop_id]
#             from_here = from_her_subset.head(1).squeeze()

#             # get all following stops
#             stop_times_after_mask = stop_times_sub.stop_sequence >= from_here.stop_sequence
#             stop_times_after = stop_times_sub[stop_times_after_mask]

#             # for all following stops, calculate time to reach
#             arrivals_zip = zip(stop_times_after.arrival_time, stop_times_after.stop_id)
#             for arrive_time, arrive_stop_id in arrivals_zip:

#                 # time to reach is diff from start time to arrival (plus any baseline cost)
#                 arrive_time_adjusted = arrive_time - departure_secs + baseline_cost

#                 new_transfers = baseline_transfers + [arrive_stop_id]

#                 # only update if does not exist yet or is faster
#                 if arrive_stop_id in time_to_stops:
#                     if time_to_stops[arrive_stop_id][0] > arrive_time_adjusted:
#                         time_to_stops[arrive_stop_id] = (arrive_time_adjusted, new_transfers)
#                 else:
#                     time_to_stops[arrive_stop_id] = (arrive_time_adjusted, new_transfers)

#     print("The final operation of potential trips num: ", potential_trips_num)
#     return time_to_stops

def stop_times_for_kth_trip(
    from_stop_id: str,
    stop_ids: List[str],
    time_to_stops_orig: Dict[str, Any],
    feed, departure_secs
) -> Dict[str, Any]:
    # prevent upstream mutation of dictionary
    time_to_stops = copy(time_to_stops_orig)
    stop_ids = list(stop_ids)
    potential_trips_num = 0
    final_trip_info = []  # List to store information about stops in the final trip

    for i, ref_stop_id in enumerate(stop_ids):
        # how long it took to get to the stop so far (0 for start node)
        # baseline_cost = time_to_stops[ref_stop_id]
        baseline_cost, baseline_transfers = time_to_stops[ref_stop_id]

        # get list of all trips associated with this stop
        potential_trips = get_trip_ids_for_stop(feed, ref_stop_id, departure_secs)
        potential_trips_num += int(len(potential_trips))

        for potential_trip in potential_trips:
            # get all the stop time arrivals for that trip
            stop_times_sub = feed.stop_times[feed.stop_times.trip_id == potential_trip]
            stop_times_sub = stop_times_sub.sort_values(by="stop_sequence")

            # get the "hop on" point
            from_her_subset = stop_times_sub[stop_times_sub.stop_id == ref_stop_id]
            from_here = from_her_subset.head(1).squeeze()

            # get all following stops
            stop_times_after_mask = stop_times_sub.stop_sequence >= from_here.stop_sequence
            stop_times_after = stop_times_sub[stop_times_after_mask]

            # for all following stops, calculate time to reach
            arrivals_zip = zip(stop_times_after.arrival_time, stop_times_after.stop_id)
            for arrive_time, arrive_stop_id in arrivals_zip:
                # time to reach is diff from start time to arrival (plus any baseline cost)
                arrive_time_adjusted = arrive_time - departure_secs + baseline_cost
                new_transfers = baseline_transfers + [arrive_stop_id]

                # only update if does not exist yet or is faster
                if arrive_stop_id in time_to_stops:
                    if time_to_stops[arrive_stop_id][0] > arrive_time_adjusted:
                        time_to_stops[arrive_stop_id] = (arrive_time_adjusted, new_transfers)
                else:
                    time_to_stops[arrive_stop_id] = (arrive_time_adjusted, new_transfers)

                # Save information about the stop in the final trip
                final_trip_info.append({
                    'Stop ID': arrive_stop_id,
                    'Arrival Time': arrive_time_adjusted,
                    'Transfer': new_transfers
                })

    print("The final operation of potential trips num: ", potential_trips_num)
    # print("Final Trip Information:")
    # print(final_trip_info)
    return time_to_stops


def add_footpath_transfers(
    stop_ids: List[str],
    time_to_stops_orig: Dict[str, Any],
    stops_gdf: gpd.GeoDataFrame,
    transfer_cost=TRANSFER_COST,
) -> Dict[str, Any]:
    # prevent upstream mutation of dictionary
    time_to_stops = copy(time_to_stops_orig)
    stop_ids = list(stop_ids)

    # add in transfers to nearby stops
    for stop_id in stop_ids:
        stop_pt = stops_gdf.loc[stop_id].geometry

        # TODO: parameterize? transfer within .2 miles
        meters_in_miles = 1610
        qual_area = stop_pt.buffer(meters_in_miles/5)

        # get all stops within a short walk of target stop
        mask = stops_gdf.intersects(qual_area)

        # time to reach new nearby stops is the transfer cost plus arrival at last stop
        # arrive_time_adjusted = time_to_stops[stop_id] + TRANSFER_COST
        arrive_time_adjusted = time_to_stops[stop_id][0] + TRANSFER_COST
        new_transfers = time_to_stops[stop_id][1] + [stop_id]

        # only update if currently inaccessible or faster than currrent option
        # for arrive_stop_id, row in stops_gdf[mask].iterrows():
        #     if arrive_stop_id in time_to_stops:
        #         if time_to_stops[arrive_stop_id] > arrive_time_adjusted:
        #             time_to_stops[arrive_stop_id] = arrive_time_adjusted
        #     else:
        #         time_to_stops[arrive_stop_id] = arrive_time_adjusted

        for arrive_stop_id, row in stops_gdf[mask].iterrows():
            if arrive_stop_id in time_to_stops:
                if time_to_stops[arrive_stop_id][0] > arrive_time_adjusted:
                    time_to_stops[arrive_stop_id] = (arrive_time_adjusted, new_transfers)
            else:
                time_to_stops[arrive_stop_id] = (arrive_time_adjusted, new_transfers)

    return time_to_stops


import time
# initialize lookup with start node taking 0 seconds to reach
time_to_stops = {from_stop_id: (0, [])}  # (time, [list of transfers])

# setting transfer limit at 1
TRANSFER_LIMIT = 1
for k in range(TRANSFER_LIMIT + 1):
    logger.info("\nAnalyzing possibilities with {} transfers".format(k))

    # generate current list of stop ids under consideration
    stop_ids = list(time_to_stops.keys())
    logger.info("\tinital qualifying stop ids count: {}".format(len(stop_ids)))

    # update time to stops calculated based on stops accessible
    tic = time.perf_counter()
    time_to_stops = stop_times_for_kth_trip(from_stop_id, stop_ids, time_to_stops, feed, departure_secs)
    toc = time.perf_counter()
    logger.info("\tstop times calculated in {:0.4f} seconds".format(toc - tic))

    added_keys_count = len((time_to_stops.keys())) - len(stop_ids)
    logger.info("\t\t{} stop ids added".format(added_keys_count))

    # now add footpath transfers and update
    tic = time.perf_counter()
    stop_ids = list(time_to_stops.keys())
    time_to_stops = add_footpath_transfers(stop_ids, time_to_stops, gdf)
    toc = time.perf_counter()
    logger.info("\tfootpath transfers calculated in {:0.4f} seconds".format(toc - tic))

    added_keys_count = len((time_to_stops.keys())) - len(stop_ids)
    logger.info("\t\t{} stop ids added".format(added_keys_count))

assert to_stop_id in time_to_stops, "Unable to find route to destination within transfer limit"

time_to_destination = time_to_stops[to_stop_id][0]
transfers = time_to_stops[to_stop_id][1]

logger.info("Time to destination: {} minutes".format(time_to_destination/60))

# 경로 결과 표 출력
import pandas as pd
transfers_info = []
transfers_info.append({'Stop ID': from_stop_id, 'Arrival Time': 0})
for stop_id in transfers:
    arrival_time = time_to_stops[stop_id][0]
    # stop_name = feed.stops.loc[feed.stops['stop_id'] == stop_id, 'stop_name'].loc[0]
    stop_name = feed.stops.loc[feed.stops['stop_id'] == stop_id, 'stop_name'].iloc[0]
    # transfers_info.append(f"{stop_id} (도착 시간: {arrival_time}초)")
    transfers_info.append({'Stop ID': stop_id, 'Stop Name': stop_name, 'Arrival Time': arrival_time/60})

df = pd.DataFrame(transfers_info)
print(df)
# transfers_str = " -> ".join(transfers_info)
# logger.info(f"Transfers and Arrival Times: {transfers_str}")

# logger.info("Transfers: {}".format(" -> ".join(transfers)))
