def stop_times_for_kth_trip(
    from_stop_id: str,
    stop_ids: List[str],
    time_to_stops_orig: Dict[str, Any],
    feed, departure_secs
) -> Dict[str, Any]:
    # prevent upstream mutation of dictionary
    time_to_stops = copy(time_to_stops_orig)
    stop_ids = list(stop_ids)

    for i, ref_stop_id in enumerate(stop_ids):
        baseline_cost, baseline_transfers = time_to_stops[ref_stop_id]

        # get list of all trips associated with this stop
        potential_trips = get_trip_ids_for_stop(feed, ref_stop_id, departure_secs)

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
            for arrive_time, arrive_stop_id in zip(stop_times_after.arrival_time, stop_times_after.stop_id):
                # time to reach is diff from start time to arrival (plus any baseline cost)
                arrive_time_adjusted = arrive_time - departure_secs + baseline_cost

                new_transfers = baseline_transfers + [arrive_stop_id] if arrive_stop_id not in baseline_transfers else baseline_transfers

                # only update if does not exist yet or is faster
                if arrive_stop_id in time_to_stops:
                    if time_to_stops[arrive_stop_id][0] > arrive_time_adjusted:
                        time_to_stops[arrive_stop_id] = (arrive_time_adjusted, new_transfers)
                else:
                    time_to_stops[arrive_stop_id] = (arrive_time_adjusted, new_transfers)

    return time_to_stops
