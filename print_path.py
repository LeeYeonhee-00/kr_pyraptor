import pandas as pd

def extract_optimal_path_info(time_to_stops, to_stop_id):
    # 최적 경로의 정류장 ID 추적
    path = [to_stop_id]
    current_stop = to_stop_id
    while current_stop != from_stop_id:
        _, transfers = time_to_stops[current_stop]
        if not transfers:
            break
        current_stop = transfers[-1]  # 마지막 환승역을 추적
        path.insert(0, current_stop)

    # 각 정류장에 대한 정보 추출
    path_info = []
    for stop_id in path:
        time, _ = time_to_stops[stop_id]
        is_transfer = 'Yes' if stop_id != from_stop_id and stop_id != to_stop_id else 'No'
        path_info.append({
            'Stop ID': stop_id,
            'Arrival Time': time,
            'Is Transfer': is_transfer
        })

    return pd.DataFrame(path_info)

# 최적 경로 추출
optimal_path_df = extract_optimal_path_info(time_to_stops, to_stop_id)

# 결과 출력
print(optimal_path_df)
