import gtfs_realtime_pb2
from google.protobuf.json_format import MessageToDict
import pandas as pd
import datetime as dt

def get_merged_df_from_path(path: str, train_trips_at_day_df:pd.DataFrame) -> pd.DataFrame:
    feed = gtfs_realtime_pb2.FeedMessage()
    with open(path, 'rb') as f:
        feed.ParseFromString(f.read())
        tripUpdates = MessageToDict(feed)

    matchable_trip_ids = train_trips_at_day_df.trip_id

    id_proof = [entity for entity in tripUpdates['entity'] if 'tripId' in entity['tripUpdate']['trip']] # avoid the empty tripId error
    valid_trip_id_sequence_d = {entity['tripUpdate']['trip']['tripId']: entity['tripUpdate']['stopTimeUpdate'] for entity in id_proof if entity['tripUpdate']['trip']['tripId'] in  matchable_trip_ids.values}
    if len(valid_trip_id_sequence_d) == 0:
        pass #Maybe catch some error, or maybe a point ?
    
    else:
        merged_dfs = []
        for key, value in valid_trip_id_sequence_d.items():

            trip_df = pd.DataFrame(value)
            columns_to_flatten = ['arrival', 'departure']
            for column in columns_to_flatten: 
                if column in trip_df.columns:
                    flattened_as_df = pd.json_normalize(trip_df[f'{column}'])
                    trip_df = pd.merge(trip_df, flattened_as_df, left_index=True, right_index=True)
                    trip_df = trip_df.drop(column, axis=1)

            merged_df = pd.merge(trip_df, train_trips_at_day_df.query(f"trip_id == '{key}'"), left_on = 'stopId', right_on = 'stop_id')
            merged_df['snapshot_time'] = dt.datetime.now().strftime("%Y-%m-%d_%H:%M")
            merged_dfs.append(merged_df) 
        
        concated_df = pd.concat(merged_dfs)

        return concated_df