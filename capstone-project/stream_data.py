from helpers import *
import boto3
import json
import pandas as pd
from datetime import datetime
from aws_configuration_parser import *

# Kinesis data stream name
my_stream_name = 'police-data-stream'

# Creating kinesis client
kinesis_client = boto3.client('kinesis',
                              region_name=REGION,
                              aws_access_key_id=ACCESS_KEY,
                              aws_secret_access_key=SECRET_KEY
                              )

def put_to_stream(data, partition_key):
    put_response = kinesis_client.put_record(
        StreamName=KINESIS['STREAM_NAME'],
        Data=json.dumps(data),
        PartitionKey=partition_key)

if __name__ == '__main__':
    neighborhood_boundaries = read_json_local('data/neighborhood_boundaries.json')
    current_year = 2017
    current_month = 1
    while datetime.strptime(f'{current_year}-{current_month:02}','%Y-%m') < datetime.now():
        temp = []
        for neighborhood_boundary in neighborhood_boundaries:
            for boundary in neighborhood_boundary['boundaries']:
                crime_url = lambda b: f"https://data.police.uk/api/crimes-street/all-crime?lat={float(b['latitude'])}&lng={float(b['longitude'])}&date={current_year}-{current_month:02}"
                try:
                    crime_data = read_from_url(crime_url(boundary), lambda d:dict(d,**{'neighborhood_id':neighborhood_boundary['neighborhood_id']}))
                except:
                    continue
                for c_d in crime_data:
                    if c_d not in temp:
                        put_to_stream(dict(c_d,**{'streamed_data_type':'crime'}),f"{current_year}-{current_month:02}")
                        try:
                            outcomes_data = read_from_url(f"https://data.police.uk/api/outcomes-for-crime/{c_d['persistent_id']}")
                            for o_d in outcomes_data:
                                put_to_stream(dict(o_d, **{'streamed_data_type': 'outcomes'}),
                                              f"{current_year}-{current_month:02}")
                        except:
                            continue
                temp = crime_data
        current_month = current_month +1
        if current_month > 12:
            current_year = current_year +1
            current_month = 1