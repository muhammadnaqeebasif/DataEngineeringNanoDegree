from helpers import *
import boto3
import json
from datetime import datetime
from helpers.aws_configuration_parser import *

# Kinesis data stream name
my_stream_name = 'police-data-stream'

# Creating kinesis client
kinesis_client = boto3.client('kinesis',
                              region_name=REGION,
                              aws_access_key_id=ACCESS_KEY,
                              aws_secret_access_key=SECRET_KEY
                              )

def put_to_stream(stream_name,data, partition_key):
    """ Puts the data into kinesis data stream
    Parameters
    ----------
    stream_name:str
        kinesis stream to which data needs to be sent
    data:dict
        data to be sent to the kinesis data stream
    partition_key:str
        partitions the data according to the key provided

    Returns
    -------
    None

    """
    put_response = kinesis_client.put_record(
        StreamName=stream_name,
        Data=json.dumps(data),
        PartitionKey=partition_key)

if __name__ == '__main__':
    # reads the data for the neighborhood boundaries
    neighborhood_boundaries = read_json_local('data/neighborhood_boundaries.json')
    # setting the current year to 2017 and month to 1 as API only allows the data from 2017-1 to present
    current_year = 2017
    current_month = 1

    # Iterates till the date is less than today;s date
    while datetime.strptime(f'{current_year}-{current_month:02}','%Y-%m') < datetime.now():
        # this temp variable is used to avoid duplicates
        temp = []
        for neighborhood_boundary in neighborhood_boundaries:
            for boundary in neighborhood_boundary['boundaries']:
                # creates crime_url to get the crime data from the police api
                crime_url = lambda b: f"https://data.police.uk/api/crimes-street/all-crime?lat={float(b['latitude'])}&lng={float(b['longitude'])}&date={current_year}-{current_month:02}"
                try:
                    # get the crime data from the api
                    crime_data = read_from_url(crime_url(boundary), lambda d:dict(d,**{'neighborhood_id':neighborhood_boundary['neighborhood_id']}))
                except:
                    continue
                for c_d in crime_data:
                    # if crime data is not present in temp then puts the data to kinesis stream
                    if c_d not in temp:
                        # puts the data to the kinesis stream
                        put_to_stream(KINESIS['STREAM_NAME'],
                                      dict(c_d,**{'streamed_data_type':'crime'}),
                                      f"{current_year}-{current_month:02}")
                        try:
                            # gets the outcome of the crime from the api
                            outcomes_data = read_from_url(f"https://data.police.uk/api/outcomes-for-crime/{c_d['persistent_id']}")
                            for o_d in outcomes_data:
                                # pushes the outcome data to kinesis stream
                                put_to_stream(KINESIS['STREAM_NAME'],
                                              dict(o_d, **{'streamed_data_type': 'outcomes'}),
                                              f"{current_year}-{current_month:02}")
                        except:
                            continue
                temp = crime_data
        # increments the month by 1
        current_month = current_month +1
        # if month is greater than 12 then increments the year by 1 and sets month to 1
        if current_month > 12:
            current_year = current_year +1
            current_month = 1