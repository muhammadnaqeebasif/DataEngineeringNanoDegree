import json
import base64
import datetime
import boto3
import os

def to_json_format(json_data):
    """ Converts json data to csv format
    Parameters
    ----------
    json_data dict
        The data which needs to be converted to csv format

    Returns
    -------
    str
        Return the CSV representation format of the data
    """
    res = [json.dumps(d) for d in json_data]
    return '\n'.join(res)


def lambda_handler(event, context):
    # Creates the S3 client

    s3 = boto3.client('s3')
    bucket_name = os.environ['S3Bucket']
    output_key_prefix = os.environ['output_key_prefix']

    # list to contain crimes specific record
    crimes = []
    # list to contain outcomes of the crimes
    outcomes = []

    for record in event['Records']:
        # converts the incoming stream of data in the usable format
        payload = base64.b64decode(record["kinesis"]["data"])
        data = json.loads(payload)

        # if data is of type outcomes then extract outcome related fields else extracts crimes related fields
        if data['streamed_data_type'] == 'outcomes':
            for outcome in data['outcomes']:
                d = {}
                d['date'] = outcome['date']
                d['person_id'] = str(outcome['person_id'])
                d['category_code'] = outcome['category']['code']
                d['category_name'] = outcome['category']['name']
                d['persistent_id'] = data['crime']['persistent_id']
                outcomes.append(d)
        else:
            d = {}
            keys = ['category', 'location_type', 'context', 'persistent_id',
                    'id', 'location_subtype', 'month', 'neighborhood_id']
            for key in keys:
                d[key] = data[key]
            d['latitude'] = float(data['location']['latitude'])
            d['longitude'] = float(data['location']['longitude'])
            d['street_name'] = data['location']['street']['name']
            crimes.append(d)

    current_time = datetime.datetime.now()
    if len(crimes) > 0:
        # puts the crime record into the S3 bucket
        response = s3.put_object(
            Bucket=bucket_name,
            Key=f"{output_key_prefix}/crimes/{current_time.year}/{current_time.month}/{current_time.day}/{current_time.hour}/" + \
                f"crime-{current_time.timestamp()}.json",
            Body=to_json_format(crimes)
        )
        # puts the outcomes records into the S3 bucket
    if len(outcomes) > 0:
        response = s3.put_object(
            Bucket=bucket_name,
            Key=f"{output_key_prefix}/outcomes/{current_time.year}/{current_time.month}/{current_time.day}/{current_time.hour}/" + \
                f"outcomes-{current_time.timestamp()}.json",
            Body=to_json_format(outcomes)
        )

    return response