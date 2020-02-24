import json
import base64
import time
import datetime
import boto3


def lambda_handler(event, context):
    s3 = boto3.client('s3')
    bucket_name = 'naqeeb-udacity-capstone-project-police'

    crimes = []
    outcomes = []
    for record in event['Records']:
        payload = base64.b64decode(record["kinesis"]["data"])
        data = json.loads(payload)
        if data['streamed_data_type'] == 'outcomes':
            for outcome in data['outcomes']:
                d = {}
                d['date'] = outcome['date']
                d['person_id'] = outcome['person_id']
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
            d['latitude'] = data['location']['latitude']
            d['longitude'] = data['location']['longitude']
            d['street_name'] = data['location']['street']['name']
            crimes.append(d)

    current_time = datetime.datetime.now()
    response = s3.put_object(
        Bucket=bucket_name,
        Key=f"crimes/{current_time.year}/{current_time.month}/{current_time.day}/{current_time.hour}/" + \
            f"crime-{current_time.timestamp()}.json",
        Body=json.dumps(crimes)
    )

    response = s3.put_object(
        Bucket=bucket_name,
        Key=f"outcomes/{current_time.year}/{current_time.month}/{current_time.day}/{current_time.hour}/" + \
            f"outcomes-{current_time.timestamp()}.json",
        Body=json.dumps(outcomes)
    )

    return response