from __future__ import print_function
import json
import datetime
import boto3
import geohash
import os
from decimal import Decimal

def lambda_handler(event, context):
    
    dynamodb = boto3.resource('dynamodb', region_name=os.environ['region'])
    table = dynamodb.Table(os.environ['dynamodb_table'])
    device_configuration = dynamodb.Table(
        os.environ['device_configuration_table']
    )
    
    for record in event.get('Records'):
        update = record.get('body')
        
        # Check if 'update' is a string
        if isinstance(update, str):
            # Change 'data' a dict if a string
            update = json.loads(update)
            
        data = update.get('data')
        topic = update.get('topic')
        
        device_id = ""
        if len(topic) > 1:
            device_id = topic.split('/')[0]
        
        # Get the 'active trip' from the device configuration table
        try:
            trip_information = device_configuration.get_item(
                Key={
                'device_id': device_id
                },
                AttributesToGet=[
                    'trip_id',
                ]
            )
            
            trip_id = trip_response['Item']['trip_id']

        except Exception as e:
            print(e)
            trip_id = 'none'
        
        geohash_result = geohash.encode(data.get('lat'), data.get('lon'))
    
        response = table.put_item(
            Item={
                "device_id":  device_id,
                "epoch_time": data.get('t'),
                "geohash": geohash_result,
                "latitude": Decimal(str(data.get('lat'))),
                "longitude": Decimal(str(data.get('lon'))),
                "speed": Decimal(str(data.get('s'))),
                "course": Decimal(str(data.get('c'))),
                "altitude": Decimal(str(data.get('a'))),
                "trip": trip_id
            }
        )
