from __future__ import print_function
import json
import datetime
import boto3
import geohash
import os
from decimal import Decimal

# This python script receives the SNS message, processes the message data and then puts it into DynamoDB.


def convert_lat_long(value, dir):
    # convert_lat_long takes the NMEA formatted location data and converts it into decimal latitude and longitude.

    deg, dec = value.split('.')
    precision = len(dec)
    mins = deg[-2:] + '.' + dec
    comp = float(mins) / 60 + float(deg[:-2])
    comp = comp if (dir is 'E' or dir is 'N') else (comp / -1)
    return round(Decimal(str(comp)), precision)


def get_epoch_time(timeobject, dateobject):
    # get_epoch_time converts time data from SNS message into epoch time

    return int(datetime.datetime.strptime(
        str(dateobject["day"]) + "-" + str(dateobject["month"]) + "-" + str(dateobject["year"]) + " " + str(
            timeobject["hour"]) + ":" + str(timeobject["minute"]) + ":" + str(timeobject["second"]),
        '%d-%m-%Y %H:%M:%S').timestamp())


def lambda_handler(event, context):
    # lambda_handler is called by the SNS message - it constructs the message and puts the data into DynamoDB.

    message = json.loads(event['Records'][0]['Sns']['Message'])
    data = message["reported"]
    time = get_epoch_time(data["fix"]["timestamp"], data["transit_data"]["datestamp"])
    long = convert_lat_long(data["fix"]["lat"], data["fix"]["lat_dir"])
    lat = convert_lat_long(data["fix"]["lon"], data["fix"]["lon_dir"])
    geohash_result = geohash.encode(lat, long)
    dynamodb = boto3.resource('dynamodb', region_name=os.environ['region'])
    table = dynamodb.Table(os.environ['dynamodb_table'])
    response = table.put_item(
        Item={
            "device_id": message["device_id"],
            "epoch_time": time,
            "geohash": geohash_result,
            "latitude": lat,
            "longitue": long,
            "speed": Decimal(str(data["transit_data"]["spd_over_grnd"])),
            "course": Decimal(str(data["transit_data"]["true_course"])),
            "altitude": Decimal(str(data["fix"]["altitude"]))
        }
    )
