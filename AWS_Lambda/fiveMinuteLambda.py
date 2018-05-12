###########################################################################
# Script Written By: Anshul Chandra
#
# For CSC 724 Project at North Carolina State University
###########################################################################

from __future__ import print_function

import os
from datetime import datetime
import boto3
from boto3.dynamodb.conditions import Key, Attr
from decimal import Decimal
from time import gmtime, strftime
from datetime import datetime, timedelta

AWS_ACCESS = os.environ['AWS_ACCESS']  # URL of the site to check, stored in the site environment variable, e.g. https://aws.amazon.com
AWS_SECRET = os.environ['AWS_SECRET']  # String expected to be on the page, stored in the expected environment variable, e.g. Amazon
AWS_REGION_NAME = os.environ['AWS_REGION_NAME']

COMBINED_TABLE = 'perMinuteTable'
COMBINED_TABLE_5_MINUTES = 'per5MinutesTable'

dynamodb = boto3.resource('dynamodb',
                    aws_access_key_id = AWS_ACCESS,
                    aws_secret_access_key = AWS_SECRET,
                    region_name = AWS_REGION_NAME)

table_combined = dynamodb.Table(COMBINED_TABLE)
table_combined_5min = dynamodb.Table(COMBINED_TABLE_5_MINUTES)

logger = logging.getLogger()

K = 10

def lambda_handler(event, context):
    logger.info('Started per-5 min lambda handler')
    try:
        currTime = datetime.now()
        newCurrTime = currTime.strftime('%Y-%m-%d-%H-%M')
        currTime = currTime.strftime('%Y-%m-%d-%H-%M')

        # Read all the data from per minute table.
        response_combined = table_combined.scan(FilterExpression=Attr('key_datetime').lt(currTime))

        logger.info('Scanning combined table for new items added in last 5 mins')
        # Read all the data from per 5 minute table.
        response_combined_5min = table_combined_5min.scan(FilterExpression=Attr('key_datetime').lt(currTime))

        items_combined = response_combined['Items']
        items_total = {}
        # Merge the new data with existing in per 5 minute table.
        for i in items_combined:
            if i['key_word'] not in items_total:
                items_total[i['key_word']] = i['score']
            else:
                items_total[i['key_word']] += i['score']

        # Extract top-K keywords according to score.
        topKKeywords = sorted(items_total, key=items_total.get, reverse=True)[:K]

        # Delete current entries from 5 minute table.
        items = response_combined_5min['Items']
        for i in items:
            logger.info('Deleting item with keyword ' + str(i['key_word']))
            table_combined_5min.delete_item(
                Key={
                    'key_word': i['key_word']
                })

        # Insert new top-K entries into per 5 minute table.
        i = 1
        for keyword in topKKeywords:
            logger.info('Inserting item with keyword ' + str(i['key_word']) + ' in per5min table')
            table_combined_5min.put_item(Item= {
                    'key_word' : keyword,
                    'key_datetime' : newCurrTime,
                    'score' : items_total[keyword],
                    'rank'  : i
                })
            i += 1

    except:
        logger.error('At per-5 min lambda. Check failed!')
        raise
    else:
        logger.error('At per-5 min lambda. Check passed!')
        return "Success"
