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
import logging

AWS_ACCESS = os.environ['AWS_ACCESS']  # URL of the site to check, stored in the site environment variable, e.g. https://aws.amazon.com
AWS_SECRET = os.environ['AWS_SECRET']  # String expected to be on the page, stored in the expected environment variable, e.g. Amazon
AWS_REGION_NAME = os.environ['AWS_REGION_NAME']

COMBINED_TABLE = 'perMinuteTable'
COMBINED_TABLE_HOURLY = 'perHourTable'

dynamodb = boto3.resource('dynamodb',
                    aws_access_key_id = AWS_ACCESS,
                    aws_secret_access_key = AWS_SECRET,
                    region_name = AWS_REGION_NAME)

table_combined = dynamodb.Table(COMBINED_TABLE)
table_combined_hr = dynamodb.Table(COMBINED_TABLE_HOURLY)

logger = logging.getLogger()

def lambda_handler(event, context):
    logger.info('Started per-hour lambda handler')
    try:
        currTime = datetime.now()
        oldTime = currTime - timedelta(hours=1)

        currTime1 = currTime.strftime('%Y-%m-%d-%H-%M')
        oldTime = oldTime.strftime('%Y-%m-%d-%H-%M')
        newCurrTime = currTime.strftime('%Y-%m-%d-%H')

        logger.info('Scanning combined table for new items added in last one hour')

        # Read the per minute table.
        response_combined = table_combined.scan(FilterExpression=Attr('key_datetime').lt(currTime1))
        items_combined = response_combined['Items']
        items_total = {}

        # Delete all the items from per minute table while retrieving the score
        # of each keyword.
        for i in items_combined:
            if i['key_word'] not in items_total:
                items_total[i['key_word']] = i['score']
            else:
                items_total[i['key_word']] += i['score']

            logger.info('Deleting item with keyword ' + str(i['key_word']))
            table_combined.delete_item(
                    Key={
                        'key_word': i['key_word'],
                        'key_datetime': i['key_datetime']
                    })

        # Sort the keywords by score.
        sorted_by_value = sorted(items_total, key = lambda x: items_total[x], reverse = True)

        # Extract top-K keywords and put into the perHourTable.
        for k in items_combined:
            i = 0
            while i < len(sorted_by_value) and i < 10:
                if k['key_word'] == sorted_by_value[i]:
                    logger.info('Inserting item with keyword ' + str(i['key_word']) + ' in perHourTable table')
                    print(table_combined_hr.put_item(Item= {
                        'key_word' : k['key_word'],
                        'key_datetime' : newCurrTime,
                        'score' : k['score'],
                        'rank'  : i + 1
                    }))
                i += 1
    except:
        logger.error('At per-hour lambda. Check failed!')
        raise
    else:
        logger.info('At per-hour lambda. Check passed!')
        return "Success"
