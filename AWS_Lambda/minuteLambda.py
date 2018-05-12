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

TWITTER_LIVE_TABLE_NAME = 'live_twitter_consys'
REDDIT_LIVE_TABLE_NAME  = 'live_reddit_consys'
COMBINED_TABLE = 'perMinuteTable'

dynamodb = boto3.resource('dynamodb',
                    aws_access_key_id = AWS_ACCESS,
                    aws_secret_access_key = AWS_SECRET,
                    region_name = AWS_REGION_NAME)

table_reddit =  dynamodb.Table(REDDIT_LIVE_TABLE_NAME)
table_twitter = dynamodb.Table(TWITTER_LIVE_TABLE_NAME)
table_combined = dynamodb.Table(COMBINED_TABLE)

logger = logging.getLogger()

def lambda_handler(event, context):
    logger.info('Started per-min lambda handler')
    try:
        currTime = datetime.now()
        currTime = str(currTime.strftime('%Y-%m-%d-%H-%M'))

        logger.info('Scanning reddit table for new items added in last one min')

        response_reddit = table_reddit.scan(FilterExpression=Attr('key_datetime').lt(currTime))

        items_reddit = response_reddit['Items']

        # Work on reddit data.
        for i in items_reddit:
            if 'score' in i and 'num_comments' in i:
                logger.info('Calculating score for reddit for keyword - ' + str(i['key_word']))
                score = int(0.2 * float(i['num_comments']) + 0.8 * float(i['score']))
                logger.info('Moving data for keyword - ' + str(i['key_word']) + ' from reddit table to combined table')

                # put the extracted information into per minute table and log the
                # return information.
                print(table_combined.put_item(Item= {
                    'key_word' : i['key_word'],
                    'key_datetime' : i['key_datetime'],
                    'score' : Decimal(score)}))
                table_reddit.delete_item(
                    Key={
                        'key_word': i['key_word'],
                        'key_datetime': i['key_datetime']
                    })
            else:
                logger.info('Score not found. Setting the score as zero.')
                logger.info('Moving data for keyword - ' + str(i['key_word']) + ' from reddit table to combined table')

                # put the extracted information into per minute table and log the
                # return information.
                print(table_combined.put_item(Item= {
                    'key_word' : i['key_word'],
                    'key_datetime' : i['key_datetime'],
                    'score' : Decimal(0)}))
                table_reddit.delete_item(
                    Key={
                        'key_word': i['key_word'],
                        'key_datetime': i['key_datetime']
                    })

        logger.info('Scanning reddit table for new items added in last one min')

        response_twitter = table_twitter.scan(FilterExpression=Attr('key_datetime').lt(currTime))
        items_twitter = response_twitter['Items']

        # Work on twitter data.
        for i in items_twitter:
            if 'replies' in i and 'retweets' in i and 'favorites' in i:
                logger.info('Calculating score for twitter for keyword - ' + str(i['key_word']))
                score = int(0.2 * float(i['replies']) + 0.5 * float(i['retweets']) + 0.3 *float(i['favorites']) )
                logger.info('Moving data for keyword - ' + str(i['key_word']) + ' from twitter table to combined table')
                print(table_combined.put_item(Item= {
                    'key_word' : i['key_word'],
                    'key_datetime' : i['key_datetime'],
                    'score' : Decimal(score)}))
                table_twitter.delete_item(
                    Key={
                        'key_word': i['key_word'],
                        'key_datetime': i['key_datetime']
                    })
            else:
                logger.info('Score not found. Setting the score as zero.')
                print(table_combined.put_item(Item= {
                    'key_word' : i['key_word'],
                    'key_datetime' : i['key_datetime'],
                    'score' : Decimal(0)}))
                table_twitter.delete_item(
                    Key={
                        'key_word': i['key_word'],
                        'key_datetime': i['key_datetime']
                    })
    except:
        logger.error('At per-min lambda. Check failed!')
        raise
    else:
        logger.error('At per-min lambda. Check passed!')
        return "Success"
