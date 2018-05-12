###########################################################################
# Script Written By: Gautam Verma
#
# For CSC 724 Project at North Carolina State University
###########################################################################

import boto3, base64, datetime
import json, time
from textblob import TextBlob

from threading import Lock

from random import randint

from Logger import Logger as LGR

from multiprocessing import Process

from AWS_CREDS import *

def run(shard_id, ERROR_LOG_PROBABILITY):
    Logger = LGR(Lock(), '../LogFiles/Consumer/Logs-', 'log_file_shard_' + str(shard_id) + "_")
    while True:
        try:
            consumer(shard_id, ERROR_LOG_PROBABILITY, Logger)
        except Exception as e:
            # print (e)
            # Run consumer again if any runtime exeption happens.
            Logger.log("Exception occured. " + str(e))
            consumer(shard_id, ERROR_LOG_PROBABILITY, Logger)

def consumer(shard_id, ERROR_LOG_PROBABILITY, Logger):
    LOG_FILE_PREFIX = "log_"

    Logger.log("Creating kinesis client in the consumer with shard_id " + str(shard_id) + ".")

    kinesis = boto3.client('kinesis',
                            region_name = AWS_REGION_NAME,
                            aws_access_key_id = AWS_ACCESS_KEY_ID,
                            aws_secret_access_key = AWS_SECRET_ACCESS_KEY)

    Logger.log("Kinesis client successfully created in the consumer with shard_id " + str(shard_id) + ".")

    Logger.log("Getting the shard iterator from Amazon for shard_id " + str(shard_id) + ".")
    # Shard iterator
    shard_it = kinesis.get_shard_iterator(StreamName=AWS_STREAM_NAME, ShardId=shard_id, \
        ShardIteratorType="LATEST")["ShardIterator"]

    # ----------- DATA KEYS --------------------------
    KEY_TITLE           = 'title'
    KEY_NUM_COMMENTS    = 'nc'
    KEY_SCORE           = 'score'
    KEY_TYPE            = 'type'
    KEY_DATA            = 'Data'

    # ----------- STREAM TYPES ------------------------
    REDDIT  = 'reddit'
    MEETUP  = 'meetup'
    TWITTER = 'twitter'

    # ----------- TABLES IN DYNAMODB ------------------
    # These lamdas only put data into the 'live' table.
    TWITTER_LIVE_TABLE_NAME = 'live_twitter_consys'
    REDDIT_LIVE_TABLE_NAME  = 'live_reddit_consys'
    MEETUPS_TABLE_NAME = 'live_meetups_consys'

    # -------------- HEADER NAMES IN TABLES ------------

    # ---------- TWITTER TABLE -------------
    TWITTER_TABLE_REPLIES_HEADER     = 'replies'
    TWITTER_TABLE_FAVORITES_HEADER   = 'favorites'
    TWITTER_TABLE_RETWEETS_HEADER    = 'retweets'

    # ---------- REDDIT TABLE --------------
    REDDIT_TABLE_SCORE_HEADER           = 'score'
    REDDIT_TABLE_NUM_COMMENTS_HEADER    = 'num_comments'

    # ---------- MEETUPS TABLE -------------
    MEETUPS_LOCATION_CITY_KEY_HEADER        = 'key_location_city'
    MEETUPS_LOCATION_STATE_KEY_HEADER       = 'key_location_state'
    MEETUPS_LOCATION_COUNTRY_KEY_HEADER     = 'key_location_country'
    MEETUPS_TABLE_CATEGORY_HEADER   = 'key_category'
    MEETUPS_TABLE_VENUE_HEADER      = 'venue'
    MEETUPS_TABLE_NAME_HEADER       = 'name'

    # ---------- COMMON HEADERS ------------
    TIME_HEADER = 'key_datetime'
    KEYWORD_HEADER = 'key_word'

    Logger.log("Creating DynamoDB client in the consumer with shard_id " + str(shard_id) + ".")
    dynamodb = boto3.resource('dynamodb',
                        aws_access_key_id = AWS_ACCESS_KEY_ID,
                        aws_secret_access_key = AWS_SECRET_ACCESS_KEY,
                        region_name = DYNAMO_DB_REGION_NAME)
    Logger.log("DynamoDB client successfully created in the consumer with shard_id " + str(shard_id) + ".")

    def extract_keywords(sentence):
        """
            Extracts Noun Phrases. Has an external dependency on textblob.
        """
        keywords = []
        blob = TextBlob(sentence)
        for np in blob.noun_phrases:
            keywords.append(np)
        return keywords

    def getTimeKey():
        return datetime.datetime.now().strftime("%Y-%m-%d-%H-%M")

    def updateFieldInTable(table, fieldName, fieldValue, timeKey, keywords, tableName):
        # print keywords
        Logger.log("Updating " + fieldName + " with value " + str(fieldValue) + \
        " in table " + tableName + " at " + str(timeKey))
        for keyword in keywords:
            table.update_item(
                Key = {
                    TIME_HEADER: timeKey,
                    KEYWORD_HEADER: keyword
                },
                UpdateExpression="set " + fieldName + " = if_not_exists(" + fieldName + ", :init) + :val",
                ExpressionAttributeValues={
                    ':val': fieldValue,
                    ':init': 0
                }
            )
        Logger.log("Successfully updated " + tableName + " table at time " + str(getTimeKey()))

    try:
        while True:
            try:
                out = kinesis.get_records(ShardIterator=shard_it)
                for o in out["Records"]:
                    data = json.loads(o["Data"])
                    # Work with data here.

                    timeKey = getTimeKey()

                    if data[KEY_TYPE] == REDDIT:
                        Logger.log("Read Reddit data from the stream.")
                        # Find keywords from the title.
                        title = data['title']
                        Logger.log("Extracting keywords from Reddit data.")
                        keywords = extract_keywords(title)

                        # Extract score and number of comments.
                        score = data['score']
                        num_comments = data['num_comments']

                        # Put these into Dynamo
                        table =  dynamodb.Table(REDDIT_LIVE_TABLE_NAME)

                        updateFieldInTable(table, REDDIT_TABLE_NUM_COMMENTS_HEADER, num_comments, timeKey, keywords, data[KEY_TYPE])
                        updateFieldInTable(table, REDDIT_TABLE_SCORE_HEADER, score, timeKey, keywords, data[KEY_TYPE])

                        if randint(1, 100) <= ERROR_LOG_PROBABILITY:
                            Logger.log("Random error in Reddit occured!")
                    elif data[KEY_TYPE] == MEETUP:
                        Logger.log("Read Meetup data from the stream.")
                        # Find keywords from the name.
                        event_name = data['name']
                        Logger.log("Extracting keywords from Meetup data.")
                        keywords   = extract_keywords(event_name)
                        if 'description' in data:
                            Logger.log("Extracting keywords from Meetup description data.")
                            keywords += extract_keywords(data['description'])

                        # Extract venue
                        # NOTE: May not be present
                        venue = "None"
                        state = "None"
                        if 'venue' in data:
                            Logger.log("Exact venue location present in Meetup.")
                            city        = data['venue']['city']
                            country     = data['venue']['country']
                            if 'state' in data['venue']:
                                state   = data['venue']['state']
                            venue          = data['venue']['name']
                            if 'address_1' in data['venue']:
                                venue += data['venue']['address_1']
                            if 'address_2' in data['venue']:
                                venue += data['venue']['address_2']
                            if 'address_3' in data['venue']:
                                venue += data['venue']['address_3']
                        else:
                            Logger.log("Exact venue location absent in Meetup.")
                            city        = data['group']['city']
                            country     = data['group']['country']
                            if 'state' in data['group']:
                                state   = data['group']['state']

                        if 'group' in data and 'category' in data['group']['category'] and 'shortname' in data['group']['category']['shortname']:
                        # Extract category
                            Logger.log("Event category present in Meetup.")
                            event_category = data['group']['category']['shortname']
                        else:
                            Logger.log("Event category absent in Meetup.")
                            event_category = 'None'

                        # Put these into Dynamo
                        table =  dynamodb.Table(MEETUPS_TABLE_NAME)
                        Logger.log("Updating Meetup table at time " + timeKey)
                        for keyword in keywords:
                            table.put_item(
                                Item={
                                        TIME_HEADER:     timeKey,                           # Part of key. To filter on time.
                                        KEYWORD_HEADER:  keyword,                           # Part of key.
                                        MEETUPS_LOCATION_CITY_KEY_HEADER: city,             # Part of key. To filter on geography.
                                        MEETUPS_LOCATION_STATE_KEY_HEADER: state,           # Part of key. To filter on geography.
                                        MEETUPS_LOCATION_COUNTRY_KEY_HEADER: country,       # Part of key. To filter on geography.
                                        MEETUPS_TABLE_CATEGORY_HEADER: event_category,      # Part of key. To allow searching for popular meetups according to category.
                                        MEETUPS_TABLE_NAME_HEADER:     event_name,          # Could be part of key. Will allow searching for particular events and find where they are popular.
                                        MEETUPS_TABLE_VENUE_HEADER:    venue                # Could be part of key. Will allow searching for popular events at a venue.
                                    }
                                )
                        Logger.log("Successfully updated Meetup table at time " + str(getTimeKey()))
                        if randint(1, 100) <= ERROR_LOG_PROBABILITY:
                            Logger.log("Random error in Meetup occured!")
                    elif data[KEY_TYPE] == TWITTER:
                        Logger.log("Read Twitter data from the stream.")
                        # Find keywords from the title.
                        if 'text' in data:
                            title       = data['text']
                            # print(data)
                            Logger.log("Extracting keywords from Twitter data.")
                            keywords    = extract_keywords(title)

                            # Extract number of retweets, favorites, and replies.
                            retweets    = data['retweet_count']
                            favorites   = data['favorite_count']
                            replies     = data['reply_count']

                            # Put these into Dynamo.
                            table       = dynamodb.Table(TWITTER_LIVE_TABLE_NAME)

                            updateFieldInTable(table, TWITTER_TABLE_RETWEETS_HEADER, retweets, timeKey, keywords, data[KEY_TYPE])
                            updateFieldInTable(table, TWITTER_TABLE_FAVORITES_HEADER, favorites, timeKey, keywords, data[KEY_TYPE])
                            updateFieldInTable(table, TWITTER_TABLE_REPLIES_HEADER, replies, timeKey, keywords, data[KEY_TYPE])

                        if randint(1, 100) <= ERROR_LOG_PROBABILITY:
                            Logger.log("Random error in Twitter occured!")
                    else:
                        Logger.log("Read Invalid data from the stream.")

                shard_it = out["NextShardIterator"]
            except KeyError as e:
                Logger.log("Exception occured. " + str(e))
            # Try without sleep
            time.sleep(1)

    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    # Spawn process for each shard.

    kinesis = boto3.client('kinesis',
                            region_name = AWS_REGION_NAME,
                            aws_access_key_id = AWS_ACCESS_KEY_ID,
                            aws_secret_access_key = AWS_SECRET_ACCESS_KEY)

    consumer_processes = []

    shardIds = []

    kinesis_description = kinesis.describe_stream(StreamName = AWS_STREAM_NAME)

    for shard in kinesis_description['StreamDescription']['Shards']:
        shardIds.append(shard['ShardId'])

    for shardId in shardIds:
        consumer_processes.append(Process(target=run, args=(shardId, 1, )))

    for consumer_process in consumer_processes:
        consumer_process.start()

    for consumer_process in consumer_processes:
        consumer_process.join()
