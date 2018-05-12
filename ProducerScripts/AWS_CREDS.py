###########################################################################
# Script Written By: Gautam Verma
#
# For CSC 724 Project at North Carolina State University
###########################################################################

import boto3
import json
from Logger import Logger
from time import gmtime, strftime
import random
class Kinesis():

    def __init__(self, logger, misconfigurationProbability = 0.0):
        self.logger = logger

        # AWS credentials
        AWS_SECRET_ACCESS_KEY   = os.environ['AWS_SECRET_ACCESS_KEY']
        AWS_ACCESS_KEY_ID       = os.environ['AWS_ACCESS_KEY_ID']
        # Used for fault injection as it allows to fail randomly.
        # Should be 0 if no fault injection is required.
        if random.random() < misconfigurationProbability: AWS_SECRET_ACCESS_KEY     = "Error"
        AWS_REGION_NAME         = os.environ['AWS_REGION_NAME']
        self.AWS_STREAM_NAME    = 'ADS_CONSYS'

        self.TOTAL_SHARDS = 0

        self.kinesis = boto3.client('kinesis', region_name=AWS_REGION_NAME, \
            aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

        self.explicit_hash_keys = []

        self.KEY_DATA = 'Data'
        self.KEY_TYPE = 'type'

        self.TYPE_REDDIT     = 'reddit'
        self.TYPE_TWITTER    = 'twitter'
        self.TYPE_MEETUPS    = 'meetup'

        self.stream_description = self.kinesis.describe_stream(StreamName=self.AWS_STREAM_NAME)

        for shard in self.stream_description['StreamDescription']['Shards']:
            self.TOTAL_SHARDS += 1
            self.explicit_hash_keys.append(shard['HashKeyRange']['StartingHashKey'])

    def putDataToKinesisStream(self, data, data_type, MULTIPLE_POST_SEND_LIMIT, KINESIS_PUT_BATCH_SIZE):
        # Puts the data into kinesis *MULTIPLE_POST_SEND_LIMIT* number of times.
        # KINESIS_PUT_BATCH_SIZE tells the max batch size to upload at once.
        self.logger.log("At Kinesis. Received data from " + data_type + " at " + strftime("%Y-%m-%d %H:%M:%S", gmtime()))
        if data_type == self.TYPE_REDDIT:
            # Required for reddit. Otherwise throws JSON fault.
            data = vars(data)
            data.pop('subreddit', None)
            data.pop('author', None)
            data.pop('_reddit', None)

        data[self.KEY_TYPE] = data_type
        # Creates a buffer storing data *MULTIPLE_POST_SEND_LIMIT/KINESIS_PUT_BATCH_SIZE* number of times.
        dataSet = []

        shard = 0
        i = 0

        self.logger.log("Starting to upload batch for " + data_type + " at " + strftime("%Y-%m-%d %H:%M:%S", gmtime()))

        while i <= MULTIPLE_POST_SEND_LIMIT:
            # Upload data as many times as requested by the appropraite sender.
            if i != 0 and i % KINESIS_PUT_BATCH_SIZE == 0:
                self.logger.log("Uploading " + data_type + " data to Kinesis at " + strftime("%Y-%m-%d %H:%M:%S", gmtime()))

                record = self.kinesis.put_records(StreamName = self.AWS_STREAM_NAME, Records = dataSet)

                if str(record['FailedRecordCount']) != "0":
                    self.logger.log("Failed uploading " + data_type + " data to Kinesis at " + strftime("%Y-%m-%d %H:%M:%S", gmtime()) + ", Failed records count: " + str(record['FailedRecordCount']))
                # Clear the buffer.
                dataSet = []
            dataSet.append({self.KEY_DATA: json.dumps(data), 'ExplicitHashKey': self.explicit_hash_keys[shard], 'PartitionKey': str(hash("partition-" + str(shard)))})
            i += 1

            # Makes the put operation Round-Robin.
            shard += 1
            if shard == self.TOTAL_SHARDS: shard = 1

        if dataSet:
            # Upload any remaining data in the buffer.
            self.logger.log("Uploading " + data_type + " data to Kinesis at " + strftime("%Y-%m-%d %H:%M:%S", gmtime()))
            self.kinesis.put_records(StreamName = self.AWS_STREAM_NAME, Records = dataSet)
            if str(record['FailedRecordCount']) != "0":
                    self.logger.log("Failed uploading " + data_type + " data to Kinesis at " + strftime("%Y-%m-%d %H:%M:%S", gmtime()) + ", Failed records count: " + str(record['FailedRecordCount']))

        self.logger.log("Finished with uploading batch for " + data_type + " at " + strftime("%Y-%m-%d %H:%M:%S", gmtime()))
