###########################################################################
# Script Written By: Gautam Verma
#
# For CSC 724 Project at North Carolina State University
###########################################################################

from TwitterAPI import TwitterAPI
from AWS_CREDS import *
import sys
from AWS_CREDS import Kinesis
from time import gmtime, strftime
import random

class Producer_Twitter():
    def run(self):
        def worker():
            try:
                api = TwitterAPI(self.consumer_key, self.consumer_secret, self.access_token_key, self.access_token_secret)

                self.logger.log("Pulled twitter data at " + strftime("%Y-%m-%d %H:%M:%S", gmtime()))

                r = api.request('statuses/filter', {'locations':'-90,-90,90,90'})
                while True:
                    for item in r.get_iterator():
                        if 'text' in item:
                            # May have runtime Error or a normal execution.
                            self.logger.log("Sending twitter data to Kinesis at " + strftime("%Y-%m-%d %H:%M:%S", gmtime()))
                            self.objKinesis.putDataToKinesisStream(item, self.objKinesis.TYPE_TWITTER, self.MULTIPLE_POST_SEND_LIMIT, self.KINESIS_PUT_BATCH_SIZE)
                        elif 'limit' in item:
                            # Runtime Warning.
                            self.logger.log('WARNING: %d tweets missed' % item['limit'].get('track'))
                        elif 'disconnect' in item:
                            # Runtime Error
                            self.logger.log('Twitter Connection failed because %s' % item['disconnect'].get('reason'))
                        elif 'message' in item:
                            # Runtime Error
                            self.logger.log('Twitter Message: %s, Error Code: %d' % (item['message'], item['code']))

            except Exception as e:
                self.logger.log("Encountered exception while processing twitter data. Timestamp: " + strftime("%Y-%m-%d %H:%M:%S", gmtime()) + ", exception: " + str(e))
                worker()
            finally:
                self.logger.updateCount()
        worker()

    def __init__(self, logger, objKinesis, misconfigurationProbability = 0.0, MULTIPLE_POST_SEND_LIMIT = 200):
        self.logger = logger

        # Configuration Errors Possible here. Used of fault injection.
        # misconfigurationProbability should be 0 if fault injection is not required.
        self.consumer_key        = None # Put in the consumer key of Twitter
        self.consumer_secret     = None # Put in the consumer secret key of Twitter
        if random.random() < misconfigurationProbability: self.consumer_secret     = "Error"
        self.access_token_key    = None # Put in the consumer access token key of Twitter
        self.access_token_secret = None # Put in the consumer access token secret key of Twitter


        self.MULTIPLE_POST_SEND_LIMIT = MULTIPLE_POST_SEND_LIMIT
        self.KINESIS_PUT_BATCH_SIZE = MULTIPLE_POST_SEND_LIMIT / 10

        self.KINESIS_PUT_BATCH_SIZE = 1 if self.KINESIS_PUT_BATCH_SIZE == 0 else self.KINESIS_PUT_BATCH_SIZE
        self.KINESIS_PUT_BATCH_SIZE = 500 if self.KINESIS_PUT_BATCH_SIZE > 500 else self.KINESIS_PUT_BATCH_SIZE

        self.count = 0
        self.objKinesis = objKinesis
