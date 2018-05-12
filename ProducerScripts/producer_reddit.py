###########################################################################
# Script Written By: Gautam Verma
#
# For CSC 724 Project at North Carolina State University
###########################################################################

# References:
# 1. Reddit: https://praw.readthedocs.io/en/latest/getting_started/quick_start.html#read-only
# 2. AWS: https://aws.amazon.com/blogs/big-data/snakes-in-the-stream-feeding-and-eating-amazon-kinesis-streams-with-python/
import praw, sys
from AWS_CREDS import *
from AWS_CREDS import Kinesis
from time import gmtime, strftime

# Fetch the data continuously. Quit on KeyboardInterrupt
class Producer_Reddit():
    def run(self):
        try:
            while True:
                try:
                    # Each for loop retrieves different posts according to categories.
                    for data in self.reddit.subreddit('all').new(limit=self.REDDIT_POST_LIMIT):
                        self.logger.log("Sending reddit data to Kinesis at " + strftime("%Y-%m-%d %H:%M:%S", gmtime()))
                        self.objKinesis.putDataToKinesisStream(data, self.objKinesis.TYPE_REDDIT, self.MULTIPLE_POST_SEND_LIMIT, self.KINESIS_PUT_BATCH_SIZE)

                    for data in self.reddit.subreddit('all').controversial(limit=self.REDDIT_POST_LIMIT):
                        self.logger.log("Sending reddit data to Kinesis at " + strftime("%Y-%m-%d %H:%M:%S", gmtime()))
                        self.objKinesis.putDataToKinesisStream(data, self.objKinesis.TYPE_REDDIT, self.MULTIPLE_POST_SEND_LIMIT, self.KINESIS_PUT_BATCH_SIZE)

                    for data in self.reddit.subreddit('all').hot(limit=self.REDDIT_POST_LIMIT):
                        self.logger.log("Sending reddit data to Kinesis at " + strftime("%Y-%m-%d %H:%M:%S", gmtime()))
                        self.objKinesis.putDataToKinesisStream(data, self.objKinesis.TYPE_REDDIT, self.MULTIPLE_POST_SEND_LIMIT, self.KINESIS_PUT_BATCH_SIZE)

                    for data in self.reddit.subreddit('all').rising(limit=self.REDDIT_POST_LIMIT):
                        self.logger.log("Sending reddit data to Kinesis at " + strftime("%Y-%m-%d %H:%M:%S", gmtime()))
                        self.objKinesis.putDataToKinesisStream(data, self.objKinesis.TYPE_REDDIT, self.MULTIPLE_POST_SEND_LIMIT, self.KINESIS_PUT_BATCH_SIZE)

                    for data in self.reddit.subreddit('all').top(limit=self.REDDIT_POST_LIMIT):
                        self.logger.log("Sending reddit data to Kinesis at " + strftime("%Y-%m-%d %H:%M:%S", gmtime()))
                        self.objKinesis.putDataToKinesisStream(data, self.objKinesis.TYPE_REDDIT, self.MULTIPLE_POST_SEND_LIMIT, self.KINESIS_PUT_BATCH_SIZE)
                except KeyboardInterrupt: break
                except Exception as e:
                    self.logger.log("Encountered exception while processing reddit data. Timestamp: " + strftime("%Y-%m-%d %H:%M:%S", gmtime()) + ", exception: " + str(e))
                finally:
                    self.logger.updateCount()
        except: pass

    def __init__(self, logger, objKinesis, MULTIPLE_POST_SEND_LIMIT = 10):
        self.logger = logger

        # Reddit credentials
        CLIENT_ID       =     None
        CLIENT_SECRET   =     None
        USER_AGENT      =     None

        # Reddit Controller
        self.reddit = praw.Reddit(client_id = CLIENT_ID, client_secret = CLIENT_SECRET, user_agent = USER_AGENT)

        self.REDDIT_POST_LIMIT = None
        self.MULTIPLE_POST_SEND_LIMIT = MULTIPLE_POST_SEND_LIMIT

        self.KINESIS_PUT_BATCH_SIZE = MULTIPLE_POST_SEND_LIMIT / 10
        self.KINESIS_PUT_BATCH_SIZE = 1 if self.KINESIS_PUT_BATCH_SIZE == 0 else self.KINESIS_PUT_BATCH_SIZE
        self.KINESIS_PUT_BATCH_SIZE = 500 if self.KINESIS_PUT_BATCH_SIZE > 500 else self.KINESIS_PUT_BATCH_SIZE

        self.objKinesis = objKinesis
