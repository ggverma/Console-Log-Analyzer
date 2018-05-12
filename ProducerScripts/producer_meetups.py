###########################################################################
# Script Written By: Gautam Verma
#
# For CSC 724 Project at North Carolina State University
###########################################################################

import pycurl, sys
from time import gmtime, strftime
from AWS_CREDS import *
from AWS_CREDS import Kinesis

class Producer_Meetups():
    def run(self):
        def on_receive(data):
            self.logger.log("Pulled meetup data at " + strftime("%Y-%m-%d %H:%M:%S", gmtime()))
            try:
                data = str(data) # python 3 requirement

                # Meetup does not send complete JSON packets. It sends it as a
                # string of arbitray number of characters. But \n is a delimeter.
                lines = (self.leftOverData + data).split("\n")

                for l in lines:
                    self.logger.log("Sending meetup data to Kinesis at " + strftime("%Y-%m-%d %H:%M:%S", gmtime()))
                    try:
                        self.objKinesis.putDataToKinesisStream(json.loads(l), self.objKinesis.TYPE_MEETUPS, self.MULTIPLE_POST_SEND_LIMIT, self.KINESIS_PUT_BATCH_SIZE)
                    except:
                        self.leftOverData += lines[-1]
                    else:
                        self.leftOverData = ''
            except KeyboardInterrupt:
                self.logger.log("Streaming interrupted by someone. Timestamp: " + strftime("%Y-%m-%d %H:%M:%S", gmtime()))
            except Exception as e:
                self.logger.log("Encountered exception while processing meetups data. Timestamp: " + strftime("%Y-%m-%d %H:%M:%S", gmtime()) + ", exception: " + str(e))
            finally:
                self.logger.updateCount()

        self.logger.log("Meetups stream opened at " + strftime("%Y-%m-%d %H:%M:%S", gmtime()))
        conn = pycurl.Curl()
        conn.setopt(pycurl.URL, self.OPEN_EVENT_STREAM)
        conn.setopt(pycurl.WRITEFUNCTION, on_receive)
        conn.perform()

    def __init__(self, logger, objKinesis, MULTIPLE_POST_SEND_LIMIT = 10):
        self.logger = logger

        self.OPEN_EVENT_STREAM="http://stream.meetup.com/2/open_events"

        self.MULTIPLE_POST_SEND_LIMIT = MULTIPLE_POST_SEND_LIMIT
        self.KINESIS_PUT_BATCH_SIZE = MULTIPLE_POST_SEND_LIMIT / 10

        self.KINESIS_PUT_BATCH_SIZE = 1 if self.KINESIS_PUT_BATCH_SIZE == 0 else self.KINESIS_PUT_BATCH_SIZE
        self.KINESIS_PUT_BATCH_SIZE = 500 if self.KINESIS_PUT_BATCH_SIZE > 500 else self.KINESIS_PUT_BATCH_SIZE

        self.leftOverData = ""

        self.objKinesis = objKinesis
