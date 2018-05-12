###########################################################################
# Script Written By: Anshul Chandra
#
# For CSC 724 Project at North Carolina State University
###########################################################################

import boto3
import os
import sys

'''
    Class for handling the Kinesis data stream creation/deletion for ConSys
'''
class KinesisHandler():

    def __init__(self):
        if os.environ.get('AWS_SECRET_ACCESS_KEY') == None or os.environ.get('AWS_ACCESS_KEY_ID') == None or os.environ.get('AWS_REGION_NAME') == None:
            print('Environment variables for AWS configuration not found')
            sys.exit()

        # Fetch Access keys from environment vars
        self.AWS_SECRET_ACCESS_KEY   = os.environ['AWS_SECRET_ACCESS_KEY']
        self.AWS_ACCESS_KEY_ID       = os.environ['AWS_ACCESS_KEY_ID']
        self.AWS_REGION_NAME         = os.environ['AWS_REGION_NAME']

        self.kinesisClient = boto3.client('kinesis',
            aws_access_key_id       = self.AWS_ACCESS_KEY_ID,
            aws_secret_access_key   = self.AWS_SECRET_ACCESS_KEY,
            region_name             = self.AWS_REGION_NAME)

    '''
        Method to create data stream
            streamName: name of the data stream to be created
            shardCount: number of shards in the new stream to be created
    '''
    def createKinesisDataStream(self, streamName, shardCount):
        response = self.kinesisClient.create_stream(
            StreamName = streamName,
            ShardCount = shardCount
        )

        if response:
            print('Kinesis data stream ' + streamName + ' created successfully\n')
            print('Response:', response)
        else:
            print('Unable to create data stream')


    '''
        Method to delete data stream
            streamName: name of the stream to be deleted
    '''
    def deleteKinesisDataStream(self, streamName):
        response = self.kinesisClient.delete_stream(
            StreamName = streamName
        )

        if response:
            print('Kinesis data stream ' + streamName + ' deleted successfully\n')
            print('Response:', response)
        else:
            print('Unable to delete data stream')

if __name__ == '__main__':

    objKinesis = KinesisHandler()

    streamName = 'ADS_CONSYS'

    if len(sys.argv) > 1:
        for i in range(1, len(sys.argv)):
            if sys.argv[i] == '-h':
                # Display help info.
                print("\n\t-c: Create Kinesis data stream")
                print("\t-d: Delete Kinesis data stream\n")
                sys.exit()

        for i in range(1, len(sys.argv)):
            if sys.argv[i] == '-c':
                # Create data stream
                objKinesis.createKinesisDataStream(streamName, 4)
            if sys.argv[i] == '-d':
                # Delete data stream
                objKinesis.deleteKinesisDataStream(streamName)
            else:
                print('Please use -h for help')
                sys.exit()
    else:
        print('Please use -h for help')
        sys.exit()
