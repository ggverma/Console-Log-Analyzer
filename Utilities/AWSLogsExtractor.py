###########################################################################
# Script Written By: Anshul Chandra
#
# For CSC 724 Project at North Carolina State University
###########################################################################

import boto3
import os
import time
from time import gmtime, strftime
from os.path import sep as DIR_SEP
import gzip
import shutil
from datetime import datetime, timedelta
import sys

class AWSLogExtractor():
    '''
        Class for extracting Lambda logs from AWS;
            1. Moves Lambda logs from AWS CLoudwatch to a S3 bucket
            2. Downloads logs to local machine from the S3 bucket
    '''
    def __init__(self, deltaTime = 1):
        if os.environ.get('AWS_SECRET_ACCESS_KEY') == None or os.environ.get('AWS_ACCESS_KEY_ID') == None or os.environ.get('AWS_REGION_NAME') == None:
            print('Environment variables for AWS configuration not found')
            sys.exit()

        # Fetch Access keys from environment vars
        self.AWS_SECRET_ACCESS_KEY   = os.environ['AWS_SECRET_ACCESS_KEY']
        self.AWS_ACCESS_KEY_ID       = os.environ['AWS_ACCESS_KEY_ID']
        self.AWS_REGION_NAME         = os.environ['AWS_REGION_NAME']

        self.deltaTime               = deltaTime      # configure the start date of the log extraction process

        self.BUCKET_NAME = 'adsbucketforlambda'

        self.LOG_GROUPS = ['/aws/lambda/per5MinuteLambda', '/aws/lambda/perHourLambda', '/aws/lambda/perMinuteLambda']

        # Extract the logs
        self.extractLogs()

    '''
        Helper function to create a directory if needed
    '''
    def createDir(self):
        self.path = os.getcwd() + DIR_SEP + 'Downloaded Logs' + DIR_SEP + strftime("%Y_%m_%d", gmtime()) + "_" + strftime("%H_%M_%S", gmtime())

        if not os.path.exists(self.path):
            os.makedirs(self.path)

    '''
        Driver method to carry out the logs extraction task
    '''
    def extractLogs(self):
        # Step 1. Move the log streams from CloudWatch to S3 (bucket - adsbucketforlambda)
        for logGroup in self.LOG_GROUPS:
            result = self.moveDataToS3(logGroup)

            if result == False:
                print('Cannot export logs for log group ', logGroup)

            # Wait for 10secs for export task
            print('Waiting for export task to get finished. . .')
            time.sleep(10)

        print('Export task completed')

        self.createDir()
        # Step 2. Download the logs from S3 bucket
        for logGroup in self.LOG_GROUPS:
            prefix = logGroup[logGroup.rfind('/') + 1:]     # acts as the resorce key
            self.downloadLogsFromS3(prefix)

    '''
        Method to move data from Log Groups in CloudWatch to a S3 bucket
    '''
    def moveDataToS3(self, logGroupName, fromDate = None, toDate = None):
        CloudWatchClient = boto3.client('logs',
            aws_access_key_id = self.AWS_ACCESS_KEY_ID,
            aws_secret_access_key = self.AWS_SECRET_ACCESS_KEY,
            region_name = self.AWS_REGION_NAME)

        fromDate    = datetime.utcnow() - timedelta(self.deltaTime)
        toDate      = datetime.utcnow()

        # Convert fromDate and to Date to number of milliseconds after Jan 1, 1970 00:00:00 UTC
        epoch       = datetime.utcfromtimestamp(0)
        fromTime    = (fromDate - epoch).total_seconds() * 1000.0
        to          = (toDate - epoch).total_seconds() * 1000.0

        # Generate the destination prefix (i.e. the key name for the bucket)
        prefix      = logGroupName[logGroupName.rfind('/') + 1:]

        response = CloudWatchClient.create_export_task(
            taskName            = 'export for ' + logGroupName,
            logGroupName        = logGroupName,
            # logStreamNamePrefix = prefix,
            fromTime            = int(fromTime),
            to                  = int(to),
            destination         = self.BUCKET_NAME,
            destinationPrefix   = prefix
        )

        if response and response['taskId'] != None:
            return True

        return False

    '''
        Method to download logs from S3 bucket to local system
    '''
    def downloadLogsFromS3(self, resourceKey):
        s3resource = boto3.resource('s3',
            aws_access_key_id = self.AWS_ACCESS_KEY_ID,
            aws_secret_access_key = self.AWS_SECRET_ACCESS_KEY)

        KEY = resourceKey

        bucket = s3resource.Bucket(self.BUCKET_NAME)

        for o in bucket.objects.all():
            bucketKey = o.key
            print('Key', bucketKey)

            parts = bucketKey.split('/')

            if parts[-1] == 'aws-logs-write-test':
                continue

            filename = 'log_' + strftime("%H_%M_%S", gmtime()) + '_'

            if len(parts) > 1:
                filename += parts[-2] + parts[-1]
            elif len(parts) > 0:
                filename += parts[-1]

            filePath = self.path + DIR_SEP + str(filename)

            print("Writing to file: ", filePath)

            # write object to a file
            with open(filePath, 'w+') as data:
                s3resource.meta.client.download_file(self.BUCKET_NAME, bucketKey, filePath)

        # Delete the contents of the bucket
        bucket.objects.all().delete()

        # Unzip all the files
        self.unzipFiles()

    '''
        Helper method to unzip the '.gz' files downloaded from AWS S3 bucket
    '''
    def unzipFiles(self):
        for subdir, dirs, files in os.walk(self.path):
            for f in files:
                fileName = f.split('.')
                filePathZipped = self.path + DIR_SEP + f
                filePathUnzipped = self.path + DIR_SEP + fileName[0]
                if fileName[-1] == 'gz':
                    with gzip.open(filePathZipped, 'rb') as f_zipped:
                        with open(filePathUnzipped, 'wb') as f_unzipped:
                            shutil.copyfileobj(f_zipped, f_unzipped)

                    # remove the zipped version
                    os.remove(filePathZipped)


if __name__ == '__main__':
    time_delta = 1

    if len(sys.argv) > 1:
        for i in range(1, len(sys.argv)):
            if sys.argv[i] == '-h':
                # Display help info.
                print("\n\t-t: Time delta (no. of days) for which logs are to be extracted; t = 1 means system will fetch logs of previous 1 day from current time")
                sys.exit()

        for i in range(1, len(sys.argv), 2):
            if sys.argv[i] == '-t':
                time_delta = int(sys.argv[i + 1])
            else:
                print('Please use -h for help')
                sys.exit()

        objExtractor = AWSLogExtractor(time_delta)
    else:
        print('Please use -h for help')
        sys.exit()
