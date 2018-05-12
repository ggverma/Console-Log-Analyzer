###########################################################################
# Script Written By: Anshul Chandra
#
# For CSC 724 Project at North Carolina State University
###########################################################################

import boto3
import os
import sys

'''
    Class for handling the creation and deletion of dynamo db tables realted to ConSys
'''
class DynamoDBHandler():

    def __init__(self, creationFlag = True):
        if os.environ.get('AWS_SECRET_ACCESS_KEY') == None or os.environ.get('AWS_ACCESS_KEY_ID') == None or os.environ.get('AWS_REGION_NAME') == None:
            print('Environment variables for AWS configuration not found')
            sys.exit()

        # Fetch Access keys from environment vars
        self.AWS_SECRET_ACCESS_KEY   = os.environ['AWS_SECRET_ACCESS_KEY']
        self.AWS_ACCESS_KEY_ID       = os.environ['AWS_ACCESS_KEY_ID']
        self.AWS_REGION_NAME         = os.environ['AWS_REGION_NAME']

        self.creationFlag = creationFlag

        # tablename => [ReadCapacity, WriteCapacity]
        self.TABLES = {
            'live_twitter_consys': [50, 100],
            'live_reddit_consys': [40, 50],
            'live_meetups_consys': [5, 30],
            'perMinuteTable': [10, 20],
            'per5MinutesTable': [5, 5],
            'perHourTable': [5, 5]
        }

        self.partitionKey = 'key_datetime'
        self.sortKey = 'key_word'

        self.dynamoClient = boto3.client('dynamodb',
            aws_access_key_id       = self.AWS_ACCESS_KEY_ID,
            aws_secret_access_key   = self.AWS_SECRET_ACCESS_KEY,
            region_name             = self.AWS_REGION_NAME)

        if self.creationFlag:
            # create tables
            self.createTables()
        else:
            # delete tables
            self.deleteTables()

    '''
        Method to create DynamoDB tables as configured in self.TABLES
    '''
    def createTables(self):

        creationFlag = True

        for table in self.TABLES:
            response = self.dynamoClient.create_table(
                    AttributeDefinitions=[
                        {
                            'AttributeName': self.partitionKey,
                            'AttributeType': 'S'
                        },
                        {
                            'AttributeName': self.sortKey,
                            'AttributeType': 'S'
                        },
                    ],
                    TableName = table,
                    KeySchema=[
                        {
                            'AttributeName': self.partitionKey,
                            'KeyType': 'HASH'       # partition key
                        },
                        {
                            'AttributeName': self.sortKey,
                            'KeyType': 'RANGE'      # sort key
                        },
                    ],
                    ProvisionedThroughput={
                        'ReadCapacityUnits': self.TABLES[table][0],
                        'WriteCapacityUnits': self.TABLES[table][1]
                    }
                )

            if not response: #or response['TableStatus'] != 'CREATING':
                creationFlag = False
                print('Unable to create table', table)

        if creationFlag:
            print('Tables created successfully')

    '''
        Method to delete DynamoDB table(s)
            tableName: name of the table to be deleted (if not provided, all the tables configured in self.TABLES are deleted)
    '''
    def deleteTables(self, tableName = None):
        deletionFlag = True

        if tableName:
            response = self.dynamoClient.delete_table(
                TableName = tableName
            )

            if response: #and response['TableStatus'] != 'DELETING':
                print('Deletion initiated for table', tableName)
            else:
                print('Unable to delete table at the moment')
        else:
            for table in self.TABLES:
                response = self.dynamoClient.delete_table(
                    TableName = table
                )

                if not response: #or response['TableStatus'] != 'DELETING':
                    deletionFlag = False
                    print('Unable to delete table', table)

        if deletionFlag:
            print('Deletion initiated for tables')


if __name__ == '__main__':

    if len(sys.argv) > 1:
        for i in range(1, len(sys.argv)):
            if sys.argv[i] == '-h':
                # Display help info.
                print("\n\t-c: Create all tables")
                print("\t-d: Delete all tables\n")
                sys.exit()

        for i in range(1, len(sys.argv)):
            if sys.argv[i] == '-c':
                # create new tables
                objDynamo = DynamoDBHandler(True)
            if sys.argv[i] == '-d':
                # delete tables
                objDynamo = DynamoDBHandler(False)
            else:
                print('Please use -h for help')
                sys.exit()
    else:
        print('Please use -h for help')
        sys.exit()
