###########################################################################
# Script Written By: Anshul Chandra
#
# For CSC 724 Project at North Carolina State University
###########################################################################

import os
from time import gmtime, strftime
from random import randint

class Logger():
    def __init__(self, lock, path = '', file_prefix = "log_file_"):
        self.log_to_path = path
        self.log_limit = 100
        self.lock = lock

        self.log_count = 0
        self.file_prefix = file_prefix
        self.create_log_file(file_prefix)

    def create_log_file(self, file_prefix):
        self.path = self.log_to_path + strftime("%Y_%m_%d", gmtime())
        if not os.path.exists(self.path):
            os.makedirs(self.path)
        self.path += os.path.sep + file_prefix + strftime("%H_%M_%S", gmtime())

    def log(self, message):
        self.lock.acquire()
        with open(self.path, "a+") as log_file:
            log_file.write(message + "\r\n")
        self.lock.release()

    def logDynamicMessage(self, message):
        tokens = message.split("*")
        message = [tokens[0]]
        for i in range(1, len(tokens)):
            message += str(randint(1, 100)),
            message += tokens[i],
        self.log(''.join(message))

    def updateCount(self):
        self.lock.acquire()
        self.log_count += 1
        if self.log_count == self.log_limit:
            self.log_count = 0
            self.create_log_file(self.file_prefix)
        self.lock.release()
