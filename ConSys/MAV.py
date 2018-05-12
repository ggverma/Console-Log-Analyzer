###########################################################################
# Script Written By: Gautam Verma
#
# For CSC 724 Project at North Carolina State University
###########################################################################

import os, sys
import re
import numpy as np
from random import randint, sample

class MAV():

    def __init__(self, templateFile):
        self.pathToVectorID = {}
        self.vectorIdToPath = {}
        self.mavMap = {}
        self.template = None
        self.vectors = []
        self.vectorIDsInTopCluster = None
        self.TEMPLATE_FILE = templateFile

    def extractLogTemplates(self, rootdir):
        print ('-> Extracting log templates')
        self.generateTemplate(self.TEMPLATE_FILE)
        print ('-> Extracted log templates.')

    def setLogTemplates(self, templates):
        self.template = templates

    def generateMAV(self, logPrefix, directoryToScan):
        rootdir = directoryToScan

        if self.template == None:
            self.extractLogTemplates(rootdir)

        i = 0
        print ('-> Creating MAV vectors.')
        for subdir, dirs, files in os.walk(rootdir):
            for f in files:
                if f.startswith(logPrefix):
                    # Valid file.
                    self.vectors.append(self.readFileAndGenerateVector(subdir + os.path.sep + f, i, f))
                    self.pathToVectorID[subdir + os.path.sep + f] = i
                    self.vectorIdToPath[i] = subdir + os.path.sep + f
                    i += 1

        if len(self.vectors) == 0:
            print("No log files!")

        print ('-> Created MAV vectors.')

    def readFileAndGenerateVector(self, path, vectorID, fileName):
        vector = [0]* len(self.template)

        print ('-> Processing file: %s' %fileName)

        # Count total lines in file.
        with open(path, "r", encoding = 'utf8') as f:
            for totalLines, line in enumerate(f): pass

        with open(path, "r", encoding = 'utf8') as log_file:
            lineNumber = 0
            for line in log_file.readlines():
                line = line.strip()
                if not line: continue
                f = False
                for position, template in enumerate(self.template):
                    # Try to match with all the templates.
                    if vector[position] == 0 and re.match(template, line):
                        # Match!
                        f = True
                        vector[position] = 1
                    if vector[position] == 1: f = True
                sys.stdout.write('\r\tCovered %f%% of the file.' %(100.0 * lineNumber / totalLines))
                if not f:
                    print("ERROR: Matched no template for line: ", line)

                lineNumber += 1
        print("")
        self.mavMap[vectorID] = vector
        return vector

    def generateVectorsFromDifflogs(self, difflogs):
        for vectorID, difflog in enumerate(difflogs):
            vector = [0]* len(self.template)
            for messageSequence in difflog:
                for vertex in messageSequence:
                    vector[vertex] = 1
            self.mavMap[vectorID] = vector
            self.vectors.append(vector)

    def generateTemplate(self, path):
        '''
            Generates tempates by reading the files on path 'path'.
            Eg. Input: abc*dd\*f Output: abc.+dd\*f
        '''
        template = []
        with open(path, "r", encoding = "utf8") as f:
            for line in f.readlines():
                line = line.strip()
                if len(line) != 0:
                    regex = []
                    for i, c in enumerate(line):
                        if c == '*' and (i == 0 or (i > 0 and line[i-1] != '\\')):
                            regex.append('.+')
                        else: regex.append(c)
                    regex = ''.join(regex)
                    regex = regex.replace('\n', '').replace('\r', '')
                    template.append(regex)
        self.template = template

    def getTemplates(self):
        return self.template

    def getPathToVectorID(self):
        return self.pathToVectorID

    def getVectorIDToPath(self):
        return self.vectorIdToPath

    def getMAVMap(self):
        return self.mavMap

    def getMAVVectors(self):
        return self.vectors

    def getTopCluster(self):
        if not self.vectorIDsInTopCluster: self.vectorIDsInTopCluster = range(len(self.vectors))
        return self.vectorIDsInTopCluster

    def getTopRandomMedoid(self):
        return sample(self.getTopCluster(), 1)[0]

if __name__ == '__main__':
    objMav = MAV('Producer')

    objMav.generateMAV()
