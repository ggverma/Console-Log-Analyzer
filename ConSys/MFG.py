###########################################################################
# Script Written By: Anshul Chandra
#
# For CSC 724 Project at North Carolina State University
###########################################################################

import os
import re

class MFG():

    def __init__(self, mavMap, templates, pathToVectorID, vectorIdToPath, ofType):
        self.mavMap = mavMap                        # MAV vectors: (vectorId => MAVVector)
        self.templates = templates

        self.mfgMap = {}                            # path of file => (node => adjacencyList)

        self.vertexMap = {}

        self.pathToVectorID = pathToVectorID
        self.vectorIdToPath = vectorIdToPath

    def generateMFG(self, cluster):

        for vectorID in cluster:
            vertices = []
            self.readFileAndGenerateMfg(self.vectorIdToPath[vectorID], vertices)
            self.vertexMap[vectorID] = vertices


    def readFileAndGenerateMfg(self, path, vertices):
        if self.pathToVectorID[path] in self.mfgMap:
            # pass in case the mfg vector already exists
            pass

        # Get a dictionary of nodes
        nodeMap = self.generateNodes(path)

        curLine = None
        nextLine = ''

        curTemplateId = -1
        nextTemplateId = -1

        with open(path, "r", encoding = 'utf8') as f:
            for totalLines, line in enumerate(f): pass

        lineNumber = 0
        with open(path, "r", encoding = "utf8") as log_file:
            encounteredVertices = set()
            curLine = log_file.readline().strip()
            while True:
                nextLine = log_file.readline().strip()
                lineNumber += 1
                if lineNumber == totalLines:
                    if curTemplateId != -1:
                        if curTemplateId not in encounteredVertices:
                            encounteredVertices.add(curTemplateId)
                            vertices.append(curTemplateId)
                    break

                if nextLine == '\r\n' or nextLine == '\n' or not nextLine:
                    continue

                # Find the template of current line and next line
                f = False
                for position, template in enumerate(self.templates):
                    if self.mavMap[self.pathToVectorID[path]][position] == 1:
                        if curTemplateId == -1 and re.match(template, curLine):
                            curTemplateId = position

                        if re.match(template, nextLine):
                            nextTemplateId = position
                            f = True
                if not f:
                    nextLine = "'" + nextLine + "'"
                    print("WARNING: No matching template for line: %s. It will be ignored." %nextLine)

                if curTemplateId in nodeMap and nextTemplateId not in nodeMap[curTemplateId]:
                    nodeMap[curTemplateId].append(nextTemplateId)

                if curTemplateId not in encounteredVertices:
                    encounteredVertices.add(curTemplateId)
                    vertices.append(curTemplateId)
                curLine = nextLine
                curTemplateId = nextTemplateId

        self.mfgMap[self.pathToVectorID[path]] = nodeMap

    def generateNodes(self, key):
        nodeMap = {}
        if self.pathToVectorID[key] in self.mavMap:
            mav = self.mavMap[self.pathToVectorID[key]]
            for index in range(len(mav)):
                if mav[index] != 0:
                    nodeMap[index] = list()

        return nodeMap

    def getMfgVectors(self):
        return self.mfgMap

    def getMFGOfVectorID(self, vectorID):
        if vectorID not in self.mfgMap:
            self.generateMFG([vectorID])
        return self.mfgMap[vectorID]

    def getVerticesOfVectorWithID(self, vectorID):
        if vectorID not in self.vertexMap:
            self.getMFGOfVectorID(vectorID)
        return self.vertexMap[vectorID]
