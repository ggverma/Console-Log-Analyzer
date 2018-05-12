###########################################################################
# Script Written By: Anshul Chandra, Gautam Verma
#
# For CSC 724 Project at North Carolina State University
###########################################################################

import os
import re
from sys import maxsize as MAX_INT
from time import gmtime, strftime
from K_Medoid_Clustering import K_Medoid_Clustering
from MAV import MAV

class Difflog(object):
    def __init__(self, abnormalLogs, medoidIDs, objMFG, verbose = True):
        self.abnormalLogs = abnormalLogs
        self.medoidIDs = medoidIDs
        self.objMFG = objMFG
        self.verbose = verbose

    def createDiffLogs(self, templates, logIDToPath, resultFolder):
        # FIRST STEP of difflog creation begins.
        difflogs = []
        for abnormalLog in self.abnormalLogs:
            anomalousMFG = self.objMFG.getMFGOfVectorID(abnormalLog[0])
            visited = set() # used to keep track of vertices visited. used by DFS.
            difflog = None
            if abnormalLog[1] == 'all':
                # Compare with normal MFG of all medoids and retain intersection.
                for medoidID in self.medoidIDs:
                    difflogWithThisNormalMFG = set()
                    flag = True
                    normalMFG = self.objMFG.getMFGOfVectorID(medoidID)
                    for vertex in self.objMFG.getVerticesOfVectorWithID(abnormalLog[0]):
                        if vertex not in visited:
                            self.compareMFGs(anomalousMFG, normalMFG, visited, [], vertex, difflogWithThisNormalMFG)
                    if flag:
                        flag = False
                        difflog = difflogWithThisNormalMFG
                    else: difflog = difflog.intersection(difflogWithThisNormalMFG)
                    visited = set() # reset
            else:
                # Compare with normal MFG specified by the MFGClustering.
                difflog = set()
                for vertex in self.objMFG.getVerticesOfVectorWithID(abnormalLog[0]):
                    if vertex not in visited:
                        normalMFG = self.objMFG.getMFGOfVectorID(abnormalLog[1])
                        self.compareMFGs(anomalousMFG, normalMFG, visited, [], vertex, difflog)
            if difflog: difflogs.append(list(difflog))

        print("-> Difflogs generated:")
        for difflog in difflogs:
            print('\t', difflog)

        # SECOND STEP: Extract MAVs of the difflogs and do clustering.
        MAV_DIAMETER_THRESHOLD = self.getDiameterThreshold()
        while MAV_DIAMETER_THRESHOLD > 0:
            objMAV = MAV(None)
            objMAV.setLogTemplates(templates)
            objMAV.generateVectorsFromDifflogs(difflogs)

            # Perform hierarchical clustering again to idetify clusters with related log messages.
            ClusteringManager = K_Medoid_Clustering(data=objMAV.getMAVVectors())
            abnormalClusters = []
            ClusteringManager.performHierarchicalClustering(objMAV.getTopCluster(), MAV_DIAMETER_THRESHOLD, abnormalClusters, objMAV.getTopRandomMedoid())
            if self.verbose: print("-> Difflog Clusters")
            for cluster in abnormalClusters:
                print ('\t', cluster[0])

            # THIRD STEP: Extract common messages from clusters.
            for abnormalCluster in abnormalClusters:
                commonTemplates = self.extractCommonTemplates(difflogs, abnormalCluster[0])

                if len(commonTemplates) == 0:
                    continue

                print ("-> Common Template Sequence:", commonTemplates)
                maxWindowSize = self.getWindowSize(commonTemplates)
                commonTemplatesTrie = self.generateCommonTemplateTrie(commonTemplates)

                for difflog in abnormalCluster[0]:
                    filepath = logIDToPath[self.abnormalLogs[difflog][0]]
                    lastIndex = filepath.rfind(os.path.sep)

                    if not os.path.exists(resultFolder):
                        os.makedirs(resultFolder)

                    resultFilePath = resultFolder + os.path.sep + 'result_' + filepath[lastIndex + 1:]

                    with open(logIDToPath[self.abnormalLogs[difflog][0]], "r", encoding = "utf8") as f:
                        with open(resultFilePath, "w+", encoding = "utf8") as resultFile:
                            print('\t-> Writing results to file:', resultFilePath)
                            window = []
                            lines = []

                            for lineNumber, line in enumerate(f):
                                # get the template
                                for position, template in enumerate(self.objMFG.templates):
                                    if re.match(template, line):
                                        if len(window) > 0 or (len(window) == 0 and position in commonTemplatesTrie):
                                            window.append(position)
                                            lines.append("Line number: " + str(lineNumber + 1) + " - " + line)
                                        break

                                if len(window) == maxWindowSize:
                                    ## check sequence and append the lines in result file
                                    endIndex = self.matchWindow(commonTemplatesTrie, window)

                                    if endIndex >= 0:
                                        for line in lines:
                                            resultFile.write(line)
                                        resultFile.write("------ \n\r")
                                        del window[0:endIndex]
                                        del lines[0:endIndex]
                                    else:
                                        del window[0]
                                        del lines[0]

                                    while(len(window) > 0):
                                        if window[0] not in commonTemplatesTrie:
                                            del window[0]
                                            del lines[0]
                                        else: break

                            if len(window) > 0:
                                endIndex = self.matchWindow(commonTemplatesTrie, window)

                                if endIndex >= 0:
                                    # write lines to file
                                    for line in lines:
                                        resultFile.write(line)
            MAV_DIAMETER_THRESHOLD = self.getDiameterThreshold()

    def matchWindow(self, commonTemplatesTrie, window):
        endIndex = 1
        sequence = commonTemplatesTrie[window[0]]

        while len(sequence) > 0 and endIndex < len(window):
            if window[endIndex] in sequence:
                sequence = sequence[window[endIndex]]
                endIndex += 1
            else:
                break

        return endIndex if len(sequence) == 0 else -1

    def getWindowSize(self, commonTemplates):
        if commonTemplates:
            return max([len(x) for x in commonTemplates])
        return 0

    def generateCommonTemplateTrie(self, commonTemplates):
        commonTemplatesTrie = {}
        for sequence in commonTemplates:
            currentMap = commonTemplatesTrie
            for tempalateID in sequence:
                if tempalateID not in currentMap:
                    currentMap[tempalateID] = {}
                currentMap = currentMap[tempalateID]
        return commonTemplatesTrie



    def compareMFGs(self, anomalousMFG, normalMFG, visited, missingPath, currentVertex, difflog):
        '''
            DFS based solution. DFS runs on the anomalousMFG.
        '''
        if currentVertex not in visited:
            visited.add(currentVertex)
            if currentVertex not in normalMFG:
                # Path until this vertex is absent in the normalMFG.
                if anomalousMFG[currentVertex]:
                    for adjacentNode in anomalousMFG[currentVertex]:
                        self.compareMFGs(anomalousMFG, normalMFG, visited, missingPath + [currentVertex], adjacentNode, difflog)
                else:
                    # Leaf.
                    if missingPath:
                        difflog.add(tuple(missingPath + [currentVertex]))
            else:
                # Add current missing vertices sequence until now.
                if missingPath:
                    difflog.add(tuple(missingPath + [currentVertex]))
                # Make new missing sequence.
                for adjacentNode in anomalousMFG[currentVertex]:
                    if adjacentNode in normalMFG[currentVertex]:
                        base = []
                    else:
                        base = [currentVertex]
                    self.compareMFGs(anomalousMFG, normalMFG, visited, base, adjacentNode, difflog)
        elif missingPath:
            if (missingPath[-1] not in normalMFG) or (missingPath[-1] in normalMFG and currentVertex not in normalMFG[missingPath[-1]]):
                missingPath.append(currentVertex)
                difflog.add(tuple(missingPath))
                missingPath.pop()
            else:
                if len(missingPath)>1: difflog.add(tuple(missingPath))

    def extractCommonTemplates(self, difflogs, diffLogIDs):
        '''
            Returns the common template sequences from difflogs.
        '''

        # Find the difflogID to iterate on. It is with the minimal set of elements.
        minS = MAX_INT
        for diffLogID in diffLogIDs:
            s = sum([len(x) for x in difflogs[diffLogID]])
            if s < minS:
                minS, difflogToIterate = s, diffLogID

        commonEdges = self.convertTo2DTuples(difflogs[difflogToIterate])
        for diffLogID in diffLogIDs:
            if diffLogID != difflogToIterate:
                # Retain only common edges from all difflogs.
                result = self.convertTo2DTuples(difflogs[diffLogID])
                commonEdges = commonEdges.intersection(result)

        if commonEdges == None:
            # No common edges.
            commonTemplateSequences = difflogs[difflogToIterate]
        else:
            print('\n-> Common Edges:', commonEdges)
            commonTemplateSequences = []

            # Iterate linearly on the pre-selected diffLogID while merging the path.
            for messageSequence in difflogs[difflogToIterate]:
                flag = True
                for i in range(1, len(messageSequence)):
                    edge = tuple([messageSequence[i-1], messageSequence[i]])
                    if edge in commonEdges:
                        if not commonTemplateSequences or flag:
                            flag = False
                            commonTemplateSequence = [messageSequence[i-1], messageSequence[i]]
                            commonTemplateSequences.append(commonTemplateSequence)
                        else:
                            if commonTemplateSequences[-1][-1] == edge[0]:
                                commonTemplateSequence = commonTemplateSequences[-1]
                                commonTemplateSequence.append(edge[1])
                            else:
                                commonTemplateSequence = [messageSequence[i-1], messageSequence[i]]
                                commonTemplateSequences.append(commonTemplateSequence)

        return commonTemplateSequences

    def convertTo2DTuples(self, difflog):
        '''
            Breaks sequences to 2D. For eg, {2,3,4,5} will be converted into {{2,3},{3,4},{4,5}}
        '''
        TwoDRep = set()
        for messageSequence in difflog:
            for i in range(1, len(messageSequence)):
                TwoDRep.add(tuple([messageSequence[i-1], messageSequence[i]]))
        return TwoDRep

    def getDiameterThreshold(self):
        return float(input('!- Enter the DIAMETER threshold for the difflog MAV clustering: '))
