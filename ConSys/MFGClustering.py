###########################################################################
# Script Written By: Anshul Chandra
#
# For CSC 724 Project at North Carolina State University
###########################################################################

from scipy.spatial.distance import pdist, squareform
import numpy as np

class MFGClustering():
    def __init__(self):
        self.templateLength = None
        self.mfgMap = None

    def updateTemplateLengthAndMFGMap(self, templateLength, mfgMap):
        self.templateLength = templateLength        # number of templates
        self.mfgMap = mfgMap                        # MFG vectors

    # Performs MFG clustering for a given cluster and returns the abnormal logs
    def performMFGClustering(self, cluster, medoid, clusteringMetric = 'cityblock', clusteringMethod = 'average'):
        data = []
        for vectorID in cluster:
            adjMat = [0] * (self.templateLength * self.templateLength)
            adjacencyList = self.mfgMap[vectorID]
            for i, row in adjacencyList.items():
                for j, col in enumerate(row):
                    adjMat[i * self.templateLength + j] = 1
            data.append(adjMat)

        editDistances = pdist(data, 'cityblock')

        editDistances = squareform(editDistances)

        nearestNeighbors = []

        abnormalLogs = []

        for i in range(len(cluster)):
            nearestNeighbour = min([v for j, v in enumerate(editDistances[i]) if i != j])
            nearestNeighbors.append(nearestNeighbour)

        OUTLIER_THRESHOLD = np.mean(nearestNeighbors) + 2 * np.std(nearestNeighbors)

        for i in range(len(cluster)):
            nearestNeighbour = min([v for j, v in enumerate(editDistances[i]) if i != j])
            if nearestNeighbour > OUTLIER_THRESHOLD:
                # Append to the list of abnormal logs
                abnormalLogs.append((cluster[i], medoid))

        return abnormalLogs
