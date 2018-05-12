###########################################################################
# Script Written By: Gautam Verma
#
# For CSC 724 Project at North Carolina State University
###########################################################################

from random import randint, sample, seed
from scipy.spatial.distance import pdist, cityblock

class K_Medoid_Clustering(object):
    def __init__(self, data):
        self.data = data
        seed(1)

    def performHierarchicalClustering(self, cluster, DIAMETER_THRESHOLD, abnormalLogs, medoidID, clusteringMetric = 'cityblock'):
        #---- Clustering begins ----
        if self.getClusterDiameter(cluster) <= DIAMETER_THRESHOLD:
            if len(cluster) <= 4:
                # All are Anomalous
                abnormalLogs += tuple((tuple(cluster), "AA")),
            else:
                abnormalLogs += tuple((tuple(cluster), "MFG", medoidID)),
        else:
            # Split the cluster.
            subCluster1, subCluster2, medoid1ID, medoid2ID = self.get2SubClusters(cluster)
            self.performHierarchicalClustering(subCluster1, DIAMETER_THRESHOLD, abnormalLogs, medoid1ID, clusteringMetric)
            self.performHierarchicalClustering(subCluster2, DIAMETER_THRESHOLD, abnormalLogs, medoid2ID, clusteringMetric)

    def get2SubClusters(self, cluster, metric = 'cityblock'):
        '''
            Returns the best possible split and medoids associated with split clusters.
        '''
        # Top-down K-medoid using PAM.
        vectors = [self.data[id] for id in cluster]

        # Randomly select intitial medoids.
        medoid1Index, medoid2Index = sample(range(len(cluster)), 2)
        medoid1Id, medoid2Id = cluster[medoid1Index], cluster[medoid2Index]
        medoid1, medoid2 = self.data[medoid1Id], self.data[medoid2Id]

        cluster1, cluster2, cost = self.create2Clusters(cluster, medoid1, medoid2, medoid1Id, medoid2Id)

        finalcluster1, finalcluster2 = cluster1, cluster2

        # Keep changing medoids and finds the best split.
        i = 1
        while i < len(cluster2):
            ic2 = cluster2[i]
            potentialMedoid2 = self.data[ic2]
            potentialCluster1, potentialCluster2, potentialCost = self.create2Clusters(cluster, medoid1, potentialMedoid2, medoid1Id, ic2)
            if potentialCost < cost:
                cost = potentialCost
                medoid2 = potentialMedoid2

                finalcluster1, finalcluster2 = potentialCluster1, potentialCluster2

                medoid2Id = ic2
            i += 1

        i = 1
        while i < len(cluster1):
            ic1 = cluster1[i]
            potentialMedoid1 = self.data[ic1]
            potentialCluster1, potentialCluster2, potentialCost = self.create2Clusters(cluster, potentialMedoid1, medoid2, ic1, medoid2Id)
            if potentialCost < cost:
                cost = potentialCost
                medoid1 = potentialMedoid1
                finalcluster1, finalcluster2 = potentialCluster1, potentialCluster2
                medoid1Id = ic1
            i += 1

        return finalcluster1, finalcluster2, medoid1Id, medoid2Id

    def create2Clusters(self, cluster, medoid1, medoid2, m1i, m2i, metric = 'cityblock'):
        '''
            Returns two lists and total cost of splitting the cluster if medoid1
            and medoid2 are the medoids of two clusters being considered.
        '''
        cluster1, cluster2, cost = [m1i], [m2i], 0
        for vectorId in cluster:
            if vectorId == m1i or vectorId == m2i: continue
            dist1, dist2 = pdist([medoid1, self.data[vectorId]], metric = metric)[0], pdist([medoid2, self.data[vectorId]], metric = metric)[0]
            if dist1 < dist2:
                cluster1.append(vectorId)
                cost += dist1
            else:
                cluster2.append(vectorId)
                cost += dist2
        return cluster1, cluster2, cost

    def getClusterDiameter(self, cluster, metric = 'cityblock'):
        '''
            Returns cluster diameter i.e. the max distance between any two points
            within the cluster.
        '''
        if len(cluster) <= 1: return 0

        vectors = [self.data[id] for id in cluster]
        distances = pdist(vectors, metric = metric)
        return max(distances) if len(distances) > 0 else 0

if __name__ == '__main__':
    createData()
    performHierarchicalClustering(data)
