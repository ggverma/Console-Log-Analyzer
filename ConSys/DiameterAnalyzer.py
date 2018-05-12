###########################################################################
# Script Written By: Anshul Chandra
#
# For CSC 724 Project at North Carolina State University
###########################################################################

from MAV import MAV
from MFG import MFG
from os.path import sep as OS_PATH_SEP
from K_Medoid_Clustering import K_Medoid_Clustering
from MFGClustering import MFGClustering
from Difflog import Difflog
from sys import argv
from Template_Extractor import Template_Extractor
import sys

class Diameter_Analyzer():
    '''
        Runs analysis on the log files (MAV and MFG only) multiple time by varying the diameter automatically.
        Requires manual input of diameter range in the code.
    '''
    def run(self, ofType, runTemplateExtractor, logPrefix, directoryToScan, templateFile, resultFolder):
        if runTemplateExtractor:
            TE = Template_Extractor(directoryToScan, templateFile, logPrefix)
            TE.run()

        objMav = MAV(ofType, templateFile)
        objMav.generateMAV(logPrefix, directoryToScan)

        MAVClusterManager = K_Medoid_Clustering(data = objMav.getMAVVectors())

        self.THRESHOLD = 0

        while True:
            # MAV begins
            DIAMETER_THRESHOLD = self.getDiameterThreshold()

            if DIAMETER_THRESHOLD <= 0 or DIAMETER_THRESHOLD > 3: break
            abnormalClusters = []

            MAVClusterManager.performHierarchicalClustering(objMav.getTopCluster(), DIAMETER_THRESHOLD, abnormalClusters, objMav.getTopRandomMedoid())

            # MFG begins

            objMfg = MFG(objMav.getMAVMap(), objMav.getTemplates(), objMav.getPathToVectorID(), objMav.getVectorIDToPath(), 'Producer')

            abnormalLogs = list()
            medoidIDs = []
            MFGClusterManager = MFGClustering()

            for i, t in enumerate(abnormalClusters):
                if t[1] == "MFG":
                    objMfg.generateMFG(t[0])
                    MFGClusterManager.updateTemplateLengthAndMFGMap(len(objMav.getTemplates()), objMfg.getMfgVectors())
                    abnormalLogs += MFGClusterManager.performMFGClustering(t[0], t[2])
                    medoidIDs += t[2],
                else:
                    abnormalLogs += [(x, 'all') for x in t[0]]

            if len(abnormalLogs) > 0:
                vecToPath = objMav.getVectorIDToPath()

                truePositive, falsePositive = 0, 0

                for instance in abnormalLogs:
                    filePath = objMav.getVectorIDToPath()[instance[0]]
                    if 'error' in filePath:
                        truePositive += 1
                    else:
                        falsePositive += 1

                print('Diameter:', DIAMETER_THRESHOLD, ', True Positive:', truePositive, ", False Positive:", falsePositive)
            else:
                print('No abnormal logs')

    def getDiameterThreshold(self):
        self.THRESHOLD += 0.1
        return self.THRESHOLD

if __name__ == '__main__':
    runTemplateExtractor = False
    logPrefix = 'log'
    forComponent = 'Producer'
    templateFile = '../LogFiles/' + forComponent +'/extracted_templates'
    resultFolder = 'ConSys_Results'
    directoryToScan = '../LogFiles'
    if len(argv) > 1:
        for i in range(1, len(sys.argv)):
            if sys.argv[i] == '-h':
                # Display help info.
                print("Use -t to specify name of the file containing the log templates or to which they will be written. DEFAULT: %s" %templateFile)
                print("Use -rt to specify whether to refresh the extracted templates (if they already exist). Accepts: T or True else false. DEFAULT: %s" %runTemplateExtractor)
                print("Use -p to specify 'prefix' i.e. the prefix of the log files to be scanned. DEFAULT: %s" %logPrefix)
                print("Use -d to specify path of the directory to to be scanned for reading log files. DEFAULT: %s" %directoryToScan)
                print("Use -c to specify the component for which Consys is run. Currently accepts: Producer, Consumer, Lambda. DEFAULT: %s" %forComponent)
                print("Use -r to specify the path where Consys will output the results. DEFAULT: %s" %resultFolder)
                sys.exit()

        for i in range(1, len(argv), 2):
            if argv[i] == '-rt':
                if argv[i+1].lower() == 't' or argv[i+1].lower() == 'true':
                    runTemplateExtractor = True
            elif argv[i] == '-p':
                logPrefix = argv[i+1]
            elif argv[i] == '-c':
                forComponent = argv[i+1]
            elif argv[i] == '-t':
                templateFile = argv[i+1]
            elif argv[i] == '-d':
                directoryToScan = argv[i+1]
            elif argv[i] == '-r':
                resultFolder = argv[i+1]
            else:
                print("Unrecognized argument. Please specify only valid arguments. Use -h to see help.")
                sys.exit()

    print("-> Running Consys on the component:", forComponent)
    diameter_analyzer = Diameter_Analyzer()
    diameter_analyzer.run(forComponent, runTemplateExtractor, logPrefix, directoryToScan, templateFile, resultFolder)
