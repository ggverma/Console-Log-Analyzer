###########################################################################
# Script Written By: Gautam Verma
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

class Driver():
    def run(self, runTemplateExtractor, logPrefix, directoryToScan, templateFile, resultFolder):
        if runTemplateExtractor:
            TE = Template_Extractor(directoryToScan, templateFile, logPrefix)
            TE.run()

        objMav = MAV(templateFile)
        objMav.generateMAV(logPrefix, directoryToScan)

        while True:
            MAVClusterManager = K_Medoid_Clustering(data = objMav.getMAVVectors())

            # MAV begins
            DIAMETER_THRESHOLD = self.getDiameterThreshold()
            if DIAMETER_THRESHOLD <= 0: break
            abnormalClusters = []

            print ('-> Performing MAV clustering.')
            MAVClusterManager.performHierarchicalClustering(objMav.getTopCluster(), DIAMETER_THRESHOLD, abnormalClusters, objMav.getTopRandomMedoid())
            print ('-> MAV clustering done.')

            print("-> Potentially abnormal clusters found: ")

            for cluster in abnormalClusters:
                if cluster[1] == 'AA': # 'AA' is unique ID that tells all instances are anomalous.
                    print("\tContains all anomalous instances. Elements:", cluster[0])
                else:
                    print("\tMFG analysis required. Elements:", cluster[0], ", Medoid:", cluster[2])

            # MFG begins
            print ('-> Performing MFG clustering.')
            objMfg = MFG(objMav.getMAVMap(), objMav.getTemplates(), objMav.getPathToVectorID(), objMav.getVectorIDToPath(), 'Producer')

            for vectorID, mav in objMav.getMAVMap().items():
                filename = objMav.getVectorIDToPath()[vectorID]
                print("\tVectorId " + str(vectorID) + ", Path: " + filename)
                print ("\tMAV:", mav)
                print ("\tMFG:", objMfg.getMFGOfVectorID(vectorID))
                print("\t------------------------------")

            abnormalLogs = list()
            medoidIDs = []
            MFGClusterManager = MFGClustering()

            for i, t in enumerate(abnormalClusters):
                sys.stdout.write('\r\tCompleted %f%% of the work.' %(100.0 * (i+1) / len(abnormalClusters)))
                if t[1] == "MFG":
                    objMfg.generateMFG(t[0])
                    MFGClusterManager.updateTemplateLengthAndMFGMap(len(objMav.getTemplates()), objMfg.getMfgVectors())
                    abnormalLogs += MFGClusterManager.performMFGClustering(t[0], t[2])
                    medoidIDs += t[2],
                else:
                    abnormalLogs += [(x, 'all') for x in t[0]]
            print ('\n-> MFG clustering done.')

            if len(abnormalLogs) > 0:
                print("\nFound the following abnormal instances:")
                vecToPath = objMav.getVectorIDToPath()

                for instance in abnormalLogs:
                    if instance[1] != 'all':
                        print("\tType: MFG, instance: " + vecToPath[instance[0]])
                    else:
                        print("\tType: MAV, instance: " + vecToPath[instance[0]])

            # Difflog creation begins
            print("\n-> Creating difflogs...")
            DifflogManager = Difflog(abnormalLogs, medoidIDs, objMfg)
            DifflogManager.createDiffLogs(objMav.getTemplates(), objMav.getVectorIDToPath(), resultFolder)

    def getDiameterThreshold(self):
        return float(input('!- Enter the DIAMETER threshold for the MAV clustering on log files: '))

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
    driver = Driver()
    driver.run(runTemplateExtractor, logPrefix, directoryToScan, templateFile, resultFolder)
