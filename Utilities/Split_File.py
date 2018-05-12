###########################################################################
# Script Written By: Gautam Verma
#
# For CSC 724 Project at North Carolina State University
###########################################################################

from sys import argv, exit as EXIT_APP
from os import walk as os_walk, makedirs as make_directory
from os.path import sep as OS_PATH_SEP, exists as check_directory_exists

class File_Splitter(object):
    '''
        Splits a file(s) into smaller chunks.
    '''
    def split(self, inputFilePrefix, directoryToScan, linesInAChunk, chunkPrefix, outputDirectory):
        lines, lineNumber, chunkNumber = [], 0, 0
        for subdir, dirs, files in os_walk(directoryToScan):
            for file in files:
                if file.startswith(inputFilePrefix):
                    # valid file.
                    file = subdir + OS_PATH_SEP + file
                    if outputDirectory == None:
                        outputDirectory = subdir + OS_PATH_SEP + 'Chunks'
                    if not check_directory_exists(outputDirectory):
                        make_directory(outputDirectory)
                    with open(file, 'r', encoding = "utf8") as f:
                        for line in f:
                            if lineNumber == linesInAChunk:
                                # Create a new chunk.
                                chunkFile = outputDirectory + OS_PATH_SEP + chunkPrefix + str(chunkNumber)
                                chunkNumber += 1
                                self.putLinesToChunk(lines, chunkFile)
                                # Clear the buffer.
                                del lines[:]
                                lineNumber = 0
                            # Append it into the buffer.
                            lines.append(line)
                            lineNumber += 1
                    if lines:
                        # Write the buffer if it's not empty.
                        chunkFile = outputDirectory + OS_PATH_SEP + chunkPrefix + str(chunkNumber)
                        self.putLinesToChunk(lines, chunkFile)

    def putLinesToChunk(self, lines, chunkFile):
        # Write the buffer into file.
        with open(chunkFile, 'w+', encoding = "utf8") as cf:
            for i, line in enumerate(lines):
                cf.write(line)
                if i != len(lines)-1:
                    cf.write('\n\r')

if __name__ == '__main__':
    inputFilePrefix, linesInAChunk, chunkPrefix = None, 200, 'chunk_'
    directoryToScan = '../LogFiles'
    outputDirectory = None
    for i in range(1, len(argv)):
        if argv[i] == '-h':
            print("Use -i to specify the prefix of the input files to be split. REQUIRED.")
            print("Use -di to specify path of the directory to to be scanned for reading input files. DEFAULT: %s" %directoryToScan)
            print("Use -p to specify the prefix of chunks. DEFAULT: %s" %chunkPrefix)
            print("Use -do to specify the directory where chunks will be written. By default, they are written in a folder named 'Chunks' where the input files are present.")
            print("Use -l to specify the number of lines to be present in each chunk. DEFAULT: %s" %linesInAChunk)
            EXIT_APP()

    for i in range(1, len(argv), 2):
        if argv[i] == '-i':
            inputFilePrefix = argv[i+1]
        elif argv[i] == '-l':
            linesInAChunk = int(argv[i+1])
        elif argv[i] == '-p':
            chunkPrefix = argv[i+1]
        elif argv[i] == '-di':
            directoryToScan = argv[i+1]
        elif argv[i] == '-do':
            outputDirectory = argv[i+1]
        else:
            print("Unrecognized argument. Please specify only valid arguments. Use -h to see help.")
            EXIT_APP()

    if file == None:
        print("Please use -i to specify the input file to be split.")
    else:
        FS = File_Splitter()
        FS.split(inputFilePrefix, directoryToScan, linesInAChunk, chunkPrefix, outputDirectory)
