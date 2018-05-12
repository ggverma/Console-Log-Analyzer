###########################################################################
# Script Written By: Gautam Verma
#
# For CSC 724 Project at North Carolina State University
###########################################################################

from sys import argv, exit as EXIT_APP
from os import walk as os_walk, makedirs as make_directory
from os.path import sep as OS_PATH_SEP, exists as check_directory_exists

class File_Filter():
    '''
        Removes certain keyword from file(s) and outputs the result into new file(s).
    '''
    def filter(self, inputFilePrefix, directoryToScan, outputDirectory, word):
        for subdir, dirs, files in os_walk(directoryToScan):
            for file in files:
                if file.startswith(inputFilePrefix):
                    # Valid file.
                    infile = subdir + OS_PATH_SEP + file
                    if outputDirectory == None:
                        outputDirectory = subdir + OS_PATH_SEP + 'Filtered_Files'
                    if not check_directory_exists(outputDirectory):
                        make_directory(outputDirectory)
                    outfile = outputDirectory + OS_PATH_SEP + file
                    with open(infile, 'r', encoding = "utf8") as inf, open(outfile, 'w+', encoding = 'utf8') as of:
                        for line in inf:
                            if word in line:
                                # Skip the line.
                                continue
                            else:
                                # Write to the new file.
                                of.write(line)

if __name__ == '__main__':
    inputFilePrefix = 'log'
    directoryToScan = '../LogFiles'
    outputDirectory = None
    word = None
    for i in range(1, len(argv)):
        if argv[i] == '-h':
            print("Use -i to specify the prefix of the input files to be read. DEFAULT:", inputFilePrefix)
            print("Use -w to specify the word that if contained in a line, the line will be removed. REQUIRED.")
            print("Use -di to specify path of the directory to to be scanned for reading input files. DEFAULT: %s" %directoryToScan)
            print("Use -do to specify the directory where filtered files will be written. By default, they are written in a folder named 'Filtered_Files' where the input files are present.")
            EXIT_APP()

    for i in range(1, len(argv), 2):
        if argv[i] == '-i':
            inputFilePrefix = argv[i+1]
        elif argv[i] == '-w':
            word = argv[i+1]
        elif argv[i] == '-di':
            directoryToScan = argv[i+1]
        elif argv[i] == '-do':
            outputDirectory = argv[i+1]
        else:
            print("Unrecognized argument. Please specify only valid arguments. Use -h to see help.")
            EXIT_APP()

    if word == None:
        print("Please use -w to specify the word that if contained in a line, the line will be removed.")
    else:
        FF = File_Filter()
        FF.filter(inputFilePrefix, directoryToScan, outputDirectory, word)
