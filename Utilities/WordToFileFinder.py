from sys import argv, exit as EXIT_APP
from os import walk as os_walk, makedirs as make_directory
from os.path import sep as OS_PATH_SEP, exists as check_directory_exists

class WordToFileFinder():
    def find(self, inputFilePrefix, directoryToScan, word):
        for subdir, dirs, files in os_walk(directoryToScan):
            for file in files:
                if file.startswith(inputFilePrefix):
                    infile = subdir + OS_PATH_SEP + file
                    with open(infile, 'r', encoding = "utf8") as f:
                        for line in f:
                            if word in line:
                                print(word, 'is contained in file:', file)
                                break

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
            EXIT_APP()

    for i in range(1, len(argv), 2):
        if argv[i] == '-i':
            inputFilePrefix = argv[i+1]
        elif argv[i] == '-w':
            word = argv[i+1]
        elif argv[i] == '-di':
            directoryToScan = argv[i+1]
        else:
            print("Unrecognized argument. Please specify only valid arguments. Use -h to see help.")
            EXIT_APP()

    if word == None:
        print("Please use -w to specify the word that if contained in a file, the file will be shown at console.")
    else:
        WFF = WordToFileFinder()
        WFF.find(inputFilePrefix, directoryToScan, word)
