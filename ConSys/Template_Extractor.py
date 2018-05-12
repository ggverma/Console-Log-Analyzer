###########################################################################
# Script Written By: Gautam Verma, Anshul Chandra
#
# For CSC 724 Project at North Carolina State University
###########################################################################

from nltk import pos_tag
from dateutil.parser import parse as parse_to_datetime
import re
from os.path import sep as OS_PATH_SEP
from os import walk as os_walk
import sys
from datetime import datetime

class Template_Extractor(object):
    def __init__(self, directoryToScan, outputFile, prefix):
        self.templates = set()

        # NOTE: This literal must be absent in all the log files.
        self.VARIABLE_IN_TEMPLATE = chr(40960)
        self.outputFile = outputFile

        self.filesToExamine = []
        for subdir, dirs, files in os_walk(directoryToScan):
            for f in files:
                if f.startswith(prefix):
                    self.filesToExamine.append(subdir + OS_PATH_SEP + f)
        self.outputFile = outputFile

    def run(self):
        print("-> Running template extractor...")
        startTime = datetime.now()
        m = {}
        for file in self.filesToExamine:
            # Count total lines in file.
            with open(file, "r", encoding = "utf8") as f:
                for totalLines, line in enumerate(f): pass
            with open(file, 'r', encoding = "utf8") as tf:
                lineNumber = 0
                for line in tf.readlines():
                    line = line.strip()
                    if not line:
                        sys.stdout.write('\r\tCovered %f%% of the log file: %s.' %(100.0 * lineNumber / totalLines, file))
                        lineNumber += 1
                        continue
                    try:
                        formattedline = []
                        pt = pos_tag(re.split(" +", line))
                        key = []
                        for i, word_tag in enumerate(pt):
                            if word_tag[1].startswith('VB') and re.match("^[a-zA-Z0-9]*$", word_tag[0]):
                                # Word is verb and does not contain a number.
                                key.append(tuple([word_tag[0], i]))
                            if word_tag[1] == 'CD' or self.isDateOrTime(word_tag[0]):
                                # A number.
                                if not formattedline or formattedline[-1] != self.VARIABLE_IN_TEMPLATE:
                                    formattedline.append(self.VARIABLE_IN_TEMPLATE)
                            else:
                                formattedline.append(word_tag[0])

                        key = tuple(key)
                        if key not in m:
                            m[key] = set()
                        m[key].add(' '.join(formattedline))
                        sys.stdout.write('\r\tCovered %f%% of the log file: %s.' %(100.0 * lineNumber / totalLines, file))
                        lineNumber += 1
                    except Exception as e:
                        print ('\nThis line caused exception: %s' %line)
                        print(e)
                        sys.exit()
                print("")
        for k, v in m.items():
            print ("Group:", k, "No. of elements:", str(len(v)))

        for k, v in m.items():
            self.extractTemplates(list(v))
        finishTime = datetime.now()
        with open(self.outputFile, 'w+', encoding = "utf8") as outputFile:
            containsUniversalTemplate = False
            for i, template in enumerate(self.templates):
                template = template.strip()
                if not template: continue
                if template == '*':
                    containsUniversalTemplate = True
                    continue
                outputFile.write(template)
                if i != len(self.templates)-1:
                    outputFile.write('\n\r')
            if containsUniversalTemplate:
                outputFile.write('\n\r')
                outputFile.write('*')
        print('-> Extracted %d templates.' %(len(self.templates)))
        print('->Templates stored in file: %s' %self.outputFile)
        print('->Time taken to extract templates: %s' %str(finishTime-startTime))

    def isDateOrTime(self, word):
        try:
            parse_to_datetime(word)
            return True
        except Exception as e:
            return False

    def extractTemplates(self, bucket, fromIndex=0):
        if fromIndex >= len(bucket): return
        s1 = bucket[fromIndex] # Default bucket.

        if len(bucket) > 1:
            unmatchedBucket = []
            for s2i in range(fromIndex+1, len(bucket)):
                # Try every other bucket.
                s2 = bucket[s2i]
                template, anyLCS = self.getTemplateUsingLCS(s1, s2)
                if anyLCS:
                    # If an LCS is found, create a template.
                    templatereg = self.insertEscapeChar(template)
                    templatereg = templatereg.replace(self.VARIABLE_IN_TEMPLATE, '.+')
                    for i in range(fromIndex, len(bucket)):
                        if not re.match(templatereg, bucket[i]):
                            unmatchedBucket.append(bucket[i])
                    if len(unmatchedBucket) > 0:
                        self.extractTemplates(unmatchedBucket)

                    template = templatereg.replace('.+', '*')
                    break
            else:
                # Otherwise, Put s1 as a template.
                templatereg = self.insertEscapeChar(s1)
                template = templatereg.replace(self.VARIABLE_IN_TEMPLATE, '*')

                # Recurse on the rest.
                self.extractTemplates(bucket, fromIndex+1)
        else:
            templatereg = self.insertEscapeChar(s1)
            template = templatereg.replace(self.VARIABLE_IN_TEMPLATE, '*')

        self.templates.add(template)

    def insertEscapeChar(self, sentence):
        templatereg = []
        for letter in sentence:
            if not letter.isalpha() and not letter.isdigit() and \
            letter != ' ' and letter != self.VARIABLE_IN_TEMPLATE \
            and not letter == '\n':
                templatereg.append('\\')
            templatereg.append(letter)

        return ''.join(templatereg)


    def getTemplateUsingLCS(self, s1, s2):
        '''
            Returns a template using Longest Common Subsequence.
        '''
        s1 = s1.strip()
        s2 = s2.strip()
        w1, w2 = re.split(" +", s1), re.split(" +", s2)
        m, n = len(w1), len(w2)
        table = [[0] * (n+1) for _ in range(m+1)]
        table[0][0] = 1
        path = [[None] * (n+1) for _ in range(m+1)]
        for i in range(1, m+1):
            for j in range(1, n+1):
                if w1[i-1] == w2[j-1]:
                    table[i][j] = table[i-1][j-1] + 1
                    path[i][j] = 'D'
                elif table[i][j-1] > table[i-1][j]:
                    table[i][j] = table[i][j-1]
                    path[i][j] = 'L'
                else:
                    table[i][j] = table[i-1][j]
                    path[i][j] = 'U'
        template = []
        i, j = m, n
        while i > 0 and j > 0:
            if path[i][j] == 'D':
                # Words match. Insert as the word is.
                if not template or (template[0] == self.VARIABLE_IN_TEMPLATE and w1[i-1] != self.VARIABLE_IN_TEMPLATE) or (template[0] != self.VARIABLE_IN_TEMPLATE):
                    template.insert(0, w1[i-1])
                i, j = i-1, j-1
            elif path[i][j] == 'U':
                # Words do not match. Insert escape character.
                i -= 1
                if not template or template[0] != self.VARIABLE_IN_TEMPLATE:
                    template.insert(0, self.VARIABLE_IN_TEMPLATE)
            else:
                # Words do not match. Insert escape character.
                j -= 1
                if not template or template[0] != self.VARIABLE_IN_TEMPLATE:
                    template.insert(0, self.VARIABLE_IN_TEMPLATE)
        if len(template) == 1 and template[0] == self.VARIABLE_IN_TEMPLATE:
            return s1, False
        return ' '.join(template), True


if __name__ == '__main__':
    prefix, outputFile, directoryToScan = 'log', '../LogFiles/', '../LogFiles/'
    if len(sys.argv) > 1:
        for i in range(1, len(sys.argv)):
            if sys.argv[i] == '-h':
                # Display help info.
                print("Use -p to specify 'prefix' i.e. the prefix of the log files to be scanned. DEFAULT:", prefix)
                print("Use -o to specify name of the file to which log templates will be written. DEFAULT:", outputFile)
                print("Use -d to specify path of the directory to be scanned for reading log files. DEFAULT:", directoryToScan)
                sys.exit()

        for i in range(1, len(sys.argv), 2):
            if sys.argv[i] == '-p':
                prefix =sys. argv[i+1]
            if sys.argv[i] == '-o':
                outputFile = sys.argv[i+1]
            if sys.argv[i] == '-d':
                directoryToScan = sys.argv[i+1]

    TE = Template_Extractor(directoryToScan, outputFile, prefix)
    TE.run()
