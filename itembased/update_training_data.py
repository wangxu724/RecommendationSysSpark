import sys, math


def read_testdata(testname):
    testdata = dict()
    with open(testname, 'r') as testfile:
        for line in testfile:
            items = line.rstrip('\n').split()
            row = int(items[0])
            col = int(items[1])
            rat = float(items[2])
            testdata[(row, col)] = rat
    return testdata

def write_traindata(inname, outname, testdata):
    with open(outname, 'w') as outfile:
        with open(inname, 'r') as infile:
            for line in infile:
                items = line.rstrip('\n').split()
                row = int(items[0])
                col = int(items[1])
                if (row, col) not in testdata:
                    outfile.write(line)
    
#----------------------------------------------------------------

if __name__ == '__main__':
    testname = 'data/testdata.txt'
    inname = 'data/sample1m.txt'
    outname = 'data/train/sample1m.txt'

    testdata = read_testdata(testname)


 
