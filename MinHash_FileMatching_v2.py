#!/usr/bin/env python
# -*- coding: utf-8 -*-



from __future__ import division
import pickle
from heapq import heapify,heappop,heappush
import random
import os

chunksize = 16384
totalShingles = 23880
pklHandler = open("docShingleDict.pkl", 'rb')
docShingleDict= pickle.load(pklHandler)
docLowestShingleID = {}
pklHandler.close()
dataShingleDict = {}
dataLowestShingleID = {}


# Note: create random number
def findRandomNos(k):
  # random.seed(0)
  randList = []
  randIndex = random.randint(0, totalShingles -1) 
  randList.append(randIndex)
  while k>0:
    while randIndex in randList:
      randIndex = random.randint(0, totalShingles-1) 
      
    randList.append(randIndex)
    k = k-1
    
  return randList


def InputDataMinhash(data):
    f = open("{}".format(data),"rb")
    count = 0
    rangeDebug = 0
    shingleDict = {}  

    temp = set()
    chunkTmp = []
    for b in __read_in_chunks(f,chunksize):
        chunkTmp.append(b)

    for chunk in chunkTmp:
        print "[+] proc", data , rangeDebug, "/ {} ... ".format(len(chunkTmp))
        for index in range(0,len(chunkTmp)-2):
            shingle = chunkTmp[index] + " " + chunkTmp[index+1] + " " + chunkTmp[index+2]

            if ( shingle not in shingleDict.iterkeys()):
                shingleDict[shingle] = count
                count = count + 1
            temp.add(shingleDict[shingle])
            
        dataShingleDict[data.split("/")[-1].split(".")[0]] = temp
        rangeDebug = rangeDebug + 1
    f.close()

    output = open("dataShingleDict.pkl", 'wb')
    pickle.dump(dataShingleDict, output)
    output.close()
    return data


def __read_in_chunks(file, size):
    print "[+] creating chunks of {}... ".format(size)
    while True:
        chunk = file.read(size)
        if chunk:
            yield chunk
        else:
            return


def extractMinhash(ShingleDict, DLowestShingleID):
	DLowestShingleID = {}
	tmpDict = {}
	for doc in ShingleDict.iterkeys():
	  shingleIDSet = ShingleDict[doc]
	  # print shingleIDSet
	  lowestShingleID = []
	  for x in range(0,200):
	    listFx = []
	    for shingleID in shingleIDSet:
	      temp = (randomNoA[x] * shingleID + randomNoB[x]) % totalShingles 
	      listFx.append(temp)
	    heapify(listFx)
	    lowestShingleID.append(heappop(listFx))
	  DLowestShingleID[doc] = lowestShingleID
	  # print doc
	  # print DLowestShingleID[doc]
	  tmpDict.update(DLowestShingleID)
	# print tmpDict
	return tmpDict
  		

def input_extractMinhash(ShingleDict_input, DLowestShingleID_input):
	DLowestShingleID_input = {}
	for doc in ShingleDict_input.iterkeys():
	  shingleIDSet = ShingleDict_input[doc]
	  # print shingleIDSet
	  lowestShingleID = []
	  for x in range(0,200):
	    listFx = []
	    for shingleID in shingleIDSet:
	      temp = (randomNoA[x] * shingleID + randomNoB[x]) % totalShingles 
	      listFx.append(temp)

	    heapify(listFx)
	    lowestShingleID.append(heappop(listFx))
	  DLowestShingleID_input[doc] = lowestShingleID
	  # print doc
	  # print DLowestShingleID_input[doc]
	return DLowestShingleID_input[doc]


def Matching(dataLowestShingleID, docLowestShingleID, filename):
	estimateMatrix = []
	totaljaccard = []
	values = docLowestShingleID.values()

	for x in range(0, len(values)):
		doc_values = values[x]
		# print doc_values
		s1 = set(doc_values)
		col = []
		data_values = dataLowestShingleID
		s2 = set(data_values)
		# print data_values
		count = 0
		for i in range(0,200):
			if doc_values[i] == data_values[i]:
				count = count + 1

		col.append(count/200)
		jaccard = (len(s1.intersection(s2)) / len(s1.union(s2)))
		# print jaccard
		totaljaccard.append(jaccard)
		estimateMatrix.append(col)
		# print estimateMatrix
		# print totaljaccard
	FinalScore = Scoring(totaljaccard)
	print filename.replace('InputDataFolder/', '') + ' is suspected to be ' + str(FinalScore) + '% of the infection.'
	return totaljaccard


def Scoring(totaljaccard):
	Max_value = max(totaljaccard)
	print Max_value
	Final_Matching_Score = Max_value * 100
	return Final_Matching_Score


def Load_file():
	global docLowestShingleID, dataLowestShingleID, randomNoA, randomNoB
	currentfilePath = os.path.realpath(__file__)
	dirPath = currentfilePath.split("/")
	dirPath[-1] = "InputDataFolder/"
	dirPath = "/".join(dirPath)
	folder = os.listdir(dirPath)

	randomNoA = findRandomNos(200)
	randomNoB = findRandomNos(200)

	for file in folder:
		print folder
		filename = InputDataMinhash("InputDataFolder/{}".format(file))

		docLowestShingleID = extractMinhash(docShingleDict, docLowestShingleID)
		dataLowestShingleID = input_extractMinhash(dataShingleDict, dataLowestShingleID)

		Matching(dataLowestShingleID,docLowestShingleID, filename)





if __name__ == '__main__' :

	Load_file()

	# randomNoA = findRandomNos(200)
	# randomNoB = findRandomNos(200)

	# docLowestShingleID = extractMinhash(docShingleDict, docLowestShingleID)
	# dataLowestShingleID = input_extractMinhash(dataShingleDict, dataLowestShingleID)

	# print docLowestShingleID
	# print dataLowestShingleID

	# Matching(dataLowestShingleID,docLowestShingleID)
	# print result

	