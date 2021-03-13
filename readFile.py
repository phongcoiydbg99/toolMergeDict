import json
import defines
import math
import matplotlib.pyplot as plt
import time
import writeFile
import gc
import os.path
import pandas as pd

global unigram
global ngram
global unigramDict
global ngramDict
global proUni
global proLogUni
global proNgram
global proLogNgram
global proXUni
global ngramArray
global proXNgram

ngramArray = []
unigram = []
ngram = []

proUni = []
proLogUni = []
proXUni = []

proNgram = []
proLogNgram = []
proXNgram = []

unigramDict = {}
ngramDict = {}

def _to_string(key,count,type,prev_array,f):
    return (key + "," + str(type) + "," + prev_array + "," + str(count) + "," + str(f) + "\n")

def _prev_array_to_string(ngram):
    temp = [] + ngram["prevArray"]
    temp.reverse()
    result = ""
    for index, item in enumerate(temp):
        if index != len(temp) - 1:
            result = result + item + " "
        else:
            result = result + item
    return result

def _ngram_to_string(ngram):
    temp = [] + ngram["prevArray"]
    temp.reverse()
    result = ""
    for index, item in enumerate(temp):
        result = result + item + " "
    return result + ngram["ngram"]

def _file_to_csv(name,test_dic_csv,folder,time):
    readFile = open(name, "r")
    writeFile = open("/media/phongnve/HDD/source_bkav/dict_csv/temp.txt", "w")
    writeFile.write("key,type,prev_array,count,f\n")
    for line in readFile:
        convert = line[0:len(line) - 1]
        try:
            temp = json.loads(convert)
        except:
            continue
        if "ngram" in temp:
            if "count" in temp:
                writeFile.write(_to_string(_ngram_to_string(temp),temp["count"],temp["ngramType"],_prev_array_to_string(temp),temp["f"]))
            else:
                writeFile.write(_to_string(_ngram_to_string(temp),0,temp["ngramType"],_prev_array_to_string(temp),temp["f"]))
        else:
            if "word" in temp:
                if temp["word"] == "":
                    word = "0"
                else:
                    word = temp["word"]
                if "count" in temp:
                    writeFile.write(_to_string(word,temp["count"],0,"",temp["f"]))
                else:
                    writeFile.write(_to_string(word,0,0,"",temp["f"]))
    writeFile.close()
    readFile.close()
    read_file = pd.read_csv ("/media/phongnve/HDD/source_bkav/dict_csv/temp.txt", low_memory=False)
    read_file.to_csv ("/media/phongnve/HDD/source_bkav/dict_csv/" +folder +"/"+ time + "/"+test_dic_csv + ".csv", index=False, header=False)

def _read_dic_to_json(name):
    if os.path.isfile(name):
        with open(name) as json_file:
            try:
                data = json.load(json_file)
            except:
                data = {}
            return data
    return {}

def _file_transfer(name):
    fileDict = open(name, "r")
    unigramDict = _read_dic_to_json('/media/phongnve/HDD/source_bkav/dictionary/unigram_json.txt')
    for line in fileDict:
        convert = line[0:len(line) - 1]
        try:
            temp = json.loads(convert)
        except:
            continue
        # Doc dic thanh tung file con
        if "ngram" in line:
            fileName = mergeNgram(temp)
            if fileName != "":
                fileName = "/media/phongnve/HDD/source_bkav/dictionary/" + fileName + ".txt"
            if os.path.isfile(fileName):
                with open(fileName, "r") as json_file:
                    try:
                        data = json.load(json_file)
                    except:
                        data = {}
                if "count" in temp:
                    if "count" in data:
                        data["count"] =  data["count"] + temp["count"]
                    else:
                        data = temp
            else:
                data = temp
            with open(fileName, "w") as json_file:
                json.dump(data, json_file)
        else:
            if "word" in line:
                if temp["word"] in unigramDict:
                    if "count" in temp:
                        if "count" in unigramDict[temp["word"]]:
                            unigramDict[temp["word"]]["count"] =  unigramDict[temp["word"]]["count"] + temp["count"]
                        else:
                            unigramDict[temp["word"]] = temp
                else:
                    unigramDict[temp["word"]] = temp
    with open('/media/phongnve/HDD/source_bkav/dictionary/unigram_json.txt', 'w') as uni_json_file:
        json.dump(unigramDict, uni_json_file)
    fileDict.close()
    return unigramDict

def _get_probability(unigram_dict):
    # uni
    globalCounter = _get_counter(unigram_dict)
    for idx, x in unigram_dict.items():
        x['f'] = math.floor(calculateProbability(0 ,globalCounter, x))
    # writeFile._write_unigram(unigram_dict, 'dictionary/unigram_pro.txt')
    with open('/media/phongnve/HDD/source_bkav/dictionary/unigram_json.txt', 'w') as uni_json_file:
        json.dump(unigram_dict, uni_json_file)

    # ngram
    for fileName in os.listdir("/media/phongnve/HDD/source_bkav/dictionary"):
        if fileName == "unigram_json.txt" or fileName == "unigram_pro.txt":
            continue
        fileName = "/media/phongnve/HDD/source_bkav/dictionary/" + fileName
        with open(fileName, "r") as json_file:
            try:
                json_data = json.load(json_file)
            except:
                json_data = {}
        probability = -1
        if "count" not in json_data:
            probability = -1
        else:
            prevWordCount = len(json_data['prevArray'])
            if prevWordCount == 1:
                if json_data["prevArray"][0] in unigram_dict:
                    if "count" in unigram_dict[json_data["prevArray"][0]]:
                        probability = calculateProbability(prevWordCount, unigram_dict[json_data["prevArray"][0]]["count"], json_data)
                    else:
                        probability = -1
            else:
                prevFileName = _prev_array_to_string(json_data)
                if prevFileName != "":
                    prevFileName = "/media/phongnve/HDD/source_bkav/dictionary/" + prevFileName + ".txt"
                if os.path.isfile(prevFileName):
                    with open(prevFileName, "r") as json_file_ngram:
                        try:
                            data = json.load(json_file_ngram)
                        except:
                            data = {}
                        if "count" in data:
                            probability = calculateProbability(prevWordCount, data["count"], json_data)
                        else:
                            probability = -1

        json_data['f'] = math.floor(probability)
        with open(fileName, "w") as json_file:
            json.dump(json_data, json_file)

class Ngram:
    def __init__(self, key, count, type):
        self.key = key
        self.count = count
        self.type = type
        self.pro = -1

    def toString(self):
        return "{} {} {} {}".format(self.key,self.count,self.type,self.pro)

def readFileWithClass(name):
    fileDict = open(name, "r")
    for line in fileDict:
        convert = line[0:len(line) - 1].split(":")
        if int(convert[2]) == 0:
            unigram.append(Ngram(convert[0],int(convert[1]),0))
        else:
            ngram.append(Ngram(convert[0],int(convert[1]),int(convert[2])))

    unigram.sort(key = lambda x: x.key)
    ngram.sort(key = lambda x: x.key)
    fileDict.close()

def readFile(name):
    fileDict = open(name, "r")
    for line in fileDict:
        convert = line[0:len(line) - 1]
        try:
            temp = json.loads(convert)
        except:
            continue
        # ngramArray.append(ngramArray)
        if "ngram" in line:
            if "count" in temp:
                ngramItem = Ngram(mergeNgram(temp), temp["count"], temp["ngramType"])
            else:
                ngramItem = Ngram(mergeNgram(temp), -1, temp["ngramType"])
            ngram.append(ngramItem)
        else:
            if "word" in line:
                if "count" in temp:
                    unigramItem = Ngram(temp["word"], temp["count"], 0)
                else:
                    unigramItem = Ngram(temp["word"], -1, 0)
            unigram.append(unigramItem)

    # writeFile.writeUnigram(unigram, "unigram.txt")
    # sort
    unigram.sort(key = lambda x: x.key)
    ngram.sort(key = lambda x: x.key)

    fileDict.close()
    # add probability
    # for x in unigram:
    #     proXUni.append(x["word"])
    #     if "f" in temp:
    #         proUni.append(x["f"])
    #     else:
    #         proUni.append(0)
    # for x in ngram:
    #     proXNgram.append(x["ngram"])
    #     if "f" in temp:
    #         proNgram.append(x["f"])
    #     else:
    #         proNgram.append(0)

def _read_file(name):
    fileDict = open(name, "r")
    # Read dic
    print("read -start")
    for line in fileDict:
        convert = line[0:len(line) - 1]
        try:
            temp = json.loads(convert)
        except:
            continue
        ngramArray.append(ngramArray)
        if "ngram" in temp:
            if mergeNgram(temp) in ngramDict:
                if "count" in temp:
                    if "count" in ngramDict[mergeNgram(temp)]:
                        ngramDict[mergeNgram(temp)]["count"] =  ngramDict[mergeNgram(temp)]["count"] + temp["count"]
                    else:
                        ngramDict[mergeNgram(temp)] = temp
            else:
                ngramDict[mergeNgram(temp)] = temp
            # ngram.append(temp)
        else:
            if "word" in temp:
                if temp["word"] in unigramDict:
                    if "count" in temp:
                        if "count" in unigramDict[temp["word"]]:
                            unigramDict[temp["word"]]["count"] =  unigramDict[temp["word"]]["count"] + temp["count"]
                        else:
                            unigramDict[temp["word"]] = temp
                else:
                    unigramDict[temp["word"]] = temp
                # unigram.append(temp)

    unigram = list(unigramDict)
    ngram = list(ngramDict)
    print("read -end {}".format(ngramDict.__sizeof__()))
    fileDict.close()

def readFileUni(name):
    result = []
    fileDict = open(name, "r")
    # Read dic
    for line in fileDict:
        if line not in result:
            result.append(line[0:len(line) - 1])
    return result

def getCounter():
    count = 0;
    for x in unigram:
        if "count" in x:
            count = count + x["count"] + x["count"]*(1+ x["count"])/2
    return count

def getCounterWithClass(unigram):
    count = 0;
    for x in unigram:
        if x.count > -1:
            count = count + x.count+ x.count*(1+ x.count)/2
    return count

def _get_counter(unigram_dict):
    count = 0;
    for x in unigram_dict.values():
        if "count" in x:
            count = count + x["count"] + x["count"]*(1+ x["count"])/2
    return count

def _calculate_robability(prevCount, globalCounter , ngramCount):
    if globalCounter != -1:
        if ngramCount > 0:
            probability = math.log2(ngramCount / max(globalCounter , defines.ASSUMED_MIN_COUNTS[prevCount])) * defines.PROBABILITY_ENCODING_SCALER + 255
        else:
            probability = 0
        if probability < 0:
            probability = 0
        probability = min(probability + 0.5 , 255) + defines.ENCODED_BACKOFF_WEIGHTS[prevCount]
        probability = min(max(probability, -1), 255)
    else:
        probability = -1
    return int(probability)

def calculateProbability(count,globalCounter,ngram):
    if "count" in ngram:
        if ngram["count"] != 0:
            probability = math.log2(ngram["count"] / max(globalCounter , defines.ASSUMED_MIN_COUNTS[count])) * defines.PROBABILITY_ENCODING_SCALER + 255
        else:
            probability = 0
        if probability < 0:
            probability = 0
        probability = min(probability + 0.5 , 255) + defines.ENCODED_BACKOFF_WEIGHTS[count]
        probability = min(max(probability, -1), 255)
    else:
        probability = -1
    return probability

def calculateProbabilityWithClass(count,globalCounter,ngram):
    if ngram.count > -1:
        if ngram.count != 0:
            probability = math.log2(ngram.count / max(globalCounter , defines.ASSUMED_MIN_COUNTS[count])) * defines.PROBABILITY_ENCODING_SCALER + 255
        else:
            probability = 0
        if probability < 0:
            probability = 0
        probability = min(probability + 0.5 , 255) + defines.ENCODED_BACKOFF_WEIGHTS[count]
        probability = min(max(probability, -1), 255)
    else:
        probability = -1
    return probability

def getProbabilityUni(unigram):
    globalCounter = getCounter()
    for x in unigram:
        x['f'] = math.floor(calculateProbability(0 ,globalCounter, x))
        proLogUni.append(math.floor(calculateProbability(0 ,globalCounter, x)))
        # print(x["word"] + " " + format(probability))

def getProbabilityUniWithClass(unigram):
    globalCounter = getCounterWithClass(unigram)
    for x in unigram:
        x.pro = math.floor(calculateProbabilityWithClass(0 ,globalCounter, x))
        # proLogUni.append(math.floor(calculateProbability(0 ,globalCounter, x)))

def _get_probability_uni(unigram_dict):
    globalCounter = _get_counter(unigram_dict)
    for idx, x in unigram_dict.items():
        x['f'] = math.floor(calculateProbability(0 ,globalCounter, x))
        proLogUni.append(math.floor(calculateProbability(0 ,globalCounter, x)))
        # print(x["word"] + " " + format(probability))

def _prev_array_to_string(ngram):
    temp = [] + ngram["prevArray"]
    temp.reverse()
    result = ""
    for index, item in enumerate(temp):
        if index != len(temp) - 1:
            result = result + item + " "
        else:
            result = result + item
    return result

def mergePrevArray(ngram):
    temp = [] + ngram["prevArray"]
    temp.reverse()
    result = ""
    for index, item in enumerate(temp):
        if index != len(temp) - 1:
            result = result + item + " "
        else:
            result = result + item
    return result

def mergeNgram(ngram):
    temp = [] + ngram["prevArray"]
    temp.reverse()
    result = ""
    for index, item in enumerate(temp):
        if index != len(temp):
            result = result + item + " "
        else:
            result = result + item
    return result + ngram["ngram"]

def getProbabilityNgram(unigram,ngram,k,n):
    # for x in ngram:
    for i in range(k, n + 1):
        # print(i)
        x = ngram[i]
        if "count" not in x:
            probability = -1
        else:
            prevWordCount = len(x['prevArray'])
            if prevWordCount == 1:
                tmp = BinarySearchUni(unigram, x["prevArray"][0])
                if tmp > -1:
                    if "count" in unigram[tmp]:
                        probability = calculateProbability(prevWordCount, unigram[tmp]["count"], x)
                    else:
                        probability = -1
                # for y in unigram:
                #     if y["word"] == x["prevArray"][0]:
                #         if "count" in y:
                #             probability = calculateProbability(prevWordCount, y["count"], x)
                #             break;
                #         else:
                #             probability = -1
            else:
                t = time.time()
                tmp = BinarySearchNgram(ngram, mergePrevArray(x))
                # print(time.time() - t)
                if time.time() - t > 500* 10**(-3):
                    print("teo")
                if tmp > -1:
                    if "count" in ngram[tmp]:
                        probability = calculateProbability(prevWordCount, ngram[tmp]["count"], x)
                    else:
                        probability = -1
                # for y in ngram:
                #     if mergeNgram(y) == mergePrevArray(x):
                #         if "count" in y:
                #             probability = calculateProbability(prevWordCount, y["count"], x)
                #             break;
                #         else:
                #             probability = -1
        x['f'] = math.floor(probability)
        proLogNgram.append(math.floor(probability))

def getProbabilityNgramWithClass(unigram,ngram,k,n):
    # for x in ngram:
    for i in range(k, n + 1):
        # print(i)
        x = ngram[i]
        prevWordArray = x.key.split()
        prevWordArray.reverse()
        prevWordArray.pop(0)
        probability = -1
        if x.count <= -1:
            probability = -1
        else:
            prevWordCount = x.type
            if prevWordCount == 1:
                if len(prevWordArray) == 0:
                    tmp = BinarySearchNgramWithClass(unigram, "")
                else:
                    tmp = BinarySearchNgramWithClass(unigram, prevWordArray[0])
                if tmp > -1:
                    if unigram[tmp].count > -1:
                        probability = calculateProbabilityWithClass(prevWordCount, unigram[tmp].count, x)
                    else:
                        probability = -1
            else:
                t = time.time()
                prevWordArray.reverse()
                tmp = BinarySearchNgramWithClass(ngram, " ".join(prevWordArray))
                # print(time.time() - t)
                if time.time() - t > 500* 10**(-3):
                    print("teo")
                if tmp > -1:
                    if ngram[tmp].count > -1:
                        probability = calculateProbabilityWithClass(prevWordCount, ngram[tmp].count, x)
                    else:
                        probability = -1
        x.pro = math.floor(probability)
        # proLogNgram.append(math.floor(probability))

def _get_probability_ngram(unigram_dict,ngram_dict):
    for idx, x in ngram_dict.items():
        if "count" not in x:
            probability = -1
        else:
            prevWordCount = len(x['prevArray'])
            if prevWordCount == 1:
                if x["prevArray"][0] in unigram_dict:
                    if "count" in unigram_dict[x["prevArray"][0]]:
                        probability = calculateProbability(prevWordCount, unigram_dict[x["prevArray"][0]]["count"], x)
                    else:
                        probability = -1
            else:
                if mergePrevArray(x) in ngram_dict:
                    if "count" in ngram_dict[mergePrevArray(x)]:
                        probability = calculateProbability(prevWordCount, ngram_dict[mergePrevArray(x)]["count"], x)
                    else:
                        probability = -1
        x['f'] = math.floor(probability)
        proLogNgram.append(math.floor(probability))

def flotDiagram(proUni,proLogUni,proXUni):
    # plt.bar(proXUni, proUni)
    plt.plot(proXUni, proUni, color = 'blue', marker = "*")
    plt.plot(proXUni, proLogUni, color = 'red', marker = "o")
    plt.show()

def flotLogDiagram(proXUni,proUni,color, marker):
    plt.plot(proXUni, proUni, color = color, marker = marker)
    plt.show()

def isNgramInDictionary(word,ngram):
    for idx, val in enumerate(ngram):
        if mergeNgram(word) == mergeNgram(val):
            return idx
    return -1

def isInDictionary(word, ngram):
    for idx, val in enumerate(ngram):
        if word == format(val["word"]):
            return idx
    return -1

def clear():
    ngramArray.clear()
    unigram.clear()
    ngram.clear()

    proUni.clear()
    proLogUni.clear()
    proXUni.clear()

    proNgram.clear()
    proLogNgram.clear()
    proXNgram.clear()

    ngramDict.clear()
    unigramDict.clear()

def BinarySearchUni(lys, val):
    first = 0
    last = len(lys)-1
    index = -1
    while (first <= last) and (index == -1):
        mid = (first+last)//2
        if lys[mid]["word"] == val:
            index = mid
        else:
            if val < lys[mid]["word"]:
                last = mid -1
            else:
                first = mid +1
    return index

def BinarySearchNgram(lys, val):
    first = 0
    last = len(lys)-1
    index = -1
    while (first <= last) and (index == -1):
        mid = (first+last)//2
        if mergeNgram(lys[mid]) == val:
            index = mid
        else:
            if val < mergeNgram(lys[mid]):
                last = mid -1
            else:
                first = mid +1
    return index

def BinarySearchNgramWithClass(lys, val):
    first = 0
    last = len(lys)-1
    index = -1
    while (first <= last) and (index == -1):
        mid = (first+last)//2
        if lys[mid].key == val:
            index = mid
        else:
            if val < lys[mid].key:
                last = mid -1
            else:
                first = mid +1
    return index