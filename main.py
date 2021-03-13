from threading import Thread
import copy
import threading
import readFile
import writeFile
import json
import os, sys, mmap
import time

folder = "test"
file = "11112020"
path = "/media/phongnve/HDD/Guide/DictTool_0.2/DictTool/file/test/DicText/"+folder+"/"
globalCounters = 0
UNIGRAM = []
NGRAM = []
UNIGRAM_DICT = {}
NGRAM_DICT = {}

def mergeDictionaryWithClass(name):
    readFile.clear()
    readFile.readFile(name)
    # Unigram
    print("Add uni")
    for x in readFile.unigram:
        # tmp = readFile.isInDictionary(x["word"], UNIGRAM)
        tmp = readFile.BinarySearchNgramWithClass(UNIGRAM , x.key)
        if tmp > -1:
            # print(UNIGRAM[tmp].key)
            UNIGRAM[tmp].count = UNIGRAM[tmp].count + x.count
        else:
            # print(x.key)
            UNIGRAM.append(x)
            UNIGRAM.sort(key = lambda x: x.key)
    # Ngram
    print("Add nrg ")
    for idx, x in enumerate(readFile.ngram):
        # tmp = readFile.isNgramInDictionary(x, NGRAM)
        t = time.time()
        tmp = readFile.BinarySearchNgramWithClass(NGRAM, x.key)
        # print(format(idx) + "/" + format(len(readFile.ngram)) + " : " + format(tmp))
        # print(time.time() - t)
        if time.time() - t > 500* 10**(-3):
            print("teo")
        if tmp > -1:
            NGRAM[tmp].count = NGRAM[tmp].count + x.count
        else:
            NGRAM.append(x)
            NGRAM.sort(key = lambda x: x.key)

def mergeDictionary(name):
    readFile.clear()
    readFile.readFile(name)
    # Unigram
    print("Add uni")
    for x in readFile.unigram:
        # tmp = readFile.isInDictionary(x["word"], UNIGRAM)
        tmp = readFile.BinarySearchUni(UNIGRAM , x["word"])
        if tmp > -1:
            if "count" in x:
                if "count" in UNIGRAM[tmp]:
                    if x["count"] != 0:
                        UNIGRAM[tmp]["count"] = UNIGRAM[tmp]["count"] + x["count"]
                else:
                    UNIGRAM[tmp] = x
        else:
            UNIGRAM.append(x)
            UNIGRAM.sort(key = lambda x: x["word"])
    # Ngram
    print("Add nrg " + format(len(readFile.ngram)))
    for idx, x in enumerate(readFile.ngram):
        # tmp = readFile.isNgramInDictionary(x, NGRAM)
        print(format(idx) + "/" + format(len(readFile.ngram)) + " : " + format(tmp))
        t = time.time()
        tmp = readFile.BinarySearchNgram(NGRAM, readFile.mergeNgram(x))
        print(time.time() - t)
        if time.time() - t > 500* 10**(-3):
            print("teo")
        if tmp > -1:
            if "count" in x:
                if "count" in NGRAM[tmp]:
                    if x["count"] != 0:
                        NGRAM[tmp]["count"] = NGRAM[tmp]["count"] + x["count"]
                else:
                    NGRAM[tmp] = x
        else:
            NGRAM.append(x)
            NGRAM.sort(key = lambda x: readFile.mergeNgram(x))

def main(s):
    for file in os.listdir(s):
        print(file)
        mergeDictionaryWithClass(s + file)
    # print(UNIGRAM)
    # print(NGRAM)

def tool():
    readFile.clear()
    readFile.readFileWithClass("DicTestV2.txt")
    global UNIGRAM
    global NGRAM
    UNIGRAM = copy.deepcopy(readFile.unigram)
    NGRAM = copy.deepcopy(readFile.ngram)
    print("start")
    for x in range(19,20):
        # if x <= 9:
        #     s = path + "0" + str(x) + folder + "/"
        # else:
        #     s = path + str(x) + folder + "/"
        s = s = path + folder + "/"
        print(s)
        main(s)
    print("get pro uni")
    readFile.getProbabilityUniWithClass(UNIGRAM)
    print("get pro ngr")
    lengthNgram = len(NGRAM)
    temp = int(lengthNgram / 9)
    print(temp)
    if lengthNgram > 9:
        print("Thread")
        try:
            t = time.time()
            t1 = threading.Thread(target=readFile.getProbabilityNgramWithClass, args=(UNIGRAM, NGRAM, 0, temp))
            # print(temp)
            t2 = threading.Thread(target=readFile.getProbabilityNgramWithClass, args=(UNIGRAM, NGRAM, temp, temp*2))
            # print(format(temp) +" " + format(temp*2) )
            t3 = threading.Thread(target=readFile.getProbabilityNgramWithClass, args=(UNIGRAM, NGRAM, temp*2, temp*3))
            # print(format(temp*2) +" " + format(temp*3) )
            t4 = threading.Thread(target=readFile.getProbabilityNgramWithClass, args=(UNIGRAM, NGRAM, temp*3, temp*4))
            # print(format(temp*3) +" " + format(temp*4) )
            t5 = threading.Thread(target=readFile.getProbabilityNgramWithClass, args=(UNIGRAM, NGRAM, temp*4, temp*5))
            # print(format(temp*4) +" " + format(temp*5) )
            t6 = threading.Thread(target=readFile.getProbabilityNgramWithClass, args=(UNIGRAM, NGRAM, temp*5, temp*6))
            # print(format(temp*5) +" " + format(temp*6) )
            t7 = threading.Thread(target=readFile.getProbabilityNgramWithClass, args=(UNIGRAM, NGRAM, temp*6, temp*7))
            # print(format(temp*6) +" " + format(temp*7) )
            t8 = threading.Thread(target=readFile.getProbabilityNgramWithClass, args=(UNIGRAM, NGRAM, temp*7, temp*8))
            # print(format(temp*7) +" " + format(temp*8) )
            t9 = threading.Thread(target=readFile.getProbabilityNgramWithClass, args=(UNIGRAM, NGRAM, temp*8, lengthNgram-1))
            # print(format(temp*8) +" " + format(lengthNgram-1) )

            t1.start()
            t2.start()
            t3.start()
            t4.start()
            t5.start()
            t6.start()
            t7.start()
            t8.start()
            t9.start()

            t1.join()
            t2.join()
            t3.join()
            t4.join()
            t5.join()
            t6.join()
            t7.join()
            t8.join()
            t9.join()

            print ("done in ", time.time()- t)
        except:
            print ("error")
    else:
        readFile.getProbabilityNgramWithClass(UNIGRAM,NGRAM,0,len(NGRAM) -1)
    writeFile.writeDictionaryWithClass(UNIGRAM, NGRAM, "DicTestV2.txt")

def _merger_dictionary(name):
    readFile.clear()
    readFile._read_file(name)
    # Unigram
    print("Add uni")
    for idx, item in readFile.unigramDict.items():
        if idx in UNIGRAM_DICT:
            if "count" in item:
                if "count" in UNIGRAM_DICT[idx]:
                    UNIGRAM_DICT[idx]["count"] =  UNIGRAM_DICT[idx]["count"] + item["count"]
                else:
                    UNIGRAM_DICT[idx] = copy.deepcopy(item)
        else:
            UNIGRAM_DICT[idx] = copy.deepcopy(item)
    # Ngram
    print("Add nrg ")
    for idx, item in readFile.ngramDict.items():
        if idx in NGRAM_DICT:
            if "count" in item:
                if "count" in NGRAM_DICT[idx]:
                    NGRAM_DICT[idx]["count"] = NGRAM_DICT[idx]["count"] + item["count"]
                else:
                    NGRAM_DICT[idx] = copy.deepcopy(item)
        else:
            NGRAM_DICT[idx] = copy.deepcopy(item)

def _main_v2(s):
    for file in os.listdir(s):
        print(file)
        _merger_dictionary(s + file)
        # writeFile._write_dictionary(UNIGRAM_DICT, NGRAM_DICT, "DicTest.txt")

def _tool():
    readFile.clear()
    readFile._read_file("DicTest.txt")
    global UNIGRAM_DICT
    global NGRAM_DICT
    UNIGRAM_DICT = copy.deepcopy(readFile.unigramDict)
    NGRAM_DICT = copy.deepcopy(readFile.ngramDict)
    # UNIGRAM_DICT = copy.deepcopy(_read_dic_to_json('unigram_json.txt'))
    # NGRAM_DICT = copy.deepcopy(_read_dic_to_json('ngram_json.txt'))
    print("start")
    for x in range(20,21):
        # if x <= 9:
        #     s = path + "0" + str(x) + folder + "/"
        # else:
        #     s = path + str(x) + folder + "/"
        s = s = path + folder + "/"
        print(s)
        _main_v2(s)
    print("get pro uni")
    readFile._get_probability_uni(UNIGRAM_DICT)
    print("get pro ngr")
    readFile._get_probability_ngram(UNIGRAM_DICT,NGRAM_DICT)
    writeFile._write_dictionary(UNIGRAM_DICT, NGRAM_DICT, "DicTest.txt")

def _write_dic_to_text():
    readFile.clear()
    readFile.readFile("DicTest.txt")
    global UNIGRAM_DICT
    global NGRAM_DICT
    UNIGRAM_DICT = copy.deepcopy(readFile.unigramDict)
    NGRAM_DICT = copy.deepcopy(readFile.ngramDict)
    writeFile._write_dic_to_text(UNIGRAM_DICT, NGRAM_DICT)

def _write_dic_to_json(name):
    readFile.clear()
    readFile.readFile(name)
    with open('unigram_json.txt', 'w') as uni_json_file:
        json.dump(readFile.unigramDict, uni_json_file)

    with open('ngram_json.txt', 'w') as ngr_json_file:
        json.dump(readFile.ngramDict, ngr_json_file)

def _read_dic_to_json(name):
    with open(name) as json_file:
        return json.load(json_file)

# main()
# _main_v2()
# _write_dic_to_text()

# Tool
tool()
# _tool()

# Write file to json
# _write_dic_to_json("DicTest.txt")
# print(_read_dic_to_json('unigram_json.txt'))
# readFile.ngram.sort(key = lambda x: readFile.mergeNgram(x))
# # readFile.ngram = sorted(readFile.ngram, key = lambda x: x["ngram"])
# print(readFile.ngram)
#
# readFile.readFile("test.txt.txt")
# UNIGRAM_DICT = copy.deepcopy(readFile.unigramDict)
# UNIGRAM_DICT = copy.deepcopy(readFile.ngramDict)
# readFile.clear()
# print(UNIGRAM_DICT)
# print(UNIGRAM_DICT)
# print("__________________________")
# readFile.readFileWithClass("DicTest.txt")
# UNIGRAM = readFile.unigram
# NGRAM = readFile.ngram
# print(readFile.BinarySearchNgramWithClass(UNIGRAM,"có"))


# with open("test.txt.txt", buffering=200000)
# print(writeFile.dumpNgram(readFile.ngram[0]))
# print(writeFile.dumpUnigram(readFile.unigram[0]))
# with open('DicTest1.txt') as f:
#     if '"sinh ","mừng ","chúc"' in f.read():
#         print("true")

# readFile.readFile("test.txt.txt")
#
# readFile.getProbabilityUni(readFile.unigram)
# readFile.getProbabilityNgram(readFile.unigram, readFile.ngram)
#
# readFile.flotLogDiagram(readFile.proXUni, readFile.proUni, 'blue', "*")
# readFile.flotLogDiagram(readFile.proXUni, readFile.proLogUni, 'red', "o")
# readFile.flotLogDiagram(readFile.proXNgram, readFile.proNgram, 'blue', "*")
# readFile.flotLogDiagram(readFile.proXNgram, readFile.proLogNgram, 'red', "o")
