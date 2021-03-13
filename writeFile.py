import json
import readFile
import pandas as pd
import os, sys, mmap
pathDic = '/media/phongnve/HDD/source_bkav/dictionary_csv'
def dumpNgram(ngram):
    # {"ngram": "n\u00ean", "f": 237, "timestamp": 0, "level": 0, "count": 1, "ngramType": 1, "prevArray": ["C\u00f3"]}
    result = "{\"ngram\" : \""+ ngram["ngram"]+"\" , \"f\": "+ format(ngram["f"])+ ", "
    if "timestamp" in ngram:
        result = result + "\"timestamp\": "+ format(ngram["timestamp"]) + ", "
    if "level" in ngram:
        result = result + "\"level\": "+ format(ngram["level"]) + ", "
    if "count" in ngram:
        result = result + "\"count\": "+ format(ngram["count"]) + ", "
    if "ngramType" in ngram:
        result = result + "\"ngramType\": "+ format(ngram["ngramType"]) + ", "
    result = result + "\"prevArray\": ["
    for idx, val in enumerate(ngram["prevArray"]):
        if idx == len(ngram["prevArray"]) -1:
            result = result + "\""+ val +"\""
        else:
            result = result + "\""+ val +"\","
    result = result + "]}"
    return result
def dumpUnigram(unigram):
    # {"ngram": "n\u00ean", "f": 237, "timestamp": 0, "level": 0, "count": 1, "ngramType": 1, "prevArray": ["C\u00f3"]}
    result = "{\"word\" : \""+ unigram["word"]+"\" , \"f\": "+ format(unigram["f"])+ ", "
    if "timestamp" in unigram:
        result = result + "\"timestamp\": "+ format(unigram["timestamp"]) + ", "
    if "level" in unigram:
        result = result + "\"level\": "+ format(unigram["level"]) + ", "
    if "count" in unigram:
        result = result + "\"count\": "+ format(unigram["count"])
    result = result + "}"
    return result
def writeDictionary(unigram, ngram, name):
    f = open(name, "w")
    # Write unigram
    for x in unigram:
        f.write(dumpUnigram(x)+"\n")
    #  Write ngram
    for x in ngram:
        f.write(dumpNgram(x)+"\n")
    f.close()

def writeDictionaryWithClass(unigram, ngram, name):
    f = open(name, "w")
    # Write unigram
    for x in unigram:
        f.write("{}:{}:{}:{}\n".format(x.key,x.count,0,x.pro))
    #  Write ngram
    for x in ngram:
        f.write("{}:{}:{}:{}\n".format(x.key,x.count,x.type,x.pro))
    f.close()

def _write_dictionary(unigram_dict, ngram_dict, name):
    f = open(name, "w")
    # Write unigram
    for x in unigram_dict.values():
        f.write(dumpUnigram(x)+"\n")
    #  Write ngram
    for x in ngram_dict.values():
        f.write(dumpNgram(x)+"\n")
    f.close()

def _write_unigram(unigram_dict, name):
    f = open(name, "w")
    # Write unigram
    for x in unigram_dict.values():
        f.write(dumpUnigram(x)+"\n")
    f.close()

def writeUnigram(unigram, name):
    f = open(name, "w")
    # Write unigram
    for x in unigram:
        f.write(dumpUnigram(x)+"\n")

def _write_dic_to_text(unigram_dict, ngram_dict):
    f_uni = open("word_uni_prob.txt", "w")
    f_bi = open("word_bi_prob.txt", "w")
    f_tri = open("word_tri_prob.txt", "w")
    f_quad = open("word_quad_prob.txt", "w")
    unigram = readFile.readFileUni("unigram_check.txt")
    # print("ách" in unigram)
    unigram_array = []
    print("ADD UNI")
    for x in unigram_dict.values():
        if x["word"].lower() in unigram:
            unigram_array.append(x["word"] + " " + str(x["f"]) + "\n")
            # f_uni.write(x["word"] + " " + str(x["f"]) + "\n")
    unigram_array.sort(key = lambda x: x.split()[0])
    for x in unigram_array:
        f_uni.write(x)
    f_uni.close()
    #  Write ngram
    print("ADD NGRAM")
    for x in ngram_dict.values():
        prevCount = len(x["prevArray"])
        check_word = True
        if x["ngram"].lower() in unigram:
            for y in x["prevArray"]:
                s = y.replace(" ", "")
                if not s.lower() in unigram:
                    check_word = False
                    break
            if check_word == True:
                if prevCount == 1:
                    f_bi.write(readFile.mergeNgram(x) + " " + str(x["f"]) + "\n")
                if prevCount == 2:
                    f_tri.write(readFile.mergeNgram(x) + " " + str(x["f"]) + "\n")
                if prevCount == 3:
                    f_quad.write(readFile.mergeNgram(x) + " " + str(x["f"]) + "\n")
    f_bi.close()
    f_tri.close()
    f_quad.close()

def _dic_to_text(s):
    f_uni = open("word_uni_prob.txt", "w")
    f_bi = open("word_bi_prob.txt", "w")
    f_tri = open("word_tri_prob.txt", "w")
    f_quad = open("word_quad_prob.txt", "w")
    # unigram = readFile.readFileUni("unigram_check.txt")
    # print("ách" in unigram)
    for file in os.listdir(s):
        col_Names=["key", "f", "count", "type", "prev_array"]
        filePath = pathDic + "/" + file
        chunk = pd.read_csv(filePath, names=col_Names)
        for index, x in chunk.iterrows():
            print(x)
            if x['type'] == 0:
                f_uni.write(x["key"] + " " + str(x["f"]) + "\n")
            if x['type'] == 1:
                f_bi.write(x["key"] + " " + str(x["f"]) + "\n")
            if x['type'] == 2:
                f_tri.write(x["key"] + " " + str(x["f"]) + "\n")
            if x['type'] == 3:
                f_quad.write(x["key"] + " " + str(x["f"]) + "\n")
    f_uni.close()
    f_bi.close()
    f_tri.close()
    f_quad.close()
