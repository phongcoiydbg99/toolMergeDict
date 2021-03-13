from threading import Thread
import copy
import threading
import readFile
import writeFile
import json
import math
import os, sys, mmap
import time
import pandas as pd
import numpy as np
import defines

folder = "112020"
file = "11112020"
path = "/media/phongnve/HDD/Guide/DictTool_0.2/DictTool/file/test/DicText/"+folder+"/"
pathDic = '/media/phongnve/HDD/source_bkav/dict_csv/main/main'
globalCounters = 0
chunkSize = 1000000
UNIGRAM = []
NGRAM = []
UNIGRAM_DICT = {}
NGRAM_DICT = {}

# readFile._file_transfer("main_vi.dict_chinh_quy_bo_ky_tu.txt")
# Lan dau
# UNIGRAM_DICT = readFile._file_transfer("text.txt")
# UNIGRAM_DICT = readFile._file_transfer("main_vi.dict_chinh_quy_bo_ky_tu.txt")
# readFile._get_probability(UNIGRAM_DICT)
# writeFile._write_unigram(UNIGRAM_DICT, "/media/phongnve/HDD/source_bkav/dictionary/unigram_pro.txt")

def _main_v2(s):
    for file in os.listdir(s):
        print(file)
        UNIGRAM_DICT = readFile._file_transfer(s + file)
        writeFile._write_unigram(UNIGRAM_DICT, "/media/phongnve/HDD/source_bkav/dictionary/unigram_pro.txt")

def _tool_v2():
    for x in range(20,31):
        if x <= 9:
            s = path + "0" + str(x) + folder + "/"
        else:
            s = path + str(x) + folder + "/"
        print(s)
        _main_v2(s)

def _get_probability_v2():
    UNIGRAM_DICT = readFile._read_dic_to_json('/media/phongnve/HDD/source_bkav/dictionary/unigram_json.txt')
    readFile._get_probability(UNIGRAM_DICT)

def _split_file_dic_csv():
    i = 0
    for chunk in pd.read_csv('/media/phongnve/HDD/source_bkav/dict_csv/main/main_dict.csv', chunksize= chunkSize):
        chunk.to_csv('/media/phongnve/HDD/source_bkav/dict_csv/main/main/main_dict_{}.csv'.format(i), index=False, header=False)
        i = i + 1

def _merge_dic_to_csv_v2(ngram, isUnigram):
    keys = []
    fs = []
    counts = []
    types = []
    prev_arrays = []
    col_Names=["key", "f", "count", "type", "prev_array"]
    valueChunk = []
    try:
        listChunk = pd.read_csv('/media/phongnve/HDD/source_bkav/dictionary_csv/test_dic_csv.csv', chunksize= chunkSize, names=col_Names)
        if os.path.exists('/media/phongnve/HDD/source_bkav/dictionary_csv/test_dic_csv.csv'):
            os.remove('/media/phongnve/HDD/source_bkav/dictionary_csv/test_dic_csv.csv')
        else:
            print("The file does not exist")
        for chunk in listChunk:
            chunk = chunk.set_index('key')
            valueChunk.append(chunk)
    except:
        valueChunk = []
    for x in ngram.values():
        isExist = False
        t = time.time()
        if isUnigram == True:
            if x["word"] != "":
                key = x["word"]
            else:
                key = "0"
            prev_array = ""
            type = 0
        else:
            key = readFile._ngram_to_string(x)
            prev_array = readFile._prev_array_to_string(x)
            type = x['ngramType']
        if 'count' in x:
            count = x['count']
        else:
            count = 0
        for chunk in valueChunk:
            try:
                dk = chunk.loc[key,]
                isExist = True
            except:
                isExist = False
            if isExist == True:
                chunk.loc[key,"count"] = chunk.loc[key,"count"] + count
                break
            else:
                continue
        if isExist == False:
            keys.append(key)
            fs.append(x['f'])
            counts.append(count)
            types.append(type)
            prev_arrays.append(prev_array)
    for chunk in valueChunk:
        chunk.to_csv('/media/phongnve/HDD/source_bkav/dictionary_csv/test_dic_csv.csv', mode='a', header=False)
    temp = {'key' : keys, 'f': fs, 'count':counts, 'type':types, 'prev_array':prev_arrays}
    df = pd.DataFrame(temp)
    df.to_csv('/media/phongnve/HDD/source_bkav/dictionary_csv/test_dic_csv.csv', mode='a', index=False, header=False)
    valueChunk.clear()

def _merge_dic_to_csv(ngram, isUnigram):
    for x in ngram.values():
        isExist = False
        t = time.time()
        if isUnigram == True:
            if x["word"] != "":
                key = x["word"]
            else:
                key = "0"
            prev_array = ""
            type = 0
        else:
            key = readFile._ngram_to_string(x)
            prev_array = readFile._prev_array_to_string(x)
            type = x['ngramType']
        if 'count' in x:
            count = x['count']
        else:
            count = 0
        for file in os.listdir(pathDic):
            filePath = pathDic + "/" + file
            col_Names=["key", "f", "count", "type", "prev_array"]
            chunk = pd.read_csv(filePath, names=col_Names)
            chunk.set_index('key')
            try:
                dk = chunk.loc[key,]
                isExist = True
            except:
                isExist = False
            if isExist == True:
                chunk.loc[key,"count"] = chunk.loc[key,"count"] + count
                chunk.to_csv(filePath, header=False)
                break
            else:
                continue
        if isExist == False:
            temp = {'key' : [key], 'f': [x['f']], 'count':[count], 'type':[type], 'prev_array':[prev_array]}
            df = pd.DataFrame(temp)
            if os.path.isfile(pathDic + "/text_{}".format(len(os.listdir(pathDic)) - 1) + ".csv"):
                d = pd.read_csv(pathDic + "/text_{}".format(len(os.listdir(pathDic)) - 1) + ".csv")
                if d.shape[0] < chunkSize:
                    df.to_csv(pathDic + "/text_{}".format(len(os.listdir(pathDic)) - 1) + ".csv", mode='a', index=False, header=False)
                else:
                    df.to_csv(pathDic + "/text_{}".format(len(os.listdir(pathDic))) + ".csv", mode='a', index=False, header=False)
            else:
                df.to_csv(pathDic + "/text_{}".format(len(os.listdir(pathDic))) + ".csv", mode='a', index=False, header=False)
        print(time.time() - t)

def _get_counter_v3():
    result = 0;
    file = "main_dict_0.csv"
    filePath = pathDic + "/" + file
    col_Names=['key','type','prev_array','count','f']
    chunk = pd.read_csv(filePath, names=col_Names)
    # chunk.set_index('key', inplace=True)
    filter_count = (chunk['count'] >= 0) & (chunk['type'] == 0)
    chunk.loc[filter_count,'count'] = chunk.loc[filter_count,'count'] + chunk.loc[filter_count,'count']*(1+ chunk.loc[filter_count,'count'])/2
    result = result + chunk.loc[filter_count,'count'].sum()
    return int(result)

def _halve_counter():
    file = "main_dict_0.csv"
    filePath = pathDic + "/" + file
    col_Names=['key','type','prev_array','count','f']
    chunk = pd.read_csv(filePath, names=col_Names)
    column = chunk["count"]
    max_value = column.max()
    i_max_value = column.argmax()
    result = _get_counter_v3()
    while max_value >= defines.COUNTER_VALUE_NEAR_LIMIT_THRESHOLD or result >= defines.TOTAL_COUNT_VALUE_NEAR_LIMIT_THRESHOLD:
        print(max_value)
        max_value = int(max_value/2)
        chunk = pd.read_csv(filePath, names=col_Names)
        chunk.loc[i_max_value,'count'] = max_value
        chunk.to_csv(filePath, mode="w", index = False, header=False)
        chunk = pd.read_csv(filePath, names=col_Names)
        result = _get_counter_v3()
        column = chunk["count"]
        max_value = column.max()
        i_max_value = column.argmax()

def _find_count(s, dic , chunk):
    if s == "":
        s = "0"
    print(s)
    if s in dic:
        return dic[s]
    col_Names=['key','type','prev_array','count','f']
    filter_count = (chunk['key'] == s)
    try:
        dk = chunk.loc[filter_count]
        result = int(dk['count'])
    except:
        result = -1
    if s not in dic:
        dic[s] = result
    return result

def _float_to_int(s):
    # if math.isnan(s):
    #     return -1
    return int(s)

def _get_probability_v3(file,k):
    if k == -1:
        globalCounters = _get_counter_v3()
        print(globalCounters)
        dic = {}
        filePathDic = pathDic + "/" + file
        col_Names=['key','type','prev_array','count','f']
        chunkDic = pd.read_csv(filePathDic, names=col_Names, low_memory=False)
        chunkDic.set_index('key')
        chunkDic = chunkDic.fillna("")
        chunkDic["f"] = chunkDic.apply(lambda x: readFile._calculate_robability(0,globalCounters,x['count']), axis=1)
        chunkDic.to_csv(filePathDic, mode="w", header=False)
    else:
        dic = {}
        filePath = pathDic + "/main_dict_{}.csv".format(k)
        filePathDic = pathDic + "/" + file
        print(filePath)
        col_Names=['key','type','prev_array','count','f']
        chunk = pd.read_csv(filePath, names=col_Names, low_memory=False)
        chunkDic = pd.read_csv(filePathDic, names=col_Names, low_memory=False)
        chunk.set_index('key')
        chunkDic.set_index('key')
        chunk = chunk.fillna("")
        chunkDic = chunkDic.fillna("")
        chunkDic["f"] = chunkDic.apply(lambda x: readFile._calculate_robability(_float_to_int(x['type']),_find_count(x['prev_array'], dic, chunk),x['count']), axis=1)
        chunkDic.to_csv(filePathDic, mode="w", header=False)

def _main_v3(s):
    i = 0
    for file in os.listdir(s):
        print(file + " {}/{}".format(i,len(os.listdir(s))))
        readFile.clear()
        readFile._read_file(s + file)
        # _merge_dic_to_csv(readFile.unigramDict, True)
        # _merge_dic_to_csv(readFile.ngramDict, False)
        _merge_dic_to_csv_v2(readFile.unigramDict, True)
        _merge_dic_to_csv_v2(readFile.ngramDict, False)
        i = i + 1

def _first_run():
    readFile.clear()
    readFile._read_file('main_vi.dict_chinh_quy_bo_ky_tu.txt')
    _merge_dic_to_csv(readFile.unigramDict, True)
    _merge_dic_to_csv(readFile.ngramDict, False)

def _first_run_v2():
    readFile.clear()
    readFile._read_file('text.txt')
    _merge_dic_to_csv_v2(readFile.unigramDict, True)
    _merge_dic_to_csv_v2(readFile.ngramDict, False)

def _tool_v3():
    for x in range(15,20):
        if x <= 9:
            s = path + "0" + str(x) + folder + "/"
        else:
            s = path + str(x) + folder + "/"
        print(s)
        _main_v3(s)

def _dic_to_n_text():
    constants = [0,1,2,3]
    col = ['key','type','prev_array','count','f']
    for i in constants:
        iter_csv = pd.read_csv("/media/phongnve/HDD/source_bkav/dict_csv/main/main_dict.csv", names=col, chunksize=1000000)
        # iter_csv = pd.read_csv(io.StringIO(temp), delimiter=",", chunksize=10)
        #concat subset with rows id == constant
        df = pd.concat([chunk[chunk['type'] == i] for chunk in iter_csv])
        #your groupby function
        # data = df.reset_index(drop=True).groupby(["id","col1"], as_index=False).sum()
        df.to_csv ("/media/phongnve/HDD/source_bkav/dict_csv/main/main_dict_{}.csv".format(i), index=None, header=False)

# _tool_v2()
# _get_probability_v2()
# readFile._file_to_csv('text.txt',"text")
# _first_run()
# _first_run_v2()
# _tool_v3()
# _get_probability_v3()
# print(_get_counter_v3())
# print(_find_count("Có nên"))

# tool dic to text
# print("tool dic to text")
# _dic_to_n_text()
# _split_file_dic_csv()
# _halve_counter()
# _get_probability_v3("main_dict_0.csv",-1)
# _get_probability_v3("main_dict_1.csv",0)
# _get_probability_v3("main_dict_2.csv",1)
# _get_probability_v3("main_dict_3.csv",2)
# writeFile._dic_to_text('/media/phongnve/HDD/source_bkav/dict_csv/main/main')

# col = ['key','type','prev_array','count','f']
# chunk = pd.read_csv("/media/phongnve/HDD/source_bkav/backup/test_dic_csv.csv", names=col)
# chunk2 = pd.read_csv("/media/phongnve/HDD/source_bkav/backup/test_dic_csv_0.csv", names=col)
# for i in range(1,6):
#     print(chunk.loc[:,'key'])
#     print(chunk)
#     print(chunk2)


