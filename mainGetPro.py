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

def _get_counter_v3():
    result = 0;
    file = "main_dict_0.csv"
    filePath = pathDic + "/" + file
    col_Names=['key','type','prev_array','count','f']
    chunk = pd.read_csv(filePath, names=col_Names)
    # chunk.set_index('key', inplace=True)
    filter_count = (chunk['count'] >= 0) & (chunk['type'] == 0)
    # chunk.loc[filter_count,'count'] = chunk.loc[filter_count,'count'] + chunk.loc[filter_count,'count']*(1+ chunk.loc[filter_count,'count'])/2
    result = result + chunk.loc[filter_count,'count'].sum()
    return int(result)

def _find_count(s, dic , k):
    if s == "":
        s = "0"
    print(s)
    if len(dic) >= 100000:
        dic = {}
    if s in dic:
        return dic[s]
    if k == 0:
        fol = "uni"
    if k == 1:
        fol = "bi"
    if k == 2:
        fol = "tri"
    filePath = pathDic + "/" + fol
    for file in os.listdir(filePath):
        col_names=['key','type','prev_array','count','f']
        filepath = filePath + "/" + file
        chunk = pd.read_csv(filepath, names=col_names, low_memory=False)
        chunk.set_index('key')
        chunk = chunk.fillna("")
        filter_count = (chunk['key'] == s)
        try:
            dk = chunk.loc[filter_count]
            result = int(dk['count'])
            break
        except:
            result = -1
    if s not in dic:
        dic[s] = result
    return result

def _float_to_int(s):
    # if math.isnan(s):
    #     return -1
    return int(s)

def _get_probability_for_dict(file1,file2,k):
    if k == -1:
        globalCounters = _get_counter_v3()
        print(globalCounters)
        filePathDic = pathDic + "/" + file
        col_Names=['key','type','prev_array','count','f']
        chunkDic = pd.read_csv(filePathDic, names=col_Names, low_memory=False)
        chunkDic.set_index('key')
        chunkDic = chunkDic.fillna("")
        chunkDic["f"] = chunkDic.apply(lambda x: readFile._calculate_robability(0,globalCounters,x['count']), axis=1)
        chunkDic.to_csv(filePathDic, mode="w", header=False)
    else:
        filePathDic1 = pathDic + "/" + file1
        filePathDic2 = pathDic + "/" + file2
        print(filePathDic1)
        print(filePathDic2)
        col_Names=['key','type','prev_array','count','f']
        df1 = pd.read_csv(filePathDic1, names=col_Names, low_memory=False)
        df1 = df1.fillna("")
        df1 = df1.set_index('prev_array')
        # print(df1.head(10))

        df2 = pd.read_csv(filePathDic2, names=col_Names, low_memory=False)
        df2 = df2.fillna("")
        df2 = df2[["key","count"]]
        df2 = df2.rename(columns = {'key': 'prev_array', 'count': 'count1'}, inplace = False)
        df2 = df2.set_index('prev_array')
        # print(df2.head(10))

        result = pd.merge(df1, df2, how="left", on='prev_array')
        result = result.fillna(-1)
        result["f"] = result.apply(lambda x: readFile._calculate_robability(_float_to_int(x['type']),x['count1'],x['count']), axis=1)
        result = result.reset_index()
        result = result[col_Names]
        print(result.head(10))
        result.to_csv(filePathDic1, mode="w",index=False, header=False)
        print("DONE: " + filePathDic1)

# _get_probability_for_dict("main_dict_0.csv","main_dict_0.csv",-1)
_get_probability_for_dict("main_dict_1.csv","main_dict_0.csv",1)
_get_probability_for_dict("main_dict_2.csv","main_dict_1.csv",1)
_get_probability_for_dict("main_dict_3.csv","main_dict_1.csv",1)