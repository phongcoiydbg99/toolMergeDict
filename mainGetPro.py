import csv
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

def _float_to_int(s):
    # if math.isnan(s):
    #     return -1
    return int(s)

def _get_probability_for_dict(file1,file2,k):
    if k == -1:
        globalCounters = _get_counter_v3()
        print(globalCounters)
        filePathDic = pathDic + "/" + file1
        col_Names=['key','type','prev_array','count','f']
        chunkDic = pd.read_csv(filePathDic, names=col_Names, low_memory=False)
        chunkDic = chunkDic.set_index('key')
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

def _merge_pro(file1,file2,file3):
    filePathDic1 = pathDic + "/" + file1
    filePathDic2 = pathDic + "/" + file2
    filePathDic3 = pathDic + "/" + file3
    print(filePathDic1)
    col_Names=['key','type','prev_array','count','f']
    df1 = pd.read_csv(filePathDic1, names=col_Names, low_memory=False)
    df2 = pd.read_csv(filePathDic2, names=col_Names, low_memory=False)
    for idx, r in df1.iterrows():
        dk = df2[df2['key'] == r['key']]
        if dk.shape[0] > 0:
            d = dk.iloc[0]
            my_dict = { 'key' : [r['key']],
                        'type' : [r['type']],
                        'prev_array': [r['prev_array']],
                        'count':[r['count']],
                        'f':[d['f']]
                        }
            out=pd.DataFrame(my_dict)
            out.to_csv(filePathDic3, mode="a", index=False, header=False)
        else:
            my_dict = { 'key' : [r['key']],
                        'type' : [r['type']],
                        'prev_array': [r['prev_array']],
                        'count':[r['count']],
                        'f':[r['f']]
                        }
            out=pd.DataFrame(my_dict)
            out.to_csv(filePathDic3, mode="a", index=False, header=False)

def _dic_to_text(name,file):
    filePathDic1 = pathDic + "/" + file
    filePathtext1 = pathDic + "/" + name
    print(filePathDic1)
    col_Names=['key','type','prev_array','count','f']
    df1 = pd.read_csv(filePathDic1, names=col_Names, low_memory=False)
    df1 = df1[["key","f"]]
    df1.to_csv(filePathtext1, mode="w",index=False, header=False,  sep=' ')

# Tinh xac suat
# _get_probability_for_dict("main_dict_0.csv","main_dict_0.csv",-1)
# _get_probability_for_dict("main_dict_1.csv","main_dict_0.csv",1)
# _get_probability_for_dict("main_dict_2.csv","main_dict_1.csv",1)
# _get_probability_for_dict("main_dict_3.csv","main_dict_2.csv",1)

# VIet ra file text
_dic_to_text("word_uni_prob.txt","main_dict_0_checked.csv")
_dic_to_text("word_bi_prob.txt","main_dict_1_checked.csv")
_dic_to_text("word_tri_prob.txt","main_dict_2_checked.csv")
_dic_to_text("word_quad_prob.txt","main_dict_3_checked.csv")

# Ghep xac xuat 2 tu dien
# _merge_pro("main_dict_current_0.csv","main_dict_0.csv","main_dict_current_uni.csv")
# _merge_pro("main_dict_current_1.csv","main_dict_1.csv","main_dict_current_bi.csv")
# _merge_pro("main_dict_current_2.csv","main_dict_2.csv","main_dict_current_tri.csv")
# _merge_pro("main_dict_current_3.csv","main_dict_3.csv","main_dict_current_quad.csv")