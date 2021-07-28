import os
import sys
import pandas as pd
import readFile
import time
from threading import Thread

folder = "022021"
file = "11112020"
path = "/media/phongnve/HDD/Guide/DictTool_0.2/DictTool/file/test/DicText/"+folder+"/"
pathDic = '/media/phongnve/HDD/source_bkav/dict_csv/main/main'
pathDicCsv = "/media/phongnve/HDD/source_bkav/dict_csv/"
globalCounters = 0
chunkSize = 10000
UNIGRAM = []
NGRAM = []
UNIGRAM_DICT = {}
NGRAM_DICT = {}
WORD = []

def _in_dict(s,f):
    s = str(s)
    if s == "0":
        return True
    for x in s.split(" "):
        if x != "":
            if x not in f:
                return False
    return True

def _tool_check_in_word(name,name1):
    pathMainDict = '/media/phongnve/HDD/source_bkav/dict_csv/main/main/'+name
    pathMainDict1 = '/media/phongnve/HDD/source_bkav/dict_csv/main/main/'+name1
    col = ['key','type','prev_array','count','f']
    print(pathMainDict)
    df = pd.read_csv(pathMainDict, names=col, low_memory=False)
    # os.remove(pathMainDict)
    # len = df.shape[0]
    fileDict = open("word.txt", "r")
    f = fileDict.read()
    dk =  df[df.apply(lambda x: _in_dict(x['key'],f), axis=1)]
    dk.to_csv(pathMainDict1, mode='w',header=False, index=False)
    # for index,row in df.iterrows():
    #     if _in_dict(row['key'],f) == True:
    #         print("{}/{} : ".format(index,len) + name)
    #         row.to_csv(pathMainDict1, mode='a',header=False, index=False)

def _tool_pro(fol):
    threads = []
    filePath = pathDic + "/" + fol
    for file in os.listdir(filePath):
        print(file)
        t = Thread(target=_tool_check_in_word, args=(file,file,fol,))
        t.start()
        threads.append(t)

    # Wait all threads to finish.
    for t in threads:
        t.join()

# Tool loc theo file word.txt (file tieng viet uni)
# _tool_check_in_word("main_dict_current_0.csv","main_dict_0_checked.csv")
# _tool_check_in_word("main_dict_current_1.csv","main_dict_1_checked.csv")
# _tool_check_in_word("main_dict_current_2.csv","main_dict_2_checked.csv")
# _tool_check_in_word("main_dict_current_3.csv","main_dict_3_checked.csv")

# _tool_pro("bi")