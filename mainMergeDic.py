import os
import sys
import pandas as pd
import readFile
import time

folder = "012021"
file = "11112020"
path = "/media/phongnve/HDD/Guide/DictTool_0.2/DictTool/file/test/DicText/"+folder+"/"
pathDic = '/media/phongnve/HDD/source_bkav/dictionary_csv'
pathDicCsv = "/media/phongnve/HDD/source_bkav/dict_csv/"
globalCounters = 0
chunkSize = 10000
UNIGRAM = []
NGRAM = []
UNIGRAM_DICT = {}
NGRAM_DICT = {}

print("Halo")
constants = [0,1,2,3]
# for i in constants:
#     col = ['key','f','count','type','array']
#     iter_csv = pd.read_csv(r'E:\Python\test_csv_mer.csv', names=col, chunksize=1000000)
#     # iter_csv = pd.read_csv(io.StringIO(temp), delimiter=",", chunksize=10)
#     #concat subset with rows id == constant
#     df = pd.concat([chunk[chunk['type'] == i] for chunk in iter_csv])
#     #your groupby function
#     # data = df.reset_index(drop=True).groupby(["id","col1"], as_index=False).sum()
#     dk = df.fillna("0").groupby(['key','type','array']).agg({'count': "sum"}).reset_index()
#     dk['f'] = 0
#     print("Wow")
#     dk.to_csv (r'E:\Python\test_csv_mer_count{}.csv'.format(i), index=None, header=False)


# print(df.iloc[100])
def _merge_dict_csv_with_chunk_size(s):
    name = "main_dict.csv"
    pathMainDict = "/media/phongnve/HDD/source_bkav/dict_csv/main/"+name
    pathDic = pathDicCsv + folder + "/" + s
    col = ['key','type','prev_array','count','f']
    array_dict = []
    for x in pd.read_csv(pathMainDict, names=col, chunksize=1000000):
        array_dict.append(x)
    for file in os.listdir(pathDic):
        df = pd.read_csv(pathDic + "/" + file, names=col)
        array_dict.append(df)
    csv = pd.concat(array_dict)
    csv.to_csv('/media/phongnve/HDD/source_bkav/dict_csv/main/temp.csv', index=None, header=False)
    for i in constants:
        iter_csv = pd.read_csv('/media/phongnve/HDD/source_bkav/dict_csv/main/temp.csv', names=col, chunksize=1000000)
        #concat subset with rows id == constant
        df = pd.concat([chunk[chunk['type'] == i] for chunk in iter_csv])
        dk = df.fillna("0").groupby(['key','type','prev_array']).agg({'count': "sum"}).reset_index()
        dk['f'] = 0
        dk.to_csv(pathMainDict, mode='a',index=None, header=False)

def _merge_dict_csv(s):
    name = "main_dict.csv"
    pathMainDict = "/media/phongnve/HDD/source_bkav/dict_csv/main/"+name
    pathDic = pathDicCsv + folder + "/" + s
    col = ['key','type','prev_array','count','f']
    main_dict = pd.read_csv(pathMainDict, names=col, low_memory=False)
    array_dict = [main_dict]
    for file in os.listdir(pathDic):
        print(pathDic + "/" + file)
        df = pd.read_csv(pathDic + "/" + file, names=col)
        array_dict.append(df)
    csv = pd.concat(array_dict)
    csv.to_csv(pathMainDict, index=None, header=False)
    df = pd.read_csv(pathMainDict, names=col,low_memory=False)
    dk = df.fillna("0").groupby(['key','type','prev_array']).agg({'count': "sum"}).reset_index()
    dk['f'] = 0
    dk.to_csv(pathMainDict, index=None, header=False)

def _tool_text_to_csv():
    for x in range(1,21):
        if x <= 9:
            s = "0" + str(x) + folder
        else:
            s = str(x) + folder
        pathText = path + s + "/"
        pathDic = pathDicCsv + folder + "/" + s
        try:
            os.mkdir(pathDic)
        except OSError:
            print ("Creation of the directory %s failed" % pathDic)
        else:
            print ("Successfully created the directory %s " % pathDic)
        for file in os.listdir(pathText):
            readFile._file_to_csv(pathText+file,file,folder,s)

def _tool_merge_csv():
    for x in range(1,21):
        if x <= 9:
            s = "0" + str(x) + folder
        else:
            s = str(x) + folder
        _merge_dict_csv(s)
_tool_text_to_csv()
_tool_merge_csv()
# readFile._file_to_csv('main_vi.dict.txt',"main_dict","main","main")
