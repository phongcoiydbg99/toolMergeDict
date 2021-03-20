import os
import sys
import pandas as pd
import readFile
import time

folder = "022021"
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

def _tool_match_key(name):
    pathMainDict = '/media/phongnve/HDD/source_bkav/dict_csv/main/main/'+name
    print(pathMainDict)
    col = ['key','type','prev_array','count','f']
    main_dict = pd.read_csv(pathMainDict, names=col, low_memory=False)
    main_dict = main_dict.fillna("0")
    main_dict['key'] = main_dict.apply(lambda x: str(x['key']).lower() , axis=1)
    main_dict['prev_array'] = main_dict.apply(lambda x: str(x['prev_array']).lower(), axis=1)
    main_dict = main_dict.fillna("0").groupby(['key','type','prev_array']).agg({'count': "sum"}).reset_index()
    main_dict['f'] = 0
    # main_dict = main_dict[main_dict['count'] > 0]
    main_dict.to_csv(pathMainDict, index=False, header=False)

def _tool_text_to_csv():
    for x in range(1,29):
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
    for x in range(1,29):
        if x <= 9:
            s = "0" + str(x) + folder
        else:
            s = str(x) + folder
        _merge_dict_csv(s)

def _tool_reduce_file(name1,name2,name3):
    # file dic so sanh
    pathMainDict1 = '/media/phongnve/HDD/source_bkav/dict_csv/main/main/'+name1
    # file dict ghep
    pathMainDict2 = '/media/phongnve/HDD/source_bkav/dict_csv/main/main/'+name2
    # file tao moi
    pathMainDict3 = '/media/phongnve/HDD/source_bkav/dict_csv/main/main/'+name3
    print(pathMainDict1)
    col = ['key','type','prev_array','count','f']
    df = pd.read_csv(pathMainDict1, names=col, low_memory=False)
    main_dict = pd.read_csv(pathMainDict2, names=col, low_memory=False)
    len = df.shape[0]
    for index, row in df.iterrows():
        print("{}/{} : ".format(index,len) + str(row['key']))
        filter_prev_array = (main_dict['prev_array'] == row['key'])
        dk = main_dict[filter_prev_array]
        dk = dk.sort_values(by=['f'], ascending=False)
        out = dk.head(18)
        out.to_csv(pathMainDict3, mode="a", index=False, header=False)
    print("DONE")

readFile._file_to_csv('test.txt.txt',"main_dict_current","main","main")

# Chuyen file text thanh file csv
# _tool_text_to_csv()
# Ghep csv cac thang vao thanh file dic csv
# _tool_merge_csv()

# Ghep cac tuwf gioong nhau trong file dic csv
# _tool_match_key("main_dict_current.csv")
# _tool_match_key("main_dict_current_0.csv")
# _tool_match_key("main_dict_current_1.csv")
# _tool_match_key("main_dict_current_2.csv")
# _tool_match_key("main_dict_current_3.csv")

# _tool_reduce_file("main_dict_0.csv","main_dict_1.csv","main_dict_bi.csv")
# _tool_reduce_file("main_dict_bi.csv","main_dict_2.csv","main_dict_tri.csv")